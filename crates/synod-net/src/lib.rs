//! The SynodSQL network transport: HTTP/2 with mutual TLS between member
//! nodes (specification Section 6.4).
//!
//! [`serve`] runs one node: a [`Peer`] on its own thread, driven by a timer
//! and by the messages that arrive, and a tokio runtime for the network.
//! Each message goes as one HTTP/2 POST to `/synod/v1/msg` on the receiver,
//! on a kept connection. The receiver checks that the client certificate
//! names the node that the `x-synod-node` header gives.
//!
//! The same port serves `GET /synod/v1/status`, `GET /metrics`
//! (Prometheus), and `POST /synod/v1/exec`, which runs a store command
//! (such as an insert) on the node, for any client with a CA certificate.

mod tls;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::ServerName;
use synod_core::NodeId;
use synod_node::{Msg, Peer};
use synod_store::Store;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc as tmpsc, oneshot};
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub use tls::{Certs, Tls, cert_names, client_config, make_certs, node_name, server_config};

/// Largest message body.
pub const MAX_BODY: usize = 512 << 20;

/// Errors of the transport.
#[derive(Debug)]
pub enum NetError {
    /// TLS setup.
    Tls(String),
    /// Network.
    Io(std::io::Error),
    /// HTTP.
    Http(String),
    /// The node's store.
    Store(synod_store::Error),
}

impl std::fmt::Display for NetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetError::Tls(e) => write!(f, "TLS: {e}"),
            NetError::Io(e) => write!(f, "network: {e}"),
            NetError::Http(e) => write!(f, "HTTP: {e}"),
            NetError::Store(e) => write!(f, "store: {e}"),
        }
    }
}

impl std::error::Error for NetError {}

impl From<std::io::Error> for NetError {
    fn from(e: std::io::Error) -> NetError {
        NetError::Io(e)
    }
}

/// One member node and its address (`host:port`).
#[derive(Clone, Debug)]
pub struct Member {
    /// Node ID.
    pub id: NodeId,
    /// Address.
    pub addr: String,
}

/// Settings of [`serve`].
pub struct ServeConfig {
    /// Address to listen on.
    pub listen: SocketAddr,
    /// All members, this node included.
    pub members: Vec<Member>,
    /// This node's TLS files.
    pub tls: Tls,
    /// Time between two ticks of the node.
    pub tick_ms: u64,
}

/// Runs a store command for `/synod/v1/exec`: the arguments of a `synod`
/// command, without the store path. Returns its output.
pub type ExecFn = Box<dyn FnMut(&mut Store, &[String]) -> Result<String, String> + Send>;

enum Inbound {
    Msg(NodeId, Vec<u8>),
    Exec(Vec<String>, oneshot::Sender<Result<String, String>>),
}

#[derive(Default)]
struct Snapshot {
    status: String,
    metrics: String,
}

struct Shared {
    me: NodeId,
    members: Vec<NodeId>,
    inbound: Mutex<mpsc::Sender<Inbound>>,
    snapshot: Mutex<Snapshot>,
}

/// Runs a node until `stop` is set or the node fails.
pub fn serve(peer: Peer, cfg: ServeConfig, exec: ExecFn, stop: Arc<AtomicBool>) -> Result<(), NetError> {
    let me = peer.store().config().node;
    let server_tls = tls::server_config(&cfg.tls)?;
    let client_tls = tls::client_config(&cfg.tls)?;
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    let (in_tx, in_rx) = mpsc::channel();
    let shared = Arc::new(Shared {
        me,
        members: cfg.members.iter().map(|m| m.id).collect(),
        inbound: Mutex::new(in_tx),
        snapshot: Mutex::new(Snapshot::default()),
    });
    // One sender task per peer, each with its own connection.
    let mut outbound = BTreeMap::new();
    for m in cfg.members.iter().filter(|m| m.id != me) {
        let (tx, rx) = tmpsc::unbounded_channel();
        outbound.insert(m.id, tx);
        runtime.spawn(send_loop(me, m.clone(), client_tls.clone(), rx));
    }
    let listener = runtime.block_on(TcpListener::bind(cfg.listen))?;
    runtime.spawn(accept_loop(listener, TlsAcceptor::from(server_tls), shared.clone()));
    let node_shared = shared.clone();
    let node_stop = stop.clone();
    let tick = Duration::from_millis(cfg.tick_ms.max(1));
    let node = std::thread::Builder::new()
        .name(format!("synod-node-{}", me.0))
        .spawn(move || node_loop(peer, in_rx, outbound, exec, node_shared, node_stop, tick))?;
    let result = node.join().unwrap_or_else(|_| Err(NetError::Http("the node thread panicked".into())));
    stop.store(true, Ordering::SeqCst);
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

fn node_loop(
    mut peer: Peer,
    inbound: mpsc::Receiver<Inbound>,
    outbound: BTreeMap<NodeId, tmpsc::UnboundedSender<Vec<u8>>>,
    mut exec: ExecFn,
    shared: Arc<Shared>,
    stop: Arc<AtomicBool>,
    tick: Duration,
) -> Result<(), NetError> {
    let mut next_tick = Instant::now();
    let mut next_snapshot = Instant::now();
    while !stop.load(Ordering::SeqCst) {
        let wait = next_tick.saturating_duration_since(Instant::now());
        match inbound.recv_timeout(wait) {
            Ok(Inbound::Msg(from, bytes)) => {
                if let Ok(msg) = Msg::decode(&bytes) {
                    peer.on_message(from, msg).map_err(NetError::Store)?;
                }
            }
            Ok(Inbound::Exec(args, reply)) => {
                let _ = reply.send(exec(peer.store_mut(), &args));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        if Instant::now() >= next_tick {
            peer.tick().map_err(NetError::Store)?;
            next_tick = Instant::now() + tick;
        }
        for (to, msg) in peer.take_outbox() {
            if let Some(tx) = outbound.get(&to) {
                let _ = tx.send(msg.encode());
            }
        }
        for e in peer.take_events() {
            if let synod_node::Event::Mismatch { from, epoch, .. } = e {
                eprintln!("synod: ALERT: root hash differs from node {from} after the merge of epoch {epoch}");
            }
        }
        if Instant::now() >= next_snapshot {
            let s = peer.store();
            let status = format!(
                "node {}\nopen_epoch {}\nmerged_epoch {}\nhead {}\nroot {}\nadopting {}\n",
                shared.me.0,
                s.open_epoch().map_err(NetError::Store)?,
                s.merged_epoch().map_err(NetError::Store)?.map_or("none".into(), |e| e.to_string()),
                hex(&s.head().map_err(NetError::Store)?),
                hex(&s.base_root().map_err(NetError::Store)?),
                peer.is_adopting(),
            );
            let metrics = peer.metrics().render(shared.me.0);
            *shared.snapshot.lock().unwrap() = Snapshot { status, metrics };
            next_snapshot = Instant::now() + Duration::from_millis(200);
        }
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

async fn accept_loop(listener: TcpListener, acceptor: TlsAcceptor, shared: Arc<Shared>) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else { continue };
        let _ = tcp.set_nodelay(true);
        let (acceptor, shared) = (acceptor.clone(), shared.clone());
        tokio::spawn(async move {
            let Ok(Ok(tls)) = tokio::time::timeout(Duration::from_secs(10), acceptor.accept(tcp)).await else { return };
            let cert = tls.get_ref().1.peer_certificates().and_then(|c| c.first()).cloned();
            let Some(cert) = cert else { return };
            let service = hyper::service::service_fn(move |req| handle(req, cert.clone(), shared.clone()));
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .initial_stream_window_size(4 << 20)
                .initial_connection_window_size(8 << 20)
                .serve_connection(TokioIo::new(tls), service)
                .await;
        });
    }
}

fn reply(code: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(body.into()));
    *r.status_mut() = code;
    r
}

async fn handle(
    req: Request<Incoming>,
    cert: rustls::pki_types::CertificateDer<'static>,
    shared: Arc<Shared>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_owned();
    let read = |body: Incoming| async move { Limited::new(body, MAX_BODY).collect().await.map(|b| b.to_bytes()) };
    Ok(match (parts.method.as_str(), path.as_str()) {
        ("POST", "/synod/v1/msg") => {
            let from = parts.headers.get("x-synod-node").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok());
            let Some(from) = from.map(NodeId).filter(|n| shared.members.contains(n) && *n != shared.me) else {
                return Ok(reply(StatusCode::BAD_REQUEST, "unknown sender"));
            };
            if !tls::cert_names(&cert, &tls::node_name(from)) {
                return Ok(reply(StatusCode::FORBIDDEN, "the certificate does not name the sender"));
            }
            let Ok(bytes) = read(body).await else { return Ok(reply(StatusCode::PAYLOAD_TOO_LARGE, "")) };
            let _ = shared.inbound.lock().unwrap().send(Inbound::Msg(from, bytes.to_vec()));
            reply(StatusCode::NO_CONTENT, "")
        }
        ("GET", "/synod/v1/status") => reply(StatusCode::OK, shared.snapshot.lock().unwrap().status.clone()),
        ("GET", "/metrics") => reply(StatusCode::OK, shared.snapshot.lock().unwrap().metrics.clone()),
        ("POST", "/synod/v1/exec") => {
            let Ok(bytes) = read(body).await else { return Ok(reply(StatusCode::PAYLOAD_TOO_LARGE, "")) };
            let Ok(text) = String::from_utf8(bytes.to_vec()) else {
                return Ok(reply(StatusCode::BAD_REQUEST, "not UTF-8"));
            };
            let args: Vec<String> = text.split('\0').map(String::from).collect();
            let (tx, rx) = oneshot::channel();
            let _ = shared.inbound.lock().unwrap().send(Inbound::Exec(args, tx));
            match rx.await {
                Ok(Ok(out)) => reply(StatusCode::OK, out),
                Ok(Err(e)) => reply(StatusCode::BAD_REQUEST, e),
                Err(_) => reply(StatusCode::SERVICE_UNAVAILABLE, "the node stopped"),
            }
        }
        _ => reply(StatusCode::NOT_FOUND, ""),
    })
}

type Sender = hyper::client::conn::http2::SendRequest<Full<Bytes>>;

async fn connect(to: &Member, config: Arc<rustls::ClientConfig>) -> Result<Sender, NetError> {
    let tcp = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&to.addr))
        .await
        .map_err(|_| NetError::Http("connect timeout".into()))??;
    tcp.set_nodelay(true)?;
    let name = ServerName::try_from(tls::node_name(to.id)).map_err(|e| NetError::Tls(e.to_string()))?;
    let tls = TlsConnector::from(config).connect(name, tcp).await?;
    let (sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(4 << 20)
        .initial_connection_window_size(8 << 20)
        .handshake(TokioIo::new(tls))
        .await
        .map_err(|e| NetError::Http(e.to_string()))?;
    tokio::spawn(conn);
    Ok(sender)
}

// Sends this node's messages to one peer. A message that cannot go now is
// dropped: the protocol asks again for what it misses.
async fn send_loop(
    me: NodeId,
    to: Member,
    config: Arc<rustls::ClientConfig>,
    mut rx: tmpsc::UnboundedReceiver<Vec<u8>>,
) {
    let mut conn: Option<Sender> = None;
    let mut retry_at = Instant::now();
    let uri = format!("https://{}/synod/v1/msg", tls::node_name(to.id));
    while let Some(msg) = rx.recv().await {
        if conn.as_ref().is_none_or(|c| c.is_closed()) {
            if Instant::now() < retry_at {
                continue;
            }
            match connect(&to, config.clone()).await {
                Ok(c) => conn = Some(c),
                Err(_) => {
                    retry_at = Instant::now() + Duration::from_millis(500);
                    continue;
                }
            }
        }
        let c = conn.as_mut().unwrap();
        if c.ready().await.is_err() {
            conn = None;
            continue;
        }
        let req = Request::post(&uri).header("x-synod-node", me.0.to_string()).body(Full::new(Bytes::from(msg)));
        let Ok(req) = req else { continue };
        let fut = c.send_request(req);
        tokio::spawn(async move {
            let _ = fut.await;
        });
    }
}

/// Sends one request to a node, as a client with a CA certificate: for the
/// status, the metrics, or a command. Returns the status code and body.
pub fn request(to: &Member, tls: &Tls, method: &str, path: &str, body: Vec<u8>) -> Result<(u16, Vec<u8>), NetError> {
    request_with(to, tls, method, path, &[], body)
}

/// As [`request`], with extra headers.
pub fn request_with(
    to: &Member,
    tls: &Tls,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> Result<(u16, Vec<u8>), NetError> {
    let config = tls::client_config(tls)?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let mut c = connect(to, config).await?;
        let uri = format!("https://{}{path}", tls::node_name(to.id));
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let req = req.body(Full::new(Bytes::from(body))).map_err(|e| NetError::Http(e.to_string()))?;
        c.ready().await.map_err(|e| NetError::Http(e.to_string()))?;
        let resp = c.send_request(req).await.map_err(|e| NetError::Http(e.to_string()))?;
        let code = resp.status().as_u16();
        let body = resp.into_body().collect().await.map_err(|e| NetError::Http(e.to_string()))?.to_bytes();
        Ok((code, body.to_vec()))
    })
}
