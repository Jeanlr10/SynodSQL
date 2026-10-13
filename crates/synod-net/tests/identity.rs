//! A node takes a peer message only from the node that its certificate
//! names, and only with a certificate from the cluster CA.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use synod_core::NodeId;
use synod_net::{Member, ServeConfig, Tls, make_certs, request, request_with};
use synod_node::{Msg, Peer, PeerConfig};
use synod_store::{Env, Store, StoreConfig};

fn tls(certs: &synod_net::Certs, name: &str, ca: &str) -> Tls {
    let (_, cert, key) = certs.issued.iter().find(|(n, _, _)| n == name).unwrap();
    Tls { ca_pem: ca.as_bytes().to_vec(), cert_pem: cert.as_bytes().to_vec(), key_pem: key.as_bytes().to_vec() }
}

#[test]
fn messages_need_the_senders_certificate() {
    let dir = std::env::temp_dir().join(format!("synod-net-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("n1.db");
    let config = StoreConfig { node: NodeId(1), genesis_ms: 0, epoch_len_ms: 1_000, max_drift_ms: 500, lateness: 8 };
    let store = Store::open(path.to_str().unwrap(), config, Env::os()).unwrap();
    let ids = [NodeId(1), NodeId(2)];
    let certs = make_certs(&ids, &["admin"]).unwrap();
    let other = make_certs(&ids, &[]).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let members =
        vec![Member { id: NodeId(1), addr: addr.to_string() }, Member { id: NodeId(2), addr: "127.0.0.1:1".into() }];
    let peer = Peer::new(store, PeerConfig::new(ids.to_vec(), 1_000)).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let cfg = ServeConfig { listen: addr, members, tls: tls(&certs, "node-1", &certs.ca_pem), tick_ms: 50 };
    let s = stop.clone();
    let server = std::thread::spawn(move || synod_net::serve(peer, cfg, Box::new(|_, _| Ok("ok".into())), s));
    let to = Member { id: NodeId(1), addr: addr.to_string() };
    let admin = tls(&certs, "admin", &certs.ca_pem);
    // Wait until the server listens: a slow machine takes a while.
    let end = Instant::now() + Duration::from_secs(20);
    while request(&to, &admin, "GET", "/synod/v1/status", Vec::new()).is_err() {
        assert!(Instant::now() < end, "the server does not answer");
        std::thread::sleep(Duration::from_millis(50));
    }

    let msg = Msg::GetStatus.encode();
    let post =
        |t: &Tls, from: &str| request_with(&to, t, "POST", "/synod/v1/msg", &[("x-synod-node", from)], msg.clone());
    let node2 = tls(&certs, "node-2", &certs.ca_pem);
    assert_eq!(post(&node2, "2").unwrap().0, 204);
    assert_eq!(post(&admin, "2").unwrap().0, 403, "admin cannot speak for node 2");
    assert_eq!(post(&node2, "1").unwrap().0, 400, "not from itself");
    assert_eq!(post(&node2, "7").unwrap().0, 400, "not a member");
    // A certificate from another CA fails the TLS handshake.
    let foreign = tls(&other, "node-2", &certs.ca_pem);
    assert!(post(&foreign, "2").is_err());
    let (code, body) = request(&to, &admin, "GET", "/synod/v1/status", Vec::new()).unwrap();
    assert_eq!(code, 200);
    assert!(String::from_utf8(body).unwrap().starts_with("node 1\n"));
    let (code, body) = request(&to, &admin, "POST", "/synod/v1/exec", b"status".to_vec()).unwrap();
    assert_eq!((code, body), (200, b"ok".to_vec()));

    stop.store(true, Ordering::SeqCst);
    server.join().unwrap().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
