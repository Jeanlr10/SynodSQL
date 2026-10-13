//! `synod`: a SynodSQL node from the command line, with offline file sync.
//!
//! Two nodes sync by files: each seals its epoch and exports its log, the
//! logs travel by any means, and each node merges them. See `synod help`.

use std::process::ExitCode;

use synod_core::NodeId;
use synod_core::log::log_hash;
use synod_store::{Env, Error, Store, StoreConfig};
use synodsql::command::{Args, STORE_COMMANDS, hex, store_command};

const USAGE: &str = "\
usage: synod <command> [arguments]

Node:
  init <db> --node <id> [--epoch-ms <ms>] [--genesis-ms <ms>] [--max-drift-ms <ms>] [--lateness <epochs>]
                                       make a new node store
  clone <src-db> <new-db> --node <id>  make a new node from a peer's base state
  status <db>                          show the node, its epochs, and its state hash

Schema and data:
  create-table <db> <statement>        add a table (CREATE TABLE ... MERGE ...)
  insert <db> <table> <col=value>...   add a row; prints its row ID
  update <db> <table> <row-id> <col=value>...
  delete <db> <table> <row-id>
  query <db> <select>                  run a read-only SQL statement

Sync:
  seal <db>                            seal every epoch that has ended
  export <db> <epoch> <file>           write a sealed log to a file
  merge <db> <log-file>...             merge the next epochs with the given logs
  conflicts <db> [--all]               list open (or all) conflicts
  resolve <db> <conflict-row-id>       mark a conflict resolved
  revert <db> --node <id> [--from <epoch>] [--to <epoch>]
                                       undo a node's writes in the kept logs, as new writes

Cluster (HTTP/2 with mutual TLS):
  certs <dir> --nodes <id,id,...> [--clients <name,...>] [--hosts <id=host,...>]
                                       make a cluster CA and certificates; --hosts
                                       also names nodes by host or IP, for HTTP clients
  serve <db> --listen <addr> --peers <id=host:port,...> --certs <dir>
        [--tick-ms <ms>] [--no-quorum] [--gossip <k>] [--query-ms <ms>] [--result-mb <MiB>]
                                       run the node: seal, exchange logs, merge;
                                       a query stops after 5000 ms or 64 MiB (0: no limit)
  remote <host:port> --to <id> --certs <dir> [--as <name>] <command> [arguments]
                                       run a store command (without <db>) on a
                                       running node; also net-status, metrics

Values: NULL, numbers, text as is, blobs as x'0a1b', sets as a,b,c.
A row ID looks like 0000000a-0000000000000003.
SYNOD_NOW_MS=<ms> fixes the clock (for scripts and tests).";

type R<T> = Result<T, String>;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if e.starts_with("missing ") {
                eprintln!("synod: {e}\n\n{USAGE}");
            } else {
                eprintln!("synod: {e}");
            }
            ExitCode::FAILURE
        }
    }
}

fn err(e: Error) -> String {
    e.to_string()
}

/// The environment. `SYNOD_NOW_MS` fixes the clock, for scripts and tests.
fn env() -> R<Env> {
    let mut env = Env::os();
    if let Ok(v) = std::env::var("SYNOD_NOW_MS") {
        let ms = v.parse().map_err(|_| format!("SYNOD_NOW_MS: not a number: {v}"))?;
        env.clock = std::sync::Arc::new(synod_store::ManualClock::new(ms));
    }
    Ok(env)
}

fn open(path: &str) -> R<Store> {
    Store::open_existing(path, env()?).map_err(err)
}

fn run(args: &[String]) -> R<()> {
    let Some((cmd, rest)) = args.split_first() else { return Err(USAGE.into()) };
    if cmd == "remote" {
        return remote(rest);
    }
    let a = Args::parse(rest, &["all", "no-quorum"]);
    if STORE_COMMANDS.contains(&cmd.as_str()) {
        let mut s = open(a.at(0, "<db>")?)?;
        print!("{}", store_command(&mut s, cmd, &a)?);
        return Ok(());
    }
    match cmd.as_str() {
        "init" => {
            a.known(&["node", "epoch-ms", "genesis-ms", "max-drift-ms", "lateness"])?;
            let path = a.at(0, "<db>")?;
            if std::path::Path::new(path).exists() {
                return Err(format!("{path} exists"));
            }
            let node = a.opt("node").ok_or("init needs --node <id>")?;
            let node = NodeId(node.parse().map_err(|_| format!("--node: not a node ID: {node}"))?);
            let config = StoreConfig {
                node,
                genesis_ms: a.num("genesis-ms", 0)?,
                epoch_len_ms: a.num("epoch-ms", 60_000)?,
                max_drift_ms: a.num("max-drift-ms", 500)?,
                lateness: a.num("lateness", 16)?,
            };
            let s = Store::open(path, config, env()?).map_err(err)?;
            println!("node {} ready in {path}; first epoch {}", s.config().node, s.open_epoch().map_err(err)?);
        }
        "clone" => {
            a.known(&["node"])?;
            let (src, dst) = (a.at(0, "<src-db>")?, a.at(1, "<new-db>")?);
            let node = a.opt("node").ok_or("clone needs --node <id>")?;
            let node = NodeId(node.parse().map_err(|_| format!("--node: not a node ID: {node}"))?);
            let s = open(src)?.clone_base(dst, node).map_err(err)?;
            println!("node {node} ready in {dst}; next epoch to merge {}", s.next_merge_epoch().map_err(err)?);
        }
        "seal" => {
            a.known(&[])?;
            let mut s = open(a.at(0, "<db>")?)?;
            let (sealed, epoch, ends_at_ms) = synodsql::seal_ended(&mut s).map_err(|e| e.0)?;
            for l in &sealed {
                println!("sealed epoch {}: {} writes, log {}", l.epoch, l.entries, hex(&l.hash));
            }
            if sealed.is_empty() {
                let wait = ends_at_ms.saturating_sub(s.now_ms());
                println!("epoch {epoch} is still open ({:.1} s left)", wait as f64 / 1000.0);
            }
        }
        "export" => {
            a.known(&[])?;
            let s = open(a.at(0, "<db>")?)?;
            let epoch: u64 = a.at(1, "<epoch>")?.parse().map_err(|_| "<epoch> must be a number")?;
            let file = a.at(2, "<file>")?;
            let bytes = s.export(epoch).map_err(err)?;
            std::fs::write(file, &bytes).map_err(|e| format!("{file}: {e}"))?;
            println!("wrote {file}: log {}", hex(&log_hash(&bytes)));
        }
        "merge" => {
            a.known(&[])?;
            let mut s = open(a.at(0, "<db>")?)?;
            let mut logs = Vec::new();
            for file in &a.positional[1..] {
                let bytes = std::fs::read(file).map_err(|e| format!("{file}: {e}"))?;
                synod_core::log::decode_log(&bytes).map_err(|e| format!("{file}: {e}"))?;
                logs.push(bytes);
            }
            let merged = synodsql::merge_logs(&mut s, &logs).map_err(|e| e.0)?;
            for (epoch, r) in &merged {
                println!(
                    "merged epoch {epoch}: {} logs, {} conflicts, root {}",
                    r.logs.len(),
                    r.conflicts.len(),
                    hex(&r.root)
                );
            }
            if merged.is_empty() {
                println!("nothing to merge: seal epoch {} first", s.next_merge_epoch().map_err(err)?);
            }
        }
        "certs" => {
            a.known(&["nodes", "clients", "hosts"])?;
            let dir = a.at(0, "<dir>")?;
            let nodes: Vec<u32> = list(a.opt("nodes").ok_or("certs needs --nodes <id,id,...>")?)
                .iter()
                .map(|n| n.parse().map_err(|_| format!("--nodes: not a node ID: {n}")))
                .collect::<R<_>>()?;
            let clients = list(a.opt("clients").unwrap_or("admin"));
            let clients: Vec<&str> = clients.iter().map(String::as_str).collect();
            let mut hosts = Vec::new();
            for h in list(a.opt("hosts").unwrap_or("")) {
                let (id, name) = h.split_once('=').ok_or_else(|| format!("--hosts: expected id=host, found {h}"))?;
                hosts.push((id.parse::<u32>().map_err(|_| format!("--hosts: not a node ID: {id}"))?, name.to_owned()));
            }
            let hosts: Vec<(u32, &str)> = hosts.iter().map(|(n, h)| (*n, h.as_str())).collect();
            let issued = synodsql::make_certs(dir, &nodes, &clients, &hosts).map_err(|e| e.0)?;
            println!("wrote the CA and {issued} certificates to {dir}");
        }
        "serve" => {
            a.known(&["listen", "peers", "certs", "tick-ms", "no-quorum", "gossip", "query-ms", "result-mb"])?;
            let store = open(a.at(0, "<db>")?)?;
            let me = store.config().node;
            let listen = a.opt("listen").ok_or("serve needs --listen <addr>")?;
            let listen = listen.parse().map_err(|_| format!("--listen: not an address: {listen}"))?;
            let mut members = Vec::new();
            for p in list(a.opt("peers").ok_or("serve needs --peers <id=host:port,...>")?) {
                let (id, addr) =
                    p.split_once('=').ok_or_else(|| format!("--peers: expected id=host:port, found {p}"))?;
                let id = NodeId(id.parse().map_err(|_| format!("--peers: not a node ID: {id}"))?);
                members.push(synod_net::Member { id, addr: addr.to_owned() });
            }
            if !members.iter().any(|m| m.id == me) {
                members.push(synod_net::Member { id: me, addr: listen_addr_string(&listen) });
            }
            let tls = load_tls(a.opt("certs").ok_or("serve needs --certs <dir>")?, &synod_net::node_name(me))?;
            let mut cfg =
                synod_node::PeerConfig::new(members.iter().map(|m| m.id).collect(), store.config().epoch_len_ms);
            cfg.quorum = !a.flag("no-quorum");
            if a.opt("gossip").is_some() {
                cfg.gossip = Some(a.num("gossip", 3)? as usize);
            }
            let tick_ms = a.num("tick-ms", (store.config().epoch_len_ms / 20).clamp(10, 1_000))?;
            let peer = synod_node::Peer::new(store, cfg).map_err(err)?;
            println!("node {me} serving on {listen} with {} members", members.len());
            let exec: synod_net::ExecFn = Box::new(synodsql::command::run);
            let d = synod_net::DEFAULT_QUERY_LIMITS;
            let query_ms = a.num("query-ms", d.time.map_or(0, |t| t.as_millis() as u64))?;
            let result_mb = a.num("result-mb", d.max_bytes.map_or(0, |b| (b >> 20) as u64))?;
            let query_limits = synod_net::QueryLimits {
                time: (query_ms > 0).then(|| std::time::Duration::from_millis(query_ms)),
                max_bytes: (result_mb > 0).then_some((result_mb as usize) << 20),
            };
            let config = synod_net::ServeConfig { listen, members, tls, tick_ms, query_limits };
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            synod_net::serve(peer, config, exec, stop).map_err(|e| e.to_string())?;
        }
        "help" | "--help" | "-h" => println!("{USAGE}"),
        other => return Err(format!("unknown command {other}\n\n{USAGE}")),
    }
    Ok(())
}

fn list(s: &str) -> Vec<String> {
    s.split(',').map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect()
}

fn listen_addr_string(a: &std::net::SocketAddr) -> String {
    a.to_string()
}

fn load_tls(dir: &str, name: &str) -> R<synod_net::Tls> {
    synodsql::load_tls(dir, name).map_err(|e| e.0)
}

/// `synod remote <host:port> --to <id> --certs <dir> [--as <name>] <command> ...`:
/// the options come before the command; the rest goes to the node as is.
fn remote(rest: &[String]) -> R<()> {
    let mut it = rest.iter();
    let addr = it.next().ok_or_else(|| format!("missing <host:port>\n\n{USAGE}"))?.clone();
    let (mut to, mut certs, mut name) = (None, None, "admin".to_owned());
    let mut command = Vec::new();
    while let Some(x) = it.next() {
        match x.as_str() {
            "--to" => to = it.next().cloned(),
            "--certs" => certs = it.next().cloned(),
            "--as" => name = it.next().cloned().ok_or("--as needs a name")?,
            _ => {
                command.push(x.clone());
                command.extend(it.by_ref().cloned());
            }
        }
    }
    let to = to.ok_or("remote needs --to <id>")?;
    let to = NodeId(to.parse().map_err(|_| format!("--to: not a node ID: {to}"))?);
    let tls = load_tls(&certs.ok_or("remote needs --certs <dir>")?, &name)?;
    let member = synod_net::Member { id: to, addr };
    let (cmd, _) = command.split_first().ok_or("remote needs a command")?;
    let (method, path, body) = match cmd.as_str() {
        "net-status" => ("GET", "/synod/v1/status", Vec::new()),
        "metrics" => ("GET", "/metrics", Vec::new()),
        _ => ("POST", "/synod/v1/exec", command.join("\0").into_bytes()),
    };
    let (code, body) = synod_net::request(&member, &tls, method, path, body).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&body);
    if code >= 300 {
        return Err(format!("node {to}: {code}: {text}"));
    }
    print!("{text}");
    Ok(())
}
