//! `synod`: a SynodSQL node from the command line, with offline file sync.
//!
//! Two nodes sync by files: each seals its epoch and exports its log, the
//! logs travel by any means, and each node merges them. See `synod help`.

use std::fmt::Write as _;
use std::process::ExitCode;

use rusqlite::types::Value as Sql;
use synod_core::log::{decode_log, log_hash};
use synod_core::{NodeId, RowId, Value, ValueSet};
use synod_store::schema::{Column, ColumnType};
use synod_store::{Env, Error, Store, StoreConfig};

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
  certs <dir> --nodes <id,id,...> [--clients <name,...>]
                                       make a cluster CA and certificates
  serve <db> --listen <addr> --peers <id=host:port,...> --certs <dir>
        [--tick-ms <ms>] [--no-quorum] [--gossip <k>]
                                       run the node: seal, exchange logs, merge
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
            eprintln!("synod: {e}");
            ExitCode::FAILURE
        }
    }
}

fn err(e: Error) -> String {
    e.to_string()
}

/// Splits `--name value` options from positional arguments.
struct Args {
    positional: Vec<String>,
    options: Vec<(String, Option<String>)>,
}

impl Args {
    fn parse(args: &[String], flags: &[&str]) -> Args {
        let mut positional = Vec::new();
        let mut options = Vec::new();
        let mut it = args.iter();
        while let Some(a) = it.next() {
            if let Some(name) = a.strip_prefix("--") {
                if flags.contains(&name) {
                    options.push((name.to_owned(), None));
                } else {
                    options.push((name.to_owned(), it.next().cloned()));
                }
            } else {
                positional.push(a.clone());
            }
        }
        Args { positional, options }
    }

    fn opt(&self, name: &str) -> Option<&str> {
        self.options.iter().find(|(n, _)| n == name).and_then(|(_, v)| v.as_deref())
    }

    fn flag(&self, name: &str) -> bool {
        self.options.iter().any(|(n, _)| n == name)
    }

    fn num(&self, name: &str, default: u64) -> R<u64> {
        self.opt(name).map_or(Ok(default), |v| v.parse().map_err(|_| format!("--{name}: not a number: {v}")))
    }

    fn at(&self, i: usize, what: &str) -> R<&str> {
        self.positional.get(i).map(String::as_str).ok_or_else(|| format!("missing {what}\n\n{USAGE}"))
    }

    fn known(&self, names: &[&str]) -> R<()> {
        match self.options.iter().find(|(n, _)| !names.contains(&n.as_str())) {
            Some((n, _)) => Err(format!("unknown option --{n}")),
            None => Ok(()),
        }
    }
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

/// Commands that work on an open store. A running node takes them too
/// (`synod remote`).
const STORE_COMMANDS: [&str; 9] =
    ["status", "create-table", "insert", "update", "delete", "query", "conflicts", "resolve", "revert"];

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
            let mut any = false;
            loop {
                match s.seal() {
                    Ok(l) => {
                        any = true;
                        println!("sealed epoch {}: {} writes, log {}", l.epoch, l.entries, hex(&l.hash));
                    }
                    Err(Error::TooEarly { epoch, ends_at_ms }) => {
                        if !any {
                            let wait = ends_at_ms.saturating_sub(s.now_ms());
                            println!("epoch {epoch} is still open ({:.1} s left)", wait as f64 / 1000.0);
                        }
                        break;
                    }
                    Err(e) => return Err(err(e)),
                }
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
            let me = s.config().node;
            // (epoch, node, bytes) of each given log.
            let mut logs = Vec::new();
            for file in &a.positional[1..] {
                let bytes = std::fs::read(file).map_err(|e| format!("{file}: {e}"))?;
                let (h, _) = decode_log(&bytes).map_err(|e| format!("{file}: {e}"))?;
                if h.node != me {
                    logs.push((h.epoch, bytes));
                }
            }
            let mut merged = 0;
            loop {
                let epoch = s.next_merge_epoch().map_err(err)?;
                if s.sealed(epoch).map_err(err)?.is_none() {
                    break;
                }
                // The logs of this epoch, and late logs that no merge took.
                let mut take = Vec::new();
                for (e, bytes) in &logs {
                    if *e == epoch || (*e < epoch && s.log_merged_in(&log_hash(bytes)).map_err(err)?.is_none()) {
                        take.push(bytes.clone());
                    }
                }
                let r = s.merge(epoch, &take).map_err(err)?;
                merged += 1;
                println!(
                    "merged epoch {epoch}: {} logs, {} conflicts, root {}",
                    r.logs.len(),
                    r.conflicts.len(),
                    hex(&r.root)
                );
            }
            if merged == 0 {
                println!("nothing to merge: seal epoch {} first", s.next_merge_epoch().map_err(err)?);
            }
        }
        "certs" => {
            a.known(&["nodes", "clients"])?;
            let dir = std::path::Path::new(a.at(0, "<dir>")?);
            let nodes: Vec<NodeId> = list(a.opt("nodes").ok_or("certs needs --nodes <id,id,...>")?)
                .iter()
                .map(|n| n.parse().map(NodeId).map_err(|_| format!("--nodes: not a node ID: {n}")))
                .collect::<R<_>>()?;
            let clients = list(a.opt("clients").unwrap_or("admin"));
            let clients: Vec<&str> = clients.iter().map(String::as_str).collect();
            let certs = synod_net::make_certs(&nodes, &clients).map_err(|e| e.to_string())?;
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
            let write = |name: String, data: &str| {
                let path = dir.join(name);
                std::fs::write(&path, data).map_err(|e| format!("{}: {e}", path.display()))
            };
            write("ca.pem".into(), &certs.ca_pem)?;
            write("ca.key".into(), &certs.ca_key_pem)?;
            for (name, cert, key) in &certs.issued {
                write(format!("{name}.pem"), cert)?;
                write(format!("{name}.key"), key)?;
            }
            println!("wrote the CA and {} certificates to {}", certs.issued.len(), dir.display());
        }
        "serve" => {
            a.known(&["listen", "peers", "certs", "tick-ms", "no-quorum", "gossip"])?;
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
            let exec: synod_net::ExecFn = Box::new(|s, args| {
                let (cmd, rest) = args.split_first().ok_or("empty command")?;
                if !STORE_COMMANDS.contains(&cmd.as_str()) {
                    return Err(format!("{cmd} cannot run on a remote node"));
                }
                let mut a = Args::parse(rest, &["all"]);
                a.positional.insert(0, "-".into());
                store_command(s, cmd, &a)
            });
            let config = synod_net::ServeConfig { listen, members, tls, tick_ms };
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
    let read = |f: String| {
        let path = std::path::Path::new(dir).join(f);
        std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))
    };
    Ok(synod_net::Tls {
        ca_pem: read("ca.pem".into())?,
        cert_pem: read(format!("{name}.pem"))?,
        key_pem: read(format!("{name}.key"))?,
    })
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

/// Runs a store command. `a.positional[0]` names the store and is not read.
fn store_command(s: &mut Store, cmd: &str, a: &Args) -> R<String> {
    let mut out = String::new();
    match cmd {
        "status" => {
            a.known(&[])?;
            let c = s.config().clone();
            let merged = s.merged_epoch().map_err(err)?.map_or("none".into(), |e| e.to_string());
            let _ = writeln!(out, "node            {}", c.node);
            let _ = writeln!(out, "epoch length    {} ms (genesis {} ms)", c.epoch_len_ms, c.genesis_ms);
            let _ = writeln!(out, "lateness        {} epochs", c.lateness);
            let _ = writeln!(out, "open epoch      {}", s.open_epoch().map_err(err)?);
            let _ = writeln!(out, "merged epoch    {merged}");
            let _ = writeln!(out, "next to merge   {}", s.next_merge_epoch().map_err(err)?);
            let _ = writeln!(out, "head manifest   {}", hex(&s.head().map_err(err)?));
            let _ = writeln!(out, "unsealed writes {}", s.unsealed_entries().map_err(err)?.len());
            let _ = writeln!(out, "base root       {}", hex(&s.base_root().map_err(err)?));
            let _ = writeln!(out, "state hash      {}", hex(&s.state_hash().map_err(err)?));
            let tables: Vec<&str> = s.tables().iter().map(|t| t.name.as_str()).collect();
            let _ = writeln!(out, "tables          {}", tables.join(", "));
        }
        "create-table" => {
            a.known(&[])?;
            let t = s.create_table(a.at(1, "<statement>")?).map_err(err)?;
            let _ = writeln!(out, "{}", t.ddl);
        }
        "insert" => {
            a.known(&[])?;
            let table = s.table(a.at(1, "<table>")?).map_err(err)?;
            let cols = assignments(&table.columns, &a.positional[2..])?;
            let given: Vec<(&str, Value)> = cols.iter().map(|(n, v)| (n.as_str(), v.clone())).collect();
            let mut t = s.begin().map_err(err)?;
            let id = t.insert(&table.name, &given).map_err(err)?;
            t.commit().map_err(err)?;
            let _ = writeln!(out, "{id}");
        }
        "update" => {
            a.known(&[])?;
            let table = s.table(a.at(1, "<table>")?).map_err(err)?;
            let id = parse_row_id(a.at(2, "<row-id>")?)?;
            let cols = assignments(&table.columns, &a.positional[3..])?;
            let given: Vec<(&str, Value)> = cols.iter().map(|(n, v)| (n.as_str(), v.clone())).collect();
            let mut t = s.begin().map_err(err)?;
            t.update(&table.name, id, &given).map_err(err)?;
            t.commit().map_err(err)?;
        }
        "delete" => {
            a.known(&[])?;
            let table = a.at(1, "<table>")?.to_owned();
            let id = parse_row_id(a.at(2, "<row-id>")?)?;
            let mut t = s.begin().map_err(err)?;
            t.delete(&table, id).map_err(err)?;
            t.commit().map_err(err)?;
        }
        "query" => {
            a.known(&[])?;
            let (names, rows) = s.query_named(a.at(1, "<select>")?, &[]).map_err(err)?;
            out += &table(&names, &rows.iter().map(|r| r.iter().map(show).collect()).collect::<Vec<_>>());
        }
        "conflicts" => {
            a.known(&["all"])?;
            let mut rows = Vec::new();
            for (id, c, status) in s.conflicts().map_err(err)? {
                if status != "open" && !a.flag("all") {
                    continue;
                }
                let recorded: Vec<String> =
                    c.recorded.iter().map(|r| format!("{}@{}: {}", r.node, r.hlc, values(&r.values))).collect();
                rows.push(vec![
                    id.to_string(),
                    c.epoch.to_string(),
                    c.kind.to_string(),
                    format!("{}{}", c.table_name, c.column_name.map(|n| format!(".{n}")).unwrap_or_default()),
                    c.row_id.to_string(),
                    c.rule.clone(),
                    c.applied.as_deref().map_or("(deleted)".into(), values),
                    recorded.join("; "),
                    status,
                ]);
            }
            let names = ["conflict", "epoch", "kind", "target", "row", "rule", "applied", "recorded", "status"];
            out += &table(&names.map(String::from), &rows);
        }
        "resolve" => {
            a.known(&[])?;
            let id = parse_row_id(a.at(1, "<conflict-row-id>")?)?;
            let mut t = s.begin().map_err(err)?;
            t.update("_conflicts", id, &[("status", Value::Text("resolved".into()))]).map_err(err)?;
            t.commit().map_err(err)?;
        }
        "revert" => {
            a.known(&["node", "from", "to"])?;
            let node = a.opt("node").ok_or("revert needs --node <id>")?;
            let node = NodeId(node.parse().map_err(|_| format!("--node: not a node ID: {node}"))?);
            let from = a.num("from", 0)?;
            let to = a.num("to", u64::MAX)?;
            let mut t = s.begin().map_err(err)?;
            let r = t.revert(node, from..=to).map_err(err)?;
            t.commit().map_err(err)?;
            let _ = writeln!(
                out,
                "reverted node {node}: {} rows deleted, {} columns restored, {} rows inserted again, {} skipped",
                r.deleted, r.restored, r.reinserted, r.skipped
            );
        }
        other => return Err(format!("unknown command {other}")),
    }
    Ok(out)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn parse_row_id(s: &str) -> R<RowId> {
    let bad = || format!("not a row ID: {s} (expected like 0000000a-0000000000000003)");
    let (n, c) = s.split_once('-').ok_or_else(bad)?;
    let node = u32::from_str_radix(n, 16).map_err(|_| bad())?;
    let counter = u64::from_str_radix(c, 16).map_err(|_| bad())?;
    Ok(RowId::new(NodeId(node), counter))
}

fn assignments(columns: &[Column], args: &[String]) -> R<Vec<(String, Value)>> {
    args.iter()
        .map(|a| {
            let (name, raw) = a.split_once('=').ok_or_else(|| format!("expected col=value, found {a}"))?;
            let column = columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| format!("no column {name}"))?;
            Ok((column.name.clone(), parse_value(&column.ty, raw)?))
        })
        .collect()
}

fn parse_value(ty: &ColumnType, raw: &str) -> R<Value> {
    if raw == "NULL" {
        return Ok(Value::Null);
    }
    let bad = || format!("{raw:?} is not a {ty}");
    Ok(match ty {
        ColumnType::Integer => Value::Integer(raw.parse().map_err(|_| bad())?),
        ColumnType::Real => Value::Real(raw.parse().map_err(|_| bad())?),
        ColumnType::Text => Value::Text(raw.to_owned()),
        ColumnType::Decimal { .. } => Value::Decimal(raw.parse().map_err(|_| bad())?),
        ColumnType::Blob => {
            let digits = raw.strip_prefix("x'").and_then(|r| r.strip_suffix('\'')).ok_or_else(bad)?;
            if digits.len() % 2 != 0 {
                return Err(bad());
            }
            let bytes: Option<Vec<u8>> =
                (0..digits.len()).step_by(2).map(|i| u8::from_str_radix(&digits[i..i + 2], 16).ok()).collect();
            Value::Blob(bytes.ok_or_else(bad)?)
        }
        ColumnType::Set(item) => {
            let items = if raw.is_empty() {
                Vec::new()
            } else {
                raw.split(',').map(|p| parse_value(item, p)).collect::<R<_>>()?
            };
            Value::Set(ValueSet::new(items).ok_or_else(bad)?)
        }
    })
}

fn value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => r.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Text(t) => t.escape_debug().to_string(),
        Value::Blob(b) => format!("x'{}'", hex(b)),
        Value::Set(s) => format!("{{{}}}", s.items().iter().map(value).collect::<Vec<_>>().join(", ")),
    }
}

fn values(vs: &[Value]) -> String {
    vs.iter().map(value).collect::<Vec<_>>().join(", ")
}

// Shows a SQL result value. A 16-byte blob that is a row ID shows as one,
// and a blob that is an encoded set shows as the set.
fn show(v: &Sql) -> String {
    match v {
        Sql::Null => "NULL".into(),
        Sql::Integer(i) => i.to_string(),
        Sql::Real(r) => r.to_string(),
        Sql::Text(t) => t.escape_debug().to_string(),
        Sql::Blob(b) => {
            if let Some(id) = b.as_slice().try_into().ok().and_then(RowId::from_be_bytes) {
                id.to_string()
            } else if let Ok(set @ Value::Set(_)) = Value::from_canonical_bytes(b) {
                value(&set)
            } else {
                format!("x'{}'", hex(b))
            }
        }
    }
}

fn table(names: &[String], rows: &[Vec<String>]) -> String {
    let mut out = String::new();
    let mut width: Vec<usize> = names.iter().map(|n| n.chars().count()).collect();
    for r in rows {
        for (w, c) in width.iter_mut().zip(r) {
            *w = (*w).max(c.chars().count());
        }
    }
    let line = |cells: &[String]| {
        let parts: Vec<String> = cells.iter().zip(&width).map(|(c, w)| format!("{c:<w$}")).collect();
        format!("{}\n", parts.join("  ").trim_end())
    };
    out += &line(names);
    for r in rows {
        out += &line(r);
    }
    let _ = writeln!(out, "({} row{})", rows.len(), if rows.len() == 1 { "" } else { "s" });
    out
}
