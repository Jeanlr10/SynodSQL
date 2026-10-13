//! SynodSQL as a library: a node store you open, write, query, sync by
//! files, or run as a cluster node in the background.
//!
//! [`Database`] is the API that the C (`synod.h`), C++ (`synod.hpp`), and
//! Python (`synodsql`) bindings wrap. The crates under it (`synod-store`,
//! `synod-node`, `synod-net`) give the full Rust API.
//!
//! ```
//! use synodsql::{Cell, Database, Options, Param};
//! # let dir = std::env::temp_dir().join(format!("synodsql-doc-{}", std::process::id()));
//! # std::fs::create_dir_all(&dir).unwrap();
//! # let path = dir.join("node.db");
//! # let path = path.to_str().unwrap();
//!
//! let mut db = Database::create(path, &Options::new(1))?;
//! db.create_table("CREATE TABLE stock (sku TEXT UNIQUE, qty INTEGER MERGE delta)")?;
//! let id = db.insert("stock", &[("sku", Param::Text("A-100".into())), ("qty", Param::Integer(800))])?;
//! db.update("stock", &id, &[("qty", Param::Integer(650))])?;
//! let rows = db.query("SELECT sku, qty FROM stock WHERE qty < ?1", &[Param::Integer(700)])?;
//! assert_eq!(rows.rows, vec![vec![Cell::Text("A-100".into()), Cell::Integer(650)]]);
//! # std::fs::remove_dir_all(&dir).ok();
//! # Ok::<(), synodsql::Error>(())
//! ```

pub mod command;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use rusqlite::types::Value as Sql;
use synod_core::log::{decode_log, log_hash};
use synod_core::{NodeId, Value, ValueSet};
use synod_store::schema::ColumnType;
use synod_store::{Env, MergeReport, SealedLog, Store, StoreConfig};

pub use synod_core;
pub use synod_net;
pub use synod_node;
pub use synod_store;

/// An error, as text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<synod_store::Error> for Error {
    fn from(e: synod_store::Error) -> Error {
        Error(e.to_string())
    }
}

impl From<synod_net::NetError> for Error {
    fn from(e: synod_net::NetError) -> Error {
        Error(e.to_string())
    }
}

impl From<String> for Error {
    fn from(e: String) -> Error {
        Error(e)
    }
}

/// Results of this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Settings of a new node store.
#[derive(Clone, Debug)]
pub struct Options {
    /// This node's ID, unique in the cluster.
    pub node: u32,
    /// Epoch length in milliseconds.
    pub epoch_ms: u64,
    /// Start of epoch 0, in milliseconds since the Unix epoch.
    pub genesis_ms: u64,
    /// Largest clock drift accepted from a peer, in milliseconds.
    pub max_drift_ms: u64,
    /// Epochs a late log can still be merged.
    pub lateness: u64,
}

impl Options {
    /// The defaults of `synod init`: epochs of 60 s, lateness 16.
    pub fn new(node: u32) -> Options {
        Options { node, epoch_ms: 60_000, genesis_ms: 0, max_drift_ms: 500, lateness: 16 }
    }
}

/// A value given to a write or a query.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    /// NULL.
    Null,
    /// An integer; it also fits REAL and DECIMAL columns.
    Integer(i64),
    /// A float; it also fits DECIMAL columns.
    Real(f64),
    /// Text; for a DECIMAL column, the decimal as text.
    Text(String),
    /// Bytes.
    Blob(Vec<u8>),
    /// The items of a SET column.
    List(Vec<Param>),
}

/// A value of a query result.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    /// NULL.
    Null,
    /// An integer.
    Integer(i64),
    /// A float.
    Real(f64),
    /// Text (DECIMAL columns read as text).
    Text(String),
    /// Bytes: blobs, row IDs (16 bytes), and encoded sets.
    Blob(Vec<u8>),
}

impl Cell {
    /// The cell as the command line shows it: row IDs and sets decoded.
    pub fn display(&self) -> String {
        command::show(&self.to_sql())
    }

    fn to_sql(&self) -> Sql {
        match self {
            Cell::Null => Sql::Null,
            Cell::Integer(i) => Sql::Integer(*i),
            Cell::Real(r) => Sql::Real(*r),
            Cell::Text(t) => Sql::Text(t.clone()),
            Cell::Blob(b) => Sql::Blob(b.clone()),
        }
    }
}

/// The result of a query.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Rows {
    /// Column names.
    pub columns: Vec<String>,
    /// Rows, each with one cell per column.
    pub rows: Vec<Vec<Cell>>,
}

/// Settings of [`Database::serve`].
#[derive(Clone, Debug)]
pub struct ServeOptions {
    /// Address to listen on, as `host:port`.
    pub listen: String,
    /// All members as (node ID, `host:port`); this node is added if missing.
    pub peers: Vec<(u32, String)>,
    /// Directory of the cluster's certificates (`synod certs`).
    pub certs: String,
    /// Time between two ticks of the node; by default 1/20 of an epoch.
    pub tick_ms: Option<u64>,
    /// Merge only with a quorum of logs (the default).
    pub quorum: bool,
    /// Gossip to this many peers instead of sending to all.
    pub gossip: Option<usize>,
}

impl ServeOptions {
    /// Options with a quorum and no gossip.
    pub fn new(listen: &str, peers: &[(u32, &str)], certs: &str) -> ServeOptions {
        ServeOptions {
            listen: listen.to_owned(),
            peers: peers.iter().map(|(id, a)| (*id, (*a).to_owned())).collect(),
            certs: certs.to_owned(),
            tick_ms: None,
            quorum: true,
            gossip: None,
        }
    }
}

enum Inner {
    Local(Store),
    Serving(synod_net::Running),
    Closed,
}

/// One node's store. While it serves ([`Database::serve`]), every call runs
/// on the node's thread, between two messages.
pub struct Database {
    inner: Inner,
    path: String,
    node: u32,
}

impl Database {
    /// Makes a new node store at `path`.
    pub fn create(path: &str, options: &Options) -> Result<Database> {
        if std::path::Path::new(path).exists() {
            return Err(Error(format!("{path} exists")));
        }
        let config = StoreConfig {
            node: NodeId(options.node),
            genesis_ms: options.genesis_ms,
            epoch_len_ms: options.epoch_ms,
            max_drift_ms: options.max_drift_ms,
            lateness: options.lateness,
        };
        Ok(Database::local(Store::open(path, config, Env::os())?))
    }

    /// Opens a node store.
    pub fn open(path: &str) -> Result<Database> {
        Ok(Database::local(Store::open_existing(path, Env::os())?))
    }

    fn local(store: Store) -> Database {
        Database { path: store.path().to_owned(), node: store.config().node.0, inner: Inner::Local(store) }
    }

    /// The store's file.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// This node's ID.
    pub fn node(&self) -> u32 {
        self.node
    }

    /// True while the node serves.
    pub fn is_serving(&self) -> bool {
        matches!(&self.inner, Inner::Serving(r) if r.is_running())
    }

    /// Runs `f` on the store: here, or on the node's thread while it serves.
    pub fn with<R: Send + 'static>(&mut self, f: impl FnOnce(&mut Store) -> R + Send + 'static) -> Result<R> {
        match &mut self.inner {
            Inner::Local(s) => Ok(f(s)),
            Inner::Serving(r) => Ok(r.call(f)?),
            Inner::Closed => Err(Error("the database is closed".into())),
        }
    }

    fn local_only(&mut self, what: &str) -> Result<&mut Store> {
        match &mut self.inner {
            Inner::Local(s) => Ok(s),
            Inner::Serving(_) => Err(Error(format!("the node is serving: it does not {what} by files"))),
            Inner::Closed => Err(Error("the database is closed".into())),
        }
    }

    /// Adds a table: `CREATE TABLE ... MERGE ...`. Returns its DDL.
    pub fn create_table(&mut self, ddl: &str) -> Result<String> {
        let ddl = ddl.to_owned();
        Ok(self.with(move |s| s.create_table(&ddl).map(|t| t.ddl.clone()))??)
    }

    /// Adds a row. Returns its row ID, as text.
    pub fn insert(&mut self, table: &str, columns: &[(&str, Param)]) -> Result<String> {
        self.write(table, None, owned(columns), false)
    }

    /// Changes columns of a row.
    pub fn update(&mut self, table: &str, row_id: &str, columns: &[(&str, Param)]) -> Result<()> {
        self.write(table, Some(row_id), owned(columns), false).map(drop)
    }

    /// As [`Database::insert`], with values as the command line gives them
    /// (`NULL`, numbers, text, `x'0a1b'`, `a,b,c` for sets).
    pub fn insert_text(&mut self, table: &str, columns: &[(&str, &str)]) -> Result<String> {
        self.write(table, None, text(columns), true)
    }

    /// As [`Database::update`], with values as text.
    pub fn update_text(&mut self, table: &str, row_id: &str, columns: &[(&str, &str)]) -> Result<()> {
        self.write(table, Some(row_id), text(columns), true).map(drop)
    }

    fn write(
        &mut self,
        table: &str,
        row: Option<&str>,
        columns: Vec<(String, Param)>,
        as_text: bool,
    ) -> Result<String> {
        let table = table.to_owned();
        let row = row.map(command::parse_row_id).transpose()?;
        self.with(move |s| -> Result<String> {
            let schema = s.table(&table)?;
            let mut values = Vec::new();
            for (name, p) in columns {
                let column = schema
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| Error(format!("no column {name} in {table}")))?;
                let v = match (as_text, p) {
                    (true, Param::Text(raw)) => command::parse_value(&column.ty, &raw)?,
                    (_, p) => typed(&column.ty, p).map_err(|e| Error(format!("{}: {e}", column.name)))?,
                };
                values.push((column.name.clone(), v));
            }
            let given: Vec<(&str, Value)> = values.iter().map(|(n, v)| (n.as_str(), v.clone())).collect();
            let mut t = s.begin()?;
            let id = match row {
                None => t.insert(&schema.name, &given)?,
                Some(id) => {
                    t.update(&schema.name, id, &given)?;
                    id
                }
            };
            t.commit()?;
            Ok(id.to_string())
        })?
    }

    /// Deletes a row.
    pub fn delete(&mut self, table: &str, row_id: &str) -> Result<()> {
        let (table, id) = (table.to_owned(), command::parse_row_id(row_id)?);
        self.with(move |s| -> Result<()> {
            let mut t = s.begin()?;
            t.delete(&table, id)?;
            t.commit()?;
            Ok(())
        })?
    }

    /// Runs a read-only SQL statement on the tables' views, with `?1`,
    /// `?2`, ... bound to `params`.
    pub fn query(&mut self, sql: &str, params: &[Param]) -> Result<Rows> {
        let sql = sql.to_owned();
        let params: Vec<Value> = params.iter().map(plain).collect::<Result<_>>()?;
        self.with(move |s| -> Result<Rows> {
            let (columns, rows) = s.query_named(&sql, &params)?;
            let rows = rows.into_iter().map(|r| r.into_iter().map(cell).collect()).collect();
            Ok(Rows { columns, rows })
        })?
    }

    /// Runs a store command of the `synod` command line, without the store
    /// path (`status`, `conflicts --all`, `resolve <id>`, `revert ...`, ...),
    /// and returns its output.
    pub fn exec(&mut self, args: &[&str]) -> Result<String> {
        let args: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
        Ok(self.with(move |s| command::run(s, &args))??)
    }

    /// Seals every epoch that has ended, for file sync. Returns the sealed
    /// epochs.
    pub fn seal(&mut self) -> Result<Vec<u64>> {
        let (logs, _, _) = seal_ended(self.local_only("seal")?)?;
        Ok(logs.iter().map(|l| l.epoch).collect())
    }

    /// The sealed log of an epoch, to send to the other nodes.
    pub fn export(&mut self, epoch: u64) -> Result<Vec<u8>> {
        Ok(self.with(move |s| s.export(epoch))??)
    }

    /// Merges the next sealed epochs with the logs of the other nodes.
    /// Returns the merged epochs.
    pub fn merge(&mut self, logs: &[Vec<u8>]) -> Result<Vec<u64>> {
        let merged = merge_logs(self.local_only("merge")?, logs)?;
        Ok(merged.iter().map(|(e, _)| *e).collect())
    }

    /// Makes a new node store at `path` from this node's base state.
    pub fn clone_to(&mut self, path: &str, node: u32) -> Result<Database> {
        let path = path.to_owned();
        Ok(Database::local(self.with(move |s| s.clone_base(&path, NodeId(node)))??))
    }

    /// Runs the node in the background: it seals, exchanges logs with its
    /// peers over HTTP/2 with mutual TLS, and merges. Calls go on working.
    pub fn serve(&mut self, options: &ServeOptions) -> Result<()> {
        let store = match std::mem::replace(&mut self.inner, Inner::Closed) {
            Inner::Local(s) => s,
            other => {
                self.inner = other;
                return Err(Error("the node already serves, or is closed".into()));
            }
        };
        match start(store, options) {
            Ok(r) => {
                self.inner = Inner::Serving(r);
                Ok(())
            }
            Err(e) => {
                self.inner = Inner::Local(Store::open_existing(&self.path, Env::os())?);
                Err(e)
            }
        }
    }

    /// Stops the node if it serves, and closes the store.
    pub fn close(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.inner, Inner::Closed) {
            Inner::Serving(r) => Ok(r.stop()?),
            _ => Ok(()),
        }
    }
}

fn start(store: Store, o: &ServeOptions) -> Result<synod_net::Running> {
    let me = store.config().node;
    let listen = o.listen.parse().map_err(|_| Error(format!("not an address: {}", o.listen)))?;
    let mut members: Vec<synod_net::Member> =
        o.peers.iter().map(|(id, addr)| synod_net::Member { id: NodeId(*id), addr: addr.clone() }).collect();
    if !members.iter().any(|m| m.id == me) {
        members.push(synod_net::Member { id: me, addr: o.listen.clone() });
    }
    let tls = load_tls(&o.certs, &synod_net::node_name(me))?;
    let mut cfg = synod_node::PeerConfig::new(members.iter().map(|m| m.id).collect(), store.config().epoch_len_ms);
    cfg.quorum = o.quorum;
    cfg.gossip = o.gossip;
    let tick_ms = o.tick_ms.unwrap_or((store.config().epoch_len_ms / 20).clamp(10, 1_000));
    let peer = synod_node::Peer::new(store, cfg)?;
    let exec: synod_net::ExecFn = Box::new(command::run);
    let config = synod_net::ServeConfig { listen, members, tls, tick_ms };
    Ok(synod_net::start(peer, config, exec, Arc::new(AtomicBool::new(false)))?)
}

fn owned(columns: &[(&str, Param)]) -> Vec<(String, Param)> {
    columns.iter().map(|(n, p)| ((*n).to_owned(), p.clone())).collect()
}

fn text(columns: &[(&str, &str)]) -> Vec<(String, Param)> {
    columns.iter().map(|(n, v)| ((*n).to_owned(), Param::Text((*v).to_owned()))).collect()
}

// A value for a column of type `ty`.
fn typed(ty: &ColumnType, p: Param) -> std::result::Result<Value, String> {
    let bad = |p: &Param| format!("{p:?} is not a {ty}");
    Ok(match (ty, p) {
        (_, Param::Null) => Value::Null,
        (ColumnType::Integer, Param::Integer(i)) => Value::Integer(i),
        (ColumnType::Real, Param::Integer(i)) => Value::Real(i as f64),
        (ColumnType::Real, Param::Real(r)) => Value::Real(r),
        (ColumnType::Decimal { .. }, Param::Integer(i)) => {
            Value::Decimal(i.to_string().parse().map_err(|_| ty.to_string())?)
        }
        (ColumnType::Decimal { .. }, p @ (Param::Real(_) | Param::Text(_))) => {
            let raw = match &p {
                Param::Real(r) => r.to_string(),
                Param::Text(t) => t.clone(),
                _ => unreachable!(),
            };
            Value::Decimal(raw.parse().map_err(|_| bad(&p))?)
        }
        (ColumnType::Text, Param::Text(t)) => Value::Text(t),
        (ColumnType::Blob, Param::Blob(b)) => Value::Blob(b),
        (ColumnType::Set(item), Param::List(items)) => {
            let items: Vec<Value> = items.into_iter().map(|p| typed(item, p)).collect::<std::result::Result<_, _>>()?;
            Value::Set(ValueSet::new(items).ok_or_else(|| format!("not a set of {item}"))?)
        }
        (_, p) => return Err(bad(&p)),
    })
}

// A query parameter.
fn plain(p: &Param) -> Result<Value> {
    Ok(match p {
        Param::Null => Value::Null,
        Param::Integer(i) => Value::Integer(*i),
        Param::Real(r) => Value::Real(*r),
        Param::Text(t) => Value::Text(t.clone()),
        Param::Blob(b) => Value::Blob(b.clone()),
        Param::List(_) => return Err(Error("a list is not a query parameter".into())),
    })
}

fn cell(v: Sql) -> Cell {
    match v {
        Sql::Null => Cell::Null,
        Sql::Integer(i) => Cell::Integer(i),
        Sql::Real(r) => Cell::Real(r),
        Sql::Text(t) => Cell::Text(t),
        Sql::Blob(b) => Cell::Blob(b),
    }
}

/// Seals every epoch of `s` that has ended. Returns the sealed logs, and
/// the open epoch with the time it ends (ms).
pub fn seal_ended(s: &mut Store) -> Result<(Vec<SealedLog>, u64, u64)> {
    let mut sealed = Vec::new();
    loop {
        match s.seal() {
            Ok(l) => sealed.push(l),
            Err(synod_store::Error::TooEarly { epoch, ends_at_ms }) => return Ok((sealed, epoch, ends_at_ms)),
            Err(e) => return Err(e.into()),
        }
    }
}

/// Merges the next sealed epochs of `s` with the given logs (other nodes'
/// logs; this node's own are skipped). Takes, for each epoch, its logs and
/// the late logs that no merge took. Returns each merged epoch's report.
pub fn merge_logs(s: &mut Store, logs: &[Vec<u8>]) -> Result<Vec<(u64, MergeReport)>> {
    let me = s.config().node;
    // (epoch, bytes) of each given log.
    let mut given = Vec::new();
    for bytes in logs {
        let (h, _) = decode_log(bytes).map_err(|e| Error(format!("not a log: {e}")))?;
        if h.node != me {
            given.push((h.epoch, bytes));
        }
    }
    let mut merged = Vec::new();
    loop {
        let epoch = s.next_merge_epoch()?;
        if s.sealed(epoch)?.is_none() {
            return Ok(merged);
        }
        let mut take = Vec::new();
        for (e, bytes) in &given {
            if *e == epoch || (*e < epoch && s.log_merged_in(&log_hash(bytes))?.is_none()) {
                take.push((*bytes).clone());
            }
        }
        merged.push((epoch, s.merge(epoch, &take)?));
    }
}

/// Reads a node's or client's TLS files from a certificate directory: the
/// CA (`ca.pem`), and `<name>.pem` and `<name>.key`.
pub fn load_tls(dir: &str, name: &str) -> Result<synod_net::Tls> {
    let read = |f: String| {
        let path = std::path::Path::new(dir).join(f);
        std::fs::read(&path).map_err(|e| Error(format!("{}: {e}", path.display())))
    };
    Ok(synod_net::Tls {
        ca_pem: read("ca.pem".into())?,
        cert_pem: read(format!("{name}.pem"))?,
        key_pem: read(format!("{name}.key"))?,
    })
}

/// Makes a cluster CA and certificates in `dir`: one per node
/// (`node-<id>`) and one per client name (such as `admin`).
pub fn make_certs(dir: &str, nodes: &[u32], clients: &[&str]) -> Result<usize> {
    let nodes: Vec<NodeId> = nodes.iter().copied().map(NodeId).collect();
    let certs = synod_net::make_certs(&nodes, clients)?;
    let dir = std::path::Path::new(dir);
    std::fs::create_dir_all(dir).map_err(|e| Error(format!("{}: {e}", dir.display())))?;
    let write = |name: String, data: &str| {
        let path = dir.join(name);
        std::fs::write(&path, data).map_err(|e| Error(format!("{}: {e}", path.display())))
    };
    write("ca.pem".into(), &certs.ca_pem)?;
    write("ca.key".into(), &certs.ca_key_pem)?;
    for (name, cert, key) in &certs.issued {
        write(format!("{name}.pem"), cert)?;
        write(format!("{name}.key"), key)?;
    }
    Ok(certs.issued.len())
}

/// Runs a store command on a running node, as the client `name` of the
/// certificate directory `certs` (`synod remote`). `net-status` and
/// `metrics` read the node's status and metrics.
pub fn remote(addr: &str, node: u32, certs: &str, name: &str, args: &[&str]) -> Result<String> {
    let tls = load_tls(certs, name)?;
    let member = synod_net::Member { id: NodeId(node), addr: addr.to_owned() };
    let (method, path, body) = match args.first().copied() {
        None => return Err(Error("remote needs a command".into())),
        Some("net-status") => ("GET", "/synod/v1/status", Vec::new()),
        Some("metrics") => ("GET", "/metrics", Vec::new()),
        Some(_) => ("POST", "/synod/v1/exec", args.join("\0").into_bytes()),
    };
    let (code, body) = synod_net::request(&member, &tls, method, path, body)?;
    let text = String::from_utf8_lossy(&body).into_owned();
    if code >= 300 {
        return Err(Error(format!("node {node}: {code}: {text}")));
    }
    Ok(text)
}
