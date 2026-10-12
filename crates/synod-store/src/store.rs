//! The single-node store.
//!
//! One SQLite file holds everything a node keeps:
//!
//! | Table | Content |
//! | --- | --- |
//! | `_synod_node` | node ID, epoch settings, counters, last HLC, open epoch, base root |
//! | `_synod_schema` | the `CREATE TABLE` statement of each managed table |
//! | `_synod_t_<name>` | rows of a managed table, with shadow columns |
//! | `<name>` (view) | the live rows of a managed table, for normal SQL reads |
//! | `_synod_log` | one entry per write, in commit order |
//! | `_synod_sealed` | sealed logs and their hashes |
//! | `_synod_rowhash` | row hash of every row, for the state hash |
//! | `_synod_dirty` | rows written by [`Txn::put_row`] since the last state hash |
//!
//! A write changes its row, its log entry, and the counters in one SQLite
//! transaction. So a committed write is in the log exactly once, and a
//! write that did not commit is in neither.

use std::sync::Arc;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, params};
use synod_core::log::{LogHeader, encode_log, log_hash};
use synod_core::prolly::{NodeHash, TreeBuilder};
use synod_core::{Cell, Hlc, HlcClock, LogEntry, NodeId, Op, Row, RowHash, RowId, Stamp, TxnId, Value};

use crate::env::Env;
use crate::error::{Error, Result};
use crate::schema::{TableSchema, parse_create_table};
use crate::values;

/// Version of the store file layout.
pub const STORE_FORMAT: i64 = 1;

const SETUP: &str = "
CREATE TABLE _synod_node (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    format INTEGER NOT NULL,
    node INTEGER NOT NULL,
    genesis_ms INTEGER NOT NULL,
    epoch_len_ms INTEGER NOT NULL,
    max_drift_ms INTEGER NOT NULL,
    next_row INTEGER NOT NULL,
    next_txn INTEGER NOT NULL,
    hlc_last INTEGER NOT NULL,
    open_epoch INTEGER NOT NULL,
    base_root BLOB NOT NULL,
    hashed_seq INTEGER NOT NULL
) STRICT;
CREATE TABLE _synod_schema (
    table_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    ddl TEXT NOT NULL
) STRICT;
CREATE TABLE _synod_log (
    seq INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    tbl INTEGER NOT NULL,
    row_id BLOB NOT NULL,
    entry BLOB NOT NULL
) STRICT;
CREATE INDEX _synod_log_epoch ON _synod_log (epoch, seq);
CREATE TABLE _synod_sealed (
    epoch INTEGER PRIMARY KEY,
    log_hash BLOB NOT NULL,
    log BLOB NOT NULL,
    entries INTEGER NOT NULL,
    root BLOB NOT NULL
) STRICT;
CREATE TABLE _synod_rowhash (
    row_id BLOB PRIMARY KEY,
    hash BLOB NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TABLE _synod_dirty (
    row_id BLOB PRIMARY KEY,
    tbl INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
";

/// Settings of a node, fixed when its store is made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreConfig {
    /// This node's ID.
    pub node: NodeId,
    /// Start of epoch 0, in milliseconds since the Unix epoch.
    pub genesis_ms: u64,
    /// Length of an epoch.
    pub epoch_len_ms: u64,
    /// HLC drift limit.
    pub max_drift_ms: u64,
}

/// A sealed epoch log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedLog {
    /// Epoch.
    pub epoch: u64,
    /// Log hash.
    pub hash: [u8; 32],
    /// The log (specification Section 5).
    pub bytes: Vec<u8>,
    /// Number of entries.
    pub entries: u64,
    /// State hash of this node's state when it sealed.
    pub root: NodeHash,
}

/// What a commit gives back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Commit {
    /// Transaction ID.
    pub txn: TxnId,
    /// HLC time of the transaction.
    pub hlc: Hlc,
    /// Epoch whose log holds the transaction.
    pub epoch: u64,
}

/// A SynodSQL node store on SQLite.
pub struct Store {
    conn: Connection,
    path: String,
    env: Env,
    config: StoreConfig,
    clock: HlcClock,
    tables: Vec<Arc<TableSchema>>,
}

fn sql_err(e: rusqlite::Error) -> Error {
    match &e {
        rusqlite::Error::SqliteFailure(f, msg) if f.code == ErrorCode::ConstraintViolation => {
            Error::Constraint(msg.clone().unwrap_or_else(|| e.to_string()))
        }
        _ => Error::Sql(e),
    }
}

fn hlc_at(ms: u64) -> Result<Hlc> {
    Hlc::new(ms, 0).ok_or(Error::Clock(synod_core::HlcError::Overflow))
}

impl Store {
    /// Opens the store at `path`, and makes it if it does not exist.
    ///
    /// The settings of an existing store must equal `config`.
    pub fn open(path: &str, config: StoreConfig, env: Env) -> Result<Store> {
        if config.epoch_len_ms == 0 {
            return Err(Error::Invalid("epoch length must be at least 1 ms".into()));
        }
        // A leftover from a base switch that did not finish.
        let next = format!("{path}.next");
        if env.fs.exists(&next)? {
            env.fs.remove(&next)?;
            env.fs.sync_dir(path)?;
        }
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = match env.fs.vfs() {
            Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs)?,
            None => Connection::open_with_flags(path, flags)?,
        };
        let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Invalid(format!("journal mode {mode} instead of WAL")));
        }
        conn.execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = OFF;")?;

        let exists: bool = conn.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = '_synod_node'",
            [],
            |r| r.get::<_, i64>(0).map(|n| n > 0),
        )?;
        if !exists {
            let now = env.clock.now_ms();
            let open_epoch = now.saturating_sub(config.genesis_ms) / config.epoch_len_ms;
            conn.execute_batch(&format!("BEGIN IMMEDIATE; {SETUP}"))?;
            conn.execute(
                "INSERT INTO _synod_node VALUES (0, ?1, ?2, ?3, ?4, ?5, 0, 0, 0, ?6, ?7, 0)",
                params![
                    STORE_FORMAT,
                    config.node.0,
                    config.genesis_ms as i64,
                    config.epoch_len_ms as i64,
                    config.max_drift_ms as i64,
                    open_epoch as i64,
                    synod_core::prolly::empty_root().to_vec(),
                ],
            )?;
            conn.execute_batch("COMMIT")?;
        }
        let (format, node, genesis, len, drift, hlc_last, open_epoch): (i64, u32, i64, i64, i64, i64, i64) = conn
            .query_row(
                "SELECT format, node, genesis_ms, epoch_len_ms, max_drift_ms, hlc_last, open_epoch FROM _synod_node",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
            )?;
        if format != STORE_FORMAT {
            return Err(Error::Corrupt(format!("store format {format}, expected {STORE_FORMAT}")));
        }
        let stored = StoreConfig {
            node: NodeId(node),
            genesis_ms: genesis as u64,
            epoch_len_ms: len as u64,
            max_drift_ms: drift as u64,
        };
        if stored != config {
            return Err(Error::Invalid(format!("store was made with {stored:?}, opened with {config:?}")));
        }

        let mut tables = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT table_id, ddl FROM _synod_schema ORDER BY table_id")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, u32>(0)?, r.get::<_, String>(1)?)))?;
            for row in rows {
                let (id, ddl) = row?;
                if id as usize != tables.len() {
                    return Err(Error::Corrupt(format!("table IDs are not 0, 1, 2, ...: found {id}")));
                }
                tables.push(Arc::new(parse_create_table(&ddl, id)?));
            }
        }

        // The clock resumes after the last committed HLC, and never inside
        // a sealed epoch (specification Section 3.4).
        let epoch_start = hlc_at(config.genesis_ms + open_epoch as u64 * config.epoch_len_ms)?;
        let clock = HlcClock::resume(Hlc::from_u64(hlc_last as u64).max(epoch_start), config.max_drift_ms);
        Ok(Store { conn, path: path.to_owned(), env, config, clock, tables })
    }

    /// Returns the settings.
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// Returns the path of the store file.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the managed tables, by table ID.
    pub fn tables(&self) -> &[Arc<TableSchema>] {
        &self.tables
    }

    /// Returns a managed table by name.
    pub fn table(&self, name: &str) -> Result<Arc<TableSchema>> {
        self.tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .cloned()
            .ok_or_else(|| Error::Invalid(format!("no managed table {name}")))
    }

    /// Returns the epoch of an HLC time.
    pub fn epoch_of(&self, hlc: Hlc) -> u64 {
        hlc.physical_ms().saturating_sub(self.config.genesis_ms) / self.config.epoch_len_ms
    }

    /// Returns the local physical time at which `epoch` starts.
    pub fn epoch_start_ms(&self, epoch: u64) -> u64 {
        self.config.genesis_ms + epoch * self.config.epoch_len_ms
    }

    /// Returns the lowest epoch that is not sealed.
    pub fn open_epoch(&self) -> Result<u64> {
        Ok(self.conn.query_row("SELECT open_epoch FROM _synod_node", [], |r| r.get::<_, i64>(0))? as u64)
    }

    /// Returns the root hash of the base state M(e−1).
    pub fn base_root(&self) -> Result<NodeHash> {
        let root: Vec<u8> = self.conn.query_row("SELECT base_root FROM _synod_node", [], |r| r.get(0))?;
        root.try_into().map_err(|_| Error::Corrupt("base_root is not 32 bytes".into()))
    }

    /// Adds a managed table. See [`crate::schema`] for the language.
    pub fn create_table(&mut self, ddl: &str) -> Result<Arc<TableSchema>> {
        let schema = parse_create_table(ddl, self.tables.len() as u32)?;
        if self.tables.iter().any(|t| t.name.eq_ignore_ascii_case(&schema.name)) {
            return Err(Error::Schema(format!("table {} exists", schema.name)));
        }
        for fk in &schema.foreign_keys {
            let parent =
                if fk.parent.eq_ignore_ascii_case(&schema.name) { &schema } else { &*self.table(&fk.parent)? };
            let col = parent
                .column_index(&fk.parent_column)
                .ok_or_else(|| Error::Schema(format!("no column {}.{}", fk.parent, fk.parent_column)))?;
            if !parent.uniques.contains(&vec![col]) {
                return Err(Error::Schema(format!("{}.{} is not UNIQUE", fk.parent, fk.parent_column)));
            }
        }
        for (column, default) in schema.columns.iter().filter_map(|c| c.default.as_ref().map(|d| (c, d))) {
            let raw: SqlValue = self.conn.query_row(&format!("SELECT {default}"), [], |r| r.get(0))?;
            values::from_expression(column, (&raw).into())?;
        }
        let tx = self.conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for sql in schema.create_statements() {
            tx.execute(&sql, [])?;
        }
        tx.execute(
            "INSERT INTO _synod_schema (table_id, name, ddl) VALUES (?1, ?2, ?3)",
            params![schema.id, schema.name, schema.ddl],
        )?;
        tx.commit()?;
        let schema = Arc::new(schema);
        self.tables.push(schema.clone());
        Ok(schema)
    }

    /// Starts a write transaction.
    pub fn begin(&mut self) -> Result<Txn<'_>> {
        let hlc = self.clock.now(self.env.clock.now_ms())?;
        let epoch = self.epoch_of(hlc);
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let read = self.conn.query_row("SELECT next_row, next_txn, open_epoch FROM _synod_node", [], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64))
        });
        let (next_row, next_txn, open_epoch) = match read {
            Ok(v) => v,
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(e.into());
            }
        };
        let txn = Txn {
            txn: TxnId { node: self.config.node, counter: next_txn },
            hlc,
            epoch,
            next_row,
            store: self,
            done: false,
        };
        if epoch < open_epoch {
            return Err(Error::Corrupt(format!("HLC {hlc} is in sealed epoch {epoch}")));
        }
        Ok(txn)
    }

    /// Runs a read-only SQL statement and returns all result rows.
    ///
    /// Read managed tables through their views. A statement that writes
    /// is rejected.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<SqlValue>>> {
        let mut stmt = self.conn.prepare(sql)?;
        if !stmt.readonly() {
            return Err(Error::Invalid("query() runs read-only statements only; use begin() to write".into()));
        }
        let n = stmt.column_count();
        let params: Vec<SqlValue> = params.iter().map(values::to_sql).collect();
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |r| (0..n).map(|i| r.get(i)).collect())?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Returns the values of a live row.
    pub fn get(&self, table: &str, row_id: RowId) -> Result<Option<Vec<Value>>> {
        let schema = self.table(table)?;
        Ok(read_row(&self.conn, &schema, row_id)?.filter(|r| !r.tombstone).map(|r| row_values(&r)))
    }

    /// Returns a row in its replicated form, tombstones included.
    pub fn row(&self, table: &str, row_id: RowId) -> Result<Option<Row>> {
        let schema = self.table(table)?;
        read_row(&self.conn, &schema, row_id)
    }

    /// Returns the log entries that are not sealed yet, in commit order.
    pub fn unsealed_entries(&self) -> Result<Vec<LogEntry>> {
        let open = self.open_epoch()?;
        self.entries_where("epoch >= ?1", open)
    }

    /// Returns all log entries, sealed or not, in commit order.
    pub fn all_entries(&self) -> Result<Vec<LogEntry>> {
        self.entries_where("?1 = ?1", 0)
    }

    fn entries_where(&self, cond: &str, arg: u64) -> Result<Vec<LogEntry>> {
        let mut stmt = self.conn.prepare(&format!("SELECT entry FROM _synod_log WHERE {cond} ORDER BY seq"))?;
        let rows = stmt.query_map([arg as i64], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|b| LogEntry::decode(&b?).map_err(|e| Error::Corrupt(format!("log entry: {e}")))).collect()
    }

    /// Returns a sealed log.
    pub fn sealed(&self, epoch: u64) -> Result<Option<SealedLog>> {
        self.conn
            .query_row("SELECT log_hash, log, entries, root FROM _synod_sealed WHERE epoch = ?1", [epoch as i64], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, i64>(2)?, r.get::<_, Vec<u8>>(3)?))
            })
            .optional()?
            .map(|(hash, bytes, entries, root)| {
                Ok(SealedLog {
                    epoch,
                    hash: hash.try_into().map_err(|_| Error::Corrupt("log hash".into()))?,
                    bytes,
                    entries: entries as u64,
                    root: root.try_into().map_err(|_| Error::Corrupt("root".into()))?,
                })
            })
            .transpose()
    }

    /// Seals the open epoch: closes its log, hashes it, and updates the
    /// state hash in one batch (specification Section 5).
    ///
    /// Fails with [`Error::TooEarly`] until the local clock reaches the end
    /// of the epoch. After the seal, every new write has an HLC time in a
    /// later epoch.
    pub fn seal(&mut self) -> Result<SealedLog> {
        let epoch = self.open_epoch()?;
        let end = self.epoch_start_ms(epoch + 1);
        let now = self.env.clock.now_ms();
        if now < end {
            return Err(Error::TooEarly { epoch, ends_at_ms: end });
        }
        // Every later HLC value is at or after the end of the epoch.
        self.clock = HlcClock::resume(self.clock.last().max(hlc_at(end)?), self.config.max_drift_ms);
        let sealed_at = self.clock.now(now)?;
        self.in_txn(|s| {
            let root = s.refresh_state_hash()?;
            let base_root: Vec<u8> = s.conn.query_row("SELECT base_root FROM _synod_node", [], |r| r.get(0))?;
            let header = LogHeader {
                node: s.config.node,
                epoch,
                base_root: base_root.try_into().map_err(|_| Error::Corrupt("base_root".into()))?,
                schema_epoch: 0,
                sealed_at,
            };
            let bodies: Vec<Vec<u8>> = {
                let mut stmt = s.conn.prepare("SELECT entry FROM _synod_log WHERE epoch = ?1 ORDER BY seq")?;
                stmt.query_map([epoch as i64], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?
            };
            let bytes = encode_log(&header, bodies.iter().map(Vec::as_slice));
            let hash = log_hash(&bytes);
            s.conn.execute(
                "INSERT INTO _synod_sealed (epoch, log_hash, log, entries, root) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![epoch as i64, hash.to_vec(), bytes, bodies.len() as i64, root.to_vec()],
            )?;
            s.conn.execute(
                "UPDATE _synod_node SET open_epoch = ?1, hlc_last = max(hlc_last, ?2)",
                params![(epoch + 1) as i64, sealed_at.as_u64() as i64],
            )?;
            Ok(SealedLog { epoch, hash, bytes, entries: bodies.len() as u64, root })
        })
    }

    /// Returns the state hash of the current local state. It brings the
    /// row hash cache up to date first.
    pub fn state_hash(&mut self) -> Result<NodeHash> {
        self.in_txn(|s| s.refresh_state_hash())
    }

    /// Computes the state hash from all rows, without the row hash cache.
    /// It must equal [`state_hash`](Store::state_hash).
    pub fn state_hash_from_scratch(&self) -> Result<NodeHash> {
        let mut items: Vec<(RowId, RowHash)> = Vec::new();
        for schema in &self.tables {
            let mut stmt = self.conn.prepare(&format!("SELECT \"_row_id\" FROM \"{}\"", schema.physical()))?;
            let ids: Vec<Vec<u8>> = stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
            for id in ids {
                let row_id = parse_row_id(&id)?;
                let row = read_row(&self.conn, schema, row_id)?.expect("row just listed");
                items.push((row_id, row.hash()));
            }
        }
        items.sort();
        Ok(synod_core::prolly::root_hash(items))
    }

    // Updates the row hash of every row that a log entry touched since the
    // last update, then builds the tree. Runs inside a transaction.
    fn refresh_state_hash(&mut self) -> Result<NodeHash> {
        let hashed: i64 = self.conn.query_row("SELECT hashed_seq FROM _synod_node", [], |r| r.get(0))?;
        let touched: Vec<(u32, Vec<u8>, i64)> = {
            let mut stmt = self.conn.prepare(
                "SELECT tbl, row_id, max(seq) FROM _synod_log WHERE seq > ?1 GROUP BY tbl, row_id
                 UNION ALL SELECT tbl, row_id, 0 FROM _synod_dirty ORDER BY 2",
            )?;
            stmt.query_map([hashed], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<rusqlite::Result<_>>()?
        };
        let mut max_seq = hashed;
        for (tbl, id, seq) in touched {
            let schema = self.tables.get(tbl as usize).ok_or_else(|| Error::Corrupt(format!("table ID {tbl}")))?;
            let row = read_row(&self.conn, schema, parse_row_id(&id)?)?
                .ok_or_else(|| Error::Corrupt("a logged row is missing".into()))?;
            self.conn.execute(
                "INSERT OR REPLACE INTO _synod_rowhash (row_id, hash) VALUES (?1, ?2)",
                params![id, row.hash().0.to_vec()],
            )?;
            max_seq = max_seq.max(seq);
        }
        self.conn.execute("UPDATE _synod_node SET hashed_seq = ?1", [max_seq])?;
        self.conn.execute("DELETE FROM _synod_dirty", [])?;
        let mut builder = TreeBuilder::new();
        let mut stmt = self.conn.prepare("SELECT row_id, hash FROM _synod_rowhash ORDER BY row_id")?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let id = parse_row_id(&r.get::<_, Vec<u8>>(0)?)?;
            let hash: [u8; 32] =
                r.get::<_, Vec<u8>>(1)?.try_into().map_err(|_| Error::Corrupt("row hash is not 32 bytes".into()))?;
            builder.push(id, RowHash(hash));
        }
        Ok(builder.finish())
    }

    fn in_txn<T>(&mut self, f: impl FnOnce(&mut Store) -> Result<T>) -> Result<T> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        match f(self) {
            Ok(v) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(e.into());
                }
                Ok(v)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Writes a copy of the store to a new file and switches to it with an
    /// atomic rename (the copy-on-write base switch, white paper
    /// Section XI-B). Returns the store opened on the new file.
    ///
    /// The merge of Phase 2 writes M(e) the same way. After a crash at any
    /// point, the store holds either the old file or the new one, never a
    /// mix.
    pub fn rewrite(self) -> Result<Store> {
        let next = format!("{}.next", self.path);
        if self.env.fs.exists(&next)? {
            self.env.fs.remove(&next)?;
        }
        self.conn.execute("VACUUM INTO ?1", [&next])?;
        self.install(&next)
    }

    /// Closes the store and replaces its file with `next`, which must be a
    /// complete store file. Returns the store opened on the new file.
    pub fn install(self, next: &str) -> Result<Store> {
        let Store { conn, path, env, config, .. } = self;
        env.fs.sync_file(next)?;
        conn.close().map_err(|(_, e)| Error::Sql(e))?;
        // A clean close checkpoints and deletes the WAL. A WAL that is left
        // would be replayed onto the new file, so stop here.
        let wal = format!("{path}-wal");
        if env.fs.exists(&wal)? {
            return Err(Error::Invalid(format!("{wal} still exists after close")));
        }
        // Make the WAL delete durable before the rename.
        env.fs.sync_dir(&path)?;
        env.fs.rename(next, &path)?;
        env.fs.sync_dir(&path)?;
        Store::open(&path, config, env)
    }
}

fn parse_row_id(bytes: &[u8]) -> Result<RowId> {
    bytes.try_into().ok().and_then(RowId::from_be_bytes).ok_or_else(|| Error::Corrupt(format!("bad row ID {bytes:?}")))
}

/// Encodes the stamps of a row for the `_stamps` column: the distinct
/// stamps once, in order of first use, then one index per column (1 byte if
/// there are at most 256 distinct stamps, else 2 bytes).
///
/// This is local storage only. The row hash uses the canonical row, with
/// one full stamp per column (specification Section 4.3). Most rows have
/// few distinct stamps, because one write stamps all the columns it
/// changes.
fn encode_stamps(stamps: &[Stamp]) -> Vec<u8> {
    let mut distinct: Vec<Stamp> = Vec::new();
    let index: Vec<usize> = stamps
        .iter()
        .map(|s| {
            distinct.iter().position(|d| d == s).unwrap_or_else(|| {
                distinct.push(*s);
                distinct.len() - 1
            })
        })
        .collect();
    let mut out = Vec::with_capacity(2 + distinct.len() * Stamp::ENCODED_LEN + index.len());
    out.extend_from_slice(&(distinct.len() as u16).to_be_bytes());
    for d in &distinct {
        out.extend_from_slice(&d.to_be_bytes());
    }
    for i in index {
        if distinct.len() <= 256 {
            out.push(i as u8);
        } else {
            out.extend_from_slice(&(i as u16).to_be_bytes());
        }
    }
    out
}

fn decode_stamps(bytes: &[u8], columns: usize) -> Option<Vec<Stamp>> {
    let k = u16::from_be_bytes(bytes.get(..2)?.try_into().ok()?) as usize;
    let table_end = 2 + k * Stamp::ENCODED_LEN;
    let distinct: Vec<Stamp> = bytes
        .get(2..table_end)?
        .chunks_exact(Stamp::ENCODED_LEN)
        .map(|c| Stamp::from_be_bytes(c.try_into().unwrap()))
        .collect();
    let wide = k > 256;
    let rest = &bytes[table_end..];
    if rest.len() != columns * if wide { 2 } else { 1 } {
        return None;
    }
    (0..columns)
        .map(|c| {
            let i = if wide { u16::from_be_bytes([rest[2 * c], rest[2 * c + 1]]) as usize } else { rest[c] as usize };
            distinct.get(i).copied()
        })
        .collect()
}

fn row_values(row: &Row) -> Vec<Value> {
    row.cells.iter().map(|c| c.value.clone()).collect()
}

fn read_row(conn: &Connection, schema: &TableSchema, row_id: RowId) -> Result<Option<Row>> {
    let cols: Vec<String> = schema.columns.iter().map(|c| format!(", \"{}\"", c.name)).collect();
    let sql =
        format!("SELECT \"_tomb\", \"_stamps\"{} FROM \"{}\" WHERE \"_row_id\" = ?1", cols.concat(), schema.physical());
    let mut stmt = conn.prepare_cached(&sql)?;
    let mut rows = stmt.query([row_id.to_be_bytes().to_vec()])?;
    let Some(r) = rows.next()? else { return Ok(None) };
    let tombstone: i64 = r.get(0)?;
    let stamps = decode_stamps(&r.get::<_, Vec<u8>>(1)?, schema.columns.len())
        .ok_or_else(|| Error::Corrupt(format!("row {row_id}: bad stamps")))?;
    let mut cells = Vec::with_capacity(schema.columns.len());
    for (i, (column, stamp)) in schema.columns.iter().zip(stamps).enumerate() {
        cells.push(Cell { stamp, value: values::from_sql(column, r.get_ref(i + 2)?)? });
    }
    Ok(Some(Row { row_id, tombstone: tombstone != 0, cells }))
}

fn write_row(conn: &Connection, schema: &TableSchema, row: &Row) -> Result<()> {
    let names: Vec<String> = schema.columns.iter().map(|c| format!(", \"{}\"", c.name)).collect();
    let marks: Vec<String> = (0..schema.columns.len()).map(|i| format!(", ?{}", i + 4)).collect();
    let sets: Vec<String> = schema.columns.iter().map(|c| format!(", \"{0}\" = excluded.\"{0}\"", c.name)).collect();
    // An upsert on the row ID only: a clash on a unique key stays an error.
    // (INSERT OR REPLACE would delete the other row.)
    let sql = format!(
        "INSERT INTO \"{}\" (\"_row_id\", \"_tomb\", \"_stamps\"{}) VALUES (?1, ?2, ?3{}) \
         ON CONFLICT (\"_row_id\") DO UPDATE SET \"_tomb\" = excluded.\"_tomb\", \"_stamps\" = excluded.\"_stamps\"{}",
        schema.physical(),
        names.concat(),
        marks.concat(),
        sets.concat()
    );
    let stamps = encode_stamps(&row.cells.iter().map(|c| c.stamp).collect::<Vec<_>>());
    let mut args: Vec<SqlValue> = vec![
        SqlValue::Blob(row.row_id.to_be_bytes().to_vec()),
        SqlValue::Integer(row.tombstone as i64),
        SqlValue::Blob(stamps),
    ];
    args.extend(row.cells.iter().map(|c| values::to_sql(&c.value)));
    conn.prepare_cached(&sql)?.execute(rusqlite::params_from_iter(args)).map_err(sql_err)?;
    Ok(())
}

/// A write transaction. It commits with [`commit`](Txn::commit); if it is
/// dropped first, it rolls back.
pub struct Txn<'a> {
    store: &'a mut Store,
    txn: TxnId,
    hlc: Hlc,
    epoch: u64,
    next_row: u64,
    done: bool,
}

impl Txn<'_> {
    /// Returns the transaction ID.
    pub fn id(&self) -> TxnId {
        self.txn
    }

    /// Returns the HLC time of the transaction.
    pub fn hlc(&self) -> Hlc {
        self.hlc
    }

    fn stamp(&self) -> Stamp {
        Stamp { hlc: self.hlc, node: self.txn.node }
    }

    // Runs one operation in a savepoint, so that a failed operation leaves
    // no trace and the transaction can go on.
    fn op<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let next_row = self.next_row;
        self.store.conn.execute_batch("SAVEPOINT synod_op")?;
        match f(self) {
            Ok(v) => {
                self.store.conn.execute_batch("RELEASE synod_op")?;
                Ok(v)
            }
            Err(e) => {
                self.next_row = next_row;
                self.store.conn.execute_batch("ROLLBACK TO synod_op; RELEASE synod_op")?;
                Err(e)
            }
        }
    }

    /// Inserts a row. Columns that are not given get their DEFAULT, or
    /// NULL. Returns the new row ID.
    pub fn insert(&mut self, table: &str, columns: &[(&str, Value)]) -> Result<RowId> {
        let schema = self.store.table(table)?;
        self.op(|t| {
            let mut given: Vec<Option<Value>> = vec![None; schema.columns.len()];
            for (name, value) in columns {
                let i = schema.column_index(name).ok_or_else(|| Error::Invalid(format!("no column {table}.{name}")))?;
                if given[i].replace(value.clone()).is_some() {
                    return Err(Error::Invalid(format!("column {name} given twice")));
                }
            }
            let mut cells = Vec::with_capacity(schema.columns.len());
            for (column, value) in schema.columns.iter().zip(given) {
                let value = match (value, &column.default) {
                    (Some(v), _) => values::check(column, v)?,
                    (None, Some(default)) => {
                        let raw: SqlValue = t.store.conn.query_row(&format!("SELECT {default}"), [], |r| r.get(0))?;
                        values::from_expression(column, (&raw).into())?
                    }
                    (None, None) => values::check(column, Value::Null)?,
                };
                cells.push(Cell { stamp: t.stamp(), value });
            }
            let row_id = RowId::new(t.txn.node, t.next_row);
            t.next_row = t.next_row.checked_add(1).ok_or_else(|| Error::Invalid("row counter exhausted".into()))?;
            let row = Row { row_id, tombstone: false, cells };
            write_row(&t.store.conn, &schema, &row)?;
            t.check_row(&schema, row_id)?;
            t.check_parents(&schema, &row, None)?;
            t.log(Op::Insert, &schema, row_id, None, Some(row_values(&row)))?;
            Ok(row_id)
        })
    }

    /// Changes columns of a live row.
    pub fn update(&mut self, table: &str, row_id: RowId, changes: &[(&str, Value)]) -> Result<()> {
        let schema = self.store.table(table)?;
        self.op(|t| t.update_inner(&schema, row_id, changes))
    }

    fn update_inner(&mut self, schema: &TableSchema, row_id: RowId, changes: &[(&str, Value)]) -> Result<()> {
        let before = self.live_row(schema, row_id)?;
        let mut after = before.clone();
        for (name, value) in changes {
            let i =
                schema.column_index(name).ok_or_else(|| Error::Invalid(format!("no column {}.{name}", schema.name)))?;
            let value = values::check(&schema.columns[i], value.clone())?;
            if value != before.cells[i].value {
                after.cells[i] = Cell { stamp: self.stamp(), value };
            }
        }
        write_row(&self.store.conn, schema, &after)?;
        self.check_row(schema, row_id)?;
        self.check_parents(schema, &after, Some(&before))?;
        self.check_children(schema, &before, Some(&after))?;
        self.log(Op::Update, schema, row_id, Some(row_values(&before)), Some(row_values(&after)))
    }

    /// Deletes a live row: it becomes a tombstone. Children through an
    /// `ON DELETE CASCADE` foreign key are deleted too; other live children
    /// make the delete fail.
    pub fn delete(&mut self, table: &str, row_id: RowId) -> Result<()> {
        let schema = self.store.table(table)?;
        self.op(|t| t.delete_inner(&schema, row_id))
    }

    fn delete_inner(&mut self, schema: &TableSchema, row_id: RowId) -> Result<()> {
        let before = self.live_row(schema, row_id)?;
        let mut after = before.clone();
        after.tombstone = true;
        write_row(&self.store.conn, schema, &after)?;
        self.check_children(schema, &before, None)?;
        self.log(Op::Delete, schema, row_id, Some(row_values(&before)), None)
    }

    /// Updates every live row that matches a SQL condition on its columns.
    /// Returns the row IDs, in row ID order.
    pub fn update_where(
        &mut self,
        table: &str,
        condition: &str,
        params: &[Value],
        changes: &[(&str, Value)],
    ) -> Result<Vec<RowId>> {
        let ids = self.find(table, condition, params)?;
        for &id in &ids {
            self.update(table, id, changes)?;
        }
        Ok(ids)
    }

    /// Deletes every live row that matches a SQL condition on its columns.
    /// Returns the row IDs, in row ID order.
    pub fn delete_where(&mut self, table: &str, condition: &str, params: &[Value]) -> Result<Vec<RowId>> {
        let ids = self.find(table, condition, params)?;
        for &id in &ids {
            // A cascade from an earlier delete can have deleted it.
            if self.get(table, id)?.is_some() {
                self.delete(table, id)?;
            }
        }
        Ok(ids)
    }

    /// Returns the IDs of the live rows that match a SQL condition.
    pub fn find(&self, table: &str, condition: &str, params: &[Value]) -> Result<Vec<RowId>> {
        let schema = self.store.table(table)?;
        let sql = format!(
            "SELECT \"_row_id\" FROM \"{}\" WHERE \"_tomb\" = 0 AND ({condition}) ORDER BY \"_row_id\"",
            schema.physical()
        );
        let mut stmt = self.store.conn.prepare(&sql)?;
        if !stmt.readonly() {
            return Err(Error::Invalid("the condition must not write".into()));
        }
        let params: Vec<SqlValue> = params.iter().map(values::to_sql).collect();
        let ids: Vec<Vec<u8>> =
            stmt.query_map(rusqlite::params_from_iter(params), |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        ids.iter().map(|b| parse_row_id(b)).collect()
    }

    /// Writes a row in its replicated form, as it is: row ID, tombstone
    /// flag, stamps, and values. This is for the merge and for imports: it
    /// writes **no log entry** and checks no constraints other than column
    /// types. A local change must use [`insert`](Txn::insert),
    /// [`update`](Txn::update), or [`delete`](Txn::delete).
    pub fn put_row(&mut self, table: &str, row: &Row) -> Result<()> {
        let schema = self.store.table(table)?;
        self.op(|t| {
            if row.cells.len() != schema.columns.len() {
                return Err(Error::Invalid(format!(
                    "row has {} cells, table {} has {} columns",
                    row.cells.len(),
                    schema.name,
                    schema.columns.len()
                )));
            }
            for (cell, column) in row.cells.iter().zip(&schema.columns) {
                // The row must be stored exactly as given, so a value that
                // the type check would change is an error.
                if values::check(column, cell.value.clone())? != cell.value {
                    return Err(Error::Constraint(format!(
                        "{:?} is not a value of column {}",
                        cell.value, column.name
                    )));
                }
            }
            write_row(&t.store.conn, &schema, row)?;
            t.store.conn.execute(
                "INSERT OR REPLACE INTO _synod_dirty (row_id, tbl) VALUES (?1, ?2)",
                params![row.row_id.to_be_bytes().to_vec(), schema.id],
            )?;
            // Never give out an ID that an imported row of this node holds.
            if row.row_id.node() == t.txn.node && row.row_id.counter() >= t.next_row {
                t.next_row = row.row_id.counter().checked_add(1).ok_or_else(|| Error::Invalid("row counter".into()))?;
            }
            Ok(())
        })
    }

    /// Returns the values of a live row, with the writes of this
    /// transaction.
    pub fn get(&self, table: &str, row_id: RowId) -> Result<Option<Vec<Value>>> {
        let schema = self.store.table(table)?;
        Ok(read_row(&self.store.conn, &schema, row_id)?.filter(|r| !r.tombstone).map(|r| row_values(&r)))
    }

    /// Commits the transaction.
    pub fn commit(mut self) -> Result<Commit> {
        self.store.conn.execute(
            "UPDATE _synod_node SET next_row = ?1, next_txn = ?2, hlc_last = max(hlc_last, ?3)",
            params![self.next_row as i64, (self.txn.counter + 1) as i64, self.hlc.as_u64() as i64],
        )?;
        self.store.conn.execute_batch("COMMIT")?;
        self.done = true;
        Ok(Commit { txn: self.txn, hlc: self.hlc, epoch: self.epoch })
    }

    fn live_row(&self, schema: &TableSchema, row_id: RowId) -> Result<Row> {
        read_row(&self.store.conn, schema, row_id)?
            .filter(|r| !r.tombstone)
            .ok_or_else(|| Error::Invalid(format!("no live row {row_id} in {}", schema.name)))
    }

    fn log(
        &mut self,
        op: Op,
        schema: &TableSchema,
        row_id: RowId,
        before: Option<Vec<Value>>,
        after: Option<Vec<Value>>,
    ) -> Result<()> {
        let entry = LogEntry { op, table: schema.id, row_id, txn: self.txn, hlc: self.hlc, before, after };
        self.store
            .conn
            .prepare_cached("INSERT INTO _synod_log (epoch, tbl, row_id, entry) VALUES (?1, ?2, ?3, ?4)")?
            .execute(params![self.epoch as i64, schema.id, row_id.to_be_bytes().to_vec(), entry.encode()])?;
        Ok(())
    }

    // CHECK constraints, on the row as stored. A CHECK fails only if its
    // expression is false; NULL passes, as in SQL.
    fn check_row(&self, schema: &TableSchema, row_id: RowId) -> Result<()> {
        for check in &schema.checks {
            let sql =
                format!("SELECT count(*) FROM \"{}\" WHERE \"_row_id\" = ?1 AND NOT ({check})", schema.physical());
            let failed: i64 = self.store.conn.query_row(&sql, [row_id.to_be_bytes().to_vec()], |r| r.get(0))?;
            if failed > 0 {
                return Err(Error::Constraint(format!("CHECK ({check}) failed on {}", schema.name)));
            }
        }
        Ok(())
    }

    // Foreign keys of this row: each non-NULL key must match a live parent.
    fn check_parents(&self, schema: &TableSchema, row: &Row, before: Option<&Row>) -> Result<()> {
        for fk in &schema.foreign_keys {
            let value = &row.cells[fk.column].value;
            if matches!(value, Value::Null) || before.is_some_and(|b| &b.cells[fk.column].value == value) {
                continue;
            }
            let parent = self.store.table(&fk.parent)?;
            let sql = format!(
                "SELECT count(*) FROM \"{}\" WHERE \"_tomb\" = 0 AND \"{}\" = ?1",
                parent.physical(),
                fk.parent_column
            );
            let found: i64 = self.store.conn.query_row(&sql, [values::to_sql(value)], |r| r.get(0))?;
            if found == 0 {
                return Err(Error::Constraint(format!(
                    "{}.{} = {value:?} has no parent in {}",
                    schema.name, schema.columns[fk.column].name, fk.parent
                )));
            }
        }
        Ok(())
    }

    // Foreign keys that point to this row: a removed or changed key value
    // must not leave live children. With ON DELETE CASCADE, a delete
    // deletes the children.
    fn check_children(&mut self, schema: &TableSchema, before: &Row, after: Option<&Row>) -> Result<()> {
        let tables = self.store.tables.clone();
        for child in &tables {
            for fk in &child.foreign_keys {
                if !fk.parent.eq_ignore_ascii_case(&schema.name) {
                    continue;
                }
                let col = schema.column_index(&fk.parent_column).expect("checked at create_table");
                let old = &before.cells[col].value;
                if matches!(old, Value::Null) || after.is_some_and(|a| &a.cells[col].value == old) {
                    continue;
                }
                let sql = format!(
                    "SELECT \"_row_id\" FROM \"{}\" WHERE \"_tomb\" = 0 AND \"{}\" = ?1 ORDER BY \"_row_id\"",
                    child.physical(),
                    child.columns[fk.column].name
                );
                let ids: Vec<Vec<u8>> = {
                    let mut stmt = self.store.conn.prepare(&sql)?;
                    stmt.query_map([values::to_sql(old)], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?
                };
                if ids.is_empty() {
                    continue;
                }
                if fk.cascade && after.is_none() {
                    for id in ids {
                        let id = parse_row_id(&id)?;
                        if self.get(&child.name, id)?.is_some() {
                            self.delete_inner(child, id)?;
                        }
                    }
                } else {
                    return Err(Error::Constraint(format!(
                        "{} rows of {} still refer to {}.{} = {old:?}",
                        ids.len(),
                        child.name,
                        schema.name,
                        fk.parent_column
                    )));
                }
            }
        }
        Ok(())
    }
}

impl Drop for Txn<'_> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.store.conn.execute_batch("ROLLBACK");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_encoding_round_trips() {
        let s = |ms: u64, n: u32| Stamp { hlc: Hlc::new(ms, 0).unwrap(), node: NodeId(n) };
        for stamps in [
            vec![],
            vec![s(1, 1); 6],
            vec![s(1, 1), s(2, 1), s(1, 1), s(1, 2)],
            (0..300).map(|i| s(i, 1)).collect::<Vec<_>>(),
        ] {
            let bytes = encode_stamps(&stamps);
            assert_eq!(decode_stamps(&bytes, stamps.len()), Some(stamps.clone()));
            assert_eq!(decode_stamps(&bytes, stamps.len() + 1), None);
        }
        assert_eq!(encode_stamps(&[s(1, 1); 6]).len(), 2 + 12 + 6);
    }
}
