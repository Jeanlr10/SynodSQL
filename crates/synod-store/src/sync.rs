//! Export, merge, and the local rebase (white paper, Sections V and XI).

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};
use synod_core::log::{LogHeader, decode_log, log_hash};
use synod_core::prolly::NodeHash;
use synod_core::{LogEntry, NodeId, Row, RowId, Stamp, Value};

use crate::error::{Error, Result};
use crate::merge::{BaseState, Conflict, LogInput, MERGE_NODE, MergeInput, failed_checks, merge};
use crate::schema::{TableSchema, schema_hash};
use crate::store::{Store, StoreConfig, hlc_at, mark_dirty, parse_row_id, read_row, record_undo, write_row};
use crate::values;

/// What a merge did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeReport {
    /// The merged epoch.
    pub epoch: u64,
    /// Root hash of M(e). Every node that merged the same logs has it.
    pub root: NodeHash,
    /// Hashes of the merged logs, sorted: the manifest.
    pub logs: Vec<[u8; 32]>,
    /// Conflicts recorded in `_conflicts`.
    pub conflicts: Vec<Conflict>,
    /// Local writes of later epochs that were applied again on top of M(e).
    pub rebased: usize,
}

/// The base state as the physical tables of a store hold it.
struct BaseView<'a> {
    conn: &'a Connection,
}

impl BaseState for BaseView<'_> {
    fn row(&self, table: &TableSchema, id: RowId) -> Result<Option<Row>> {
        read_row(self.conn, table, id)
    }

    fn find_live(&self, table: &TableSchema, columns: &[usize], key: &[Value]) -> Result<Vec<RowId>> {
        let cond: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, &c)| format!(" AND \"{}\" = ?{}", table.columns[c].name, i + 1))
            .collect();
        let sql = format!(
            "SELECT \"_row_id\" FROM \"{}\" WHERE \"_tomb\" = 0{} ORDER BY \"_row_id\"",
            table.physical(),
            cond.concat()
        );
        let args: Vec<_> = key.iter().map(values::to_sql).collect();
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let ids: Vec<Vec<u8>> =
            stmt.query_map(rusqlite::params_from_iter(args), |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        ids.iter().map(|b| parse_row_id(b)).collect()
    }

    fn failed_checks(&self, table: &TableSchema, row: &Row) -> Result<Vec<usize>> {
        failed_checks(self.conn, table, row)
    }
}

impl Store {
    /// Returns the sealed log of an epoch, to send to the other nodes.
    pub fn export(&self, epoch: u64) -> Result<Vec<u8>> {
        Ok(self.sealed(epoch)?.ok_or_else(|| Error::Invalid(format!("epoch {epoch} is not sealed")))?.bytes)
    }

    /// Returns the last merged epoch, if any.
    pub fn merged_epoch(&self) -> Result<Option<u64>> {
        let e: Option<i64> = self.conn.query_row("SELECT merged_epoch FROM _synod_node", [], |r| r.get(0))?;
        Ok(e.map(|e| e as u64))
    }

    /// Returns the epoch whose merge took the log with this hash, if any.
    pub fn log_merged_in(&self, hash: &[u8; 32]) -> Result<Option<u64>> {
        let e: Option<i64> = self
            .conn
            .query_row("SELECT merged_in FROM _synod_merged_log WHERE log_hash = ?1", [hash.to_vec()], |r| r.get(0))
            .optional()?;
        Ok(e.map(|e| e as u64))
    }

    /// Returns the epoch that the next merge must merge.
    pub fn next_merge_epoch(&self) -> Result<u64> {
        let (first, merged): (i64, Option<i64>) =
            self.conn
                .query_row("SELECT first_epoch, merged_epoch FROM _synod_node", [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(merged.map_or(first as u64, |m| m as u64 + 1))
    }

    /// Merges epoch `epoch`: this node's sealed log of the epoch and
    /// `remote_logs`, the sealed logs of the other nodes (in any order). A
    /// log of an earlier epoch that no merge has taken yet is a late log.
    ///
    /// The merge writes M(e) to a new file, applies this node's later
    /// writes again on top of it, and switches to the file (specification
    /// Section 5.3). Every node that merges the same logs gets the same
    /// M(e), the same conflicts, and the same root hash.
    ///
    /// If the merge fails before the switch, the store is unchanged. If the
    /// switch itself fails, reopen the store.
    pub fn merge(&mut self, epoch: u64, remote_logs: &[Vec<u8>]) -> Result<MergeReport> {
        let expected = self.next_merge_epoch()?;
        if epoch != expected {
            return Err(Error::Invalid(format!("the next epoch to merge is {expected}, not {epoch}")));
        }
        let own = self.sealed(epoch)?.ok_or_else(|| Error::Invalid(format!("seal epoch {epoch} first")))?;
        let schema = schema_hash(self.tables.iter().map(|t| &**t));
        let base_root = self.base_root()?;
        let me = self.config.node;
        let mut logs: BTreeMap<[u8; 32], (LogHeader, Vec<LogEntry>)> = BTreeMap::new();
        for bytes in std::iter::once(&own.bytes).chain(remote_logs) {
            let hash = log_hash(bytes);
            let (header, entries) = decode_log(bytes).map_err(|e| Error::Invalid(format!("log: {e}")))?;
            let who = format!("log of node {} for epoch {}", header.node, header.epoch);
            if header.schema_hash != schema {
                return Err(Error::Invalid(format!("{who} has another schema")));
            }
            if header.epoch > epoch {
                return Err(Error::Invalid(format!("{who} is for a later epoch")));
            }
            if header.epoch == epoch && header.base_root != base_root {
                return Err(Error::Invalid(format!("{who} was written on another base state")));
            }
            if (header.node == me && hash != own.hash) || header.node == MERGE_NODE {
                return Err(Error::Invalid(format!("{who} has this node's ID")));
            }
            if let Some(e) = self.log_merged_in(&hash)? {
                return Err(Error::Invalid(format!("{who} was already merged in epoch {e}")));
            }
            logs.insert(hash, (header, entries));
        }

        let next = format!("{}.next", self.path);
        if self.env.fs.exists(&next)? {
            self.env.fs.remove(&next)?;
        }
        self.conn.execute("VACUUM INTO ?1", [&next])?;
        let built = Store::open(&next, self.config.clone(), self.env.clone()).and_then(|mut n| {
            let report = n.merge_here(epoch, &logs, base_root)?;
            n.conn.close().map_err(|(_, e)| Error::Sql(e))?;
            Ok(report)
        });
        match built {
            Ok(report) => {
                self.switch_to(&next)?;
                Ok(report)
            }
            Err(e) => {
                let _ = self.env.fs.remove(&next);
                Err(e)
            }
        }
    }

    // Runs in the copy: restore M(e−1), merge, rebase.
    fn merge_here(
        &mut self,
        epoch: u64,
        logs: &BTreeMap<[u8; 32], (LogHeader, Vec<LogEntry>)>,
        base_root: NodeHash,
    ) -> Result<MergeReport> {
        let me = self.config.node;
        let conflict_stamp = Stamp { hlc: hlc_at(self.epoch_start_ms(epoch + 1))?, node: MERGE_NODE };
        self.in_txn(|s| {
            let indexes = s.drop_unique_indexes()?;
            s.restore_base(base_root)?;

            // 2. Merge.
            let tables = s.tables.clone();
            let input = MergeInput {
                tables: &tables,
                epoch,
                logs: logs
                    .values()
                    .map(|(h, entries)| LogInput { node: h.node, epoch: h.epoch, entries: entries.clone() })
                    .collect(),
                conflict_stamp,
                record_conflicts: true,
            };
            let out = merge(&BaseView { conn: &s.conn }, &input)?;
            for (id, (t, row)) in &out.rows {
                write_row(&s.conn, &tables[*t as usize], row)?;
                mark_dirty(&s.conn, *t, *id)?;
            }
            let root = s.refresh_state_hash()?;
            let manifest: Vec<u8> = logs.keys().flatten().copied().collect();
            s.conn.execute(
                "UPDATE _synod_node SET merged_epoch = ?1, base_root = ?2",
                params![epoch as i64, root.to_vec()],
            )?;
            s.conn.execute(
                "INSERT INTO _synod_merged (epoch, manifest, root, conflicts) VALUES (?1, ?2, ?3, ?4)",
                params![epoch as i64, manifest, root.to_vec(), out.conflicts.len() as i64],
            )?;
            for (hash, (h, _)) in logs {
                s.conn.execute(
                    "INSERT INTO _synod_merged_log (log_hash, merged_in, node, log_epoch) VALUES (?1, ?2, ?3, ?4)",
                    params![hash.to_vec(), epoch as i64, h.node.0, h.epoch as i64],
                )?;
            }

            // 3. Rebase: this node's writes of later epochs, applied again on
            // top of M(e) by the same rules as late writes. Conflicts are not
            // recorded here; the merge of their own epoch records them.
            let inflight: Vec<LogEntry> = {
                let mut stmt = s.conn.prepare("SELECT entry FROM _synod_log WHERE epoch > ?1 ORDER BY seq")?;
                let rows = stmt.query_map([epoch as i64], |r| r.get::<_, Vec<u8>>(0))?;
                rows.map(|b| LogEntry::decode(&b?).map_err(|e| Error::Corrupt(format!("log entry: {e}"))))
                    .collect::<Result<_>>()?
            };
            let rebased = inflight.len();
            if !inflight.is_empty() {
                let input = MergeInput {
                    tables: &tables,
                    epoch: epoch + 1,
                    logs: vec![LogInput { node: me, epoch: epoch + 1, entries: inflight }],
                    conflict_stamp,
                    record_conflicts: false,
                };
                let local = merge(&BaseView { conn: &s.conn }, &input)?;
                for (id, (t, row)) in &local.rows {
                    let schema = &tables[*t as usize];
                    let base = read_row(&s.conn, schema, *id)?;
                    record_undo(&s.conn, *t, *id, base.as_ref())?;
                    write_row(&s.conn, schema, row)?;
                    mark_dirty(&s.conn, *t, *id)?;
                }
            }

            s.create_indexes(&indexes)?;
            s.refresh_state_hash()?;
            Ok(MergeReport { epoch, root, logs: logs.keys().copied().collect(), conflicts: out.conflicts, rebased })
        })
    }

    // Drops the unique indexes and returns them. Rows are then written one
    // by one, and a key can move between rows; the indexes come back at the
    // end with `create_indexes`.
    fn drop_unique_indexes(&mut self) -> Result<Vec<(String, String)>> {
        let indexes: Vec<(String, String)> = {
            let mut stmt = self.conn.prepare(
                "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND name LIKE '\\_synod\\_u\\_%' ESCAPE '\\' ORDER BY name",
            )?;
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?
        };
        for (name, _) in &indexes {
            self.conn.execute(&format!("DROP INDEX \"{name}\""), [])?;
        }
        Ok(indexes)
    }

    fn create_indexes(&mut self, indexes: &[(String, String)]) -> Result<()> {
        for (name, sql) in indexes {
            self.conn
                .execute(sql, [])
                .map_err(|e| Error::Corrupt(format!("unique index {name} cannot be rebuilt: {e}")))?;
        }
        Ok(())
    }

    // Writes back the base-state version of every row changed since the
    // last merge, and checks that the result has the base root hash.
    fn restore_base(&mut self, base_root: NodeHash) -> Result<()> {
        let undo: Vec<(Vec<u8>, u32, Option<Vec<u8>>)> = {
            let mut stmt = self.conn.prepare("SELECT row_id, tbl, row FROM _synod_undo ORDER BY row_id")?;
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<rusqlite::Result<_>>()?
        };
        for (id, tbl, row) in undo {
            let schema = self.tables[tbl as usize].clone();
            let row_id = parse_row_id(&id)?;
            match row {
                None => {
                    self.conn
                        .execute(&format!("DELETE FROM \"{}\" WHERE \"_row_id\" = ?1", schema.physical()), [id])?;
                }
                Some(bytes) => {
                    let row =
                        Row::from_canonical_bytes(&bytes).map_err(|e| Error::Corrupt(format!("undo row: {e}")))?;
                    write_row(&self.conn, &schema, &row)?;
                }
            }
            mark_dirty(&self.conn, tbl, row_id)?;
        }
        self.conn.execute("DELETE FROM _synod_undo", [])?;
        if self.refresh_state_hash()? != base_root {
            return Err(Error::Corrupt("the restored base state does not have the base root hash".into()));
        }
        Ok(())
    }

    /// Writes a copy of this node's base state M(e−1), without its local
    /// writes since the last merge, to `path`, as the store of a new node
    /// `node`. The new node starts at the next epoch to merge. Use it to
    /// add a node: all nodes must merge from the same base.
    pub fn clone_base(&self, path: &str, node: NodeId) -> Result<Store> {
        if node == self.config.node || node == MERGE_NODE {
            return Err(Error::Invalid(format!("node ID {node} cannot be used for a clone")));
        }
        if self.env.fs.exists(path)? {
            return Err(Error::Invalid(format!("{path} exists")));
        }
        let base_root = self.base_root()?;
        let next_epoch = self.next_merge_epoch()?;
        self.conn.execute("VACUUM INTO ?1", [path])?;
        {
            let mut copy = Store::open(path, self.config.clone(), self.env.clone())?;
            copy.in_txn(|s| {
                let indexes = s.drop_unique_indexes()?;
                s.restore_base(base_root)?;
                s.create_indexes(&indexes)?;
                // The copy has no log of its own. Log sequence numbers start
                // again, so the state hash cursor starts again too.
                s.conn.execute_batch("DELETE FROM _synod_log; DELETE FROM _synod_sealed; DELETE FROM _synod_dirty;")?;
                s.conn.execute(
                    "UPDATE _synod_node SET node = ?1, next_row = 0, next_txn = 0, hashed_seq = 0, open_epoch = ?2, first_epoch = ?2",
                    params![node.0, next_epoch as i64],
                )?;
                Ok(())
            })?;
            copy.conn.close().map_err(|(_, e)| Error::Sql(e))?;
        }
        self.env.fs.sync_file(path)?;
        self.env.fs.sync_dir(path)?;
        Store::open(path, StoreConfig { node, ..self.config.clone() }, self.env.clone())
    }

    /// Returns the conflicts in `_conflicts` (live rows), with their row IDs
    /// and status, in row ID order.
    pub fn conflicts(&self) -> Result<Vec<(RowId, Conflict, String)>> {
        let schema = self.tables[0].clone();
        let mut stmt = self.conn.prepare(&format!(
            "SELECT \"_row_id\" FROM \"{}\" WHERE \"_tomb\" = 0 ORDER BY \"_row_id\"",
            schema.physical()
        ))?;
        let ids: Vec<Vec<u8>> = stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        let mut out = Vec::new();
        for id in ids {
            let row_id = parse_row_id(&id)?;
            let row = read_row(&self.conn, &schema, row_id)?.expect("row just listed");
            let values: Vec<Value> = row.cells.into_iter().map(|c| c.value).collect();
            let (mut conflict, status) = Conflict::from_values(&values)?;
            if let Ok(t) = self.table(&conflict.table_name) {
                conflict.table = t.id;
                conflict.column = conflict.column_name.as_deref().and_then(|c| t.column_index(c)).map(|c| c as u16);
            }
            out.push((row_id, conflict, status));
        }
        Ok(out)
    }

    /// Closes the store, replaces its file with `next` (synced first), and
    /// opens it again (specification Section 5.3).
    pub(crate) fn switch_to(&mut self, next: &str) -> Result<()> {
        let env = self.env.clone();
        env.fs.sync_file(next)?;
        let conn = std::mem::replace(&mut self.conn, Connection::open_in_memory()?);
        conn.close().map_err(|(_, e)| Error::Sql(e))?;
        // A clean close checkpoints and deletes the WAL. A WAL that is left
        // would be replayed onto the new file, so stop here.
        let wal = format!("{}-wal", self.path);
        if env.fs.exists(&wal)? {
            return Err(Error::Invalid(format!("{wal} still exists after close")));
        }
        // Make the WAL delete durable before the rename.
        env.fs.sync_dir(&self.path)?;
        env.fs.rename(next, &self.path)?;
        env.fs.sync_dir(&self.path)?;
        *self = Store::open(&self.path, self.config.clone(), env)?;
        Ok(())
    }
}
