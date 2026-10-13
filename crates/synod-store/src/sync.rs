//! Logs, merges, unmerges, catch-up, and the local rebase (white paper,
//! Sections V and XI; specification Sections 6 and 7).
//!
//! Every change of the base state runs in [`Store::rebuild`], in one SQLite
//! transaction: it restores and checks M(e−1), changes the base, checks
//! the new base root, indexes the new base for diffs, and rebases the local
//! writes. The commit is the atomic switch.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use rusqlite::{Connection, OptionalExtension, params};
use synod_core::log::{decode_log, log_hash};
use synod_core::manifest::{LogRef, Manifest};
use synod_core::prolly::NodeHash;
use synod_core::{LogEntry, NodeId, Row, RowHash, RowId, Stamp, Value};

use crate::error::{Error, Result};
use crate::merge::{
    BaseState, Conflict, ConflictKind, LogInput, MERGE_NODE, MergeInput, Recorded, encode_recorded, failed_checks,
    given_up_id, merge, validate_log,
};
use crate::schema::{TableSchema, schema_hash};
use crate::store::{
    Store, StoreConfig, Txn, WrittenHashes, hlc_at, mark_dirty, parse_row_id, read_row, record_undo, write_row,
};
use crate::values;

/// What a merge did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeReport {
    /// The merged epoch.
    pub epoch: u64,
    /// Root hash of M(e). Every node that merged the same manifest on the
    /// same base has it.
    pub root: NodeHash,
    /// Hash of the manifest.
    pub manifest_hash: [u8; 32],
    /// Hashes of the merged logs, sorted.
    pub logs: Vec<[u8; 32]>,
    /// Conflicts recorded in `_conflicts`.
    pub conflicts: Vec<Conflict>,
    /// Rows removed by garbage collection.
    pub removed: usize,
    /// Local writes not in this merge that were applied again on top of
    /// M(e).
    pub rebased: usize,
}

/// One merge in a node's history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergedRecord {
    /// Epoch.
    pub epoch: u64,
    /// The manifest.
    pub manifest: Manifest,
    /// The manifest's hash.
    pub hash: [u8; 32],
    /// Root hash before the merge, M(e−1).
    pub base_before: NodeHash,
    /// Root hash after the merge, M(e).
    pub root: NodeHash,
    /// True if this node made the merge and keeps what it needs to undo
    /// it; false for a merge taken over from a peer.
    pub undoable: bool,
}

/// A log that a merge took.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MergedLog {
    /// Log hash.
    pub hash: [u8; 32],
    /// The epoch whose merge took it.
    pub merged_in: u64,
    /// Node that wrote it.
    pub node: NodeId,
    /// Its epoch.
    pub log_epoch: u64,
}

/// What a peer sends, beside the rows, so that a node can take over its
/// base state (catch-up and repair).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaseImage {
    /// The peer's last merged epoch.
    pub epoch: Option<u64>,
    /// Root hash of the peer's base state.
    pub root: NodeHash,
    /// The peer's merge history for the last L epochs.
    pub merged: Vec<MergedRecord>,
    /// The logs that those merges took.
    pub merged_logs: Vec<MergedLog>,
}

/// A row of the base state with its table ID, or `None` if the base
/// state does not have it.
pub type BaseRow = Option<(u32, Row)>;

/// The base state as the physical tables of a store hold it (inside a
/// rebuild, after the restore).
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

    fn gc_candidates(&self, tables: &[std::sync::Arc<TableSchema>], epoch: u64) -> Result<Vec<(u32, RowId)>> {
        let mut out = Vec::new();
        for t in tables {
            let mut sql =
                format!("SELECT \"_row_id\" FROM \"{}\" WHERE \"_tomb\" > 0 AND \"_tomb\" <= ?1", t.physical());
            if t.id == 0 {
                sql.push_str(&format!(
                    " UNION ALL SELECT \"_row_id\" FROM \"{}\" WHERE \"_tomb\" = 0 AND \"status\" = 'resolved' AND \"epoch\" <= ?2",
                    t.physical()
                ));
            }
            let mut stmt = self.conn.prepare_cached(&sql)?;
            let args: &[&dyn rusqlite::ToSql] =
                if t.id == 0 { &[&(epoch as i64 + 1), &(epoch as i64)] } else { &[&(epoch as i64 + 1)] };
            let ids: Vec<Vec<u8>> = stmt.query_map(args, |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
            for id in ids {
                out.push((t.id, parse_row_id(&id)?));
            }
        }
        out.sort_by_key(|(_, id)| *id);
        Ok(out)
    }
}

fn hash32(v: Vec<u8>, what: &str) -> Result<[u8; 32]> {
    v.try_into().map_err(|_| Error::Corrupt(format!("{what} is not 32 bytes")))
}

impl Store {
    // ------------------------------------------------------------ logs

    /// Returns the sealed log of an epoch, to send to the other nodes.
    pub fn export(&self, epoch: u64) -> Result<Vec<u8>> {
        Ok(self.sealed(epoch)?.ok_or_else(|| Error::Invalid(format!("epoch {epoch} is not sealed")))?.bytes)
    }

    /// Keeps a sealed log from another node, so that a merge can take it.
    /// Checks its format, schema, and entries. Keeping a log twice does
    /// nothing.
    pub fn add_log(&mut self, bytes: &[u8]) -> Result<LogRef> {
        let hash = log_hash(bytes);
        let known: Option<(u32, i64)> = self
            .conn
            .prepare_cached("SELECT node, epoch FROM _synod_logs WHERE log_hash = ?1")?
            .query_row([hash.to_vec()], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        if let Some((node, epoch)) = known {
            return Ok(LogRef { node: NodeId(node), epoch: epoch as u64, hash });
        }
        let (h, entries) = decode_log(bytes).map_err(|e| Error::Invalid(format!("log: {e}")))?;
        let who = format!("log of node {} for epoch {}", h.node, h.epoch);
        if h.schema_hash != schema_hash(self.tables.iter().map(|t| &**t)) {
            return Err(Error::Invalid(format!("{who} has another schema")));
        }
        if h.node == MERGE_NODE {
            return Err(Error::Invalid(format!("{who} has the merge node's ID")));
        }
        if h.node == self.config.node && self.sealed(h.epoch)?.is_none_or(|s| s.hash != hash) {
            return Err(Error::Invalid(format!("{who} has this node's ID but is not its log")));
        }
        validate_log(&self.tables, &LogInput { node: h.node, epoch: h.epoch, entries })?;
        self.conn
            .prepare_cached("INSERT OR IGNORE INTO _synod_logs (log_hash, node, epoch, log) VALUES (?1, ?2, ?3, ?4)")?
            .execute(params![hash.to_vec(), h.node.0, h.epoch as i64, bytes])?;
        Ok(LogRef { node: h.node, epoch: h.epoch, hash })
    }

    /// Returns this node's own logs of epochs `from` and later that are
    /// not given up, by epoch, with true if a merge took the log.
    pub fn own_logs(&self, from: u64) -> Result<Vec<(LogRef, bool)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT l.epoch, l.log_hash, m.log_hash IS NOT NULL FROM _synod_logs l
             LEFT JOIN _synod_merged_log m ON m.log_hash = l.log_hash
             WHERE l.node = ?1 AND l.epoch >= ?2 AND l.abandoned = 0 ORDER BY l.epoch",
        )?;
        let me = self.config.node;
        let rows = stmt.query_map(params![me.0, from as i64], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, bool>(2)?))
        })?;
        rows.map(|r| {
            let (epoch, hash, merged) = r?;
            Ok((LogRef { node: me, epoch: epoch as u64, hash: hash32(hash, "log hash")? }, merged))
        })
        .collect()
    }

    /// Returns the number of nodes whose log of `epoch` this node keeps.
    pub fn log_nodes(&self, epoch: u64) -> Result<usize> {
        let n: i64 = self
            .conn
            .prepare_cached("SELECT count(DISTINCT node) FROM _synod_logs WHERE epoch = ?1 AND abandoned = 0")?
            .query_row([epoch as i64], |r| r.get(0))?;
        Ok(n as usize)
    }

    /// True if this node keeps the log with this hash.
    pub fn has_log(&self, hash: &[u8; 32]) -> Result<bool> {
        Ok(self
            .conn
            .prepare_cached("SELECT 1 FROM _synod_logs WHERE log_hash = ?1")?
            .query_row([hash.to_vec()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// Returns a log that this node keeps.
    pub fn log(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .conn
            .query_row("SELECT log FROM _synod_logs WHERE log_hash = ?1", [hash.to_vec()], |r| r.get(0))
            .optional()?)
    }

    /// Returns the logs that this node keeps, of epochs `from` and later,
    /// sorted by (node, epoch), with true if a merge took the log.
    pub fn known_logs(&self, from: u64) -> Result<Vec<(LogRef, bool)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT l.node, l.epoch, l.log_hash, m.log_hash IS NOT NULL FROM _synod_logs l
             LEFT JOIN _synod_merged_log m ON m.log_hash = l.log_hash
             WHERE l.epoch >= ?1 AND l.abandoned = 0 ORDER BY l.node, l.epoch",
        )?;
        let rows = stmt.query_map([from as i64], |r| {
            Ok((r.get::<_, u32>(0)?, r.get::<_, i64>(1)?, r.get::<_, Vec<u8>>(2)?, r.get::<_, bool>(3)?))
        })?;
        rows.map(|r| {
            let (node, epoch, hash, merged) = r?;
            Ok((LogRef { node: NodeId(node), epoch: epoch as u64, hash: hash32(hash, "log hash")? }, merged))
        })
        .collect()
    }

    /// Returns the epoch whose merge took the log with this hash, if any.
    pub fn log_merged_in(&self, hash: &[u8; 32]) -> Result<Option<u64>> {
        let e: Option<i64> = self
            .conn
            .query_row("SELECT merged_in FROM _synod_merged_log WHERE log_hash = ?1", [hash.to_vec()], |r| r.get(0))
            .optional()?;
        Ok(e.map(|e| e as u64))
    }

    // ------------------------------------------------------------ history

    /// Returns the last merged epoch, if any.
    pub fn merged_epoch(&self) -> Result<Option<u64>> {
        let e: Option<i64> =
            self.conn.prepare_cached("SELECT merged_epoch FROM _synod_node")?.query_row([], |r| r.get(0))?;
        Ok(e.map(|e| e as u64))
    }

    /// Returns the hash of the last merge's manifest, the parent of the
    /// next manifest: all zeros before the first merge.
    pub fn head(&self) -> Result<[u8; 32]> {
        let h: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT m.manifest_hash FROM _synod_node n JOIN _synod_merged m ON m.epoch = n.merged_epoch",
                [],
                |r| r.get(0),
            )
            .optional()?;
        match h {
            Some(h) => hash32(h, "manifest hash"),
            None if self.merged_epoch()?.is_none() => Ok([0; 32]),
            None => Err(Error::Corrupt("the last merge has no record".into())),
        }
    }

    /// Returns the epoch that the next merge must merge.
    pub fn next_merge_epoch(&self) -> Result<u64> {
        let (first, merged): (i64, Option<i64>) =
            self.conn
                .query_row("SELECT first_epoch, merged_epoch FROM _synod_node", [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(merged.map_or(first as u64, |m| m as u64 + 1))
    }

    /// Returns the merge of an epoch, if this node keeps it.
    pub fn merged_record(&self, epoch: u64) -> Result<Option<MergedRecord>> {
        Ok(self.merged_records()?.into_iter().find(|r| r.epoch == epoch))
    }

    /// Returns this node's merge history (the last L epochs), by epoch.
    pub fn merged_records(&self) -> Result<Vec<MergedRecord>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT epoch, manifest, base_before, root, undoable, manifest_hash FROM _synod_merged ORDER BY epoch",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, bool>(4)?,
                r.get::<_, Vec<u8>>(5)?,
            ))
        })?;
        rows.map(|r| {
            let (epoch, m, before, root, undoable, hash) = r?;
            Ok(MergedRecord {
                epoch: epoch as u64,
                manifest: Manifest::decode(&m).map_err(|e| Error::Corrupt(format!("manifest: {e}")))?,
                hash: hash32(hash, "manifest hash")?,
                base_before: hash32(before, "base root")?,
                root: hash32(root, "root")?,
                undoable,
            })
        })
        .collect()
    }

    fn merged_logs(&self) -> Result<Vec<MergedLog>> {
        let mut stmt = self
            .conn
            .prepare("SELECT log_hash, merged_in, node, log_epoch FROM _synod_merged_log ORDER BY log_hash")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?, r.get::<_, u32>(2)?, r.get::<_, i64>(3)?))
        })?;
        rows.map(|r| {
            let (h, m, n, e) = r?;
            Ok(MergedLog { hash: hash32(h, "log hash")?, merged_in: m as u64, node: NodeId(n), log_epoch: e as u64 })
        })
        .collect()
    }

    // ------------------------------------------------------------ merge

    /// Merges epoch `epoch` offline: this node's sealed log of the epoch
    /// and `remote_logs`, the sealed logs of the other nodes (in any order).
    /// A log of an earlier epoch that no merge has taken is a late log.
    /// Every node that merges the same logs gets the same M(e), the same
    /// conflicts, and the same root hash.
    pub fn merge(&mut self, epoch: u64, remote_logs: &[Vec<u8>]) -> Result<MergeReport> {
        let expected = self.next_merge_epoch()?;
        if epoch != expected {
            return Err(Error::Invalid(format!("the next epoch to merge is {expected}, not {epoch}")));
        }
        let own = self.sealed(epoch)?.ok_or_else(|| Error::Invalid(format!("seal epoch {epoch} first")))?;
        let mut refs = vec![LogRef { node: self.config.node, epoch, hash: own.hash }];
        for bytes in remote_logs {
            refs.push(self.add_log(bytes)?);
        }
        // The same manifest on every node: no leader, no garbage collection.
        let parent = self.head()?;
        self.merge_manifest(&Manifest::new(epoch, 0, MERGE_NODE, parent, None, refs))
    }

    /// Merges the next epoch with a manifest. Its parent must be this
    /// node's [`head`](Store::head), and every log of the manifest must be
    /// kept by this node ([`add_log`](Store::add_log)).
    ///
    /// If the merge fails before the switch, the store is unchanged. If the
    /// switch itself fails, reopen the store.
    pub fn merge_manifest(&mut self, m: &Manifest) -> Result<MergeReport> {
        let epoch = m.epoch;
        let expected = self.next_merge_epoch()?;
        if epoch != expected {
            return Err(Error::Invalid(format!("the next epoch to merge is {expected}, not {epoch}")));
        }
        if m.parent != self.head()? {
            return Err(Error::Invalid(format!("{m} does not follow this node's last merge")));
        }
        let lateness = self.config.lateness;
        let mut logs = Vec::new();
        for r in &m.logs {
            let who = format!("log of node {} for epoch {}", r.node, r.epoch);
            if r.epoch + lateness < epoch {
                return Err(Error::Invalid(format!("{who} is older than the lateness bound")));
            }
            if let Some(e) = self.log_merged_in(&r.hash)? {
                return Err(Error::Invalid(format!("{who} was already merged in epoch {e}")));
            }
            let bytes = self.log(&r.hash)?.ok_or_else(|| Error::Invalid(format!("{who} is missing")))?;
            let (h, entries) = decode_log(&bytes).map_err(|e| Error::Corrupt(format!("kept log: {e}")))?;
            if (h.node, h.epoch) != (r.node, r.epoch) {
                return Err(Error::Invalid(format!("{who} does not match its log")));
            }
            logs.push(LogInput { node: h.node, epoch: h.epoch, entries });
        }
        let conflict_stamp = Stamp { hlc: hlc_at(self.epoch_start_ms(epoch + 1))?, node: MERGE_NODE };
        let manifest_hash = m.hash();
        let manifest = m.clone();
        self.rebuild(move |s| {
            let base_before = s.base_root()?;
            let tables = s.tables.clone();
            let input = MergeInput {
                tables: &tables,
                epoch,
                logs,
                conflict_stamp,
                record_conflicts: true,
                gc_through: manifest.gc_through,
            };
            let out = merge(&BaseView { conn: &s.conn }, &input)?;
            // Keep M(e−1) of every changed row, to unmerge.
            for (id, t) in out.rows.iter().map(|(id, (t, _))| (id, t)).chain(out.removed.iter()) {
                let before = read_row(&s.conn, &tables[*t as usize], *id)?;
                s.conn.prepare_cached("INSERT OR REPLACE INTO _synod_merge_undo (epoch, row_id, tbl, row) VALUES (?1, ?2, ?3, ?4)")?.execute(params![epoch as i64, id.to_be_bytes().to_vec(), t, before.map(|r| r.to_canonical_bytes())],
                )?;
            }
            let puts = out.rows.iter().map(|(id, (t, row))| (*t, *id, Some(row.clone())));
            let written = s.put_rows(puts.chain(out.removed.iter().map(|(id, t)| (*t, *id, None))).collect())?;
            let root = s.refresh_state_hash_with(&written)?;
            s.conn.prepare_cached("UPDATE _synod_node SET merged_epoch = ?1, base_root = ?2")?.execute(params![epoch as i64, root.to_vec()],
            )?;
            s.conn.prepare_cached("INSERT INTO _synod_merged (epoch, manifest, manifest_hash, rank, base_before, root, conflicts, undoable)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)")?.execute(params![
                    epoch as i64,
                    manifest.encode(),
                    manifest_hash.to_vec(),
                    manifest.rank,
                    base_before.to_vec(),
                    root.to_vec(),
                    out.conflicts.len() as i64
                ],
            )?;
            for r in &manifest.logs {
                s.conn.prepare_cached("INSERT INTO _synod_merged_log (log_hash, merged_in, node, log_epoch) VALUES (?1, ?2, ?3, ?4)")?.execute(params![r.hash.to_vec(), epoch as i64, r.node.0, r.epoch as i64],
                )?;
            }
            s.gc_local(epoch)?;
            Ok(MergeReport {
                epoch,
                root,
                manifest_hash,
                logs: manifest.logs.iter().map(|l| l.hash).collect(),
                conflicts: out.conflicts,
                removed: out.removed.len(),
                rebased: 0,
            })
        })
        .map(|(mut report, rebased)| {
            report.rebased = rebased;
            report
        })
    }

    /// Undoes the last merge: the base state goes back to M(e−1), and the
    /// merged logs are unmerged again. Use it when a manifest of higher
    /// priority for that epoch appears (specification Section 6). Returns
    /// the unmerged epoch.
    pub fn unmerge(&mut self) -> Result<u64> {
        let epoch = self.merged_epoch()?.ok_or_else(|| Error::Invalid("no merge to undo".into()))?;
        // A merge that this node made keeps its undo records as long as its
        // record; a merge taken over from a peer has none.
        let Some(record) = self.merged_record(epoch)?.filter(|r| r.undoable) else {
            return Err(Error::Invalid(format!("the merge of epoch {epoch} cannot be undone")));
        };
        self.rebuild(move |s| {
            let undo: Vec<(Vec<u8>, u32, Option<Vec<u8>>)> = {
                let mut stmt = s
                    .conn
                    .prepare("SELECT row_id, tbl, row FROM _synod_merge_undo WHERE epoch = ?1 ORDER BY row_id")?;
                stmt.query_map([epoch as i64], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                    .collect::<rusqlite::Result<_>>()?
            };
            let puts = undo
                .into_iter()
                .map(|(id, tbl, row)| {
                    let row = row.map(|b| Row::from_canonical_bytes(&b)).transpose();
                    Ok((tbl, parse_row_id(&id)?, row.map_err(|e| Error::Corrupt(format!("undo: {e}")))?))
                })
                .collect::<Result<_>>()?;
            s.put_rows(puts)?;
            s.conn.execute_batch(&format!(
                "DELETE FROM _synod_merge_undo WHERE epoch = {epoch};
                 DELETE FROM _synod_merged WHERE epoch = {epoch};
                 DELETE FROM _synod_merged_log WHERE merged_in = {epoch};"
            ))?;
            // Merges are consecutive from the first epoch: the merged epoch
            // goes back by one, or to none.
            s.conn
                .prepare_cached(
                    "UPDATE _synod_node SET base_root = ?1,
                   merged_epoch = CASE WHEN ?2 > first_epoch THEN ?2 - 1 ELSE NULL END",
                )?
                .execute(params![record.base_before.to_vec(), epoch as i64])?;
            Ok(epoch)
        })
        .map(|(e, _)| e)
    }

    /// Takes over a peer's base state (catch-up after a long absence, or
    /// repair after a root hash mismatch): `rows` are the peer's versions of
    /// the rows that differ, `removed` the rows that the peer does not have.
    /// The result must have the peer's root hash. This node's own logs that
    /// the peer did not merge stay unmerged: they go into a later manifest
    /// as late logs.
    pub fn adopt_base(&mut self, image: &BaseImage, rows: &[(u32, Row)], removed: &[RowId]) -> Result<()> {
        let image = image.clone();
        let rows = rows.to_vec();
        let removed = removed.to_vec();
        self.rebuild(move |s| {
            let mut puts = Vec::new();
            for (t, row) in rows {
                if t as usize >= s.tables.len() {
                    return Err(Error::Invalid(format!("table {t}")));
                }
                puts.push((t, row.row_id, Some(row)));
            }
            for id in &removed {
                if let Some(t) = s.base_table_of(*id)? {
                    puts.push((t, *id, None));
                }
            }
            s.put_rows(puts)?;
            s.conn.execute_batch("DELETE FROM _synod_merged; DELETE FROM _synod_merged_log; DELETE FROM _synod_merge_undo;")?;
            for r in &image.merged {
                s.conn.prepare_cached("INSERT INTO _synod_merged (epoch, manifest, manifest_hash, rank, base_before, root, conflicts, undoable)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, 0)")?.execute(params![r.epoch as i64, r.manifest.encode(), r.hash.to_vec(), r.manifest.rank, r.base_before.to_vec(), r.root.to_vec()],
                )?;
            }
            for l in &image.merged_logs {
                s.conn.prepare_cached("INSERT OR REPLACE INTO _synod_merged_log (log_hash, merged_in, node, log_epoch) VALUES (?1, ?2, ?3, ?4)")?.execute(params![l.hash.to_vec(), l.merged_in as i64, l.node.0, l.log_epoch as i64],
                )?;
            }
            // The open epoch stays: this node's unsealed writes are sealed in
            // their own epochs, then merged late or given up.
            match image.epoch {
                Some(e) => s.conn.prepare_cached("UPDATE _synod_node SET merged_epoch = ?1, base_root = ?2")?.execute(params![e as i64, image.root.to_vec()],
                )?,
                None => s.conn.prepare_cached("UPDATE _synod_node SET merged_epoch = NULL, base_root = ?1")?.execute([image.root.to_vec()])?,
            };
            Ok(())
        })
        .map(|_| ())
    }

    /// Test hook: changes one column of a row in the base state, and makes
    /// the result the base state, as a faulty merge would. The node then
    /// differs from its peers on the next merge, and the verify step must
    /// find and repair it.
    #[cfg(feature = "test-hooks")]
    pub fn diverge_for_tests(&mut self, table: &str, row_id: RowId, column: usize, value: Value) -> Result<()> {
        let schema = self.table(table)?;
        self.rebuild(move |s| {
            let mut row = read_row(&s.conn, &schema, row_id)?.ok_or_else(|| Error::Invalid("no such row".into()))?;
            row.cells[column].value = value;
            write_row(&s.conn, &schema, &row)?;
            mark_dirty(&s.conn, schema.id, row_id)?;
            let root = s.refresh_state_hash()?;
            s.conn.prepare_cached("UPDATE _synod_node SET base_root = ?1")?.execute([root.to_vec()])?;
            Ok(())
        })
        .map(|_| ())
    }

    /// Returns this node's merge history and base root, for a peer that
    /// takes over its base state.
    pub fn base_image(&self) -> Result<BaseImage> {
        Ok(BaseImage {
            epoch: self.merged_epoch()?,
            root: self.base_root()?,
            merged: self.merged_records()?,
            merged_logs: self.merged_logs()?,
        })
    }

    /// Returns a node of the Prolly tree of the base state (for diffs).
    pub fn base_tree_node(&self, hash: &NodeHash) -> Result<Option<Vec<u8>>> {
        crate::tree::base_node(&self.conn, hash)
    }

    /// Returns the row hashes of the base state with row IDs in `(lo, hi]`
    /// (an open end is `None`), in row ID order.
    pub fn base_hashes(&self, lo: Option<RowId>, hi: Option<RowId>) -> Result<Vec<(RowId, RowHash)>> {
        let lo = lo.map_or(Vec::new(), |r| r.to_be_bytes().to_vec());
        let hi = hi.map_or(vec![0xFF; 17], |r| r.to_be_bytes().to_vec());
        let mut stmt = self.conn.prepare_cached(
            "SELECT row_id, hash FROM _synod_rowhash WHERE row_id > ?1 AND row_id <= ?2 ORDER BY row_id",
        )?;
        let rows = stmt.query_map(params![lo, hi], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)))?;
        rows.map(|r| {
            let (id, h) = r?;
            Ok((parse_row_id(&id)?, RowHash(hash32(h, "row hash")?)))
        })
        .collect()
    }

    /// Returns rows of the base state (without this node's local writes
    /// since the last merge), with their table IDs; `None` for a row that
    /// is not in the base state.
    pub fn base_rows(&self, ids: &[RowId]) -> Result<Vec<(RowId, BaseRow)>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let undo: Option<(u32, Option<Vec<u8>>)> = self
                .conn
                .query_row("SELECT tbl, row FROM _synod_undo WHERE row_id = ?1", [id.to_be_bytes().to_vec()], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()?;
            let row = match undo {
                Some((t, Some(b))) => {
                    Some((t, Row::from_canonical_bytes(&b).map_err(|e| Error::Corrupt(format!("undo: {e}")))?))
                }
                Some((_, None)) => None,
                None => match self.base_table_of(*id)? {
                    Some(t) => read_row(&self.conn, &self.tables[t as usize], *id)?.map(|r| (t, r)),
                    None => None,
                },
            };
            out.push((*id, row));
        }
        Ok(out)
    }

    fn base_table_of(&self, id: RowId) -> Result<Option<u32>> {
        for t in &self.tables {
            let found: bool = self
                .conn
                .prepare_cached(&format!("SELECT count(*) > 0 FROM \"{}\" WHERE \"_row_id\" = ?1", t.physical()))?
                .query_row([id.to_be_bytes().to_vec()], |r| r.get(0))?;
            if found {
                return Ok(Some(t.id));
            }
        }
        Ok(None)
    }

    // ------------------------------------------------------------ lateness

    /// Gives up one of this node's own sealed logs that no merge can take
    /// any more (older than the lateness bound): its writes go to
    /// `_conflicts` as stale writes, by a normal local write, they leave
    /// the local state, and the log is not sent again. Returns the number
    /// of entries.
    pub fn abandon_log(&mut self, epoch: u64) -> Result<usize> {
        let own = self.sealed(epoch)?.ok_or_else(|| Error::Invalid(format!("epoch {epoch} is not sealed")))?;
        if self.log_merged_in(&own.hash)?.is_some() {
            return Err(Error::Invalid(format!("the log of epoch {epoch} was merged")));
        }
        let (_, entries) = decode_log(&own.bytes).map_err(|e| Error::Corrupt(format!("own log: {e}")))?;
        if entries.is_empty() {
            self.conn
                .prepare_cached("UPDATE _synod_logs SET abandoned = 1 WHERE log_hash = ?1")?
                .execute([own.hash.to_vec()])?;
            return Ok(0);
        }
        let mut by_row: BTreeMap<RowId, (u32, Vec<Recorded>)> = BTreeMap::new();
        for e in &entries {
            let values = e.after.clone().or_else(|| e.before.clone()).unwrap_or_default();
            by_row.entry(e.row_id).or_insert((e.table, Vec::new())).1.push(Recorded {
                node: e.txn.node,
                hlc: e.hlc,
                values,
            });
        }
        let tables = self.tables.clone();
        let me = self.config.node;
        let mut t = self.begin()?;
        for (row_id, (table, recorded)) in by_row {
            let schema = &tables[table as usize];
            let id = given_up_id(me, epoch, &schema.name, row_id);
            t.insert(
                "_conflicts",
                &[
                    ("conflict_id", Value::Blob(id.to_vec())),
                    ("epoch", Value::Integer(epoch as i64)),
                    ("tbl", Value::Text(schema.name.clone())),
                    ("target", Value::Blob(row_id.to_be_bytes().to_vec())),
                    ("kind", Value::Text(ConflictKind::StaleWrite.as_str().into())),
                    ("rule", Value::Text("too_late".into())),
                    ("recorded", Value::Blob(encode_recorded(&recorded))),
                ],
            )?;
        }
        t.store_conn().execute("UPDATE _synod_logs SET abandoned = 1 WHERE log_hash = ?1", [own.hash.to_vec()])?;
        t.commit()?;
        // Take the given-up writes out of the local state. If this stops
        // (a crash), the next merge does it.
        self.rebuild(|_| Ok(()))?;
        Ok(entries.len())
    }

    // ------------------------------------------------------------ rebuild

    /// Changes the base state with `change`, in one SQLite transaction: the
    /// new pages go to the WAL beside the old ones, and the commit is the
    /// atomic switch (white paper, Section XI-B). `change` runs after
    /// M(e−1) is restored and checked; it must leave the base root in
    /// `_synod_node` equal to the state it makes. Returns its value and the
    /// number of rebased entries. On an error nothing changes.
    fn rebuild<T>(&mut self, change: impl FnOnce(&mut Store) -> Result<T>) -> Result<(T, usize)> {
        self.in_txn(|s| {
            let base_root = s.base_root()?;
            s.restore_base(base_root)?;
            let value = change(s)?;
            let root = s.refresh_state_hash()?;
            if root != s.base_root()? {
                return Err(Error::Corrupt("the new base state does not have its root hash".into()));
            }
            // The stored tree is now the tree of the new base state. The
            // rebased writes stay out of it, in `_synod_undo`.
            let rebased = s.rebase_local()?;
            Ok((value, rebased))
        })
    }

    // Applies this node's unmerged writes again on top of the base state:
    // its sealed logs that no merge took (and not given up), and its
    // unsealed writes, by the rules of late writes. Conflicts are not
    // recorded: the merge of their epoch records them.
    fn rebase_local(&mut self) -> Result<usize> {
        let me = self.config.node;
        let entries: Vec<LogEntry> = {
            // By epoch, through the epoch index: the kept log also holds
            // the merged epochs of the last 4 L epochs, which a scan would
            // read at every merge.
            let mut stmt = self.conn.prepare_cached(
                "SELECT entry FROM (
                   SELECT seq, entry FROM _synod_log WHERE epoch IN
                     (SELECT s.epoch FROM _synod_sealed s JOIN _synod_logs l USING (log_hash)
                      WHERE l.abandoned = 0 AND s.log_hash NOT IN
                        (SELECT log_hash FROM _synod_merged_log WHERE node = ?1))
                   UNION ALL
                   SELECT seq, entry FROM _synod_log WHERE epoch >= (SELECT open_epoch FROM _synod_node))
                 ORDER BY seq",
            )?;
            let rows = stmt.query_map([me.0], |r| r.get::<_, Vec<u8>>(0))?;
            rows.map(|b| LogEntry::decode(&b?).map_err(|e| Error::Corrupt(format!("log entry: {e}"))))
                .collect::<Result<_>>()?
        };
        if entries.is_empty() {
            return Ok(0);
        }
        let n = entries.len();
        let tables = self.tables.clone();
        let epoch = self.next_merge_epoch()?;
        let input = MergeInput {
            tables: &tables,
            epoch,
            logs: vec![LogInput { node: me, epoch, entries }],
            conflict_stamp: Stamp::default(),
            record_conflicts: false,
            gc_through: None,
        };
        let local = merge(&BaseView { conn: &self.conn }, &input)?;
        for (id, (t, _)) in &local.rows {
            let base = read_row(&self.conn, &tables[*t as usize], *id)?;
            record_undo(&self.conn, *t, *id, base.as_ref())?;
        }
        self.put_rows(local.rows.into_iter().map(|(id, (t, row))| (t, id, Some(row))).collect())?;
        Ok(n)
    }

    // Drops what is older than the lateness bound: log entries and logs,
    // sealed logs, merge records, and undo records.
    fn gc_local(&mut self, epoch: u64) -> Result<()> {
        let me = self.config.node.0;
        if let Some(cutoff) = epoch.checked_sub(self.config.lateness + 1) {
            self.conn.execute_batch(&format!(
                "DELETE FROM _synod_logs WHERE epoch <= {cutoff} AND node != {me};
                 DELETE FROM _synod_merged WHERE epoch <= {cutoff};
                 DELETE FROM _synod_merge_undo WHERE epoch <= {cutoff};"
            ))?;
        }
        // Own logs, and which logs were merged, stay longer: if this node
        // later takes over a peer's chain that did not merge an own log,
        // the log goes into a later manifest, or to `_conflicts`.
        if let Some(cutoff) = epoch.checked_sub(self.retention() + 1) {
            self.conn
                .prepare_cached(
                    "DELETE FROM _synod_log WHERE epoch <= ?1 AND epoch IN
                   (SELECT log_epoch FROM _synod_merged_log WHERE node = ?2
                    UNION SELECT epoch FROM _synod_logs WHERE node = ?2 AND abandoned = 1)",
                )?
                .execute(params![cutoff as i64, me])?;
            // An own log that no merge took stays until it is given up. The
            // sealed copies go first: the next statement deletes the given-up
            // logs that select them.
            self.conn.execute_batch(&format!(
                "DELETE FROM _synod_sealed WHERE epoch <= {cutoff} AND log_hash IN
                   (SELECT log_hash FROM _synod_merged_log UNION SELECT log_hash FROM _synod_logs WHERE abandoned = 1);
                 DELETE FROM _synod_logs WHERE epoch <= {cutoff} AND (abandoned = 1
                   OR log_hash IN (SELECT log_hash FROM _synod_merged_log));
                 DELETE FROM _synod_merged_log WHERE merged_in <= {cutoff};"
            ))?;
        }
        Ok(())
    }

    /// Number of epochs that this node keeps its own logs and the record of
    /// merged logs: 4 L.
    pub fn retention(&self) -> u64 {
        4 * self.config.lateness
    }

    // Writes a batch of rows (`None`: remove the row) and marks them for
    // the state hash. All rows of the batch go first, then the new
    // versions come in: a unique key can move from one row of the batch to
    // another with the unique indexes in place.
    //
    // A row whose indexed columns do not change is updated in place, and
    // its index entries are not written.
    //
    // Returns the row hash of each row written (`None`: removed), for a
    // state hash refresh that follows at once.
    fn put_rows(&mut self, rows: Vec<(u32, RowId, Option<Row>)>) -> Result<WrittenHashes> {
        let mut written = WrittenHashes::new();
        let mut in_place = Vec::with_capacity(rows.len());
        for (t, id, row) in &rows {
            let schema = &self.tables[*t as usize];
            let keep = match row {
                Some(new) => read_row(&self.conn, schema, *id)?.is_some_and(|old| {
                    old.deleted == new.deleted
                        && indexed_columns(schema).iter().all(|&c| old.cells[c].value == new.cells[c].value)
                }),
                None => false,
            };
            if !keep {
                delete_row(&self.conn, schema, *id)?;
            }
            in_place.push(keep);
        }
        for ((t, id, row), keep) in rows.iter().zip(in_place) {
            let schema = &self.tables[*t as usize];
            if let Some(row) = row {
                if keep {
                    update_unindexed(&self.conn, schema, row)?;
                } else {
                    write_row(&self.conn, schema, row).map_err(|e| match e {
                        Error::Sql(e) => Error::Corrupt(format!("a merged row breaks a constraint: {e}")),
                        e => e,
                    })?;
                }
            }
            mark_dirty(&self.conn, *t, *id)?;
            written.insert(id.to_be_bytes().to_vec(), row.as_ref().map(|r| r.hash().0.to_vec()));
        }
        Ok(written)
    }

    // Writes back the base-state version of every row changed since the
    // last merge, and checks that the result has the base root hash.
    fn restore_base(&mut self, base_root: NodeHash) -> Result<()> {
        let undo: Vec<(Vec<u8>, u32, Option<Vec<u8>>)> = {
            let mut stmt = self.conn.prepare_cached("SELECT row_id, tbl, row FROM _synod_undo ORDER BY row_id")?;
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<rusqlite::Result<_>>()?
        };
        let puts = undo
            .into_iter()
            .map(|(id, tbl, row)| {
                let row = row.map(|b| Row::from_canonical_bytes(&b)).transpose();
                Ok((tbl, parse_row_id(&id)?, row.map_err(|e| Error::Corrupt(format!("undo row: {e}")))?))
            })
            .collect::<Result<_>>()?;
        let written = self.put_rows(puts)?;
        self.conn.prepare_cached("DELETE FROM _synod_undo")?.execute([])?;
        if self.refresh_state_hash_with(&written)? != base_root {
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
        let me = self.config.node.0;
        self.conn.prepare_cached("VACUUM INTO ?1")?.execute([path])?;
        {
            let mut copy = Store::open(path, self.config.clone(), self.env.clone())?;
            copy.in_txn(|s| {
                s.restore_base(base_root)?;
                // The copy has no log of its own. Log sequence numbers start
                // again, so the state hash cursor starts again too.
                s.conn.execute_batch("DELETE FROM _synod_log; DELETE FROM _synod_sealed; DELETE FROM _synod_dirty;")?;
                s.conn.prepare_cached("DELETE FROM _synod_logs WHERE node = ?1 AND log_hash NOT IN (SELECT log_hash FROM _synod_merged_log)")?.execute([me])?;
                s.conn.prepare_cached("UPDATE _synod_node SET node = ?1, next_row = 0, next_txn = 0, hashed_seq = 0, open_epoch = max(open_epoch, ?2)")?.execute(params![node.0, next_epoch as i64],
                )?;
                Ok(())
            })?;
            // The copy dropped the source's logs: give their pages back.
            copy.conn.execute_batch("VACUUM")?;
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
        let mut stmt = self.conn.prepare_cached(&format!(
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

// The columns that an index of the table reads, besides `_tomb` (which
// every index reads): unique keys, foreign keys, and for `_conflicts` the
// columns of the index of resolved conflicts. See
// `TableSchema::create_statements`.
fn indexed_columns(schema: &TableSchema) -> Vec<usize> {
    let mut cols: Vec<usize> = schema.uniques.iter().flatten().copied().collect();
    cols.extend(schema.foreign_keys.iter().map(|f| f.column));
    if schema.id == 0 {
        cols.extend(
            schema.columns.iter().enumerate().filter(|(_, c)| c.name == "epoch" || c.name == "status").map(|(i, _)| i),
        );
    }
    cols
}

// Writes the stamps and the columns that no index reads, of a row whose
// other columns and `_tomb` do not change.
fn update_unindexed(conn: &Connection, schema: &TableSchema, row: &Row) -> Result<()> {
    let indexed = indexed_columns(schema);
    let free: Vec<usize> = (0..schema.columns.len()).filter(|c| !indexed.contains(c)).collect();
    let sets: String =
        free.iter().enumerate().map(|(k, &c)| format!(", \"{}\" = ?{}", schema.columns[c].name, k + 3)).collect();
    let sql = format!("UPDATE \"{}\" SET \"_stamps\" = ?2{sets} WHERE \"_row_id\" = ?1", schema.physical());
    let stamps = crate::store::encode_stamps(&row.cells.iter().map(|c| c.stamp).collect::<Vec<_>>());
    let mut args: Vec<rusqlite::types::Value> =
        vec![rusqlite::types::Value::Blob(row.row_id.to_be_bytes().to_vec()), rusqlite::types::Value::Blob(stamps)];
    args.extend(free.iter().map(|&c| crate::values::to_sql(&row.cells[c].value)));
    conn.prepare_cached(&sql)?.execute(rusqlite::params_from_iter(args))?;
    Ok(())
}

fn delete_row(conn: &Connection, schema: &TableSchema, id: RowId) -> Result<()> {
    conn.prepare_cached(&format!("DELETE FROM \"{}\" WHERE \"_row_id\" = ?1", schema.physical()))?
        .execute([id.to_be_bytes().to_vec()])?;
    Ok(())
}

/// What [`Txn::revert`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RevertReport {
    /// Rows inserted by the node that were deleted.
    pub deleted: usize,
    /// Columns set back to their earlier values.
    pub restored: usize,
    /// Rows deleted by the node that were inserted again, as new rows.
    pub reinserted: usize,
    /// Changes that could not be reverted: the row or column changed again
    /// since, or a constraint failed.
    pub skipped: usize,
}

impl Txn<'_> {
    /// Writes a revert: normal local writes that undo the changes of `node`
    /// in the logs of `epochs` that this node keeps (the last L epochs),
    /// like `git revert` (white paper, Section XI-E). For each row, the
    /// state before the node's first change in the range comes back: a row
    /// that the node inserted is deleted, a row that it deleted comes back
    /// as a new row (with a new row ID), and each column that it changed
    /// gets its earlier value. A column that changed again since is left
    /// as it is.
    pub fn revert(&mut self, node: NodeId, epochs: RangeInclusive<u64>) -> Result<RevertReport> {
        // Per row: table, values before the first change, after the last.
        type Span = (u32, Option<Vec<Value>>, Option<Vec<Value>>);
        let mut rows: BTreeMap<RowId, Span> = BTreeMap::new();
        for (r, _) in self.store_ref().known_logs(*epochs.start())? {
            if r.node != node || !epochs.contains(&r.epoch) {
                continue;
            }
            let bytes = self.store_ref().log(&r.hash)?.ok_or_else(|| Error::Corrupt("kept log is missing".into()))?;
            let (_, entries) = decode_log(&bytes).map_err(|e| Error::Corrupt(format!("log: {e}")))?;
            for e in entries {
                let span = rows.entry(e.row_id).or_insert_with(|| (e.table, e.before.clone(), None));
                span.2 = e.after;
            }
        }
        let mut report = RevertReport::default();
        let tables = self.store_ref().tables.clone();
        for (row_id, (table, before, after)) in rows {
            let schema = &tables[table as usize];
            let current = self.get(&schema.name, row_id)?;
            let result = match (before, after, current) {
                (None, None, _) => Ok(()),
                (None, Some(_), Some(_)) => self.delete(&schema.name, row_id).map(|()| report.deleted += 1),
                (Some(before), None, _) => {
                    let cols: Vec<(&str, Value)> =
                        schema.columns.iter().zip(before).map(|(c, v)| (c.name.as_str(), v)).collect();
                    self.insert(&schema.name, &cols).map(|_| report.reinserted += 1)
                }
                (Some(before), Some(after), Some(cur)) => {
                    let changed: Vec<usize> = (0..schema.columns.len()).filter(|&c| before[c] != after[c]).collect();
                    let (same, moved): (Vec<usize>, Vec<usize>) =
                        changed.into_iter().partition(|&c| cur[c] == after[c]);
                    report.skipped += moved.len();
                    let changes: Vec<(&str, Value)> =
                        same.iter().map(|&c| (schema.columns[c].name.as_str(), before[c].clone())).collect();
                    if changes.is_empty() {
                        Ok(())
                    } else {
                        self.update(&schema.name, row_id, &changes).map(|()| report.restored += changes.len())
                    }
                }
                (_, Some(_), None) => {
                    report.skipped += 1;
                    Ok(())
                }
            };
            match result {
                Ok(()) => {}
                Err(Error::Constraint(_) | Error::Invalid(_)) => report.skipped += 1,
                Err(e) => return Err(e),
            }
        }
        Ok(report)
    }
}
