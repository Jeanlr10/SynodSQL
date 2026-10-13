//! The deterministic epoch merge (specification Section 7).
//!
//! [`merge`] is a pure function of the base state M(e−1) and the logs of a
//! manifest. It reads the base through [`BaseState`], and it returns the
//! rows that change and the conflicts. It iterates only over sorted maps,
//! compares values by their canonical encoding (or by the type order of
//! `max` and `min`), and reads no clock, random number, or local index. So
//! every node that merges the same base with the same logs gets the same
//! rows, the same conflicts, and the same root hash, whatever the order of
//! the logs.

mod conflict;
mod constraints;
mod mem;
mod policy;
mod text3;

use std::collections::BTreeMap;
use std::sync::Arc;

use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use synod_core::{Cell, LogEntry, NodeId, Op, Row, RowId, Stamp, Value};

pub use conflict::{
    CONFLICT_ID_CONTEXT, CONFLICTS_DDL, CONFLICTS_TABLE, Conflict, ConflictKind, MERGE_NODE, Recorded, conflict_id,
    decode_image, decode_recorded, encode_image, encode_recorded, given_up_id,
};
pub use mem::MemState;

use crate::error::{Error, Result};
use crate::schema::{DeleteRule, TableSchema};
use crate::values;
use policy::{ColumnConflict, Write};

/// Returns the schema of `_conflicts` (table 0 of every store).
pub fn conflicts_schema() -> TableSchema {
    crate::schema::parse_system_table(CONFLICTS_DDL, 0).expect("valid built-in schema")
}

/// Read access to the base state M(e−1).
pub trait BaseState {
    /// Returns a row, tombstones included.
    fn row(&self, table: &TableSchema, id: RowId) -> Result<Option<Row>>;

    /// Returns the live rows whose `columns` equal `values` as SQL compares
    /// them (no value is NULL), in row ID order.
    fn find_live(&self, table: &TableSchema, columns: &[usize], values: &[Value]) -> Result<Vec<RowId>>;

    /// Returns the indexes of the CHECK constraints of `table` that `row`
    /// fails. The rows of the base state do not matter here.
    fn failed_checks(&self, table: &TableSchema, row: &Row) -> Result<Vec<usize>>;

    /// Returns the rows that garbage collection through `epoch` removes, in
    /// row ID order: tombstones deleted in `epoch` or earlier, and
    /// `_conflicts` rows with status `resolved` from `epoch` or earlier.
    fn gc_candidates(&self, tables: &[Arc<TableSchema>], epoch: u64) -> Result<Vec<(u32, RowId)>>;
}

/// One log of a manifest.
#[derive(Clone, Debug)]
pub struct LogInput {
    /// Node that wrote it.
    pub node: NodeId,
    /// Epoch of the log. A log of an earlier epoch is a late log.
    pub epoch: u64,
    /// Entries, in commit order.
    pub entries: Vec<LogEntry>,
}

/// What to merge.
pub struct MergeInput<'a> {
    /// Managed tables, by table ID. Table 0 is `_conflicts`.
    pub tables: &'a [Arc<TableSchema>],
    /// Epoch of the merge.
    pub epoch: u64,
    /// The logs. Their order does not matter.
    pub logs: Vec<LogInput>,
    /// Stamp of the conflict rows. It must be the same on every node (the
    /// store uses the end of the epoch and [`MERGE_NODE`]).
    pub conflict_stamp: Stamp,
    /// If false, conflicts are found but not returned or written (for the
    /// local rebase of in-flight writes).
    pub record_conflicts: bool,
    /// Garbage collection: remove the tombstones deleted in this epoch or
    /// earlier, and the resolved conflicts of this epoch or earlier
    /// (specification Section 7.7). The manifest sets it.
    pub gc_through: Option<u64>,
}

/// What the merge changes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergeOutput {
    /// Every row of M(e) that differs from M(e−1), with its table ID. The
    /// conflict rows are here too, in table 0.
    pub rows: BTreeMap<RowId, (u32, Row)>,
    /// The conflicts, sorted by (table, row, column, kind).
    pub conflicts: Vec<Conflict>,
    /// Rows of M(e−1) that are not in M(e) (garbage collection), with their
    /// table IDs.
    pub removed: BTreeMap<RowId, u32>,
}

/// One node's changes to one row, from all of its logs in the manifest.
#[derive(Clone, Debug)]
struct NodeChange {
    created: bool,
    deleted: bool,
    /// Before-image of the node's first change; `None` if it created the
    /// row.
    first_before: Option<Vec<Value>>,
    /// Values after the node's last change (the before-image of a delete).
    last: Vec<Value>,
    /// Stamp of the node's last write to each column.
    col_stamp: Vec<Option<Stamp>>,
    /// Stamp of the node's last entry for the row.
    last_stamp: Stamp,
}

impl NodeChange {
    fn changed(&self, column: usize) -> bool {
        self.first_before.as_ref().is_none_or(|b| b[column] != self.last[column])
    }
}

/// Merges `input` into `base`. See the module documentation.
pub fn merge(base: &dyn BaseState, input: &MergeInput<'_>) -> Result<MergeOutput> {
    let tables = input.tables;
    let mut logs: Vec<&LogInput> = input.logs.iter().collect();
    logs.sort_by_key(|l| (l.node, l.epoch));
    if logs.windows(2).any(|w| (w[0].node, w[0].epoch) == (w[1].node, w[1].epoch)) {
        return Err(Error::Invalid("two logs of one node and epoch".into()));
    }

    // Group by row, then by node.
    let mut changes: BTreeMap<RowId, (u32, BTreeMap<NodeId, NodeChange>)> = BTreeMap::new();
    for log in &logs {
        for e in &log.entries {
            group(tables, &mut changes, log.node, e)?;
        }
    }

    let mut ctx = constraints::Ctx {
        base,
        tables,
        epoch: input.epoch,
        overlay: BTreeMap::new(),
        conflicts: BTreeMap::new(),
        replaced: BTreeMap::new(),
        keys: Default::default(),
    };
    for (row_id, (table, writers)) in &changes {
        let schema = &tables[*table as usize];
        let base_row = base.row(schema, *row_id)?;
        if let Some(row) = merge_row(&mut ctx, schema, *row_id, base_row.as_ref(), writers)? {
            ctx.overlay.insert(*row_id, (*table, row));
        }
    }
    ctx.constraint_pass()?;

    let mut conflicts: Vec<Conflict> = std::mem::take(&mut ctx.conflicts).into_values().collect();
    conflicts.sort_by_key(Conflict::sort_key);
    let mut rows = BTreeMap::new();
    for (id, (table, row)) in std::mem::take(&mut ctx.overlay) {
        if base.row(&tables[table as usize], id)?.as_ref() != Some(&row) {
            rows.insert(id, (table, row));
        }
    }
    // Garbage collection: only rows that this merge did not touch.
    let mut removed = BTreeMap::new();
    if let Some(through) = input.gc_through {
        for (table, id) in base.gc_candidates(tables, through)? {
            if !rows.contains_key(&id) && !changes.contains_key(&id) {
                removed.insert(id, table);
            }
        }
    }
    if !input.record_conflicts {
        return Ok(MergeOutput { rows, conflicts: Vec::new(), removed });
    }
    for c in &conflicts {
        rows.insert(c.row_id_in_table(), (0, c.to_row(input.conflict_stamp)));
    }
    Ok(MergeOutput { rows, conflicts, removed })
}

/// Checks that a log is well formed for these tables: entries of its own
/// node, known tables, images of the right length, and a valid order of
/// operations per row. A leader puts only valid logs in a manifest; the
/// merge rejects a manifest with an invalid log.
pub fn validate_log(tables: &[Arc<TableSchema>], log: &LogInput) -> Result<()> {
    let mut changes = BTreeMap::new();
    log.entries.iter().try_for_each(|e| group(tables, &mut changes, log.node, e))
}

fn group(
    tables: &[Arc<TableSchema>],
    changes: &mut BTreeMap<RowId, (u32, BTreeMap<NodeId, NodeChange>)>,
    node: NodeId,
    e: &LogEntry,
) -> Result<()> {
    let bad = |msg: &str| Error::Invalid(format!("log of node {node}, row {}: {msg}", e.row_id));
    if e.txn.node != node {
        return Err(bad("entry from another node"));
    }
    let schema = tables.get(e.table as usize).ok_or_else(|| bad("unknown table"))?;
    let n = schema.columns.len();
    if e.before.as_ref().is_some_and(|b| b.len() != n) || e.after.as_ref().is_some_and(|a| a.len() != n) {
        return Err(bad("image length differs from the schema"));
    }
    let (table, writers) = changes.entry(e.row_id).or_insert_with(|| (e.table, BTreeMap::new()));
    if *table != e.table {
        return Err(bad("row in two tables"));
    }
    let stamp = Stamp { hlc: e.hlc, node };
    match (e.op, writers.get_mut(&node)) {
        (Op::Insert, None) => {
            if e.row_id.node() != node {
                return Err(bad("insert of another node's row ID"));
            }
            writers.insert(
                node,
                NodeChange {
                    created: true,
                    deleted: false,
                    first_before: None,
                    last: e.after.clone().unwrap(),
                    col_stamp: vec![Some(stamp); n],
                    last_stamp: stamp,
                },
            );
        }
        (Op::Insert, Some(_)) => return Err(bad("insert after another change")),
        (_, Some(c)) if c.deleted => return Err(bad("change after a delete")),
        (op, existing) => {
            let before = e.before.clone().unwrap();
            let c = match existing {
                Some(c) => c,
                None => writers.entry(node).or_insert(NodeChange {
                    created: false,
                    deleted: false,
                    first_before: Some(before.clone()),
                    last: before.clone(),
                    col_stamp: vec![None; n],
                    last_stamp: stamp,
                }),
            };
            if op == Op::Delete {
                c.deleted = true;
            } else {
                let after = e.after.as_ref().unwrap();
                for i in 0..n {
                    if after[i] != before[i] {
                        c.col_stamp[i] = Some(stamp);
                    }
                }
                c.last = after.clone();
            }
            c.last_stamp = stamp;
        }
    }
    Ok(())
}

fn recorded(node: NodeId, stamp: Stamp, values: Vec<Value>) -> Recorded {
    Recorded { node, hlc: stamp.hlc, values }
}

// Records entries that the merge cannot apply: a change to a row that is
// not in the base state (for example removed by garbage collection), or a
// new row that is already there. The row stays as it is.
fn stale(
    ctx: &mut constraints::Ctx<'_>,
    schema: &TableSchema,
    row_id: RowId,
    rule: &str,
    base: Option<&Row>,
    writers: &BTreeMap<NodeId, NodeChange>,
) {
    let rec = writers.iter().map(|(n, c)| recorded(*n, c.last_stamp, c.last.clone())).collect();
    let base = base.map(|r| r.cells.iter().map(|c| c.value.clone()).collect());
    ctx.add(schema, row_id, None, ConflictKind::StaleWrite, rule, base.clone(), rec, base, None);
}

// Merges one row. Returns the new row, or None if the row does not change.
fn merge_row(
    ctx: &mut constraints::Ctx<'_>,
    schema: &TableSchema,
    row_id: RowId,
    base_row: Option<&Row>,
    writers: &BTreeMap<NodeId, NodeChange>,
) -> Result<Option<Row>> {
    let epoch = ctx.epoch;
    // A new row: only its creator can have changed it.
    if let Some((_, c)) = writers.iter().find(|(_, c)| c.created) {
        if writers.len() > 1 || base_row.is_some() {
            stale(ctx, schema, row_id, "row_exists", base_row, writers);
            return Ok(None);
        }
        let cells =
            c.last.iter().zip(&c.col_stamp).map(|(v, s)| Cell { stamp: s.unwrap(), value: v.clone() }).collect();
        return Ok(Some(Row { row_id, deleted: c.deleted.then_some(epoch), cells }));
    }
    let Some(base_row) = base_row else {
        stale(ctx, schema, row_id, "row_missing", None, writers);
        return Ok(None);
    };
    let base_values: Vec<Value> = base_row.cells.iter().map(|c| c.value.clone()).collect();
    let deleters: Vec<(&NodeId, &NodeChange)> = writers.iter().filter(|(_, c)| c.deleted).collect();
    let updaters: Vec<(&NodeId, &NodeChange)> =
        writers.iter().filter(|(_, c)| !c.deleted && (0..schema.columns.len()).any(|i| c.changed(i))).collect();
    // A row deleted in this merge gets this epoch; a row that was already a
    // tombstone keeps its epoch.
    let deleted = Some(base_row.deleted.unwrap_or(epoch));

    let merge_columns = |ctx: &mut constraints::Ctx<'_>, record: bool| -> Vec<Cell> {
        let mut cells = Vec::with_capacity(schema.columns.len());
        for (i, column) in schema.columns.iter().enumerate() {
            let writes: Vec<Write> = updaters
                .iter()
                .filter(|(_, c)| c.changed(i))
                .map(|(node, c)| Write {
                    node: **node,
                    stamp: c.col_stamp[i].unwrap_or(c.last_stamp),
                    before: c.first_before.as_ref().unwrap()[i].clone(),
                    after: c.last[i].clone(),
                })
                .collect();
            let (cell, conflict) = policy::resolve(column, &base_row.cells[i], &writes);
            if let (true, Some(ColumnConflict { rule, recorded: rec })) = (record, conflict) {
                ctx.add(
                    schema,
                    row_id,
                    Some(i),
                    ConflictKind::SameColumn,
                    rule,
                    Some(vec![base_row.cells[i].value.clone()]),
                    rec.into_iter().map(|(n, s, v)| recorded(n, s, vec![v])).collect(),
                    Some(vec![cell.value.clone()]),
                    None,
                );
            }
            cells.push(cell);
        }
        cells
    };

    if !base_row.is_tombstone() && deleters.is_empty() {
        let cells = merge_columns(ctx, true);
        let row = Row { row_id, deleted: None, cells };
        return Ok((row != *base_row).then_some(row));
    }
    // A delete in this merge, or a row already deleted in the base.
    if updaters.is_empty() {
        let row = Row { deleted, ..base_row.clone() };
        return Ok((row != *base_row).then_some(row));
    }
    let from_updaters =
        || updaters.iter().map(|(n, c)| recorded(**n, c.last_stamp, c.last.clone())).collect::<Vec<_>>();
    let from_deleters =
        || deleters.iter().map(|(n, c)| recorded(**n, c.last_stamp, c.last.clone())).collect::<Vec<_>>();
    let (row, rule, rec, applied) = match schema.delete_rule {
        DeleteRule::DeleteWins => (Row { deleted, ..base_row.clone() }, "delete_wins", from_updaters(), None),
        DeleteRule::UpdateWins => {
            let cells = merge_columns(ctx, true);
            let applied = cells.iter().map(|c| c.value.clone()).collect();
            (Row { row_id, deleted: None, cells }, "update_wins", from_deleters(), Some(applied))
        }
        DeleteRule::Flag => {
            let cells = merge_columns(ctx, false);
            let applied = cells.iter().map(|c| c.value.clone()).collect();
            let mut rec = from_updaters();
            rec.extend(from_deleters());
            (Row { row_id, deleted, cells }, "flag", rec, Some(applied))
        }
    };
    ctx.add(schema, row_id, None, ConflictKind::UpdateDelete, rule, Some(base_values), rec, applied, None);
    Ok((row != *base_row).then_some(row))
}

/// Returns the indexes of the CHECK constraints that `row` fails, by
/// evaluating them in SQLite on a temporary table with the same column
/// types as the physical table. A CHECK fails only if it is false.
pub(crate) fn failed_checks(conn: &Connection, schema: &TableSchema, row: &Row) -> Result<Vec<usize>> {
    if schema.checks.is_empty() {
        return Ok(Vec::new());
    }
    let temp = format!("_synod_check_{}", schema.name);
    let cols: Vec<String> = schema.columns.iter().map(|c| format!("\"{}\" {}", c.name, c.ty.storage())).collect();
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE IF NOT EXISTS \"{temp}\" ({}); DELETE FROM temp.\"{temp}\";",
        cols.join(", ")
    ))?;
    let marks: Vec<String> = (1..=schema.columns.len()).map(|i| format!("?{i}")).collect();
    let args: Vec<SqlValue> = row.cells.iter().map(|c| values::to_sql(&c.value)).collect();
    conn.prepare_cached(&format!("INSERT INTO temp.\"{temp}\" VALUES ({})", marks.join(", ")))?
        .execute(rusqlite::params_from_iter(args))?;
    let mut failed = Vec::new();
    for (i, check) in schema.checks.iter().enumerate() {
        let n: i64 = conn
            .prepare_cached(&format!("SELECT count(*) FROM temp.\"{temp}\" WHERE NOT ({check})"))?
            .query_row([], |r| r.get(0))?;
        if n > 0 {
            failed.push(i);
        }
    }
    conn.prepare_cached(&format!("DELETE FROM temp.\"{temp}\""))?.execute([])?;
    Ok(failed)
}
