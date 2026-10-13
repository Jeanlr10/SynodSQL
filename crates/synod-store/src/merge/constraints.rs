//! The constraint pass (specification Section 7.4) and the merge context.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use synod_core::{Row, RowId, Stamp, Value};

use super::BaseState;
use super::conflict::{Conflict, ConflictKind, Recorded, conflict_id};
use super::policy::key_eq;
use crate::error::Result;
use crate::schema::TableSchema;

/// Rounds of the unique and foreign key passes. A restored parent or a
/// cascaded delete can make new work for the next round.
const MAX_ROUNDS: usize = 8;

pub(crate) struct Ctx<'a> {
    pub base: &'a dyn BaseState,
    pub tables: &'a [Arc<TableSchema>],
    pub epoch: u64,
    /// Rows of M(e) that the merge wrote, by row ID: the result is this
    /// overlay on the base.
    pub overlay: BTreeMap<RowId, (u32, Row)>,
    pub conflicts: BTreeMap<[u8; 32], Conflict>,
    /// Versions of rows that the constraint pass replaced. A parent key
    /// value that any of them held can have children that need a check.
    pub replaced: BTreeMap<RowId, Vec<Row>>,
    /// Live rows of the overlay by (table, key columns) and key value, so
    /// that a key lookup does not scan the overlay. Built at the first
    /// lookup of a key; [`Ctx::replace`] keeps it up to date.
    pub keys: RefCell<KeyIndex>,
}

/// See [`Ctx::keys`].
pub(crate) type KeyIndex = BTreeMap<(u32, Vec<usize>), BTreeMap<Vec<u8>, BTreeSet<RowId>>>;

/// The key value as bytes, equal for values that are equal as keys
/// ([`key_eq`]): REAL −0.0 is 0.0. None if a value is NULL: such a key
/// matches no row.
fn key_bytes<'v>(values: impl IntoIterator<Item = &'v Value>) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for v in values {
        match v {
            Value::Null => return None,
            Value::Real(x) if *x == 0.0 => Value::Real(0.0).encode_into(&mut out),
            v => v.encode_into(&mut out),
        }
    }
    Some(out)
}

fn row_key(row: &Row, columns: &[usize]) -> Option<Vec<u8>> {
    if row.is_tombstone() {
        return None;
    }
    key_bytes(columns.iter().map(|&c| &row.cells[c].value))
}

fn values(row: &Row) -> Vec<Value> {
    row.cells.iter().map(|c| c.value.clone()).collect()
}

fn newest(row: &Row, columns: &[usize]) -> Stamp {
    columns.iter().map(|&c| row.cells[c].stamp).max().unwrap_or_default()
}

impl Ctx<'_> {
    /// Adds a conflict. A second conflict with the same ID adds its
    /// recorded values to the first.
    #[allow(clippy::too_many_arguments)]
    pub fn add(
        &mut self,
        schema: &TableSchema,
        row_id: RowId,
        column: Option<usize>,
        kind: ConflictKind,
        rule: &str,
        base: Option<Vec<Value>>,
        recorded: Vec<Recorded>,
        applied: Option<Vec<Value>>,
        other_row: Option<RowId>,
    ) {
        let column = column.map(|c| c as u16);
        let id = conflict_id(self.epoch, &schema.name, row_id, column, kind);
        let entry = self.conflicts.entry(id).or_insert_with(|| Conflict {
            id,
            epoch: self.epoch,
            table: schema.id,
            table_name: schema.name.clone(),
            row_id,
            column,
            column_name: column.map(|c| schema.columns[c as usize].name.clone()),
            kind,
            rule: rule.to_owned(),
            base,
            recorded: Vec::new(),
            applied,
            other_row,
        });
        for r in recorded {
            if !entry.recorded.contains(&r) {
                entry.recorded.push(r);
            }
        }
    }

    fn table_named(&self, name: &str) -> &Arc<TableSchema> {
        self.tables.iter().find(|t| t.name.eq_ignore_ascii_case(name)).expect("parent table checked at create_table")
    }

    /// A row of M(e) as the merge has it so far.
    fn get(&self, schema: &TableSchema, id: RowId) -> Result<Option<Row>> {
        match self.overlay.get(&id) {
            Some((_, row)) => Ok(Some(row.clone())),
            None => self.base.row(schema, id),
        }
    }

    /// Live rows of M(e) so far with these key values.
    fn find_live(&self, schema: &TableSchema, columns: &[usize], key: &[Value]) -> Result<Vec<RowId>> {
        let mut ids: BTreeSet<RowId> = self
            .base
            .find_live(schema, columns, key)?
            .into_iter()
            .filter(|id| !self.overlay.contains_key(id))
            .collect();
        let Some(wanted) = key_bytes(key) else { return Ok(ids.into_iter().collect()) };
        let mut keys = self.keys.borrow_mut();
        let index = keys.entry((schema.id, columns.to_vec())).or_insert_with(|| {
            let mut index: BTreeMap<Vec<u8>, BTreeSet<RowId>> = BTreeMap::new();
            for (id, (_, row)) in self.overlay.iter().filter(|(_, (t, _))| *t == schema.id) {
                if let Some(k) = row_key(row, columns) {
                    index.entry(k).or_default().insert(*id);
                }
            }
            index
        });
        if let Some(found) = index.get(&wanted) {
            debug_assert!(found.iter().all(|id| {
                let row = &self.overlay[id].1;
                columns.iter().zip(key).all(|(&c, v)| key_eq(&row.cells[c].value, v))
            }));
            ids.extend(found.iter().copied());
        }
        Ok(ids.into_iter().collect())
    }

    // Replaces a row of M(e), and keeps the version it replaces.
    fn replace(&mut self, table: u32, row: Row) -> Result<()> {
        let schema = self.tables[table as usize].clone();
        if let Some(old) = self.get(&schema, row.row_id)? {
            self.replaced.entry(row.row_id).or_default().push(old);
        }
        let previous = self.overlay.get(&row.row_id).map(|(_, r)| r);
        for ((t, columns), index) in self.keys.get_mut().iter_mut().filter(|((t, _), _)| *t == table) {
            debug_assert_eq!(*t, table);
            if let Some(k) = previous.and_then(|r| row_key(r, columns))
                && let Some(set) = index.get_mut(&k)
            {
                set.remove(&row.row_id);
                if set.is_empty() {
                    index.remove(&k);
                }
            }
            if let Some(k) = row_key(&row, columns) {
                index.entry(k).or_default().insert(row.row_id);
            }
        }
        self.overlay.insert(row.row_id, (table, row));
        Ok(())
    }

    fn tombstone(&mut self, table: u32, mut row: Row) -> Result<()> {
        row.deleted = Some(self.epoch);
        self.replace(table, row)
    }

    pub fn constraint_pass(&mut self) -> Result<()> {
        let mut dirty: BTreeSet<RowId> = self.overlay.iter().filter(|(_, (t, _))| *t != 0).map(|(id, _)| *id).collect();
        for _ in 0..MAX_ROUNDS {
            if dirty.is_empty() {
                break;
            }
            let mut next = self.unique_pass(&dirty)?;
            dirty.extend(next.iter().copied());
            next.extend(self.foreign_key_pass(&dirty)?);
            dirty = next;
        }
        self.check_pass()
    }

    // Unique keys: of the live rows with one key value, the row with the
    // earliest stamp on its key columns keeps it. For rows inserted in this
    // epoch, that is the earliest insert; a row that held the value in
    // M(e−1) has an older stamp than any write of this epoch. The other
    // rows become tombstones.
    fn unique_pass(&mut self, dirty: &BTreeSet<RowId>) -> Result<BTreeSet<RowId>> {
        let mut changed = BTreeSet::new();
        for &id in dirty {
            for key_index in 0.. {
                let Some((table, row)) = self.overlay.get(&id).cloned() else { break };
                let schema = self.tables[table as usize].clone();
                let Some(columns) = schema.uniques.get(key_index) else { break };
                if row.is_tombstone() {
                    break;
                }
                let key: Vec<Value> = columns.iter().map(|&c| row.cells[c].value.clone()).collect();
                if key.iter().any(|v| matches!(v, Value::Null)) {
                    continue;
                }
                let group = self.find_live(&schema, columns, &key)?;
                if group.len() < 2 {
                    continue;
                }
                let mut rows = Vec::new();
                for g in group {
                    rows.push(self.get(&schema, g)?.expect("found row"));
                }
                let winner = rows.iter().min_by_key(|r| (newest(r, columns), r.row_id)).unwrap().row_id;
                for loser in rows.into_iter().filter(|r| r.row_id != winner) {
                    let stamp = newest(&loser, columns);
                    let base = self.base.row(&schema, loser.row_id)?.map(|b| values(&b));
                    let rec = Recorded { node: stamp.node, hlc: stamp.hlc, values: values(&loser) };
                    self.add(
                        &schema,
                        loser.row_id,
                        None,
                        ConflictKind::UniqueKey,
                        "earliest_insert",
                        base,
                        vec![rec],
                        None,
                        Some(winner),
                    );
                    changed.insert(loser.row_id);
                    self.tombstone(schema.id, loser)?;
                }
            }
        }
        Ok(changed)
    }

    // Foreign keys: a live child needs a live parent. If the parent was
    // deleted in this merge, the fix undoes the delete (restores the parent
    // from M(e−1)); with ON DELETE CASCADE, it deletes the child instead. A
    // parent whose key changed keeps the change, and the break is recorded.
    fn foreign_key_pass(&mut self, dirty: &BTreeSet<RowId>) -> Result<BTreeSet<RowId>> {
        let mut changed = BTreeSet::new();
        for child in self.tables.to_vec().iter().filter(|t| t.id != 0) {
            for fk in &child.foreign_keys {
                let parent = self.table_named(&fk.parent).clone();
                let pcol = parent.column_index(&fk.parent_column).expect("checked at create_table");
                // Children to check: changed live children, and live children
                // of parent key values that a changed parent row gave up.
                let mut kids = BTreeSet::new();
                for &id in dirty {
                    let Some((table, row)) = self.overlay.get(&id).cloned() else { continue };
                    if table == child.id && !row.is_tombstone() && !matches!(row.cells[fk.column].value, Value::Null) {
                        kids.insert(id);
                    }
                    if table == parent.id {
                        // Every live version of this parent: in M(e−1), and
                        // the ones this pass replaced.
                        let mut versions: Vec<Row> = self.base.row(&parent, id)?.into_iter().collect();
                        versions.extend(self.replaced.get(&id).into_iter().flatten().cloned());
                        for old in versions.iter().filter(|v| !v.is_tombstone()) {
                            let v = &old.cells[pcol].value;
                            if !matches!(v, Value::Null) && (row.is_tombstone() || !key_eq(&row.cells[pcol].value, v)) {
                                kids.extend(self.find_live(child, &[fk.column], std::slice::from_ref(v))?);
                            }
                        }
                    }
                }
                for kid in kids {
                    let Some(row) = self.get(child, kid)?.filter(|r| !r.is_tombstone()) else { continue };
                    let v = row.cells[fk.column].value.clone();
                    if matches!(v, Value::Null)
                        || !self.find_live(&parent, &[pcol], std::slice::from_ref(&v))?.is_empty()
                    {
                        continue;
                    }
                    let stamp = newest(&row, &(0..row.cells.len()).collect::<Vec<_>>());
                    let rec = Recorded { node: stamp.node, hlc: stamp.hlc, values: values(&row) };
                    let base_kid = self.base.row(child, kid)?.map(|b| values(&b));
                    if fk.cascade {
                        self.add(
                            child,
                            kid,
                            None,
                            ConflictKind::ForeignKey,
                            "cascade",
                            base_kid,
                            vec![rec],
                            None,
                            None,
                        );
                        self.tombstone(child.id, row)?;
                        changed.insert(kid);
                        continue;
                    }
                    // The parent that held the value in M(e−1), if it is now
                    // deleted.
                    let mut restorable = None;
                    if let Some(pid) =
                        self.base.find_live(&parent, &[pcol], std::slice::from_ref(&v))?.into_iter().next()
                        && self.get(&parent, pid)?.is_some_and(|r| r.is_tombstone())
                    {
                        restorable = Some(pid);
                    }
                    match restorable {
                        Some(pid) => {
                            let restored = self.base.row(&parent, pid)?.expect("found row");
                            let image = values(&restored);
                            self.add(
                                &parent,
                                pid,
                                None,
                                ConflictKind::ForeignKey,
                                "undo_delete",
                                Some(image.clone()),
                                vec![rec],
                                Some(image),
                                Some(kid),
                            );
                            self.replace(parent.id, restored)?;
                            changed.insert(pid);
                        }
                        None => {
                            let applied = Some(values(&row));
                            self.add(
                                child,
                                kid,
                                None,
                                ConflictKind::ForeignKey,
                                "record",
                                base_kid,
                                vec![rec],
                                applied,
                                None,
                            );
                        }
                    }
                }
            }
        }
        Ok(changed)
    }

    // CHECK constraints: a merged row that breaks one stays as it is, and
    // the break is recorded (white paper, Section VII-F).
    fn check_pass(&mut self) -> Result<()> {
        let rows: Vec<(u32, Row)> =
            self.overlay.values().filter(|(t, r)| *t != 0 && !r.is_tombstone()).cloned().collect();
        for (table, row) in rows {
            let schema = self.tables[table as usize].clone();
            let failed = self.base.failed_checks(&schema, &row)?;
            if failed.is_empty() {
                continue;
            }
            let rule =
                format!("record: {}", failed.iter().map(|&i| schema.checks[i].as_str()).collect::<Vec<_>>().join("; "));
            let base = self.base.row(&schema, row.row_id)?.map(|b| values(&b));
            self.add(&schema, row.row_id, None, ConflictKind::Check, &rule, base, Vec::new(), Some(values(&row)), None);
        }
        Ok(())
    }
}
