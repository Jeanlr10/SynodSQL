//! A base state in memory.

use std::collections::BTreeMap;
use std::sync::Arc;

use rusqlite::Connection;
use synod_core::prolly::{NodeHash, root_hash};
use synod_core::{Row, RowId, Value};

use super::policy::key_eq;
use super::{BaseState, MergeOutput, failed_checks};
use crate::error::Result;
use crate::schema::TableSchema;

/// A whole state in memory: rows by ID, with their table IDs. Use it to run
/// the merge without a store, for example in tests.
pub struct MemState {
    tables: Vec<Arc<TableSchema>>,
    rows: BTreeMap<RowId, (u32, Row)>,
    // Evaluates CHECK constraints, as the store does.
    sql: Connection,
}

impl MemState {
    /// Makes an empty state for these tables (table 0 is `_conflicts`).
    pub fn new(tables: Vec<Arc<TableSchema>>) -> Result<MemState> {
        Ok(MemState { tables, rows: BTreeMap::new(), sql: Connection::open_in_memory()? })
    }

    /// Returns a copy with its own SQL connection.
    pub fn try_clone(&self) -> Result<MemState> {
        Ok(MemState { tables: self.tables.clone(), rows: self.rows.clone(), sql: Connection::open_in_memory()? })
    }

    /// Returns the tables.
    pub fn tables(&self) -> &[Arc<TableSchema>] {
        &self.tables
    }

    /// Returns all rows.
    pub fn rows(&self) -> &BTreeMap<RowId, (u32, Row)> {
        &self.rows
    }

    /// Writes a row as it is.
    pub fn put(&mut self, table: u32, row: Row) {
        self.rows.insert(row.row_id, (table, row));
    }

    /// Applies the result of a merge: this state becomes M(e).
    pub fn apply(&mut self, output: &MergeOutput) {
        for (id, (table, row)) in &output.rows {
            self.rows.insert(*id, (*table, row.clone()));
        }
    }

    /// Returns the state hash.
    pub fn root(&self) -> NodeHash {
        root_hash(self.rows.iter().map(|(id, (_, row))| (*id, row.hash())))
    }
}

impl BaseState for MemState {
    fn row(&self, table: &TableSchema, id: RowId) -> Result<Option<Row>> {
        Ok(self.rows.get(&id).filter(|(t, _)| *t == table.id).map(|(_, r)| r.clone()))
    }

    fn find_live(&self, table: &TableSchema, columns: &[usize], values: &[Value]) -> Result<Vec<RowId>> {
        Ok(self
            .rows
            .iter()
            .filter(|(_, (t, r))| {
                *t == table.id && !r.tombstone && columns.iter().zip(values).all(|(&c, v)| key_eq(&r.cells[c].value, v))
            })
            .map(|(id, _)| *id)
            .collect())
    }

    fn failed_checks(&self, table: &TableSchema, row: &Row) -> Result<Vec<usize>> {
        failed_checks(&self.sql, table, row)
    }
}
