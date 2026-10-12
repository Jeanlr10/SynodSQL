//! Differential test: one SynodSQL node gives the same query results as
//! plain SQLite, for the same writes.
//!
//! Plain SQLite holds the same table with real UNIQUE and CHECK
//! constraints. Both get the same random operations. Each operation must
//! succeed on both or fail on both, and the visible rows must stay equal.

mod common;

use std::collections::BTreeMap;

use common::*;
use rusqlite::Connection;
use rusqlite::types::Value as Sql;
use synod_core::{RowId, Value, ValueSet};
use synod_sim::{SimRng, check_seeds};
use synod_store::Error;
use synod_store::values::to_sql;

const PLAIN: &str = "CREATE TABLE inventory (
    sku TEXT UNIQUE, name TEXT, price TEXT, qty INTEGER CHECK (qty >= 0), tags BLOB, notes TEXT
) STRICT";

const COLUMNS: [&str; 6] = ["sku", "name", "price", "qty", "tags", "notes"];

/// A random item of a list of strings.
fn pick<'a>(rng: &mut SimRng, items: &[&'a str]) -> &'a str {
    items[rng.below(items.len() as u64) as usize]
}

fn value(rng: &mut SimRng, column: usize) -> Value {
    match column {
        0 => text(&format!("S-{}", rng.below(15))),
        1 => text(pick(rng, &["bolt", "nut", "washer"])),
        2 => dec(pick(rng, &["0.10", "0.11", "1.50", "2"])),
        3 => Value::Integer(rng.range(0, 40) as i64 - 4),
        4 => Value::Set(ValueSet::new(["x", "y"].iter().filter(|_| rng.chance(500_000)).map(|s| text(s))).unwrap()),
        _ if rng.chance(300_000) => Value::Null,
        _ => text(&format!("n{}", rng.below(4))),
    }
}

fn plain_rows(c: &Connection) -> Vec<Vec<Sql>> {
    let mut stmt = c.prepare("SELECT sku, name, price, qty, tags, notes FROM inventory ORDER BY sku").unwrap();
    stmt.query_map([], |r| (0..6).map(|i| r.get(i)).collect()).unwrap().map(Result::unwrap).collect()
}

#[test]
fn same_results_as_plain_sqlite() {
    check_seeds("same_results_as_plain_sqlite", 30, |seed| {
        let mut rng = SimRng::new(seed);
        let node = Node::new(1, seed);
        let mut store = node.open().unwrap();
        store.create_table(INVENTORY).unwrap();
        let plain = Connection::open_in_memory().unwrap();
        plain.execute_batch(PLAIN).unwrap();
        // SynodSQL row ID -> plain SQLite rowid, for live rows.
        let mut ids: BTreeMap<RowId, i64> = BTreeMap::new();
        let (mut accepted, mut rejected) = (0, 0);

        for _ in 0..150 {
            let mut txn = store.begin().unwrap();
            plain.execute_batch("BEGIN").unwrap();
            let mut new_ids = ids.clone();
            for _ in 0..rng.range(1, 4) {
                let live: Vec<RowId> = new_ids.keys().copied().collect();
                let op = if live.is_empty() { 0 } else { rng.below(3) };
                let (ours, theirs) = match op {
                    0 => {
                        let values: Vec<Value> = (0..6).map(|c| value(&mut rng, c)).collect();
                        let cols: Vec<(&str, Value)> = COLUMNS.iter().copied().zip(values.clone()).collect();
                        let ours = txn.insert("inventory", &cols);
                        let theirs = plain.execute(
                            "INSERT INTO inventory VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            rusqlite::params_from_iter(values.iter().map(to_sql)),
                        );
                        if let (Ok(id), Ok(_)) = (&ours, &theirs) {
                            new_ids.insert(*id, plain.last_insert_rowid());
                        }
                        (ours.map(|_| ()), theirs.map(|_| ()))
                    }
                    1 => {
                        let id = *rng.pick(&live);
                        let column = rng.below(6) as usize;
                        let v = value(&mut rng, column);
                        let ours = txn.update("inventory", id, &[(COLUMNS[column], v.clone())]);
                        let sql = format!("UPDATE inventory SET {} = ?1 WHERE rowid = ?2", COLUMNS[column]);
                        let theirs = plain.execute(&sql, rusqlite::params![to_sql(&v), new_ids[&id]]);
                        (ours, theirs.map(|_| ()))
                    }
                    _ => {
                        let id = *rng.pick(&live);
                        let ours = txn.delete("inventory", id);
                        let theirs = plain.execute("DELETE FROM inventory WHERE rowid = ?1", [new_ids[&id]]);
                        if ours.is_ok() {
                            new_ids.remove(&id);
                        }
                        (ours, theirs.map(|_| ()))
                    }
                };
                match (&ours, &theirs) {
                    (Ok(()), Ok(())) => accepted += 1,
                    (Err(Error::Constraint(_)), Err(rusqlite::Error::SqliteFailure(f, _)))
                        if f.code == rusqlite::ErrorCode::ConstraintViolation =>
                    {
                        rejected += 1
                    }
                    _ => panic!("seed {seed}: SynodSQL {ours:?}, SQLite {theirs:?}"),
                }
            }
            // Commit or roll back both, at random.
            if rng.chance(850_000) {
                txn.commit().unwrap();
                plain.execute_batch("COMMIT").unwrap();
                ids = new_ids;
            } else {
                drop(txn);
                plain.execute_batch("ROLLBACK").unwrap();
            }
            let ours =
                store.query("SELECT sku, name, price, qty, tags, notes FROM inventory ORDER BY sku", &[]).unwrap();
            assert_eq!(ours, plain_rows(&plain), "seed {seed}");
        }
        assert!(accepted > 100 && rejected > 10, "accepted {accepted}, rejected {rejected}");
    });
}
