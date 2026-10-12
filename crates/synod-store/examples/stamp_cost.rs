//! Measures the space cost of per-column stamps (Phase 1 note in the
//! roadmap).
//!
//! Fills the white paper's inventory table with N rows three ways, and
//! prints bytes per row from SQLite's `dbstat`:
//!
//! - plain SQLite, the same columns, no replication data;
//! - SynodSQL as built: per-column stamps, stored as each distinct stamp
//!   once plus a 1-byte index per column;
//! - the alternative: one 12-byte stamp per row and a 2-byte version per
//!   column.
//!
//! Run with `cargo run --release -p synod-store --example stamp_cost [N]`.

use std::sync::Arc;

use rusqlite::Connection;
use synod_core::{NodeId, Value, ValueSet};
use synod_store::{Env, ManualClock, Store, StoreConfig};

const INVENTORY: &str = "CREATE TABLE inventory (
    row_id ROWID, sku TEXT UNIQUE, name TEXT, price DECIMAL(10,2) MERGE lww_flag,
    qty INTEGER CHECK (qty >= 0) MERGE delta, tags SET<TEXT> MERGE union, notes TEXT MERGE text3)";

fn row(i: u64) -> [Value; 6] {
    let tags = ["m6", "steel", "zinc"].iter().take((i % 3) as usize + 1).map(|t| Value::Text(t.to_string()));
    [
        Value::Text(format!("SKU-{i:07}")),
        Value::Text(format!("Part number {i}")),
        Value::Decimal(synod_core::Decimal::new((i % 10_000) as i128, 2).unwrap()),
        Value::Integer((i * 37 % 5_000) as i64),
        Value::Set(ValueSet::new(tags).unwrap()),
        if i.is_multiple_of(4) { Value::Text("reorder at 100".into()) } else { Value::Null },
    ]
}

fn bytes(c: &Connection, name: &str) -> u64 {
    c.query_row("SELECT coalesce(sum(pgsize), 0) FROM dbstat WHERE name = ?1", [name], |r| r.get::<_, i64>(0)).unwrap()
        as u64
}

fn main() {
    let n: u64 = std::env::args().nth(1).map_or(50_000, |a| a.parse().expect("N"));
    let dir = std::env::temp_dir().join(format!("synod-stamp-cost-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cols = ["sku", "name", "price", "qty", "tags", "notes"];

    // Plain SQLite.
    let plain = Connection::open(dir.join("plain.db")).unwrap();
    plain
        .execute_batch(
            "CREATE TABLE inventory (sku TEXT UNIQUE, name TEXT, price TEXT, qty INTEGER CHECK (qty >= 0), tags BLOB, notes TEXT) STRICT; BEGIN;",
        )
        .unwrap();
    for i in 0..n {
        let v = row(i);
        plain
            .execute(
                "INSERT INTO inventory VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params_from_iter(v.iter().map(synod_store::values::to_sql)),
            )
            .unwrap();
    }
    plain.execute_batch("COMMIT").unwrap();
    let plain_table = bytes(&plain, "inventory");
    let plain_index = bytes(&plain, "sqlite_autoindex_inventory_1");

    // SynodSQL.
    let path = dir.join("synod.db");
    let env = Env { clock: Arc::new(ManualClock::new(1_791_763_200_000)), ..Env::os() };
    let config =
        StoreConfig { node: NodeId(1), genesis_ms: 1_791_763_200_000, epoch_len_ms: 60_000, max_drift_ms: 500 };
    let mut store = Store::open(path.to_str().unwrap(), config, env).unwrap();
    store.create_table(INVENTORY).unwrap();
    let mut t = store.begin().unwrap();
    for i in 0..n {
        let v = row(i);
        let given: Vec<(&str, Value)> = cols.iter().copied().zip(v).collect();
        t.insert("inventory", &given).unwrap();
    }
    t.commit().unwrap();
    store.state_hash().unwrap();
    drop(store);
    let synod = Connection::open(&path).unwrap();
    let table = bytes(&synod, "_synod_t_inventory");
    let index = bytes(&synod, "_synod_u_inventory_0");
    let log = bytes(&synod, "_synod_log");
    let rowhash = bytes(&synod, "_synod_rowhash");

    // The alternative layout: same table, with 12 + 2 × 6 bytes of stamps
    // (only the size matters here).
    synod
        .execute_batch(
            "CREATE TABLE alt AS SELECT * FROM _synod_t_inventory WHERE 0;
             INSERT INTO alt SELECT _row_id, _tomb, substr(_stamps, 1, 12) || zeroblob(12), sku, name, price, qty, tags, notes
             FROM _synod_t_inventory;",
        )
        .unwrap();
    let alt = bytes(&synod, "alt");

    let per = |b: u64| b as f64 / n as f64;
    println!("rows: {n}");
    println!("{:<44} {:>9}", "layout", "bytes/row");
    println!("{:<44} {:>9.1}", "plain SQLite table", per(plain_table));
    println!("{:<44} {:>9.1}", "SynodSQL table, distinct stamps + index", per(table));
    println!("{:<44} {:>9.1}", "SynodSQL table, stamp per row + u16 versions", per(alt));
    println!("{:<44} {:>9.1}", "  replication cost over plain (built)", per(table) - per(plain_table));
    println!("{:<44} {:>9.1}", "  further saving of the alternative", per(table) - per(alt));
    println!("{:<44} {:>9.1}", "unique index (plain / SynodSQL)", per(plain_index));
    println!("{:<44} {:>9.1}", "", per(index));
    println!("{:<44} {:>9.1}", "row hash cache", per(rowhash));
    println!("{:<44} {:>9.1}", "epoch log (one insert entry per row)", per(log));
    std::fs::remove_dir_all(&dir).ok();
}
