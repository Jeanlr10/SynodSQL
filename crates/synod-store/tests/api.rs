//! Functional tests of the store: schema, writes, constraints, reads,
//! seal, and the base switch.

mod common;

use common::*;
use rusqlite::types::Value as Sql;
use synod_core::log::decode_log;
use synod_core::{Op, Value, ValueSet};
use synod_store::{Error, Store};

fn store() -> (Node, Store) {
    let node = Node::new(7, 1);
    let mut s = node.open().unwrap();
    s.create_table(INVENTORY).unwrap();
    (node, s)
}

fn part(sku: &str, qty: i64) -> Vec<(&'static str, Value)> {
    vec![("sku", text(sku)), ("name", text("part")), ("price", dec("0.10")), ("qty", Value::Integer(qty))]
}

#[test]
fn insert_update_delete_and_views() {
    let (_n, mut s) = store();
    let mut t = s.begin().unwrap();
    let a = t.insert("inventory", &part("A-100", 500)).unwrap();
    let b = t.insert("inventory", &part("A-200", 800)).unwrap();
    t.update("inventory", b, &[("qty", Value::Integer(650))]).unwrap();
    t.commit().unwrap();

    assert_eq!(a.node().0, 7);
    assert_eq!(b.counter(), a.counter() + 1);
    let rows = s.query("SELECT sku, price, qty FROM inventory ORDER BY sku", &[]).unwrap();
    assert_eq!(
        rows,
        vec![
            vec![Sql::Text("A-100".into()), Sql::Text("0.1".into()), Sql::Integer(500)],
            vec![Sql::Text("A-200".into()), Sql::Text("0.1".into()), Sql::Integer(650)],
        ]
    );
    // The view shows the row ID under the ROWID column name.
    let ids = s.query("SELECT row_id FROM inventory WHERE sku = ?1", &[text("A-100")]).unwrap();
    assert_eq!(ids, vec![vec![Sql::Blob(a.to_be_bytes().to_vec())]]);

    let mut t = s.begin().unwrap();
    t.delete("inventory", a).unwrap();
    t.commit().unwrap();
    assert_eq!(s.query("SELECT count(*) FROM inventory", &[]).unwrap(), vec![vec![Sql::Integer(1)]]);
    assert_eq!(s.get("inventory", a).unwrap(), None);
    let tomb = s.row("inventory", a).unwrap().unwrap();
    assert!(tomb.tombstone);
    assert_eq!(tomb.cells[0].value, text("A-100"), "a tombstone keeps its values");

    let ops: Vec<Op> = s.all_entries().unwrap().iter().map(|e| e.op).collect();
    assert_eq!(ops, [Op::Insert, Op::Insert, Op::Update, Op::Delete]);
}

#[test]
fn stamps_change_only_for_changed_columns() {
    let (n, mut s) = store();
    let mut t = s.begin().unwrap();
    let a = t.insert("inventory", &part("A-100", 500)).unwrap();
    let first = t.commit().unwrap();
    n.clock.advance(10);
    let mut t = s.begin().unwrap();
    t.update("inventory", a, &[("qty", Value::Integer(400)), ("name", text("part"))]).unwrap();
    let second = t.commit().unwrap();
    assert!(second.hlc > first.hlc);
    let row = s.row("inventory", a).unwrap().unwrap();
    let stamps: Vec<_> = row.cells.iter().map(|c| c.stamp.hlc).collect();
    assert_eq!(stamps, [first.hlc, first.hlc, first.hlc, second.hlc, first.hlc, first.hlc]);
}

#[test]
fn local_constraints() {
    let (_n, mut s) = store();
    let mut t = s.begin().unwrap();
    let a = t.insert("inventory", &part("A-100", 5)).unwrap();
    // Unique among live rows.
    assert!(matches!(t.insert("inventory", &part("A-100", 1)), Err(Error::Constraint(_))));
    // CHECK.
    assert!(matches!(t.update("inventory", a, &[("qty", Value::Integer(-1))]), Err(Error::Constraint(_))));
    // Types, precision, and NaN.
    assert!(t.update("inventory", a, &[("qty", text("many"))]).is_err());
    assert!(t.update("inventory", a, &[("price", dec("0.123"))]).is_err());
    assert!(t.update("inventory", a, &[("name", Value::Integer(1))]).is_err());
    let bad_set = Value::Set(ValueSet::new([Value::Integer(1)]).unwrap());
    assert!(t.update("inventory", a, &[("tags", bad_set)]).is_err());
    // Unknown columns and rows.
    assert!(matches!(t.insert("inventory", &[("nope", Value::Null)]), Err(Error::Invalid(_))));
    assert!(matches!(
        t.update("inventory", synod_core::RowId::new(synod_core::NodeId(7), 99), &[]),
        Err(Error::Invalid(_))
    ));
    // The failed operations left no trace; the transaction goes on.
    t.update("inventory", a, &[("qty", Value::Integer(4))]).unwrap();
    t.delete("inventory", a).unwrap();
    // A deleted key can be used again.
    t.insert("inventory", &part("A-100", 9)).unwrap();
    t.commit().unwrap();
    let ops: Vec<Op> = s.all_entries().unwrap().iter().map(|e| e.op).collect();
    assert_eq!(ops, [Op::Insert, Op::Update, Op::Delete, Op::Insert]);
}

#[test]
fn foreign_keys_and_defaults() {
    let node = Node::new(1, 2);
    let mut s = node.open().unwrap();
    s.create_table("CREATE TABLE customer (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'anon')").unwrap();
    s.create_table("CREATE TABLE note (customer INTEGER REFERENCES customer(id) ON DELETE CASCADE, body TEXT)")
        .unwrap();
    s.create_table("CREATE TABLE orders (customer INTEGER REFERENCES customer(id), total DECIMAL DEFAULT 0)").unwrap();
    assert!(
        s.create_table("CREATE TABLE bad (c INTEGER REFERENCES customer(name))").is_err(),
        "parent column must be UNIQUE"
    );

    let mut t = s.begin().unwrap();
    let c7 = t.insert("customer", &[("id", Value::Integer(7))]).unwrap();
    assert_eq!(t.get("customer", c7).unwrap().unwrap(), vec![Value::Integer(7), text("anon")]);
    assert!(matches!(t.insert("orders", &[("customer", Value::Integer(8))]), Err(Error::Constraint(_))));
    let o = t.insert("orders", &[("customer", Value::Integer(7))]).unwrap();
    assert_eq!(t.get("orders", o).unwrap().unwrap(), vec![Value::Integer(7), dec("0")]);
    t.insert("note", &[("customer", Value::Integer(7)), ("body", text("hi"))]).unwrap();
    // RESTRICT from orders.
    assert!(matches!(t.delete("customer", c7), Err(Error::Constraint(_))));
    assert!(matches!(t.update("customer", c7, &[("id", Value::Integer(9))]), Err(Error::Constraint(_))));
    t.delete("orders", o).unwrap();
    // CASCADE to note.
    t.delete("customer", c7).unwrap();
    t.commit().unwrap();
    assert_eq!(s.query("SELECT count(*) FROM note", &[]).unwrap(), vec![vec![Sql::Integer(0)]]);
    let tables: Vec<u32> = s.all_entries().unwrap().iter().filter(|e| e.op == Op::Delete).map(|e| e.table).collect();
    assert_eq!(tables, [3, 2, 1], "orders, then the cascaded note, then customer (table 0 is _conflicts)");
}

#[test]
fn where_helpers_and_read_only_queries() {
    let (_n, mut s) = store();
    let mut t = s.begin().unwrap();
    for (i, sku) in ["A", "B", "C"].iter().enumerate() {
        t.insert("inventory", &part(sku, i as i64)).unwrap();
    }
    let changed =
        t.update_where("inventory", "qty >= ?1", &[Value::Integer(1)], &[("qty", Value::Integer(0))]).unwrap();
    assert_eq!(changed.len(), 2);
    assert_eq!(t.delete_where("inventory", "sku = 'A'", &[]).unwrap().len(), 1);
    assert!(t.find("inventory", "1; DROP TABLE x", &[]).is_err());
    t.commit().unwrap();
    assert!(matches!(s.query("DELETE FROM _synod_t_inventory", &[]), Err(Error::Invalid(_))));
    assert_eq!(s.query("SELECT sum(qty) FROM inventory", &[]).unwrap(), vec![vec![Sql::Integer(0)]]);
}

#[test]
fn dropped_transaction_rolls_back() {
    let (_n, mut s) = store();
    {
        let mut t = s.begin().unwrap();
        t.insert("inventory", &part("A", 1)).unwrap();
    }
    assert!(s.all_entries().unwrap().is_empty());
    let mut t = s.begin().unwrap();
    let id = t.insert("inventory", &part("A", 1)).unwrap();
    t.commit().unwrap();
    assert_eq!(id.counter(), 0, "a rolled-back insert does not use up its row ID");
}

#[test]
fn seal_closes_the_epoch() {
    let (n, mut s) = store();
    assert!(matches!(s.seal(), Err(Error::TooEarly { epoch: 0, .. })));
    let mut t = s.begin().unwrap();
    t.insert("inventory", &part("A", 1)).unwrap();
    assert_eq!(t.commit().unwrap().epoch, 0);

    n.clock.set(START_MS + EPOCH_MS);
    let sealed = s.seal().unwrap();
    assert_eq!((sealed.epoch, sealed.entries), (0, 1));
    assert_eq!(sealed.hash, synod_core::log::log_hash(&sealed.bytes));
    let (header, entries) = decode_log(&sealed.bytes).unwrap();
    assert_eq!(header.epoch, 0);
    assert_eq!(header.node.0, 7);
    assert_eq!(header.base_root, synod_core::prolly::empty_root());
    assert!(header.sealed_at.physical_ms() >= START_MS + EPOCH_MS);
    assert_eq!(entries, s.all_entries().unwrap());
    assert_eq!(sealed.root, s.state_hash_from_scratch().unwrap());
    assert_eq!(s.sealed(0).unwrap().unwrap(), sealed);
    assert_eq!(s.open_epoch().unwrap(), 1);

    // The clock is behind the boundary: new writes still go to epoch 1.
    n.clock.set(START_MS + EPOCH_MS - 5_000);
    let mut t = s.begin().unwrap();
    t.insert("inventory", &part("B", 1)).unwrap();
    assert_eq!(t.commit().unwrap().epoch, 1);
    assert!(s.unsealed_entries().unwrap().len() == 1);
}

#[test]
fn reopen_keeps_state_and_clock() {
    let (n, mut s) = store();
    let mut t = s.begin().unwrap();
    let a = t.insert("inventory", &part("A", 1)).unwrap();
    let c = t.commit().unwrap();
    drop(s);
    // The physical clock goes backward over the restart.
    n.clock.set(START_MS - 10_000);
    let mut s = n.open().unwrap();
    assert_eq!(s.tables().len(), 2, "_conflicts and inventory");
    let mut t = s.begin().unwrap();
    assert!(t.hlc() > c.hlc, "the HLC must not go backward across a restart");
    let b = t.insert("inventory", &part("B", 1)).unwrap();
    t.commit().unwrap();
    assert!(b > a);
    let mut wrong = n.config.clone();
    wrong.node = synod_core::NodeId(8);
    drop(s);
    assert!(Store::open(PATH, wrong, n.env()).is_err());
}

#[test]
fn rewrite_switches_to_an_equal_copy() {
    let (_n, mut s) = store();
    let mut t = s.begin().unwrap();
    for i in 0..50 {
        t.insert("inventory", &part(&format!("S-{i}"), i)).unwrap();
    }
    t.commit().unwrap();
    let root = s.state_hash().unwrap();
    let entries = s.all_entries().unwrap();
    let s = s.rewrite().unwrap();
    assert_eq!(s.state_hash_from_scratch().unwrap(), root);
    assert_eq!(s.all_entries().unwrap(), entries);
}

#[test]
fn put_row_writes_without_a_log_entry() {
    let (_n, mut s) = store();
    let mut t = s.begin().unwrap();
    let id = t.insert("inventory", &part("A", 1)).unwrap();
    t.commit().unwrap();
    let mut row = s.row("inventory", id).unwrap().unwrap();
    row.row_id = synod_core::RowId::new(synod_core::NodeId(7), 40);
    row.cells[0].value = text("B");
    let mut t = s.begin().unwrap();
    t.put_row("inventory", &row).unwrap();
    let mut wrong = row.clone();
    wrong.cells[3].value = dec("1");
    assert!(t.put_row("inventory", &wrong).is_err(), "a value must already have the column's type");
    // The counter moves past an imported row of this node.
    assert_eq!(t.insert("inventory", &part("C", 1)).unwrap().counter(), 41);
    t.commit().unwrap();
    assert_eq!(s.all_entries().unwrap().len(), 2);
    assert_eq!(s.state_hash().unwrap(), s.state_hash_from_scratch().unwrap());
}
