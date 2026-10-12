//! Cross-platform test: a fixed set of writes gives a pinned state hash.
//! CI runs this on x86-64 and ARM64. The rows are also built by hand, so
//! the store is checked against an independent construction.

mod common;

use common::*;
use synod_core::{Cell, Hlc, NodeId, Row, RowId, Stamp, Value, ValueSet};

#[test]
fn fixed_writes_give_the_pinned_state_hash() {
    let node = Node::new(5, 0);
    let mut s = node.open().unwrap();
    s.create_table(INVENTORY).unwrap();
    let parts =
        [("A-100", "Bolt, M6", "0.10", 500), ("A-200", "Nut, M6", "0.05", 800), ("B-310", "Washer, M6", "0.02", 1000)];
    let mut ids = Vec::new();
    for (sku, name, price, qty) in parts {
        let mut t = s.begin().unwrap();
        let tags = Value::Set(ValueSet::new([text("m6")]).unwrap());
        ids.push(
            t.insert(
                "inventory",
                &[
                    ("sku", text(sku)),
                    ("name", text(name)),
                    ("price", dec(price)),
                    ("qty", Value::Integer(qty)),
                    ("tags", tags),
                ],
            )
            .unwrap(),
        );
        t.commit().unwrap();
    }
    node.clock.advance(1_000);
    let mut t = s.begin().unwrap();
    t.update("inventory", ids[0], &[("price", dec("0.12"))]).unwrap();
    t.delete("inventory", ids[2]).unwrap();
    t.commit().unwrap();

    // The same rows, by hand. The clock resumes after (start of epoch 0,
    // 0), so the three inserts get logical 1, 2, 3 in that millisecond;
    // after the clock moves, the update gets logical 0.
    let stamp = |ms: u64, logical: u16| Stamp { hlc: Hlc::new(ms, logical).unwrap(), node: NodeId(5) };
    let row = |i: usize, tomb: bool| {
        let (sku, name, price, qty) = parts[i];
        let s = stamp(START_MS, i as u16 + 1);
        let values = [
            text(sku),
            text(name),
            dec(price),
            Value::Integer(qty),
            Value::Set(ValueSet::new([text("m6")]).unwrap()),
            Value::Null,
        ];
        Row {
            row_id: RowId::new(NodeId(5), i as u64),
            tombstone: tomb,
            cells: values.into_iter().map(|value| Cell { stamp: s, value }).collect(),
        }
    };
    let mut r0 = row(0, false);
    r0.cells[2] = Cell { stamp: stamp(START_MS + 1_000, 0), value: dec("0.12") };
    let expected = [r0, row(1, false), row(2, true)];
    for r in &expected {
        assert_eq!(s.row("inventory", r.row_id).unwrap().as_ref(), Some(r));
    }
    let root = s.state_hash().unwrap();
    assert_eq!(root, synod_core::prolly::root_hash(expected.iter().map(|r| (r.row_id, r.hash()))));
    let hex: String = root.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, "7f3431351f36d3768ca1132f09e7f5f96625102377d1ce1c82b5e2becd5c452c");
}
