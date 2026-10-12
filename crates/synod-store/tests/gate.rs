//! Phase 1 exit gate.
//!
//! 1. Every committed write appears exactly once in the epoch log, with
//!    correct images.
//! 2. The same set of rows, inserted in two different orders, gives the
//!    same root hash.
//! 3. A simulated crash at any point loses no committed write.
//!
//! Gates 1 and 3 run one workload against an independent model. The crash
//! file system dies at a chosen write; the test then loses unsynced data,
//! reopens the store, and checks the log and the state against the model.

mod common;

use std::collections::BTreeMap;

use common::*;
use synod_core::log::decode_log;
use synod_core::{Cell, Hlc, LogEntry, NodeId, Op, Row, RowId, Stamp, Value, ValueSet};
use synod_sim::{SimRng, check_seeds};
use synod_store::{Error, Store};

const COLUMNS: [&str; 6] = ["sku", "name", "price", "qty", "tags", "notes"];

/// What the test knows: the committed rows and the log entries, by epoch.
#[derive(Clone, Default)]
struct Model {
    rows: BTreeMap<RowId, (bool, Vec<Value>)>,
    entries: Vec<(u64, LogEntry)>,
}

impl Model {
    fn live(&self) -> Vec<RowId> {
        self.rows.iter().filter(|(_, (tomb, _))| !tomb).map(|(id, _)| *id).collect()
    }
}

/// A random item of a list of strings.
fn pick<'a>(rng: &mut SimRng, items: &[&'a str]) -> &'a str {
    items[rng.below(items.len() as u64) as usize]
}

fn random_value(rng: &mut SimRng, column: usize) -> Value {
    match column {
        0 => text(&format!("S-{}", rng.below(12))),
        1 => text(pick(rng, &["bolt", "nut", "washer", "spacer"])),
        2 => dec(pick(rng, &["0.10", "0.11", "1.5", "2", "0.05"])),
        3 => Value::Integer(rng.range(0, 30) as i64 - 3),
        4 => {
            let items = ["a", "b", "c"].iter().filter(|_| rng.chance(500_000)).map(|s| text(s));
            Value::Set(ValueSet::new(items).unwrap())
        }
        _ => {
            if rng.chance(300_000) {
                Value::Null
            } else {
                text(&format!("note {}", rng.below(5)))
            }
        }
    }
}

enum Step {
    Done,
    Crashed,
}

/// Runs `steps` random steps. Ops that break a constraint are skipped. A
/// transaction whose commit fails is returned as `in_flight`.
fn workload(
    store: &mut Option<Store>,
    node: &Node,
    rng: &mut SimRng,
    model: &mut Model,
    in_flight: &mut Option<Model>,
    steps: usize,
) -> Step {
    macro_rules! io {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(e) => {
                    assert!(node.fs.is_dead(), "error while the disk works: {e}");
                    return Step::Crashed;
                }
            }
        };
    }
    for _ in 0..steps {
        let s = store.as_mut().unwrap();
        match rng.below(100) {
            0..80 => {
                let mut next = model.clone();
                let mut t = io!(s.begin());
                let (txn, hlc) = (t.id(), t.hlc());
                let epoch = s_epoch(node, hlc);
                for _ in 0..rng.range(1, 3) {
                    let live = next.live();
                    let choice = if live.is_empty() { 0 } else { rng.below(3) };
                    let result = match choice {
                        0 => {
                            let values: Vec<Value> = (0..6).map(|c| random_value(rng, c)).collect();
                            let cols: Vec<(&str, Value)> = COLUMNS.iter().copied().zip(values.clone()).collect();
                            t.insert("inventory", &cols).map(|id| {
                                assert!(!next.rows.contains_key(&id), "row ID {id} given out twice");
                                let e = entry(Op::Insert, id, txn, hlc, None, Some(values.clone()));
                                next.rows.insert(id, (false, values));
                                next.entries.push((epoch, e));
                            })
                        }
                        1 => {
                            let id = *rng.pick(&live);
                            let before = next.rows[&id].1.clone();
                            let mut after = before.clone();
                            let mut changes = Vec::new();
                            for (c, name) in COLUMNS.iter().enumerate() {
                                if rng.chance(400_000) {
                                    let v = random_value(rng, c);
                                    after[c] = v.clone();
                                    changes.push((*name, v));
                                }
                            }
                            t.update("inventory", id, &changes).map(|()| {
                                next.entries
                                    .push((epoch, entry(Op::Update, id, txn, hlc, Some(before), Some(after.clone()))));
                                next.rows.insert(id, (false, after));
                            })
                        }
                        _ => {
                            let id = *rng.pick(&live);
                            let before = next.rows[&id].1.clone();
                            t.delete("inventory", id).map(|()| {
                                next.entries.push((epoch, entry(Op::Delete, id, txn, hlc, Some(before.clone()), None)));
                                next.rows.insert(id, (true, before));
                            })
                        }
                    };
                    match result {
                        Ok(()) | Err(Error::Constraint(_)) => {}
                        Err(e) => {
                            assert!(node.fs.is_dead(), "error while the disk works: {e}");
                            return Step::Crashed;
                        }
                    }
                }
                match t.commit() {
                    Ok(c) => {
                        assert_eq!(c.epoch, epoch);
                        *model = next;
                    }
                    Err(e) => {
                        assert!(node.fs.is_dead(), "commit failed while the disk works: {e}");
                        *in_flight = Some(next);
                        return Step::Crashed;
                    }
                }
            }
            80..90 => {
                node.clock.advance(EPOCH_MS / 2);
                match s.seal() {
                    Ok(_) | Err(Error::TooEarly { .. }) => {}
                    Err(e) => {
                        assert!(node.fs.is_dead(), "seal failed while the disk works: {e}");
                        return Step::Crashed;
                    }
                }
            }
            90..95 => {
                let s = store.take().unwrap();
                *store = Some(io!(s.rewrite()));
            }
            _ => {
                drop(store.take());
                *store = Some(io!(node.open()));
            }
        }
        node.clock.advance(rng.range(0, 2_000));
    }
    Step::Done
}

fn s_epoch(_node: &Node, hlc: Hlc) -> u64 {
    (hlc.physical_ms() - START_MS) / EPOCH_MS
}

fn entry(
    op: Op,
    row_id: RowId,
    txn: synod_core::TxnId,
    hlc: Hlc,
    before: Option<Vec<Value>>,
    after: Option<Vec<Value>>,
) -> LogEntry {
    LogEntry { op, table: 0, row_id, txn, hlc, before, after }
}

/// Checks the reopened store against the model, or against the model with
/// the in-flight transaction, whichever the log shows. Returns the model
/// that matched.
fn verify(store: &mut Store, model: &Model, in_flight: Option<&Model>) -> Model {
    let entries = store.all_entries().unwrap();
    let expected: Vec<LogEntry> = model.entries.iter().map(|(_, e)| e.clone()).collect();
    let matched = if entries == expected {
        model.clone()
    } else {
        let flight = in_flight.unwrap_or_else(|| panic!("log has {} entries, model {}", entries.len(), expected.len()));
        let with: Vec<LogEntry> = flight.entries.iter().map(|(_, e)| e.clone()).collect();
        assert_eq!(entries, with, "log is neither the committed writes nor those plus the in-flight transaction");
        flight.clone()
    };

    // Gate 1, images: replaying the log gives every stored row exactly,
    // stamps included.
    let mut replayed = BTreeMap::new();
    replay(&mut replayed, &entries);
    assert_eq!(replayed.len(), matched.rows.len());
    for (id, row) in &replayed {
        assert_eq!(store.row("inventory", *id).unwrap().as_ref(), Some(row), "row {id}");
        let (tomb, values) = &matched.rows[id];
        assert_eq!(row.tombstone, *tomb);
        assert_eq!(&row.cells.iter().map(|c| c.value.clone()).collect::<Vec<_>>(), values);
    }
    let stored = store.query("SELECT count(*) FROM _synod_t_inventory", &[]).unwrap();
    assert_eq!(stored[0][0], rusqlite::types::Value::Integer(matched.rows.len() as i64));

    // Sealed logs hold exactly the entries of their epoch.
    for epoch in 0..store.open_epoch().unwrap() {
        if let Some(sealed) = store.sealed(epoch).unwrap() {
            let (header, logged) = decode_log(&sealed.bytes).unwrap();
            assert_eq!(header.epoch, epoch);
            let want: Vec<LogEntry> =
                matched.entries.iter().filter(|(e, _)| *e == epoch).map(|(_, e)| e.clone()).collect();
            assert_eq!(logged, want, "sealed log of epoch {epoch}");
        }
    }
    assert_eq!(store.state_hash().unwrap(), store.state_hash_from_scratch().unwrap());
    matched
}

/// Runs the workload, crashes at `crash_at` (if given), recovers, checks,
/// runs more, and checks again. Returns the number of disk operations.
fn crash_run(seed: u64, steps: usize, crash_at: Option<u64>) -> u64 {
    let node = Node::new(3, seed);
    let mut rng = SimRng::new(seed);
    let mut store = Some(node.open().unwrap());
    store.as_mut().unwrap().create_table(INVENTORY).unwrap();
    let mut model = Model::default();
    let mut in_flight = None;
    let start = node.fs.ops();
    if let Some(n) = crash_at {
        node.fs.crash_after(n);
    }
    let step = workload(&mut store, &node, &mut rng, &mut model, &mut in_flight, steps);
    drop(store.take());
    // Operations of the workload and the close, counted from the arming.
    let ops = node.fs.ops() - start;
    node.fs.disarm();
    if matches!(step, Step::Crashed) || node.fs.is_dead() {
        node.fs.crash(&mut rng);
    }
    let mut store = Some(node.open().unwrap_or_else(|e| panic!("reopen after crash: {e}")));
    model = verify(store.as_mut().unwrap(), &model, in_flight.as_ref());

    // The recovered store keeps working, and never reuses a row ID.
    let mut in_flight = None;
    assert!(matches!(workload(&mut store, &node, &mut rng, &mut model, &mut in_flight, 20), Step::Done));
    verify(store.as_mut().unwrap(), &model, None);
    ops
}

#[test]
fn log_is_exact_and_crashes_lose_nothing() {
    check_seeds("log_is_exact_and_crashes_lose_nothing", 40, |seed| {
        let total = crash_run(seed, 60, None);
        let mut rng = SimRng::new(seed ^ 0xC0FFEE);
        crash_run(seed, 60, Some(rng.below(total)));
    });
}

/// Gate 3 at every point: for each disk operation of a short run, crash
/// exactly there.
#[test]
fn crash_at_every_write() {
    let seed = 11;
    let total = crash_run(seed, 15, None);
    assert!(total > 50, "run too short: {total} operations");
    for at in 0..total {
        crash_run(seed, 15, Some(at));
    }
}

// ------------------------------------------------------------ gate 2

fn random_row(rng: &mut SimRng, id: RowId) -> Row {
    let cells = (0..6)
        .map(|c| Cell {
            stamp: Stamp { hlc: Hlc::from_u64(rng.next_u64() >> 1), node: NodeId(rng.below(4) as u32) },
            // Live rows of a merged state never share a unique key, so each
            // row gets its own SKU.
            value: match c {
                0 => text(&format!("S-{id}")),
                3 => Value::Integer(rng.below(100) as i64),
                _ => random_value(rng, c),
            },
        })
        .collect();
    Row { row_id: id, tombstone: rng.chance(200_000), cells }
}

fn put_all(node: &Node, rows: &[Row]) -> Store {
    let mut s = node.open().unwrap();
    s.create_table(INVENTORY).unwrap();
    let mut t = s.begin().unwrap();
    for r in rows {
        t.put_row("inventory", r).unwrap();
    }
    t.commit().unwrap();
    s
}

/// Gate 2: the same set of rows gives the same root hash, whatever the
/// order and history of the writes.
#[test]
fn same_rows_same_root_in_any_order() {
    check_seeds("same_rows_same_root_in_any_order", 20, |seed| {
        let mut rng = SimRng::new(seed);
        let ids: Vec<RowId> = (0..rng.range(0, 400))
            .map(|_| RowId::new(NodeId(rng.below(5) as u32), rng.below(1_000_000)))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let rows: Vec<Row> = ids.iter().map(|&id| random_row(&mut rng, id)).collect();

        let a = put_all(&Node::new(1, seed), &rows);

        // Node B: shuffled order, and older versions of some rows first.
        let mut shuffled = rows.clone();
        for i in (1..shuffled.len()).rev() {
            shuffled.swap(i, rng.below(i as u64 + 1) as usize);
        }
        let mut with_history: Vec<Row> = Vec::new();
        for r in &shuffled {
            if rng.chance(300_000) {
                with_history.push(random_row(&mut rng, r.row_id));
            }
        }
        with_history.extend(shuffled);
        let b_node = Node::new(2, seed + 1);
        let mut b = put_all(&b_node, &with_history);

        let mut a = a;
        let root = a.state_hash().unwrap();
        assert_eq!(b.state_hash().unwrap(), root);
        assert_eq!(a.state_hash_from_scratch().unwrap(), root);
        let reference = synod_core::prolly::root_hash(rows.iter().map(|r| (r.row_id, r.hash())));
        assert_eq!(root, reference);
        // A rewrite of the file does not change the hash.
        let mut b = b.rewrite().unwrap();
        assert_eq!(b.state_hash().unwrap(), root);
        drop(b_node);
    });
}

/// A second node that applies the first node's log gets the same state
/// and root hash: the log carries everything the state needs.
#[test]
fn log_shipping_reproduces_the_state() {
    check_seeds("log_shipping_reproduces_the_state", 10, |seed| {
        let a = Node::new(3, seed);
        let mut rng = SimRng::new(seed);
        let mut store = Some(a.open().unwrap());
        store.as_mut().unwrap().create_table(INVENTORY).unwrap();
        let (mut model, mut in_flight) = (Model::default(), None);
        assert!(matches!(workload(&mut store, &a, &mut rng, &mut model, &mut in_flight, 80), Step::Done));
        let mut source = store.unwrap();
        let entries = source.all_entries().unwrap();

        let mut rows = BTreeMap::new();
        replay(&mut rows, &entries);
        let b = Node::new(9, seed);
        let mut target = put_all(&b, &rows.into_values().collect::<Vec<_>>());
        assert_eq!(target.state_hash().unwrap(), source.state_hash().unwrap());
    });
}
