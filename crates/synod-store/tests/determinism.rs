//! Phase 2 exit gate 1: random histories with 2 to 10 nodes. Every order of
//! the logs gives the same M(e), the same `_conflicts` rows, and the same
//! root hash.
//!
//! Each history has two tables that use every merge policy, a random delete
//! rule, and a random ON DELETE CASCADE. Each node writes to its own copy of
//! M(e−1) and keeps its local constraints, as a store does. Some nodes are
//! offline at the end of an epoch: their log goes into the next manifest
//! as a late log. Each epoch is merged with the logs in several random
//! orders, and the outputs must be equal.
//!
//! `SYNOD_MERGE_RUNS` sets the number of histories (the CI simulation job
//! runs 100,000).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;

use synod_core::{Cell, Hlc, LogEntry, NodeId, Op, Row, RowId, Stamp, TxnId, Value, ValueSet};
use synod_sim::{SimRng, check_seeds_with};
use synod_store::merge::{ConflictKind, LogInput, MemState, MergeInput, MergeOutput, conflicts_schema, merge};
use synod_store::schema::{TableSchema, parse_create_table};

const CUSTOMER: u32 = 1;
const ORDERS: u32 = 2;
const T0: u64 = 1_791_763_200_000;

fn schemas(rng: &mut SimRng) -> Vec<Arc<TableSchema>> {
    let rule = ["delete_wins", "update_wins", "flag"][rng.below(3) as usize];
    let cascade = if rng.chance(500_000) { " ON DELETE CASCADE" } else { "" };
    let customer = format!(
        "CREATE TABLE customer (id INTEGER PRIMARY KEY, name TEXT MERGE lww_flag, \
         credit INTEGER CHECK (credit >= 0) MERGE delta, tags SET<TEXT> MERGE union, best INTEGER MERGE max) \
         MERGE DELETE {rule}"
    );
    let orders = format!(
        "CREATE TABLE orders (customer INTEGER REFERENCES customer(id){cascade}, note TEXT MERGE text3, \
         total DECIMAL(10,2) MERGE delta, status TEXT MERGE lww, flagged INTEGER MERGE flag, low INTEGER MERGE min)"
    );
    vec![
        Arc::new(conflicts_schema()),
        Arc::new(parse_create_table(&customer, CUSTOMER).unwrap()),
        Arc::new(parse_create_table(&orders, ORDERS).unwrap()),
    ]
}

/// One node during one epoch: its local state and its log.
struct Writer<'a> {
    node: NodeId,
    tables: &'a [Arc<TableSchema>],
    rows: BTreeMap<RowId, (u32, Row)>,
    entries: Vec<LogEntry>,
    hlc: u64,
    txn: TxnId,
}

impl Writer<'_> {
    fn live(&self, table: u32) -> Vec<RowId> {
        self.rows.iter().filter(|(_, (t, r))| *t == table && !r.tombstone).map(|(id, _)| *id).collect()
    }

    fn live_customer(&self, id: i64) -> Option<RowId> {
        self.live(CUSTOMER).into_iter().find(|r| self.rows[r].1.cells[0].value == Value::Integer(id))
    }

    fn children(&self, id: i64) -> Vec<RowId> {
        self.live(ORDERS).into_iter().filter(|r| self.rows[r].1.cells[0].value == Value::Integer(id)).collect()
    }

    fn write(&mut self, op: Op, table: u32, row_id: RowId, after: Option<Vec<Value>>) {
        let hlc = Hlc::from_u64(self.hlc);
        let stamp = Stamp { hlc, node: self.node };
        let old = self.rows.get(&row_id).map(|(_, r)| r.clone());
        let before = old.as_ref().map(|r| r.cells.iter().map(|c| c.value.clone()).collect::<Vec<_>>());
        let row = match (&op, old) {
            (Op::Insert, _) => {
                let cells = after.clone().unwrap().into_iter().map(|value| Cell { stamp, value }).collect();
                Row { row_id, tombstone: false, cells }
            }
            (Op::Update, Some(mut r)) => {
                for (cell, v) in r.cells.iter_mut().zip(after.clone().unwrap()) {
                    if cell.value != v {
                        *cell = Cell { stamp, value: v };
                    }
                }
                r
            }
            (Op::Delete, Some(r)) => Row { tombstone: true, ..r },
            _ => unreachable!(),
        };
        self.rows.insert(row_id, (table, row));
        self.entries.push(LogEntry {
            op,
            table,
            row_id,
            txn: self.txn,
            hlc,
            before: if op == Op::Insert { None } else { before },
            after,
        });
    }

    fn values(&self, id: RowId) -> Vec<Value> {
        self.rows[&id].1.cells.iter().map(|c| c.value.clone()).collect()
    }
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn note(rng: &mut SimRng) -> Value {
    let lines = ["a\n", "b\n", "c\n", "d\n", "B\n", "C\n"];
    let n = rng.range(0, 4);
    Value::Text((0..n).map(|_| lines[rng.below(lines.len() as u64) as usize]).collect())
}

fn tags(rng: &mut SimRng) -> Value {
    Value::Set(ValueSet::new(["x", "y", "z"].iter().filter(|_| rng.chance(500_000)).map(|s| text(s))).unwrap())
}

fn money(rng: &mut SimRng) -> Value {
    Value::Decimal(synod_core::Decimal::new(rng.range(0, 5_000) as i128, 2).unwrap())
}

/// Random valid local writes.
fn local_writes(w: &mut Writer<'_>, rng: &mut SimRng, counter: &mut u64, clock: &mut u64, ops: u64) {
    for _ in 0..ops {
        *clock += rng.range(0, 3);
        w.hlc = (w.hlc + 1).max(Hlc::new(T0 + *clock, 0).unwrap().as_u64());
        w.txn.counter += 1;
        let customers = w.live(CUSTOMER);
        let orders = w.live(ORDERS);
        match rng.below(7) {
            0 | 1 => {
                let id = rng.range(1, 6) as i64;
                if w.live_customer(id).is_some() {
                    continue;
                }
                let row_id = RowId::new(w.node, *counter);
                *counter += 1;
                let v = vec![
                    Value::Integer(id),
                    text(&format!("n{}", rng.below(3))),
                    Value::Integer(rng.range(0, 20) as i64),
                    tags(rng),
                    Value::Integer(rng.range(0, 9) as i64),
                ];
                w.write(Op::Insert, CUSTOMER, row_id, Some(v));
            }
            2 if !customers.is_empty() => {
                let id = customers[rng.below(customers.len() as u64) as usize];
                let mut v = w.values(id);
                if rng.chance(400_000) {
                    v[1] = text(&format!("n{}", rng.below(3)));
                }
                if let Value::Integer(c) = v[2]
                    && rng.chance(600_000)
                {
                    v[2] = Value::Integer((c + rng.range(0, 10) as i64 - 6).max(0));
                }
                if rng.chance(400_000) {
                    v[3] = tags(rng);
                }
                if rng.chance(300_000) {
                    v[4] = Value::Integer(rng.range(0, 9) as i64);
                }
                // Rarely, a new key value, if no live order refers to the old one.
                if let Value::Integer(old) = v[0]
                    && rng.chance(100_000)
                    && w.children(old).is_empty()
                {
                    let new = rng.range(1, 6) as i64;
                    if w.live_customer(new).is_none() {
                        v[0] = Value::Integer(new);
                    }
                }
                w.write(Op::Update, CUSTOMER, id, Some(v));
            }
            3 if !customers.is_empty() => {
                let id = customers[rng.below(customers.len() as u64) as usize];
                let Value::Integer(key) = w.values(id)[0] else { continue };
                let kids = w.children(key);
                let cascade = w.tables[ORDERS as usize].foreign_keys[0].cascade;
                if !kids.is_empty() && !cascade {
                    continue;
                }
                for k in kids {
                    w.write(Op::Delete, ORDERS, k, None);
                }
                w.write(Op::Delete, CUSTOMER, id, None);
            }
            4 => {
                let parent = if customers.is_empty() || rng.chance(200_000) {
                    Value::Null
                } else {
                    w.values(customers[rng.below(customers.len() as u64) as usize])[0].clone()
                };
                let row_id = RowId::new(w.node, *counter);
                *counter += 1;
                let v = vec![
                    parent,
                    note(rng),
                    money(rng),
                    text(&format!("s{}", rng.below(3))),
                    Value::Integer(rng.range(0, 3) as i64),
                    Value::Integer(rng.range(0, 9) as i64),
                ];
                w.write(Op::Insert, ORDERS, row_id, Some(v));
            }
            5 if !orders.is_empty() => {
                let id = orders[rng.below(orders.len() as u64) as usize];
                let mut v = w.values(id);
                for (c, value) in v.iter_mut().enumerate().skip(1) {
                    if rng.chance(400_000) {
                        *value = match c {
                            1 => note(rng),
                            2 => money(rng),
                            3 => text(&format!("s{}", rng.below(3))),
                            4 => Value::Integer(rng.range(0, 3) as i64),
                            _ => Value::Integer(rng.range(0, 9) as i64),
                        };
                    }
                }
                w.write(Op::Update, ORDERS, id, Some(v));
            }
            6 if !orders.is_empty() => {
                let id = orders[rng.below(orders.len() as u64) as usize];
                w.write(Op::Delete, ORDERS, id, None);
            }
            _ => {}
        }
    }
}

fn check_invariants(state: &MemState, out: &MergeOutput) {
    let live = |t: u32| state.rows().values().filter(move |(tt, r)| *tt == t && !r.tombstone).map(|(_, r)| r);
    let mut keys = BTreeSet::new();
    for c in live(CUSTOMER) {
        assert!(keys.insert(c.cells[0].value.clone()), "two live customers with id {:?}", c.cells[0].value);
    }
    for o in live(ORDERS) {
        let parent = &o.cells[0].value;
        if matches!(parent, Value::Null) || keys.contains(parent) {
            continue;
        }
        let recorded = out
            .conflicts
            .iter()
            .any(|c| c.kind == ConflictKind::ForeignKey && c.row_id == o.row_id && c.rule == "record");
        // A child can also be left from an earlier epoch's recorded break.
        let earlier = state.rows().values().any(|(t, r)| {
            *t == 0
                && r.cells[3].value == Value::Blob(o.row_id.to_be_bytes().to_vec())
                && r.cells[5].value == text("foreign_key")
        });
        assert!(recorded || earlier, "order {} has no parent {parent:?} and no conflict", o.row_id);
    }
    let keys: Vec<_> = out.conflicts.iter().map(|c| c.sort_key()).collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]), "conflicts are not sorted and unique");
    for c in &out.conflicts {
        assert!(out.rows.contains_key(&c.row_id_in_table()), "conflict row missing");
    }
}

/// How often each case happened, over all histories.
static SEEN: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());

fn seen(what: impl Into<String>) {
    *SEEN.lock().unwrap().entry(what.into()).or_default() += 1;
}

fn history(seed: u64) {
    let mut rng = SimRng::new(seed);
    let tables = schemas(&mut rng);
    let n = rng.range(2, 10) as usize;
    let mut ids = BTreeSet::new();
    while ids.len() < n {
        ids.insert(NodeId(rng.below(1_000) as u32 + 1));
    }
    let mut nodes: Vec<NodeId> = ids.into_iter().collect();
    // Node order in the vector is not node ID order.
    for i in (1..nodes.len()).rev() {
        nodes.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let mut counters = vec![0u64; n];
    let mut last_hlc = vec![0u64; n];
    let mut txns = vec![0u64; n];
    let mut state = MemState::new(tables.clone()).unwrap();
    let mut clock = 0u64;
    let mut late: Vec<LogInput> = Vec::new();
    let mut offline: BTreeSet<usize> = BTreeSet::new();

    for epoch in 0..rng.range(1, 3) {
        let mut logs = std::mem::take(&mut late);
        let mut next_offline = BTreeSet::new();
        for (i, &node) in nodes.iter().enumerate() {
            if offline.contains(&i) {
                continue; // It was offline at the end of the last epoch.
            }
            let mut w = Writer {
                node,
                tables: &tables,
                rows: state.rows().clone(),
                entries: Vec::new(),
                hlc: last_hlc[i],
                txn: TxnId { node, counter: txns[i] },
            };
            let ops = rng.range(0, 6);
            local_writes(&mut w, &mut rng, &mut counters[i], &mut clock, ops);
            last_hlc[i] = w.hlc;
            txns[i] = w.txn.counter;
            let log = LogInput { node, epoch, entries: w.entries };
            if rng.chance(150_000) {
                next_offline.insert(i);
                late.push(log);
                seen("late log");
            } else {
                logs.push(log);
            }
        }
        offline = next_offline;

        let input = |logs: Vec<LogInput>| MergeInput {
            tables: &tables,
            epoch,
            logs,
            conflict_stamp: Stamp { hlc: Hlc::new(T0 + 1_000_000 * (epoch + 1), 0).unwrap(), node: NodeId(u32::MAX) },
            record_conflicts: true,
        };
        let first = merge(&state, &input(logs.clone())).unwrap_or_else(|e| panic!("seed {seed}, epoch {epoch}: {e}"));
        for _ in 0..3 {
            let mut shuffled = logs.clone();
            for i in (1..shuffled.len()).rev() {
                shuffled.swap(i, rng.below(i as u64 + 1) as usize);
            }
            // A fresh copy of the base: a different node.
            let other = state.try_clone().unwrap();
            let out = merge(&other, &input(shuffled)).unwrap();
            assert!(out == first, "seed {seed}, epoch {epoch}: another log order gave another result");
        }
        let mut a = state.try_clone().unwrap();
        a.apply(&first);
        let mut b = state.try_clone().unwrap();
        b.apply(&merge(&state, &input(logs.iter().rev().cloned().collect())).unwrap());
        assert_eq!(a.root(), b.root());
        state.apply(&first);
        check_invariants(&state, &first);
        for c in &first.conflicts {
            seen(format!("{} / {}", c.kind, c.rule.split(':').next().unwrap()));
        }
    }
}

#[test]
fn every_log_order_gives_the_same_result() {
    check_seeds_with("every_log_order_gives_the_same_result", "SYNOD_MERGE_RUNS", 1_000, history);
    let seen = SEEN.lock().unwrap();
    eprintln!("cases seen: {seen:#?}");
    // With enough histories, every kind of conflict and rule must occur, or
    // the generator does not test what it claims to.
    let runs = std::env::var("SYNOD_MERGE_RUNS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(1_000);
    if std::env::var("SYNOD_SEED").is_err() && runs >= 1_000 {
        for case in [
            "late log",
            "same_column / lww_flag",
            "same_column / text3",
            "same_column / flag",
            "update_delete / delete_wins",
            "update_delete / update_wins",
            "update_delete / flag",
            "unique_key / earliest_insert",
            "foreign_key / undo_delete",
            "foreign_key / cascade",
            "check / record",
        ] {
            assert!(seen.get(case).copied().unwrap_or(0) > 0, "no history produced {case}");
        }
    }
}
