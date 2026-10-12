//! Phase 2: multi-node sync between stores, with writes during the sync,
//! checked against the pure merge; and crashes during a merge.

mod common;

use common::*;
use synod_core::log::decode_log;
use synod_core::{Hlc, Stamp, Value, ValueSet};
use synod_sim::{SimRng, check_seeds};
use synod_store::merge::{LogInput, MERGE_NODE, MemState, MergeInput, merge};
use synod_store::{Error, Store};

const COLUMNS: [&str; 6] = ["sku", "name", "price", "qty", "tags", "notes"];

fn pick<'a>(rng: &mut SimRng, items: &[&'a str]) -> &'a str {
    items[rng.below(items.len() as u64) as usize]
}

fn random_value(rng: &mut SimRng, column: usize) -> Value {
    match column {
        0 => text(&format!("S-{}", rng.below(8))),
        1 => text(pick(rng, &["bolt", "nut", "washer"])),
        2 => dec(pick(rng, &["0.10", "0.11", "1.5", "2"])),
        3 => Value::Integer(rng.range(0, 30) as i64),
        4 => {
            Value::Set(ValueSet::new(["a", "b", "c"].iter().filter(|_| rng.chance(500_000)).map(|s| text(s))).unwrap())
        }
        _ => text(pick(rng, &["x\ny\n", "x\nY\n", "X\ny\n", "z\n"])),
    }
}

/// Random local writes on one store. Writes that break a local constraint
/// are skipped.
fn writes(s: &mut Store, rng: &mut SimRng, n: u64) {
    for _ in 0..n {
        let mut t = s.begin().unwrap();
        let live = t.find("inventory", "1", &[]).unwrap();
        let r = match rng.below(4) {
            0 | 1 => {
                let cols: Vec<(&str, Value)> =
                    COLUMNS.iter().enumerate().map(|(c, n)| (*n, random_value(rng, c))).collect();
                t.insert("inventory", &cols).map(|_| ())
            }
            2 if !live.is_empty() => {
                let id = live[rng.below(live.len() as u64) as usize];
                let c = rng.below(6) as usize;
                t.update("inventory", id, &[(COLUMNS[c], random_value(rng, c))])
            }
            3 if !live.is_empty() => t.delete("inventory", live[rng.below(live.len() as u64) as usize]),
            _ => Ok(()),
        };
        match r {
            Ok(()) | Err(Error::Constraint(_)) => {}
            Err(e) => panic!("{e}"),
        }
        t.commit().unwrap();
    }
}

#[test]
fn stores_converge_and_match_the_pure_merge() {
    check_seeds("stores_converge_and_match_the_pure_merge", 15, |seed| {
        let mut rng = SimRng::new(seed);
        let n = rng.range(2, 4) as usize;
        let nodes: Vec<Node> = (0..n).map(|i| Node::new(10 + i as u32, seed * 10 + i as u64)).collect();
        let mut stores: Vec<Store> = nodes
            .iter()
            .map(|node| {
                let mut s = node.open().unwrap();
                s.create_table(INVENTORY).unwrap();
                s
            })
            .collect();
        let mut reference = MemState::new(stores[0].tables().to_vec()).unwrap();

        for epoch in 0..3u64 {
            let start = START_MS + epoch * EPOCH_MS;
            for (i, s) in stores.iter_mut().enumerate() {
                nodes[i].clock.set(start + 10 + rng.below(1_000));
                let k = rng.range(0, 6);
                writes(s, &mut rng, k);
            }
            // Seal, then write in the next epoch before the merge: these
            // writes are in flight.
            let end = start + EPOCH_MS;
            let mut logs = Vec::new();
            for (i, s) in stores.iter_mut().enumerate() {
                nodes[i].clock.set(end + rng.below(50));
                s.seal().unwrap();
                logs.push(s.export(epoch).unwrap());
                let k = rng.range(0, 3);
                writes(s, &mut rng, k);
            }
            let mut roots = Vec::new();
            for (i, s) in stores.iter_mut().enumerate() {
                let inflight = s.unsealed_entries().unwrap();
                let others: Vec<Vec<u8>> =
                    logs.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, l)| l.clone()).collect();
                let report = s.merge(epoch, &others).unwrap();
                assert_eq!(report.rebased, inflight.len());
                assert_eq!(s.unsealed_entries().unwrap(), inflight, "in-flight writes stay in the log");
                assert_eq!(s.state_hash().unwrap(), s.state_hash_from_scratch().unwrap());
                roots.push((report.root, s.conflicts().unwrap()));
            }
            assert!(roots.iter().all(|r| *r == roots[0]), "seed {seed}, epoch {epoch}: nodes differ");

            // The store's M(e) equals the pure merge of the same logs.
            let input = MergeInput {
                tables: reference.tables(),
                epoch,
                logs: logs
                    .iter()
                    .map(|b| {
                        let (h, entries) = decode_log(b).unwrap();
                        LogInput { node: h.node, epoch: h.epoch, entries }
                    })
                    .collect(),
                conflict_stamp: Stamp { hlc: Hlc::new(end, 0).unwrap(), node: MERGE_NODE },
                record_conflicts: true,
            };
            let out = merge(&reference, &input).unwrap();
            reference.apply(&out);
            assert_eq!(reference.root(), roots[0].0, "seed {seed}, epoch {epoch}: store and pure merge differ");
        }
    });
}

/// Builds two nodes with one merged epoch and writes in epoch 1, seals
/// epoch 1, and writes in flight. Returns the nodes, node A's store, and
/// node B's log of epoch 1.
fn scenario(seed: u64) -> (Node, Node, Store, Vec<u8>) {
    let mut rng = SimRng::new(seed);
    let (a, b) = (Node::new(1, seed), Node::new(2, seed + 1));
    let mut sa = a.open().unwrap();
    let mut sb = b.open().unwrap();
    sa.create_table(INVENTORY).unwrap();
    sb.create_table(INVENTORY).unwrap();
    for epoch in 0..2u64 {
        let start = START_MS + epoch * EPOCH_MS;
        a.clock.set(start + 5);
        writes(&mut sa, &mut rng, 4);
        b.clock.set(start + 7);
        writes(&mut sb, &mut rng, 4);
        a.clock.set(start + EPOCH_MS);
        b.clock.set(start + EPOCH_MS);
        sa.seal().unwrap();
        sb.seal().unwrap();
        if epoch == 0 {
            let (la, lb) = (sa.export(0).unwrap(), sb.export(0).unwrap());
            sa.merge(0, &[lb]).unwrap();
            sb.merge(0, &[la]).unwrap();
        }
    }
    writes(&mut sa, &mut rng, 2); // in flight
    let lb = sb.export(1).unwrap();
    (a, b, sa, lb)
}

/// A crash at any disk operation of a merge leaves either the old state or
/// the merged one; a merge run again gives the same root.
#[test]
fn crash_during_merge() {
    let seed = 3;
    let (a, _b, mut s, lb) = scenario(seed);
    let before = a.fs.ops();
    let reference = s.merge(1, std::slice::from_ref(&lb)).unwrap();
    let total = a.fs.ops() - before;
    let local = s.state_hash().unwrap();
    drop(s);
    assert!(total > 20, "merge too short: {total} operations");

    for at in 0..total {
        let (a, _b, mut s, lb) = scenario(seed);
        let entries = s.all_entries().unwrap();
        a.fs.crash_after(at);
        let _ = s.merge(1, std::slice::from_ref(&lb));
        drop(s);
        a.fs.crash(&mut SimRng::new(at));
        a.fs.disarm();
        let mut s = a.open().unwrap_or_else(|e| panic!("op {at}: reopen: {e}"));
        assert_eq!(s.all_entries().unwrap(), entries, "op {at}: the log changed");
        if s.merged_epoch().unwrap() != Some(1) {
            let again = s.merge(1, std::slice::from_ref(&lb)).unwrap_or_else(|e| panic!("op {at}: merge again: {e}"));
            assert_eq!(again.root, reference.root, "op {at}");
        }
        assert_eq!(s.base_root().unwrap(), reference.root, "op {at}");
        assert_eq!(s.state_hash().unwrap(), local, "op {at}: local state with in-flight writes");
        assert_eq!(s.state_hash_from_scratch().unwrap(), local, "op {at}");
    }
}
