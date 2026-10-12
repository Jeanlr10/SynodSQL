//! Phase 3 store features: merges by manifest, garbage collection after L
//! epochs, unmerge, catch-up by Prolly diff, stale and abandoned writes,
//! and revert.

mod common;

use std::collections::BTreeSet;

use common::*;
use synod_core::manifest::{LogRef, Manifest};
use synod_core::prolly::{NodeHash, TreeWalk, diff_hashes};
use synod_core::{NodeId, RowId, Value};
use synod_sim::{SimRng, check_seeds};
use synod_store::merge::{LogInput, MERGE_NODE, MemState, MergeInput, merge};
use synod_store::{Error, Store};

struct Cluster {
    nodes: Vec<Node>,
    stores: Vec<Store>,
}

impl Cluster {
    fn new(n: u32, seed: u64, lateness: u64) -> Cluster {
        let nodes: Vec<Node> = (1..=n)
            .map(|i| {
                let mut node = Node::new(i, seed * 16 + i as u64);
                node.config.lateness = lateness;
                node
            })
            .collect();
        let stores = nodes
            .iter()
            .map(|node| {
                let mut s = node.open().unwrap();
                s.create_table(INVENTORY).unwrap();
                s
            })
            .collect();
        Cluster { nodes, stores }
    }

    fn at(&self, epoch: u64, offset_ms: u64) {
        for n in &self.nodes {
            n.clock.set(START_MS + epoch * EPOCH_MS + offset_ms);
        }
    }

    /// Seals every open epoch through `epoch` on the nodes in `who`, and
    /// gives the logs to every node in `to`. Returns the log references.
    fn seal(&mut self, epoch: u64, who: &[usize], to: &[usize]) -> Vec<LogRef> {
        self.at(epoch + 1, 5);
        let mut refs = Vec::new();
        for &i in who {
            while self.stores[i].open_epoch().unwrap() <= epoch {
                let log = self.stores[i].seal().unwrap();
                for &j in to {
                    if j != i {
                        self.stores[j].add_log(&log.bytes).unwrap();
                    }
                }
                refs.push(LogRef { node: self.stores[i].config().node, epoch: log.epoch, hash: log.hash });
            }
        }
        refs
    }

    /// Merges `m` on the nodes in `who`; they must agree.
    fn merge(&mut self, m: &Manifest, who: &[usize]) -> NodeHash {
        let roots: Vec<NodeHash> = who.iter().map(|&i| self.stores[i].merge_manifest(m).unwrap().root).collect();
        assert!(roots.iter().all(|r| *r == roots[0]), "{m}: roots differ");
        roots[0]
    }
}

fn manifest(epoch: u64, logs: Vec<LogRef>, lateness: u64) -> Manifest {
    Manifest::new(epoch, 0, NodeId(1), epoch.checked_sub(lateness), logs)
}

fn insert(s: &mut Store, sku: &str, qty: i64) -> RowId {
    let mut t = s.begin().unwrap();
    let id = t.insert("inventory", &[("sku", text(sku)), ("qty", Value::Integer(qty))]).unwrap();
    t.commit().unwrap();
    id
}

fn update(s: &mut Store, id: RowId, column: &str, v: Value) {
    let mut t = s.begin().unwrap();
    t.update("inventory", id, &[(column, v)]).unwrap();
    t.commit().unwrap();
}

fn delete(s: &mut Store, id: RowId) {
    let mut t = s.begin().unwrap();
    t.delete("inventory", id).unwrap();
    t.commit().unwrap();
}

/// Rows that a node has, tombstones included.
fn row_ids(s: &Store) -> BTreeSet<RowId> {
    s.base_hashes(None, None).unwrap().into_iter().map(|(id, _)| id).collect()
}

#[test]
fn gc_removes_tombstones_and_resolved_conflicts_after_l_epochs() {
    const L: u64 = 2;
    let mut c = Cluster::new(3, 1, L);
    let all = [0, 1, 2];
    c.at(0, 100);
    let gone = insert(&mut c.stores[0], "A-1", 5);
    let kept = insert(&mut c.stores[0], "A-2", 5);
    let refs = c.seal(0, &all, &all);
    c.merge(&manifest(0, refs, L), &all);

    // Epoch 1: one row deleted, one price conflict (lww_flag).
    c.at(1, 100);
    delete(&mut c.stores[1], gone);
    update(&mut c.stores[0], kept, "price", dec("1.00"));
    update(&mut c.stores[2], kept, "price", dec("2.00"));
    let refs = c.seal(1, &all, &all);
    let report = c.stores[0].merge_manifest(&manifest(1, refs.clone(), L)).unwrap();
    assert_eq!(report.conflicts.len(), 1);
    c.merge(&manifest(1, refs, L), &[1, 2]);
    let (conflict, _, status) = c.stores[0].conflicts().unwrap().remove(0);
    assert_eq!(status, "open");
    assert!(c.stores[0].row("inventory", gone).unwrap().unwrap().is_tombstone());

    // Epoch 2: the conflict is resolved.
    c.at(2, 100);
    let mut t = c.stores[1].begin().unwrap();
    t.update("_conflicts", conflict, &[("status", text("resolved"))]).unwrap();
    t.commit().unwrap();

    let mut reference = MemState::new(c.stores[0].tables().to_vec()).unwrap();
    for epoch in 2..=5u64 {
        let refs = c.seal(epoch, &all, &all);
        let m = manifest(epoch, refs, L);
        let root = c.merge(&m, &all);
        let has = |id: RowId| row_ids(&c.stores[0]).contains(&id);
        // The tombstone and the conflict are from epoch 1: they go when
        // gc_through reaches it (the conflict is resolved by then).
        assert_eq!(has(gone), epoch < 1 + L, "epoch {epoch}");
        assert_eq!(has(conflict), epoch < 1 + L, "epoch {epoch}");
        assert!(has(kept));
        assert_eq!(c.stores[0].state_hash().unwrap(), root);
        assert_eq!(c.stores[0].state_hash_from_scratch().unwrap(), root);
        if epoch == 5 {
            // Rebuild the state in memory from the store and merge an
            // empty epoch with garbage collection: no change.
            for id in row_ids(&c.stores[0]) {
                let (t, row) = c.stores[0].base_rows(&[id]).unwrap().remove(0).1.unwrap();
                reference.put(t, row);
            }
            assert_eq!(reference.root(), root);
        }
    }

    // History beyond L is gone; the last L + 1 merges are kept.
    let merged: Vec<u64> = c.stores[0].merged_records().unwrap().iter().map(|r| r.epoch).collect();
    assert_eq!(merged, vec![3, 4, 5]);
    assert!(c.stores[0].known_logs(0).unwrap().iter().all(|(r, _)| r.epoch >= 3));
    assert!(c.stores[0].sealed(1).unwrap().is_none());
    assert!(c.stores[0].sealed(5).unwrap().is_some());
}

#[test]
fn the_pure_merge_collects_the_same_garbage() {
    // A tombstone and a resolved conflict in memory, collected by the pure
    // merge exactly when the store collects them.
    let c = Cluster::new(1, 2, 2);
    let tables = c.stores[0].tables().to_vec();
    let mut state = MemState::new(tables.clone()).unwrap();
    let mut s = Node::new(9, 3).open().unwrap();
    s.create_table(INVENTORY).unwrap();
    let id = insert(&mut s, "Z", 1);
    let mut row = s.row("inventory", id).unwrap().unwrap();
    row.deleted = Some(4);
    state.put(1, row);
    let input = |epoch: u64, gc: Option<u64>| MergeInput {
        tables: &tables,
        epoch,
        logs: Vec::<LogInput>::new(),
        conflict_stamp: synod_core::Stamp { hlc: hlc(START_MS), node: MERGE_NODE },
        record_conflicts: true,
        gc_through: gc,
    };
    assert!(merge(&state, &input(6, Some(3))).unwrap().removed.is_empty());
    let out = merge(&state, &input(6, Some(4))).unwrap();
    assert_eq!(out.removed.keys().copied().collect::<Vec<_>>(), vec![id]);
    state.apply(&out);
    assert!(state.rows().is_empty());
}

#[test]
fn unmerge_goes_back_and_a_better_manifest_merges() {
    check_seeds("unmerge_goes_back_and_a_better_manifest_merges", 10, |seed| {
        let mut rng = SimRng::new(seed);
        let mut c = Cluster::new(3, seed, 4);
        let all = [0, 1, 2];
        c.at(0, 100);
        let ids: Vec<RowId> = (0..4).map(|i| insert(&mut c.stores[i % 3], &format!("S-{i}"), 10)).collect();
        let refs = c.seal(0, &all, &all);
        c.merge(&manifest(0, refs, 4), &all);

        c.at(1, 100);
        for s in c.stores.iter_mut() {
            for _ in 0..3 {
                let id = ids[rng.below(4) as usize];
                update(s, id, "qty", Value::Integer(rng.range(0, 50) as i64));
            }
        }
        let refs = c.seal(1, &all, &all);
        // Node 0 merges a manifest without node 2's log (rank 1), then the
        // rank 0 manifest with all logs appears.
        let partial = Manifest::new(1, 1, NodeId(2), None, refs[..2].to_vec());
        let full = Manifest::new(1, 0, NodeId(1), None, refs.clone());
        let before = c.stores[0].base_root().unwrap();
        c.at(1, EPOCH_MS + 200);
        let inflight = insert(&mut c.stores[0], "late", 1);
        c.stores[0].merge_manifest(&partial).unwrap();
        assert_eq!(c.stores[0].unmerge().unwrap(), 1);
        assert_eq!(c.stores[0].base_root().unwrap(), before);
        assert_eq!(c.stores[0].next_merge_epoch().unwrap(), 1);
        assert!(c.stores[0].get("inventory", inflight).unwrap().is_some(), "local writes stay");
        let root = c.merge(&full, &all);
        assert!(c.stores[0].get("inventory", inflight).unwrap().is_some());
        assert_eq!(c.stores[0].merged_record(1).unwrap().unwrap().root, root);
    });
}

#[test]
fn unmerge_of_the_first_merge() {
    let mut c = Cluster::new(2, 4, 4);
    c.at(0, 100);
    insert(&mut c.stores[0], "A", 1);
    let refs = c.seal(0, &[0, 1], &[0, 1]);
    let empty = c.stores[0].base_root().unwrap();
    c.merge(&manifest(0, refs.clone(), 4), &[0]);
    assert_eq!(c.stores[0].unmerge().unwrap(), 0);
    assert_eq!(c.stores[0].merged_epoch().unwrap(), None);
    assert_eq!(c.stores[0].base_root().unwrap(), empty);
    assert!(matches!(c.stores[0].unmerge(), Err(Error::Invalid(_))));
    c.merge(&manifest(0, refs, 4), &[0, 1]);
}

/// Takes over `from`'s base state by a Prolly diff, as a returning node
/// does.
fn adopt(c: &mut Cluster, to: usize, from: usize) -> usize {
    let (src, dst) = if to < from {
        let (a, b) = c.stores.split_at_mut(from);
        (&b[0], &mut a[to])
    } else {
        let (a, b) = c.stores.split_at_mut(to);
        (&a[from], &mut b[0])
    };
    // Walk the peer's tree; only nodes that this side lacks are fetched.
    let image = src.base_image().unwrap();
    let mut walk = TreeWalk::new(image.root);
    loop {
        let fetch = walk.step(|h| dst.base_tree_node(h).unwrap()).unwrap();
        if fetch.is_empty() {
            break;
        }
        for h in fetch {
            walk.receive(h, src.base_tree_node(&h).unwrap().unwrap()).unwrap();
        }
    }
    let theirs = walk.finish(image.root).unwrap();
    assert_eq!(theirs, src.base_hashes(None, None).unwrap());
    let ours = dst.base_hashes(None, None).unwrap();
    let (changed, removed) = diff_hashes(&ours, &theirs);
    let rows: Vec<(u32, synod_core::Row)> =
        src.base_rows(&changed).unwrap().into_iter().map(|(_, r)| r.expect("peer row")).collect();
    let n = rows.len() + removed.len();
    dst.adopt_base(&image, &rows, &removed).unwrap();
    assert_eq!(dst.base_root().unwrap(), src.base_root().unwrap());
    n
}

#[test]
fn a_returning_node_catches_up_by_diff_and_its_writes_merge_late() {
    const L: u64 = 2;
    let mut c = Cluster::new(3, 5, L);
    let all = [0, 1, 2];
    let up = [0, 1];
    c.at(0, 100);
    let ids: Vec<RowId> = (0..6).map(|i| insert(&mut c.stores[0], &format!("S-{i}"), 10)).collect();
    let refs = c.seal(0, &all, &all);
    c.merge(&manifest(0, refs, L), &all);

    // Node 2 is cut off from epoch 1 on. It writes in epoch 1 (on a row
    // that is deleted in epoch 1 elsewhere) and in epoch 6.
    c.at(1, 100);
    update(&mut c.stores[2], ids[0], "qty", Value::Integer(99));
    update(&mut c.stores[2], ids[1], "name", text("kept"));
    delete(&mut c.stores[0], ids[0]);
    for epoch in 1..=6 {
        c.at(epoch, 200);
        update(&mut c.stores[1], ids[2], "qty", Value::Integer(epoch as i64));
        let refs = c.seal(epoch, &up, &up);
        c.merge(&manifest(epoch, refs, L), &up);
    }
    c.at(6, 300);
    let fresh = insert(&mut c.stores[2], "C-new", 1);

    // Node 2 returns: its own logs of epochs 1..6 are sealed; those older
    // than the lateness bound are given up.
    let own = c.seal(6, &[2], &[]);
    assert_eq!(own.len(), 6);
    let changed = adopt(&mut c, 2, 0);
    assert!(changed > 0);
    assert_eq!(c.stores[2].next_merge_epoch().unwrap(), 7);
    // Epoch 7 may take logs of epoch 5 and later.
    let mut stale = 0;
    for r in &own {
        if r.epoch + L < 7 {
            stale += c.stores[2].abandon_log(r.epoch).unwrap();
        } else {
            let bytes = c.stores[2].log(&r.hash).unwrap().unwrap();
            c.stores[0].add_log(&bytes).unwrap();
            c.stores[1].add_log(&bytes).unwrap();
        }
    }
    assert_eq!(stale, 2, "the two writes of epoch 1");
    // The writes of epoch 6 are still visible on node 2 (rebased); the
    // given-up writes of epoch 1 are not.
    assert!(c.stores[2].get("inventory", fresh).unwrap().is_some());
    assert_eq!(c.stores[2].get("inventory", ids[1]).unwrap().unwrap()[1], Value::Null);
    let late: Vec<LogRef> = own.iter().filter(|r| r.epoch + L >= 7).copied().collect();
    let mut refs = c.seal(7, &all, &all);
    refs.extend(late);
    let root = c.merge(&manifest(7, refs.clone(), L), &all);
    // The give-up conflicts are local writes of epoch 6 or 7 on node 2:
    // after the merge of epoch 7 every node has them.
    let kinds: Vec<String> =
        c.stores[1].conflicts().unwrap().iter().map(|(_, c, _)| c.kind.as_str().to_string()).collect();
    assert_eq!(kinds.iter().filter(|k| *k == "stale_write").count(), 2, "{kinds:?}");
    assert!(c.stores[0].get("inventory", fresh).unwrap().is_some());
    for s in &mut c.stores {
        assert_eq!(s.state_hash().unwrap(), root);
    }
}

#[test]
fn a_late_write_to_a_collected_row_is_a_stale_write() {
    const L: u64 = 2;
    let mut c = Cluster::new(3, 6, L);
    let all = [0, 1, 2];
    let up = [0, 1];
    c.at(0, 100);
    let id = insert(&mut c.stores[0], "X", 1);
    let refs = c.seal(0, &all, &all);
    c.merge(&manifest(0, refs, L), &all);

    // Node 0 deletes the row in epoch 1. Node 2 is cut off and updates it
    // in epoch 3, on its old base.
    c.at(1, 100);
    delete(&mut c.stores[0], id);
    c.at(3, 100);
    update(&mut c.stores[2], id, "qty", Value::Integer(2));
    for epoch in 1..=4 {
        let refs = c.seal(epoch, &up, &up);
        c.merge(&manifest(epoch, refs, L), &up);
    }
    assert!(!row_ids(&c.stores[0]).contains(&id), "collected at gc_through 1");

    // Node 2 returns in epoch 5: it takes over the base state, gives up
    // its logs of epochs 1 and 2, and sends its logs of epochs 3 and 4.
    let own = c.seal(4, &[2], &[]);
    adopt(&mut c, 2, 0);
    assert!(c.stores[2].get("inventory", id).unwrap().is_none(), "the write has no row to go on");
    let mut refs = c.seal(5, &all, &all);
    for r in &own {
        if r.epoch + L < 5 {
            assert_eq!(c.stores[2].abandon_log(r.epoch).unwrap(), 0);
        } else {
            let bytes = c.stores[2].log(&r.hash).unwrap().unwrap();
            c.stores[0].add_log(&bytes).unwrap();
            c.stores[1].add_log(&bytes).unwrap();
            refs.push(*r);
        }
    }
    let report = c.stores[0].merge_manifest(&manifest(5, refs.clone(), L)).unwrap();
    c.merge(&manifest(5, refs, L), &[1, 2]);
    assert_eq!(report.conflicts.len(), 1);
    assert_eq!(report.conflicts[0].kind.as_str(), "stale_write");
    assert_eq!(c.stores[2].base_root().unwrap(), report.root);
    assert!(c.stores[0].get("inventory", id).unwrap().is_none());
}

#[test]
fn logs_older_than_the_lateness_bound_are_refused() {
    let mut c = Cluster::new(2, 7, 1);
    c.at(0, 100);
    insert(&mut c.stores[1], "B", 1);
    let late = c.seal(0, &[1], &[0]);
    for epoch in 0..=1 {
        let refs = c.seal(epoch, &[0], &[0]);
        c.merge(&manifest(epoch, refs, 1), &[0]);
    }
    let refs = c.seal(2, &[0], &[0]);
    let mut logs = refs.clone();
    logs.extend(late);
    let err = c.stores[0].merge_manifest(&manifest(2, logs, 1)).unwrap_err();
    assert!(err.to_string().contains("lateness"), "{err}");
    c.merge(&manifest(2, refs, 1), &[0]);
}

#[test]
fn revert_undoes_a_nodes_writes() {
    let mut c = Cluster::new(2, 8, 8);
    let all = [0, 1];
    c.at(0, 100);
    let a = insert(&mut c.stores[0], "A", 10);
    let b = insert(&mut c.stores[0], "B", 10);
    let refs = c.seal(0, &all, &all);
    c.merge(&manifest(0, refs, 8), &all);

    // Node 2 (index 1) makes bad writes in epoch 1.
    c.at(1, 100);
    let bad = insert(&mut c.stores[1], "BAD", 1);
    update(&mut c.stores[1], a, "name", text("wrong"));
    update(&mut c.stores[1], b, "name", text("wrong too"));
    delete(&mut c.stores[1], b);
    let refs = c.seal(1, &all, &all);
    c.merge(&manifest(1, refs, 8), &all);
    // Node 1 changes `a` again later: that change stays.
    c.at(2, 100);
    update(&mut c.stores[0], a, "name", text("fixed by hand"));
    let refs = c.seal(2, &all, &all);
    c.merge(&manifest(2, refs, 8), &all);

    c.at(3, 100);
    let mut t = c.stores[0].begin().unwrap();
    let report = t.revert(NodeId(2), 1..=1).unwrap();
    t.commit().unwrap();
    assert_eq!(report, synod_store::RevertReport { deleted: 1, restored: 0, reinserted: 1, skipped: 1 });
    let s = &c.stores[0];
    assert!(s.get("inventory", bad).unwrap().is_none());
    assert_eq!(s.get("inventory", a).unwrap().unwrap()[1], text("fixed by hand"));
    let back = s.query("SELECT name FROM inventory WHERE sku = 'B'", &[]).unwrap();
    assert_eq!(back, vec![vec![rusqlite::types::Value::Null]], "B comes back as a new row, as it was");
    // The revert travels as normal writes.
    let refs = c.seal(3, &all, &all);
    c.merge(&manifest(3, refs, 8), &all);
    assert_eq!(c.stores[1].query("SELECT name FROM inventory WHERE sku = 'B'", &[]).unwrap().len(), 1);
}
