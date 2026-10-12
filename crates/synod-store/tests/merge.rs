//! Phase 2: merges between stores, by offline file sync.
//!
//! The first test is exit gate 2: the worked example of the white paper
//! (Section VIII) gives exactly its published result.

mod common;

use common::*;
use rusqlite::types::Value as Sql;
use synod_core::{NodeId, RowId, Value};
use synod_store::merge::ConflictKind;
use synod_store::{Error, MergeReport, Store};

const A: u32 = 0xA;
const B: u32 = 0xB;
const C: u32 = 0xC;

struct Cluster {
    nodes: Vec<Node>,
    stores: Vec<Store>,
}

impl Cluster {
    fn new(ids: &[u32]) -> Cluster {
        let nodes: Vec<Node> = ids.iter().map(|&id| Node::new(id, id as u64)).collect();
        let stores = nodes
            .iter()
            .map(|n| {
                let mut s = n.open().unwrap();
                s.create_table(INVENTORY).unwrap();
                s
            })
            .collect();
        Cluster { nodes, stores }
    }

    fn at(&mut self, i: usize, ms: u64) -> &mut Store {
        self.nodes[i].clock.set(ms);
        &mut self.stores[i]
    }

    /// Every node seals `epoch`, sends its log to every other node, and
    /// merges. Returns the reports.
    fn sync(&mut self, epoch: u64) -> Vec<MergeReport> {
        let end = START_MS + (epoch + 1) * EPOCH_MS;
        let logs: Vec<Vec<u8>> = (0..self.stores.len())
            .map(|i| {
                self.at(i, end);
                let sealed = self.stores[i].seal().unwrap();
                assert_eq!(sealed.epoch, epoch);
                self.stores[i].export(epoch).unwrap()
            })
            .collect();
        (0..self.stores.len())
            .map(|i| {
                let others: Vec<Vec<u8>> =
                    logs.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, l)| l.clone()).collect();
                self.stores[i].merge(epoch, &others).unwrap()
            })
            .collect()
    }
}

fn find(s: &Store, sku: &str) -> RowId {
    let ids = s.query("SELECT row_id FROM inventory WHERE sku = ?1", &[text(sku)]).unwrap();
    let Sql::Blob(b) = &ids[0][0] else { panic!() };
    RowId::from_be_bytes(b.as_slice().try_into().unwrap()).unwrap()
}

fn set(t: &mut synod_store::Txn<'_>, id: RowId, col: &str, v: Value) {
    t.update("inventory", id, &[(col, v)]).unwrap();
}

#[test]
fn white_paper_worked_example() {
    let mut c = Cluster::new(&[A, B, C]);

    // Base state M(e−1): node A makes r1, r2, r3 in epoch 0; all merge.
    {
        let s = c.at(0, START_MS + 1);
        let mut t = s.begin().unwrap();
        for (sku, name, price, qty) in [
            ("A-100", "Bolt, M6", "0.10", 500),
            ("A-200", "Nut, M6", "0.05", 800),
            ("B-310", "Washer, M6", "0.02", 1000),
        ] {
            t.insert(
                "inventory",
                &[("sku", text(sku)), ("name", text(name)), ("price", dec(price)), ("qty", Value::Integer(qty))],
            )
            .unwrap();
        }
        t.commit().unwrap();
    }
    let base = c.sync(0);
    assert!(base.iter().all(|r| r.root == base[0].root && r.conflicts.is_empty()));
    let (r1, r2, r3) = (find(&c.stores[0], "A-100"), find(&c.stores[0], "A-200"), find(&c.stores[0], "B-310"));

    // Writes during epoch 1, at the HLC times of the white paper's Table VII
    // (milliseconds after the start of the epoch).
    let t0 = START_MS + EPOCH_MS;
    let write = |c: &mut Cluster, node: usize, ms: u64, f: &mut dyn FnMut(&mut synod_store::Txn<'_>)| {
        let s = c.at(node, t0 + ms);
        let mut t = s.begin().unwrap();
        assert_eq!(t.hlc().physical_ms(), t0 + ms);
        f(&mut t);
        t.commit().unwrap();
    };
    write(&mut c, 0, 10, &mut |t| set(t, r1, "price", dec("0.12")));
    write(&mut c, 1, 11, &mut |t| set(t, r1, "name", text("Bolt, M6 x 20")));
    write(&mut c, 0, 12, &mut |t| set(t, r2, "qty", Value::Integer(650)));
    write(&mut c, 1, 13, &mut |t| set(t, r2, "qty", Value::Integer(700)));
    write(&mut c, 0, 14, &mut |t| t.delete("inventory", r3).unwrap());
    write(&mut c, 1, 15, &mut |t| set(t, r3, "qty", Value::Integer(900)));
    let mut r4 = None;
    write(&mut c, 1, 16, &mut |t| {
        let cols =
            [("sku", text("C-400")), ("name", text("Spacer")), ("price", dec("0.08")), ("qty", Value::Integer(200))];
        r4 = Some(t.insert("inventory", &cols).unwrap());
    });
    write(&mut c, 2, 17, &mut |t| set(t, r1, "price", dec("0.11")));
    let mut r5 = None;
    write(&mut c, 2, 18, &mut |t| {
        let cols = [
            ("sku", text("C-400")),
            ("name", text("Spacer 10 mm")),
            ("price", dec("0.09")),
            ("qty", Value::Integer(50)),
        ];
        r5 = Some(t.insert("inventory", &cols).unwrap());
    });
    let (r4, r5) = (r4.unwrap(), r5.unwrap());

    let reports = c.sync(1);

    // All three nodes compute the same tables and the same root hash.
    for r in &reports {
        assert_eq!(r.root, reports[0].root);
        assert_eq!(r.conflicts, reports[0].conflicts);
        assert_eq!(r.logs.len(), 3);
    }
    for s in &mut c.stores {
        assert_eq!(s.state_hash().unwrap(), reports[0].root);
        assert_eq!(s.state_hash_from_scratch().unwrap(), reports[0].root);
    }

    // Result M(e): Table VIII.
    let s = &c.stores[0];
    let rows = s.query("SELECT sku, name, price, qty FROM inventory ORDER BY sku", &[]).unwrap();
    let row = |sku: &str, name: &str, price: &str, qty: i64| {
        vec![Sql::Text(sku.into()), Sql::Text(name.into()), Sql::Text(price.into()), Sql::Integer(qty)]
    };
    assert_eq!(
        rows,
        vec![
            row("A-100", "Bolt, M6 x 20", "0.11", 500),
            row("A-200", "Nut, M6", "0.05", 550),
            row("C-400", "Spacer", "0.08", 200)
        ]
    );
    assert!(s.row("inventory", r3).unwrap().unwrap().is_tombstone());
    assert!(s.row("inventory", r5).unwrap().unwrap().is_tombstone());

    // _conflicts for epoch e: Table IX, exactly three rows.
    let conflicts = &reports[0].conflicts;
    assert_eq!(conflicts.len(), 3, "{conflicts:#?}");
    let price = &conflicts[0];
    assert_eq!((price.row_id, price.column_name.as_deref(), price.kind), (r1, Some("price"), ConflictKind::SameColumn));
    assert_eq!(price.rule, "lww_flag");
    assert_eq!(price.applied, Some(vec![dec("0.11")]));
    assert_eq!(price.recorded.len(), 1);
    assert_eq!((price.recorded[0].node, &price.recorded[0].values), (NodeId(A), &vec![dec("0.12")]));
    let delete = &conflicts[1];
    assert_eq!((delete.row_id, delete.kind, delete.rule.as_str()), (r3, ConflictKind::UpdateDelete, "delete_wins"));
    assert_eq!(delete.applied, None);
    assert_eq!(delete.recorded.len(), 1);
    assert_eq!((delete.recorded[0].node, &delete.recorded[0].values[3]), (NodeId(B), &Value::Integer(900)));
    let unique = &conflicts[2];
    assert_eq!((unique.row_id, unique.kind, unique.rule.as_str()), (r5, ConflictKind::UniqueKey, "earliest_insert"));
    assert_eq!(unique.other_row, Some(r4), "r4 keeps C-400");
    assert_eq!((unique.recorded[0].node, &unique.recorded[0].values[1]), (NodeId(C), &text("Spacer 10 mm")));

    // The conflicts are rows of the replicated _conflicts table.
    let stored = c.stores[2].conflicts().unwrap();
    assert_eq!(stored.len(), 3);
    assert!(stored.iter().all(|(_, _, status)| status == "open"));

    // A person resolves one conflict on node B. The resolution travels in
    // the next epoch like any other write.
    let (cid, _, _) = stored.iter().find(|(_, c, _)| c.kind == ConflictKind::UniqueKey).unwrap().clone();
    {
        let s = c.at(1, START_MS + 2 * EPOCH_MS + 5);
        let mut t = s.begin().unwrap();
        t.update("_conflicts", cid, &[("status", text("resolved"))]).unwrap();
        t.commit().unwrap();
    }
    let next = c.sync(2);
    assert!(next.iter().all(|r| r.root == next[0].root && r.conflicts.is_empty()));
    for s in &c.stores {
        let open = s.query("SELECT count(*) FROM _conflicts WHERE status = 'open'", &[]).unwrap();
        assert_eq!(open[0][0], Sql::Integer(2));
    }
}

#[test]
fn merge_checks_its_inputs() {
    let mut c = Cluster::new(&[A, B]);
    c.at(0, START_MS + EPOCH_MS);
    assert!(matches!(c.stores[0].merge(0, &[]), Err(Error::Invalid(_))), "own log must be sealed");
    c.stores[0].seal().unwrap();
    assert!(matches!(c.stores[0].merge(1, &[]), Err(Error::Invalid(_))), "epochs merge in order");
    let a0 = c.stores[0].export(0).unwrap();
    // Another schema.
    let other = Node::new(B, 99);
    let mut odd = other.open().unwrap();
    odd.create_table("CREATE TABLE inventory (sku TEXT)").unwrap();
    other.clock.set(START_MS + EPOCH_MS);
    odd.seal().unwrap();
    let wrong_schema = odd.export(0).unwrap();
    assert!(matches!(c.stores[0].merge(0, &[wrong_schema]), Err(Error::Invalid(m)) if m.contains("schema")));
    // A log with this node's ID that is not its own log.
    assert!(c.stores[1].merge(0, &[]).is_err(), "node B has not sealed");
    c.at(1, START_MS + EPOCH_MS);
    c.stores[1].seal().unwrap();
    let b0 = c.stores[1].export(0).unwrap();
    assert!(c.stores[1].merge(0, std::slice::from_ref(&b0)).is_ok(), "the own log given twice is the same log");
    c.stores[0].merge(0, std::slice::from_ref(&b0)).unwrap();
    // Merged logs cannot come back.
    c.at(0, START_MS + 2 * EPOCH_MS);
    c.stores[0].seal().unwrap();
    assert!(matches!(c.stores[0].merge(1, &[b0]), Err(Error::Invalid(m)) if m.contains("already merged")));
    let _ = a0;
}

/// A new node clones a peer's base state, then both write and merge.
#[test]
fn a_clone_joins_and_converges() {
    let mut c = Cluster::new(&[A]);
    let insert = |s: &mut Store, sku: &str| {
        let mut t = s.begin().unwrap();
        let id = t.insert("inventory", &[("sku", text(sku)), ("qty", Value::Integer(1))]).unwrap();
        t.commit().unwrap();
        id
    };
    insert(c.at(0, START_MS + 1), "A-1");
    c.sync(0);
    insert(c.at(0, START_MS + EPOCH_MS + 1), "A-2"); // not merged yet: not in the clone

    // Node B joins: a copy of node A's base state M(0), on B's own disk.
    let b = Node::new(B, 77);
    let clone = c.stores[0].clone_base("/clone.db", NodeId(B)).unwrap();
    assert_eq!(clone.base_root().unwrap(), c.stores[0].base_root().unwrap());
    drop(clone);
    // Copy the file to node B's disk.
    b.fs.write_file(PATH, &c.nodes[0].fs.read("/clone.db").unwrap());
    let mut sb = Store::open(PATH, synod_store::StoreConfig { node: NodeId(B), ..b.config.clone() }, b.env()).unwrap();
    assert_eq!(sb.next_merge_epoch().unwrap(), 1);
    assert_eq!(sb.query("SELECT sku FROM inventory", &[]).unwrap(), vec![vec![Sql::Text("A-1".into())]]);
    b.clock.set(START_MS + EPOCH_MS + 2);
    let id = insert(&mut sb, "B-1");
    assert_eq!((id.node(), id.counter()), (NodeId(B), 0));

    c.nodes.push(b);
    c.stores.push(sb);
    let reports = c.sync(1);
    assert_eq!(reports[0].root, reports[1].root);
    let skus = c.stores[1].query("SELECT sku FROM inventory ORDER BY sku", &[]).unwrap();
    assert_eq!(skus.len(), 3);
}
