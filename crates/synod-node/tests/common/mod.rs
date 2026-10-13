//! The Phase 3 cluster harness: SynodSQL nodes on SQLite stores, run by the
//! simulator, with an oracle that checks every merge and every committed
//! write.

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::rc::Rc;
use std::sync::Arc;

use synod_core::log::decode_log;
use synod_core::manifest::Manifest;
use synod_core::prolly::NodeHash;
use synod_core::{NodeId, TxnId, Value};
use synod_node::{Event, Msg, Peer, PeerConfig};
use synod_sim::disk::CrashSignal;
use synod_sim::{Config, CrashFs, Ctx, Node, NodeIndex, Sim, SimRng};
use synod_store::{Env, Error, FileSystem, ManualClock, Store, StoreConfig};

pub const START_MS: u64 = 1_791_763_200_000;
pub const PATH: &str = "/node.db";

pub const SCHEMA: &str = "CREATE TABLE stock (
    sku   TEXT UNIQUE               MERGE lww,
    name  TEXT                      MERGE lww,
    price DECIMAL(10,2)             MERGE lww_flag,
    qty   INTEGER CHECK (qty >= 0)  MERGE delta,
    tags  SET<TEXT>                 MERGE union
)";

/// The crash file system, seen through the store's trait.
pub struct SimFs(pub Arc<CrashFs>);

impl FileSystem for SimFs {
    fn vfs(&self) -> Option<&str> {
        Some(self.0.vfs_name())
    }
    fn exists(&self, path: &str) -> io::Result<bool> {
        Ok(self.0.exists(path))
    }
    fn sync_file(&self, path: &str) -> io::Result<()> {
        self.0.sync_file(path)
    }
    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        self.0.rename(from, to)
    }
    fn sync_dir(&self, _path: &str) -> io::Result<()> {
        self.0.sync_dir()
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.0.remove(path)
    }
}

/// Settings of one run.
#[derive(Clone, Debug)]
pub struct Params {
    pub nodes: usize,
    pub epochs: u64,
    pub epoch_ms: u64,
    pub lateness: u64,
    pub quorum: bool,
    pub gossip: Option<usize>,
    /// Chance per million, per tick, that a node writes.
    pub write_per_million: u32,
    /// Number of distinct keys: fewer keys, more conflicts.
    pub keys: u64,
    /// `Some`: the conflict model workload instead of random writes.
    pub model: Option<Model>,
    /// The schema of the one table, `stock`.
    pub schema: &'static str,
    pub sim: Config,
}

/// An access pattern over rows.
#[derive(Clone, Copy, Debug)]
pub enum Dist {
    Uniform,
    /// Zipf with this exponent.
    Zipf(f64),
    /// Hot 1% of rows: 90% of writes; warm 9%: 9%; cold 90%: 1%.
    Tiered,
}

impl Dist {
    /// The probability of each of `rows` rows.
    pub fn probs(&self, rows: usize) -> Vec<f64> {
        let w: Vec<f64> = match *self {
            Dist::Uniform => vec![1.0; rows],
            Dist::Zipf(s) => (1..=rows).map(|k| 1.0 / (k as f64).powf(s)).collect(),
            Dist::Tiered => {
                let hot = (rows / 100).max(1);
                let warm = (rows * 9 / 100).max(1);
                let cold = rows - hot - warm;
                (0..rows)
                    .map(|i| {
                        if i < hot {
                            0.90 / hot as f64
                        } else if i < hot + warm {
                            0.09 / warm as f64
                        } else {
                            0.01 / cold as f64
                        }
                    })
                    .collect()
            }
        };
        let sum: f64 = w.iter().sum();
        w.iter().map(|x| x / sum).collect()
    }
}

/// The conflict model workload (white paper, Section IX): `rows` rows
/// exist before the run; in each epoch, each node writes `w` rows drawn
/// from `dist`.
#[derive(Clone, Copy, Debug)]
pub struct Model {
    pub rows: usize,
    pub w: u32,
    pub dist: Dist,
    /// Write after the merge of the previous epoch (4/5 into the epoch)
    /// instead of at its start. A write at the start is in flight: it does
    /// not see the other nodes' writes of the previous epoch.
    pub after_merge: bool,
}

impl Model {
    /// Writes after the merge, as the model assumes.
    pub fn new(rows: usize, w: u32, dist: Dist) -> Model {
        Model { rows, w, dist, after_merge: true }
    }
}

/// What the oracle saw.
#[derive(Default)]
pub struct Oracle {
    /// Root hash after each (epoch, manifest), from the first node that
    /// merged it.
    pub roots: BTreeMap<(u64, [u8; 32]), NodeHash>,
    pub manifests: BTreeMap<[u8; 32], Manifest>,
    /// Sealed logs by (node, epoch).
    /// Sealed logs by (node, epoch): hash, and the transactions in it.
    pub sealed: BTreeMap<(u32, u64), ([u8; 32], Vec<TxnId>)>,
    pub abandoned: BTreeSet<(u32, u64)>,
    /// Committed transactions, with their epoch.
    pub committed: Vec<(TxnId, u64)>,
    pub merges: u64,
    pub unmerges: u64,
    pub catch_ups: u64,
    pub repairs: u64,
    pub mismatches: Vec<String>,
    pub published: u64,
    pub conflicts_by_epoch: BTreeMap<(u64, [u8; 32]), usize>,
    /// Wall time in node code, by kind (ticks, and messages by name).
    pub time_ms: BTreeMap<&'static str, (u64, f64)>,
    /// Messages sent, by kind: count and encoded bytes.
    pub sent: BTreeMap<&'static str, (u64, u64)>,
}

pub struct SimNode {
    peer: Option<Peer>,
    fs: Arc<CrashFs>,
    // Conflict model: cumulative probabilities, and the last epoch written.
    cumulative: Vec<f64>,
    model_epoch: u64,
    clock: ManualClock,
    rng: SimRng,
    oracle: Rc<RefCell<Oracle>>,
    members: Vec<NodeId>,
    params: Params,
    tick_ms: u64,
}

fn id_of(i: NodeIndex) -> NodeId {
    NodeId(i as u32 + 1)
}

fn index_of(id: NodeId) -> NodeIndex {
    id.0 as usize - 1
}

impl SimNode {
    fn boot(ctx: &mut Ctx<'_>, fs: Arc<CrashFs>, oracle: Rc<RefCell<Oracle>>, params: Params) -> SimNode {
        let clock = ManualClock::new(ctx.now_ms());
        let env = Env { fs: Arc::new(SimFs(fs.clone())), clock: Arc::new(clock.clone()) };
        let members: Vec<NodeId> = (0..params.nodes).map(id_of).collect();
        let mut node = SimNode {
            peer: None,
            cumulative: node_params_cumulative(&params),
            model_epoch: 0,
            fs,
            clock,
            rng: ctx.rng().fork(),
            oracle,
            members: members.clone(),
            tick_ms: params.epoch_ms / 5,
            params,
        };
        let r = Store::open_existing(PATH, env);
        let store = node.check(r);
        let mut cfg = PeerConfig::new(members, node.params.epoch_ms);
        cfg.quorum = node.params.quorum;
        cfg.gossip = node.params.gossip;
        cfg.status_interval_ms = node.params.epoch_ms;
        // Logs can arrive as late as the largest clock skew plus the
        // largest network delay after the leader's end of the epoch.
        let sim = &node.params.sim;
        cfg.log_deadline_ms = 2 * sim.clocks.max_offset_ms + sim.net.max_delay_ms + 50;
        let r = Peer::new(store, cfg);
        let peer = node.check(r);
        node.peer = Some(peer);
        node.flush(ctx);
        let first = node.rng.below(node.tick_ms);
        ctx.set_timer(first, 0);
        node
    }

    // Unwraps a store result. An error on a dead file system is a crash;
    // what the node did before it still counts.
    fn check<T>(&mut self, r: synod_store::Result<T>) -> T {
        match r {
            Ok(v) => v,
            Err(_) if self.fs.is_dead() => {
                self.record_events();
                std::panic::panic_any(CrashSignal)
            }
            Err(e) => panic!("node error: {e}"),
        }
    }

    fn flush(&mut self, ctx: &mut Ctx<'_>) {
        let peer = self.peer.as_mut().unwrap();
        let mut o = self.oracle.borrow_mut();
        for (to, msg) in peer.take_outbox() {
            let bytes = msg.encode();
            let e = o.sent.entry(msg.name()).or_default();
            e.0 += 1;
            e.1 += bytes.len() as u64;
            ctx.send(index_of(to), bytes);
        }
        drop(o);
        self.record_events();
    }

    fn record_events(&mut self) {
        let Some(peer) = self.peer.as_mut() else { return };
        let events = peer.take_events();
        let me = index_of(self.peer.as_ref().unwrap().store().config().node);
        let mut o = self.oracle.borrow_mut();
        for e in events {
            match e {
                Event::Sealed { epoch, hash, bytes, .. } => {
                    let key = (id_of(me).0, epoch);
                    if let Some((h, _)) = o.sealed.get(&key) {
                        assert_eq!(*h, hash, "node {me} sealed epoch {epoch} twice, differently");
                    }
                    let (_, entries) = decode_log(&bytes).unwrap();
                    let mut txns: Vec<TxnId> = entries.iter().map(|e| e.txn).collect();
                    txns.dedup();
                    o.sealed.insert(key, (hash, txns));
                }
                Event::Published(m) => {
                    o.published += 1;
                    o.manifests.insert(m.hash(), m);
                }
                Event::Merged { manifest, root, conflicts } => {
                    o.merges += 1;
                    let h = manifest.hash();
                    let key = (manifest.epoch, h);
                    match o.roots.get(&key) {
                        Some(r) if *r != root => {
                            let line =
                                format!("n{me}: epoch {} manifest {:02x?}: root differs", manifest.epoch, &h[..4]);
                            o.mismatches.push(line);
                        }
                        _ => {
                            o.roots.insert(key, root);
                        }
                    }
                    o.conflicts_by_epoch.insert(key, conflicts);
                    o.manifests.insert(h, manifest);
                }
                Event::Unmerged(_) => o.unmerges += 1,
                Event::Adopted { repair, .. } => {
                    if repair {
                        o.repairs += 1;
                    } else {
                        o.catch_ups += 1;
                    }
                }
                Event::Abandoned { epoch, .. } => {
                    o.abandoned.insert((id_of(me).0, epoch));
                }
                Event::Mismatch { from, epoch, .. } => {
                    o.mismatches.push(format!("n{me}: mismatch with {from} at epoch {epoch}"));
                }
            }
        }
    }

    fn time(&self, what: &'static str, t: std::time::Instant) {
        let mut o = self.oracle.borrow_mut();
        let e = o.time_ms.entry(what).or_default();
        e.0 += 1;
        e.1 += t.elapsed().as_secs_f64() * 1000.0;
    }

    // One write of the conflict model: a new price (lww_flag) and a sale
    // (delta) on a row drawn from the access pattern.
    fn model_write(&mut self) {
        let x = (self.rng.below(1 << 53) as f64) / (1u64 << 53) as f64;
        let k = self.cumulative.partition_point(|c| *c <= x).min(self.cumulative.len() - 1);
        let sku = Value::Text(format!("K-{k}"));
        let price = Value::Decimal(format!("{}.{:02}", self.rng.below(1000), self.rng.below(100)).parse().unwrap());
        let store = self.peer.as_mut().unwrap().store_mut();
        let r = store.begin().and_then(|mut t| {
            let id = t.find("stock", "sku = ?1", std::slice::from_ref(&sku))?[0];
            let Value::Integer(q) = t.get("stock", id)?.unwrap()[3] else { unreachable!() };
            t.update("stock", id, &[("price", price), ("qty", Value::Integer(q - 1))])?;
            t.commit()
        });
        match r {
            Ok(c) => self.oracle.borrow_mut().committed.push((c.txn, c.epoch)),
            Err(_) if self.fs.is_dead() => std::panic::panic_any(CrashSignal),
            Err(e) => panic!("model write: {e}"),
        }
    }

    fn write(&mut self) {
        let keys = self.params.keys;
        let k = self.rng.below(keys);
        let sku = Value::Text(format!("K-{k}"));
        let kind = self.rng.below(10);
        let qty_delta = self.rng.range(1, 5) as i64;
        let tag = Value::Text(format!("t{}", self.rng.below(4)));
        let price = Value::Decimal(format!("{}.{:02}", self.rng.below(10), self.rng.below(100)).parse().unwrap());
        let peer = self.peer.as_mut().unwrap();
        let store = peer.store_mut();
        let mut txn = match store.begin() {
            Ok(t) => t,
            Err(_) if self.fs.is_dead() => std::panic::panic_any(CrashSignal),
            Err(e) => panic!("begin: {e}"),
        };
        let found = txn.find("stock", "sku = ?1", std::slice::from_ref(&sku));
        let r: synod_store::Result<()> = found.and_then(|ids| match (ids.first(), kind) {
            (None, _) => txn
                .insert(
                    "stock",
                    &[("sku", sku.clone()), ("name", Value::Text("item".into())), ("qty", Value::Integer(10))],
                )
                .map(|_| ()),
            (Some(&id), 0) => txn.delete("stock", id),
            (Some(&id), 1..=3) => {
                let cur = txn.get("stock", id)?.unwrap();
                let Value::Integer(q) = cur[3] else { panic!("{cur:?}") };
                txn.update(
                    "stock",
                    id,
                    &[("qty", Value::Integer((q - qty_delta).max(0) + if q < qty_delta { 5 } else { 0 }))],
                )
            }
            (Some(&id), 4..=6) => txn.update("stock", id, &[("price", price)]),
            (Some(&id), 7..=8) => {
                let cur = txn.get("stock", id)?.unwrap();
                let old = match &cur[4] {
                    Value::Set(s) => s.items().to_vec(),
                    _ => Vec::new(),
                };
                let set = synod_core::ValueSet::new(old.into_iter().chain([tag])).unwrap();
                txn.update("stock", id, &[("tags", Value::Set(set))])
            }
            (Some(&id), _) => txn.update("stock", id, &[("name", Value::Text(format!("n{kind}")))]),
        });
        match r {
            Ok(()) | Err(Error::Constraint(_)) => {}
            Err(_) if self.fs.is_dead() => std::panic::panic_any(CrashSignal),
            Err(e) => panic!("write: {e}"),
        }
        match txn.commit() {
            Ok(c) if c.entries > 0 => self.oracle.borrow_mut().committed.push((c.txn, c.epoch)),
            Ok(_) => {}
            Err(_) if self.fs.is_dead() => std::panic::panic_any(CrashSignal),
            Err(e) => panic!("commit: {e}"),
        }
    }
}

impl Node for SimNode {
    fn on_message(&mut self, ctx: &mut Ctx<'_>, from: NodeIndex, msg: &[u8]) {
        self.clock.set(ctx.now_ms());
        let msg = Msg::decode(msg).expect("message from a peer");
        let name = msg.name();
        let t = std::time::Instant::now();
        let r = self.peer.as_mut().unwrap().on_message(id_of(from), msg);
        self.time(name, t);
        self.check(r);
        self.flush(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>, _timer: u64) {
        self.clock.set(ctx.now_ms());
        match self.params.model {
            None => {
                if self.rng.chance(self.params.write_per_million) {
                    self.write();
                }
            }
            Some(m) => {
                let shift = if m.after_merge { self.params.epoch_ms * 4 / 5 } else { 0 };
                let epoch = ctx.now_ms().saturating_sub(START_MS + shift) / self.params.epoch_ms;
                if epoch > self.model_epoch && epoch < self.params.epochs {
                    self.model_epoch = epoch;
                    for _ in 0..m.w {
                        self.model_write();
                    }
                }
            }
        }
        let t = std::time::Instant::now();
        let r = self.peer.as_mut().unwrap().tick();
        self.time("tick", t);
        self.check(r);
        self.flush(ctx);
        ctx.set_timer(self.tick_ms, 0);
    }

    fn arm_crash(&mut self, ops: u64) -> bool {
        self.fs.crash_after(ops);
        true
    }
}

impl Drop for SimNode {
    // A crash: nothing more reaches the disk, then unsynced data is lost.
    fn drop(&mut self) {
        self.fs.crash_after(0);
        drop(self.peer.take());
        self.fs.crash(&mut self.rng);
    }
}

fn node_params_cumulative(params: &Params) -> Vec<f64> {
    let Some(m) = params.model else { return Vec::new() };
    let mut acc = 0.0;
    m.dist
        .probs(m.rows)
        .into_iter()
        .map(|p| {
            acc += p;
            acc
        })
        .collect()
}

/// Makes the stores of all nodes, durable, before the run. With the
/// conflict model, node 1 makes the rows and merges them alone in epoch 0,
/// and the other nodes are clones of it.
fn make_stores(params: &Params, seed: u64) -> Vec<Arc<CrashFs>> {
    if let Some(m) = params.model {
        let fss: Vec<Arc<CrashFs>> =
            (0..params.nodes).map(|i| Arc::new(CrashFs::new(seed.wrapping_mul(1000) + i as u64))).collect();
        let clock = ManualClock::new(START_MS + 1);
        let env = Env { fs: Arc::new(SimFs(fss[0].clone())), clock: Arc::new(clock.clone()) };
        let config = StoreConfig {
            node: id_of(0),
            genesis_ms: START_MS,
            epoch_len_ms: params.epoch_ms,
            max_drift_ms: 500,
            lateness: params.lateness,
        };
        let mut s = Store::open(PATH, config, env).unwrap();
        s.create_table(params.schema).unwrap();
        let mut t = s.begin().unwrap();
        for k in 0..m.rows {
            t.insert("stock", &[("sku", Value::Text(format!("K-{k}"))), ("qty", Value::Integer(1_000_000))]).unwrap();
        }
        t.commit().unwrap();
        clock.set(START_MS + params.epoch_ms);
        s.seal().unwrap();
        s.merge(0, &[]).unwrap();
        for i in 1..params.nodes {
            let path = format!("/clone-{i}.db");
            drop(s.clone_base(&path, id_of(i)).unwrap());
            fss[i].write_file(PATH, &fss[0].read(&path).unwrap());
        }
        drop(s);
        for f in fss[0].list() {
            let data = fss[0].read(&f).unwrap();
            fss[0].write_file(&f, &data);
        }
        return fss;
    }
    (0..params.nodes)
        .map(|i| {
            let fs = Arc::new(CrashFs::new(seed.wrapping_mul(1000) + i as u64));
            let env = Env { fs: Arc::new(SimFs(fs.clone())), clock: Arc::new(ManualClock::new(START_MS)) };
            let config = StoreConfig {
                node: id_of(i),
                genesis_ms: START_MS,
                epoch_len_ms: params.epoch_ms,
                max_drift_ms: 500,
                lateness: params.lateness,
            };
            let mut s = Store::open(PATH, config, env).unwrap();
            s.create_table(params.schema).unwrap();
            drop(s);
            for f in fs.list() {
                let data = fs.read(&f).unwrap();
                fs.write_file(&f, &data);
            }
            fs
        })
        .collect()
}

/// The result of a run.
pub struct Run {
    pub sim: Sim<SimNode>,
    pub oracle: Rc<RefCell<Oracle>>,
    pub fss: Vec<Arc<CrashFs>>,
    pub params: Params,
}

/// Runs a cluster: `epochs` epochs with faults, then a quiet time with no
/// faults, so that every node converges.
pub fn run(seed: u64, params: Params) -> Run {
    let mut run = start(seed, params);
    run.sim.run_for(run.params.epochs * run.params.epoch_ms);
    run.finish();
    run
}

/// Makes the cluster; nothing runs yet.
pub fn start(seed: u64, params: Params) -> Run {
    let fss = make_stores(&params, seed);
    let oracle = Rc::new(RefCell::new(Oracle::default()));
    if params.model.is_some() {
        // The merge of epoch 0 that made the rows, before the run.
        let env = Env { fs: Arc::new(SimFs(fss[0].clone())), clock: Arc::new(ManualClock::new(START_MS)) };
        let s = Store::open_existing(PATH, env).unwrap();
        let m = s.merged_record(0).unwrap().unwrap().manifest;
        oracle.borrow_mut().manifests.insert(m.hash(), m);
        drop(s);
        for f in fss[0].list() {
            let data = fss[0].read(&f).unwrap();
            fss[0].write_file(&f, &data);
        }
    }
    let (o, f, p) = (oracle.clone(), fss.clone(), params.clone());
    let mut sim =
        Sim::new(seed, params.sim.clone(), move |ctx| SimNode::boot(ctx, f[ctx.node()].clone(), o.clone(), p.clone()));
    // Long runs keep only the digest of the trace.
    if params.epochs * params.nodes as u64 > 5_000 {
        sim.keep_trace(false);
    }
    Run { sim, oracle, fss, params }
}

impl Run {
    /// Stops the faults and runs until all nodes agree, or fails.
    pub fn finish(&mut self) {
        self.sim.stop_faults();
        for fs in &self.fss {
            fs.disarm();
        }
        let epoch_ms = self.params.epoch_ms;
        // Enough for long downtimes to end and for a catch-up.
        let max = self.params.sim.faults.long_downtime_ms.1 / epoch_ms + 4 * self.params.lateness + 40;
        for _ in 0..max {
            self.sim.run_for(epoch_ms);
            if self.converged() {
                return;
            }
        }
        panic!("no convergence after {max} quiet epochs: {}", self.positions());
    }

    /// Returns a node that is up.
    pub fn peer_mut(&mut self, i: NodeIndex) -> Option<&mut Peer> {
        self.sim.node_mut(i).and_then(|n| n.peer.as_mut())
    }

    pub fn positions(&self) -> String {
        (0..self.params.nodes)
            .map(|i| match self.sim.node(i).and_then(|n| n.peer.as_ref()) {
                None => format!("n{i}: down"),
                Some(p) => {
                    let s = p.store();
                    format!("n{i}: merged {:?} head {:02x?}", s.merged_epoch().unwrap(), &s.head().unwrap()[..4])
                }
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn converged(&self) -> bool {
        let mut heads = BTreeSet::new();
        for i in 0..self.params.nodes {
            let Some(p) = self.sim.node(i).and_then(|n| n.peer.as_ref()) else { return false };
            let s = p.store();
            heads.insert((s.merged_epoch().unwrap(), s.head().unwrap(), s.base_root().unwrap()));
        }
        heads.len() == 1
    }

    pub fn peer(&self, i: NodeIndex) -> &Peer {
        self.sim.node(i).unwrap().peer.as_ref().unwrap()
    }

    /// Checks the gate: no root hash mismatch on one manifest, every node
    /// on one state, and every committed write in a sealed log that the
    /// final chain merged or that its node gave up to `_conflicts`.
    pub fn check(&self) {
        let o = self.oracle.borrow();
        assert!(o.mismatches.is_empty(), "root hash mismatches: {:?}", o.mismatches);
        for i in 0..self.params.nodes {
            assert_eq!(self.peer(i).metrics().hash_mismatches, 0);
        }
        // The final chain, back from the common head.
        let head = self.peer(0).store().head().unwrap();
        let mut chain_logs: BTreeMap<(u32, u64), [u8; 32]> = BTreeMap::new();
        let mut h = head;
        let mut first_epoch = None;
        while h != [0; 32] {
            let Some(m) = o.manifests.get(&h) else { break };
            for l in &m.logs {
                chain_logs.insert((l.node.0, l.epoch), l.hash);
            }
            first_epoch = Some(m.epoch);
            h = m.parent;
        }
        assert_eq!(first_epoch, Some(0), "the chain goes back to the first epoch");
        let next = self.peer(0).store().next_merge_epoch().unwrap();
        // Every sealed log that no merge can take any more is merged or
        // given up.
        let lateness = self.params.lateness;
        for ((node, epoch), (hash, _)) in &o.sealed {
            if epoch + lateness >= next {
                continue;
            }
            let merged = chain_logs.get(&(*node, *epoch)) == Some(hash);
            let given_up = o.abandoned.contains(&(*node, *epoch));
            assert!(merged || given_up, "log of node {node} epoch {epoch}: neither merged nor given up");
        }
        // Every committed transaction is in its node's sealed log.
        let mut in_logs: BTreeSet<TxnId> = BTreeSet::new();
        let mut last_sealed: BTreeMap<u32, u64> = BTreeMap::new();
        for ((node, epoch), (_, txns)) in &o.sealed {
            in_logs.extend(txns.iter().copied());
            let l = last_sealed.entry(*node).or_default();
            *l = (*l).max(*epoch);
        }
        let mut checked = 0;
        for (txn, epoch) in &o.committed {
            if last_sealed.get(&txn.node.0).is_some_and(|l| epoch <= l) {
                assert!(in_logs.contains(txn), "committed txn {txn:?} of epoch {epoch} is in no sealed log");
                checked += 1;
            }
        }
        assert!(checked > 0 || o.committed.is_empty());
    }
}

/// Settings for a quick run.
pub fn params(nodes: usize, epochs: u64) -> Params {
    let mut sim = Config { nodes, start_ms: START_MS, ..Config::default() };
    sim.clocks.max_drift_ppm = 20;
    Params {
        nodes,
        epochs,
        epoch_ms: 1_000,
        lateness: 6,
        quorum: true,
        gossip: None,
        write_per_million: 300_000,
        keys: 40,
        model: None,
        schema: SCHEMA,
        sim,
    }
}

/// Random settings for one seed: 5 to `max_nodes` nodes, quorum or not,
/// all-to-all or gossip, late nodes (down for longer than L epochs), and
/// clock drift.
pub fn random_params(seed: u64, max_nodes: usize, epochs: u64) -> Params {
    let mut rng = SimRng::new(seed ^ 0x005E_ED0F_C1A5);
    let nodes = rng.range(5, max_nodes as u64 + 1) as usize;
    let mut p = params(nodes, epochs);
    p.lateness = rng.range(3, 10);
    p.quorum = rng.chance(700_000);
    p.gossip = rng.chance(300_000).then(|| rng.range(2, 4) as usize);
    p.keys = rng.range(10, 200);
    p.write_per_million = rng.range(50_000, 600_000) as u32;
    p.sim.clocks.max_offset_ms = rng.range(0, 300);
    p.sim.clocks.max_drift_ppm = rng.range(0, 100) as u32;
    p.sim.net.loss_per_million = rng.range(0, 50_000) as u32;
    p.sim.faults.long_downtime_per_million = rng.range(0, 300_000) as u32;
    p.sim.faults.long_downtime_ms = (p.lateness * 1_000, p.lateness * 3_000);
    p
}
