//! Phase 0 exit gate: HLC and row ID properties hold in a simulated cluster
//! with clock drift, message loss, partitions, and crashes at any disk
//! write; and one seed replays a run byte for byte.
//!
//! Each node, on a timer, allocates a row ID, takes an HLC value, and sends
//! both to a random peer. A receiver merges the HLC. A shared ledger checks
//! that no row ID is ever given out twice, across all nodes and crashes.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::rc::Rc;

use synod_core::{CounterStore, Hlc, HlcClock, HlcError, NodeId, RowId, RowIdAllocator};
use synod_sim::{Config, Ctx, Disk, Node, NodeIndex, Sim, check_seeds};

const MAX_DRIFT_MS: u64 = 500;
const COUNTER_FILE: &str = "row_id.high_water";

/// A counter store on the simulated disk.
struct DiskCounter {
    disk: Disk,
    sync: bool,
}

impl CounterStore for DiskCounter {
    type Error = Infallible;

    fn load(&mut self) -> Result<u64, Infallible> {
        Ok(self.disk.read(COUNTER_FILE).map_or(0, |b| u64::from_be_bytes(b.try_into().unwrap())))
    }

    fn persist(&mut self, high_water: u64) -> Result<(), Infallible> {
        self.disk.write(COUNTER_FILE, &high_water.to_be_bytes());
        if self.sync {
            self.disk.sync(COUNTER_FILE);
        }
        Ok(())
    }
}

/// What the test harness sees across all nodes and crashes.
#[derive(Default)]
struct Ledger {
    row_ids: BTreeSet<RowId>,
    duplicates: Vec<RowId>,
    accepted: u64,
    rejected_from: BTreeSet<NodeIndex>,
}

struct Peer {
    clock: HlcClock,
    ids: RowIdAllocator<DiskCounter>,
    ledger: Rc<RefCell<Ledger>>,
}

impl Peer {
    fn tick(&mut self, ctx: &mut Ctx<'_>) {
        let id = self.ids.allocate().unwrap();
        {
            let mut ledger = self.ledger.borrow_mut();
            if !ledger.row_ids.insert(id) {
                ledger.duplicates.push(id);
            }
        }
        let hlc = self.stamp(ctx, None);
        let peers = ctx.nodes() as u64 - 1;
        let mut to = ctx.rng().below(peers) as usize;
        if to >= ctx.node() {
            to += 1;
        }
        let mut msg = hlc.to_be_bytes().to_vec();
        msg.extend_from_slice(&id.to_be_bytes());
        ctx.send(to, msg);
        let delay = ctx.rng().range(5, 50);
        ctx.set_timer(delay, 0);
    }

    // Takes an HLC value for a local event or a receive, and checks the
    // HLC properties.
    fn stamp(&mut self, ctx: &mut Ctx<'_>, remote: Option<Hlc>) -> Hlc {
        let before = self.clock.last();
        let local = ctx.now_ms();
        let h = match remote {
            None => self.clock.now(local).unwrap(),
            Some(r) => {
                let h = self.clock.update(r, local).unwrap();
                assert!(h > r, "receive {h:?} not after send {r:?}");
                h
            }
        };
        assert!(h > before, "HLC went from {before:?} to {h:?}");
        assert!(h.physical_ms() >= local);
        assert!(h.physical_ms() <= local + MAX_DRIFT_MS + 1, "HLC {h:?} too far ahead of {local}");
        h
    }
}

impl Node for Peer {
    fn on_message(&mut self, ctx: &mut Ctx<'_>, from: NodeIndex, msg: &[u8]) {
        let remote = Hlc::from_be_bytes(msg[..8].try_into().unwrap());
        let id = RowId::from_be_bytes(msg[8..].try_into().unwrap()).unwrap();
        assert_eq!(id.node(), NodeId(from as u32));
        match self.clock.check(remote, ctx.now_ms()) {
            Ok(()) => {
                self.stamp(ctx, Some(remote));
                self.ledger.borrow_mut().accepted += 1;
            }
            Err(HlcError::DriftExceeded { .. }) => {
                self.ledger.borrow_mut().rejected_from.insert(from);
            }
            Err(e) => panic!("{e}"),
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>, _timer: u64) {
        self.tick(ctx);
    }
}

struct Run {
    sim: Sim<Peer>,
    ledger: Rc<RefCell<Ledger>>,
}

fn cluster(seed: u64, config: Config, sync: bool) -> Run {
    let ledger = Rc::new(RefCell::new(Ledger::default()));
    let shared = ledger.clone();
    let sim = Sim::new(seed, config, move |ctx| {
        let store = DiskCounter { disk: ctx.disk(), sync };
        // A small block, so the counter file is written often.
        let ids = RowIdAllocator::open(NodeId(ctx.node() as u32), store, 4).unwrap();
        let delay = ctx.rng().range(0, 20);
        ctx.set_timer(delay, 0);
        // The HLC is not persisted: after a restart it starts again from the
        // physical clock. Row IDs must stay unique all the same.
        Peer { clock: HlcClock::new(MAX_DRIFT_MS), ids, ledger: shared.clone() }
    });
    Run { sim, ledger }
}

fn config(seed: u64) -> Config {
    Config { nodes: 3 + (seed % 6) as usize, ..Config::default() }
}

#[test]
fn hlc_and_row_ids_under_faults() {
    check_seeds("hlc_and_row_ids_under_faults", 20, |seed| {
        let mut run = cluster(seed, config(seed), true);
        run.sim.run_for(10 * 60 * 1000);
        let ledger = run.ledger.borrow();
        let stats = run.sim.stats();
        assert!(ledger.duplicates.is_empty(), "duplicate row IDs: {:?}", ledger.duplicates);
        assert!(ledger.rejected_from.is_empty(), "drift rejections from {:?}", ledger.rejected_from);
        assert!(ledger.accepted > 1000, "{} messages accepted", ledger.accepted);
        assert!(stats.crashes > 0 && stats.crashes_at_write > 0 && stats.partitions > 0, "{stats:?}");
    });
}

#[test]
fn peer_with_fast_clock_is_rejected() {
    check_seeds("peer_with_fast_clock_is_rejected", 5, |seed| {
        let mut run = cluster(seed, config(seed), true);
        run.sim.set_clock(0, 10_000, 0);
        run.sim.run_for(60 * 1000);
        let ledger = run.ledger.borrow();
        assert_eq!(ledger.rejected_from, BTreeSet::from([0]));
        assert!(ledger.duplicates.is_empty());
    });
}

#[test]
fn same_seed_replays_byte_for_byte() {
    let trace = |seed| {
        let mut run = cluster(seed, config(seed), true);
        run.sim.run_for(2 * 60 * 1000);
        (run.sim.trace().to_vec(), run.sim.stats().clone())
    };
    for seed in [1, 2, 3] {
        let (a, stats_a) = trace(seed);
        let (b, stats_b) = trace(seed);
        assert!(a == b, "seed {seed} did not replay");
        assert_eq!(stats_a, stats_b);
        assert!(a.len() > 10_000);
    }
    assert_ne!(trace(1).0, trace(2).0);
}

/// The harness must find a real bug: a counter store that does not sync
/// loses its high-water mark at a crash, and row IDs get reused.
#[test]
fn harness_finds_missing_sync() {
    let found = (0..50).find(|&seed| {
        let mut run = cluster(seed, config(seed), false);
        run.sim.run_for(10 * 60 * 1000);
        !run.ledger.borrow().duplicates.is_empty()
    });
    assert!(found.is_some(), "no seed found the missing sync");
}
