//! The simulated cluster: virtual time, clocks, network, and faults.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write as _;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Once;

use crate::disk::{CrashSignal, Disk};
use crate::rng::SimRng;

/// Index of a node in the simulation, from 0 to `nodes - 1`.
pub type NodeIndex = usize;

/// Network behavior.
#[derive(Clone, Debug)]
pub struct NetConfig {
    /// Smallest message delay.
    pub min_delay_ms: u64,
    /// Largest message delay. Messages can arrive out of order.
    pub max_delay_ms: u64,
    /// Chance that a message is lost, per million.
    pub loss_per_million: u32,
}

impl Default for NetConfig {
    fn default() -> NetConfig {
        NetConfig { min_delay_ms: 1, max_delay_ms: 50, loss_per_million: 10_000 }
    }
}

/// Local clock behavior. Each node gets a random fixed offset and a random
/// rate error.
#[derive(Clone, Debug)]
pub struct ClockConfig {
    /// Largest offset from true time, in either direction.
    pub max_offset_ms: u64,
    /// Largest rate error in parts per million, in either direction.
    pub max_drift_ppm: u32,
}

impl Default for ClockConfig {
    fn default() -> ClockConfig {
        ClockConfig { max_offset_ms: 100, max_drift_ppm: 200 }
    }
}

/// Crash and partition behavior. A `None` interval turns the fault off.
#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// Time between two crashes in the cluster, chosen uniformly in this
    /// range.
    pub crash_interval_ms: Option<(u64, u64)>,
    /// How long a crashed node stays down.
    pub downtime_ms: (u64, u64),
    /// Chance per million that a crash happens inside the node's next disk
    /// operations, not between events.
    pub crash_at_write_per_million: u32,
    /// Time between two network partitions.
    pub partition_interval_ms: Option<(u64, u64)>,
    /// How long a partition lasts.
    pub partition_duration_ms: (u64, u64),
}

impl Default for FaultConfig {
    fn default() -> FaultConfig {
        FaultConfig {
            crash_interval_ms: Some((1_000, 10_000)),
            downtime_ms: (100, 3_000),
            crash_at_write_per_million: 500_000,
            partition_interval_ms: Some((5_000, 30_000)),
            partition_duration_ms: (500, 10_000),
        }
    }
}

/// Simulation settings.
#[derive(Clone, Debug)]
pub struct Config {
    /// Number of nodes.
    pub nodes: usize,
    /// True time at the start, in milliseconds since the Unix epoch.
    pub start_ms: u64,
    /// Network behavior.
    pub net: NetConfig,
    /// Clock behavior.
    pub clocks: ClockConfig,
    /// Fault behavior.
    pub faults: FaultConfig,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            nodes: 3,
            start_ms: 1_791_763_200_000, // 2026-10-12 00:00 UTC
            net: NetConfig::default(),
            clocks: ClockConfig::default(),
            faults: FaultConfig::default(),
        }
    }
}

/// The code of one simulated node.
///
/// The simulator makes a node with the boot function when the node starts
/// and after each crash. A node keeps state across a crash only through its
/// [`Disk`].
pub trait Node {
    /// Handles a message from another node.
    fn on_message(&mut self, ctx: &mut Ctx<'_>, from: NodeIndex, msg: &[u8]);
    /// Handles a timer that this node set with [`Ctx::set_timer`].
    fn on_timer(&mut self, ctx: &mut Ctx<'_>, timer: u64);
}

enum Action {
    Send { to: NodeIndex, msg: Vec<u8> },
    Timer { after_ms: u64, id: u64 },
}

/// What a node can see and do in a callback.
pub struct Ctx<'a> {
    node: NodeIndex,
    nodes: usize,
    local_ms: u64,
    disk: &'a Disk,
    rng: &'a mut SimRng,
    actions: Vec<Action>,
    trace: &'a mut Vec<u8>,
    true_ms: u64,
}

impl Ctx<'_> {
    /// Returns this node's index.
    pub fn node(&self) -> NodeIndex {
        self.node
    }

    /// Returns the number of nodes.
    pub fn nodes(&self) -> usize {
        self.nodes
    }

    /// Returns this node's local physical clock, in milliseconds. It has
    /// the node's offset and rate error; the node never sees true time.
    pub fn now_ms(&self) -> u64 {
        self.local_ms
    }

    /// Returns this node's disk.
    pub fn disk(&self) -> Disk {
        self.disk.clone()
    }

    /// Returns this node's random number generator.
    pub fn rng(&mut self) -> &mut SimRng {
        self.rng
    }

    /// Sends a message. The network can delay, reorder, or lose it.
    pub fn send(&mut self, to: NodeIndex, msg: Vec<u8>) {
        assert!(to < self.nodes, "send to node {to} of {}", self.nodes);
        self.actions.push(Action::Send { to, msg });
    }

    /// Sets a timer. A crash cancels all timers of the node.
    pub fn set_timer(&mut self, after_ms: u64, id: u64) {
        self.actions.push(Action::Timer { after_ms, id });
    }

    /// Adds a line to the simulation trace.
    pub fn trace(&mut self, args: fmt::Arguments<'_>) {
        writeln!(self.trace, "{} n{}: {}", self.true_ms, self.node, args).unwrap();
    }
}

#[derive(Debug)]
enum Event {
    Boot { node: NodeIndex },
    Deliver { from: NodeIndex, to: NodeIndex, msg: Vec<u8> },
    Timer { node: NodeIndex, incarnation: u64, id: u64 },
    Crash,
    Partition,
    Heal,
}

struct Slot<N> {
    node: Option<N>,
    incarnation: u64,
    disk: Disk,
    rng: SimRng,
    offset_ms: i64,
    drift_ppm: i64,
    side: bool,
}

/// Counts of what happened in a run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Messages delivered.
    pub delivered: u64,
    /// Messages lost to the network, a partition, or a node that was down.
    pub dropped: u64,
    /// Node crashes.
    pub crashes: u64,
    /// Crashes inside a disk operation.
    pub crashes_at_write: u64,
    /// Unsynced disk operations lost or torn at crashes.
    pub lost_writes: u64,
    /// Network partitions.
    pub partitions: u64,
}

type BootFn<N> = Box<dyn FnMut(&mut Ctx<'_>) -> N>;

/// A deterministic simulation of a cluster.
///
/// All randomness comes from the seed, and events at the same time run in a
/// fixed order, so one seed always gives the same run and the same
/// [`trace`](Sim::trace).
pub struct Sim<N> {
    config: Config,
    now: u64,
    seq: u64,
    queue: BTreeMap<(u64, u64), Event>,
    slots: Vec<Slot<N>>,
    rng: SimRng,
    boot: Option<BootFn<N>>,
    trace: Vec<u8>,
    partitioned: bool,
    stats: Stats,
}

impl<N: Node> Sim<N> {
    /// Makes a simulation. `boot` makes a node at start and after each
    /// crash; it can read the disk and set timers.
    pub fn new(seed: u64, config: Config, boot: impl FnMut(&mut Ctx<'_>) -> N + 'static) -> Sim<N> {
        install_crash_hook();
        let mut rng = SimRng::new(seed);
        let max_offset = config.clocks.max_offset_ms;
        let max_drift = config.clocks.max_drift_ppm as u64;
        let slots = (0..config.nodes)
            .map(|_| Slot {
                node: None,
                incarnation: 0,
                disk: Disk::new(),
                rng: rng.fork(),
                offset_ms: rng.range(0, 2 * max_offset) as i64 - max_offset as i64,
                drift_ppm: rng.range(0, 2 * max_drift) as i64 - max_drift as i64,
                side: false,
            })
            .collect();
        let mut sim = Sim {
            now: config.start_ms,
            seq: 0,
            queue: BTreeMap::new(),
            slots,
            rng,
            boot: Some(Box::new(boot)),
            trace: Vec::new(),
            partitioned: false,
            stats: Stats::default(),
            config,
        };
        for node in 0..sim.config.nodes {
            sim.schedule(0, Event::Boot { node });
        }
        if let Some(range) = sim.config.faults.crash_interval_ms {
            let after = sim.rng.range(range.0, range.1);
            sim.schedule(after, Event::Crash);
        }
        if let Some(range) = sim.config.faults.partition_interval_ms {
            let after = sim.rng.range(range.0, range.1);
            sim.schedule(after, Event::Partition);
        }
        sim
    }

    /// Sets the clock offset and rate error of one node. Call it before
    /// the first step.
    pub fn set_clock(&mut self, node: NodeIndex, offset_ms: i64, drift_ppm: i64) {
        self.slots[node].offset_ms = offset_ms;
        self.slots[node].drift_ppm = drift_ppm;
    }

    /// Returns the true time.
    pub fn now_ms(&self) -> u64 {
        self.now
    }

    /// Returns the local clock of a node at the current true time.
    pub fn local_ms(&self, node: NodeIndex) -> u64 {
        let slot = &self.slots[node];
        let elapsed = (self.now - self.config.start_ms) as i128;
        let local = self.now as i128 + slot.offset_ms as i128 + elapsed * slot.drift_ppm as i128 / 1_000_000;
        local.max(0) as u64
    }

    /// Returns a node, or `None` while it is down.
    pub fn node(&self, node: NodeIndex) -> Option<&N> {
        self.slots[node].node.as_ref()
    }

    /// Returns the disk of a node.
    pub fn disk(&self, node: NodeIndex) -> &Disk {
        &self.slots[node].disk
    }

    /// Returns the counts of what happened.
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Returns the trace: one line per event.
    pub fn trace(&self) -> &[u8] {
        &self.trace
    }

    /// Returns the BLAKE3 hash of the trace, as hex.
    pub fn trace_digest(&self) -> String {
        blake3::hash(&self.trace).to_hex().to_string()
    }

    /// Runs until true time reaches `self.now_ms() + ms`.
    pub fn run_for(&mut self, ms: u64) {
        let end = self.now + ms;
        while self.queue.first_key_value().is_some_and(|(&(t, _), _)| t <= end) {
            self.step();
        }
        self.now = end;
    }

    /// Runs the next event. Returns false if there are no events.
    pub fn step(&mut self) -> bool {
        let Some(((time, _), event)) = self.queue.pop_first() else {
            return false;
        };
        self.now = time;
        match event {
            Event::Boot { node } => {
                self.log(format_args!("boot n{node}"));
                self.slots[node].incarnation += 1;
                let mut boot = self.boot.take().expect("boot function in use");
                let made = self.callback(node, |ctx| boot(ctx));
                self.boot = Some(boot);
                self.slots[node].node = made;
            }
            Event::Deliver { from, to, msg } => {
                let slot = &self.slots[to];
                if slot.node.is_none() || (self.partitioned && slot.side != self.slots[from].side) {
                    self.stats.dropped += 1;
                    self.log(format_args!("drop n{from}->n{to} {}B", msg.len()));
                } else {
                    self.stats.delivered += 1;
                    self.log(format_args!("deliver n{from}->n{to} {}B", msg.len()));
                    self.with_node(to, |n, ctx| n.on_message(ctx, from, &msg));
                }
            }
            Event::Timer { node, incarnation, id } => {
                if self.slots[node].incarnation == incarnation && self.slots[node].node.is_some() {
                    self.log(format_args!("timer n{node} #{id}"));
                    self.with_node(node, |n, ctx| n.on_timer(ctx, id));
                }
            }
            Event::Crash => {
                let up: Vec<NodeIndex> = (0..self.slots.len()).filter(|&i| self.slots[i].node.is_some()).collect();
                if !up.is_empty() {
                    let node = *self.rng.pick(&up);
                    if self.rng.chance(self.config.faults.crash_at_write_per_million) {
                        let ops = self.rng.range(0, 4);
                        self.log(format_args!("arm crash n{node} after {ops} disk ops"));
                        self.slots[node].disk.arm_crash(ops);
                    } else {
                        self.crash(node);
                    }
                }
                let range = self.config.faults.crash_interval_ms.unwrap();
                let after = self.rng.range(range.0, range.1);
                self.schedule(after, Event::Crash);
            }
            Event::Partition => {
                let n = self.slots.len();
                if n >= 2 {
                    // Each node takes a random side; node 0 and one other
                    // node are on different sides, so both sides are used.
                    let other = 1 + self.rng.below(n as u64 - 1) as usize;
                    for i in 0..n {
                        let side = match i {
                            0 => false,
                            i if i == other => true,
                            _ => self.rng.chance(500_000),
                        };
                        self.slots[i].side = side;
                    }
                    self.partitioned = true;
                    self.stats.partitions += 1;
                    let sides: String = self.slots.iter().map(|s| if s.side { 'B' } else { 'A' }).collect();
                    self.log(format_args!("partition {sides}"));
                }
                let range = self.config.faults.partition_duration_ms;
                let after = self.rng.range(range.0, range.1);
                self.schedule(after, Event::Heal);
            }
            Event::Heal => {
                self.partitioned = false;
                self.log(format_args!("heal"));
                let range = self.config.faults.partition_interval_ms.unwrap();
                let after = self.rng.range(range.0, range.1);
                self.schedule(after, Event::Partition);
            }
        }
        true
    }

    /// Crashes a node now: it loses its memory and its unsynced writes, and
    /// it boots again after a random downtime. Does nothing if the node is
    /// down.
    pub fn crash(&mut self, node: NodeIndex) {
        if self.slots[node].node.take().is_some() {
            self.go_down(node);
        }
    }

    // The node value is already gone: lose unsynced writes, cancel timers,
    // and schedule the reboot.
    fn go_down(&mut self, node: NodeIndex) {
        let slot = &mut self.slots[node];
        slot.incarnation += 1;
        let lost = slot.disk.crash(&mut self.rng);
        self.stats.crashes += 1;
        self.stats.lost_writes += lost as u64;
        self.log(format_args!("crash n{node}, {lost} unsynced ops lost"));
        let (lo, hi) = self.config.faults.downtime_ms;
        let after = self.rng.range(lo, hi);
        self.schedule(after, Event::Boot { node });
    }

    fn with_node(&mut self, node: NodeIndex, f: impl FnOnce(&mut N, &mut Ctx<'_>)) {
        let Some(mut n) = self.slots[node].node.take() else { return };
        if self.callback(node, |ctx| f(&mut n, ctx)).is_some() {
            self.slots[node].node = Some(n);
        }
    }

    // Runs a node callback. Returns `None` if the node crashed inside it.
    fn callback<R>(&mut self, node: NodeIndex, f: impl FnOnce(&mut Ctx<'_>) -> R) -> Option<R> {
        let local_ms = self.local_ms(node);
        let incarnation = self.slots[node].incarnation;
        let slot = &mut self.slots[node];
        let mut ctx = Ctx {
            node,
            nodes: self.config.nodes,
            local_ms,
            disk: &slot.disk,
            rng: &mut slot.rng,
            actions: Vec::new(),
            trace: &mut self.trace,
            true_ms: self.now,
        };
        let result = panic::catch_unwind(AssertUnwindSafe(|| f(&mut ctx)));
        let actions = std::mem::take(&mut ctx.actions);
        // Messages sent before a crash are already on the network; timers
        // keep the old incarnation, so a crash cancels them.
        for action in actions {
            match action {
                Action::Send { to, msg } => {
                    if self.rng.chance(self.config.net.loss_per_million) {
                        self.stats.dropped += 1;
                        self.log(format_args!("lose n{node}->n{to} {}B", msg.len()));
                    } else {
                        let delay = self.rng.range(self.config.net.min_delay_ms, self.config.net.max_delay_ms);
                        self.schedule(delay, Event::Deliver { from: node, to, msg });
                    }
                }
                Action::Timer { after_ms, id } => {
                    self.schedule(after_ms, Event::Timer { node, incarnation, id });
                }
            }
        }
        match result {
            Ok(r) => Some(r),
            Err(payload) if payload.is::<CrashSignal>() => {
                self.stats.crashes_at_write += 1;
                self.log(format_args!("crash n{node} inside a disk operation"));
                self.go_down(node);
                None
            }
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    fn schedule(&mut self, after_ms: u64, event: Event) {
        self.seq += 1;
        self.queue.insert((self.now + after_ms, self.seq), event);
    }

    fn log(&mut self, args: fmt::Arguments<'_>) {
        writeln!(self.trace, "{} {}", self.now, args).unwrap();
    }
}

/// Installs a panic hook that keeps injected crashes quiet. Other panics
/// go to the previous hook.
pub(crate) fn install_crash_hook() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if !info.payload().is::<CrashSignal>() {
                previous(info);
            }
        }));
    });
}
