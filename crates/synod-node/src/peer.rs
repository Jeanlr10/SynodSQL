//! The node state machine: seal, exchange logs, publish and choose
//! manifests, merge, verify, catch up, and repair (white paper, Sections V
//! and XI; specification Section 6).
//!
//! [`Peer`] does no I/O of its own. The caller gives it time
//! ([`tick`](Peer::tick)) and messages ([`on_message`](Peer::on_message)),
//! and sends what [`take_outbox`](Peer::take_outbox) returns. The same code
//! runs in the simulator and behind the network transport.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use synod_core::NodeId;
use synod_core::manifest::{LogRef, Manifest, leader};
use synod_core::prolly::{NodeHash, TreeWalk, diff_hashes};
use synod_core::{Row, RowId};
use synod_store::{BaseImage, Error, Result, Store};

use crate::metrics::Metrics;
use crate::msg::{Link, Msg, Status};

/// Settings of the epoch cycle.
#[derive(Clone, Debug)]
pub struct PeerConfig {
    /// All member nodes, this one included, sorted by node ID.
    pub members: Vec<NodeId>,
    /// Time after the end of an epoch for logs to arrive, before the
    /// leader of rank 0 publishes without the missing ones.
    pub log_deadline_ms: u64,
    /// Time between two leader ranks: the leader of rank r publishes at
    /// the deadline plus r slots, if no manifest came before.
    pub leader_slot_ms: u64,
    /// If true, a leader publishes only with the logs of the epoch from a
    /// majority of the members. A minority side then waits, and its logs
    /// go into later manifests as late logs, instead of making its own
    /// chain of manifests.
    pub quorum: bool,
    /// `None`: send each log to every member. `Some(k)`: send it to k
    /// random members, which pass on logs that are new to them (gossip).
    pub gossip: Option<usize>,
    /// Time between two status messages to random peers (anti-entropy).
    pub status_interval_ms: u64,
    /// Time before a request without an answer is sent again.
    pub retry_ms: u64,
}

impl PeerConfig {
    /// Settings for these members, with times that fit an epoch length.
    pub fn new(mut members: Vec<NodeId>, epoch_len_ms: u64) -> PeerConfig {
        members.sort();
        members.dedup();
        let unit = (epoch_len_ms / 20).clamp(50, 2_000);
        PeerConfig {
            members,
            log_deadline_ms: unit * 2,
            leader_slot_ms: unit * 2,
            quorum: true,
            gossip: None,
            status_interval_ms: unit * 2,
            retry_ms: unit * 4,
        }
    }
}

/// Something that happened on the node, for tests, traces, and alerts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// An epoch was sealed.
    Sealed {
        /// Epoch.
        epoch: u64,
        /// Log hash.
        hash: [u8; 32],
        /// Number of entries.
        entries: u64,
        /// The log.
        bytes: Vec<u8>,
    },
    /// This node published a manifest as leader.
    Published(Manifest),
    /// A manifest was merged.
    Merged {
        /// The manifest.
        manifest: Manifest,
        /// Root hash of M(e).
        root: NodeHash,
        /// Number of conflicts.
        conflicts: usize,
    },
    /// The merge of an epoch was undone, for a manifest of higher priority.
    Unmerged(u64),
    /// The base state was taken over from a peer.
    Adopted {
        /// The peer.
        from: NodeId,
        /// The peer's merged epoch.
        epoch: Option<u64>,
        /// The base root.
        root: NodeHash,
        /// True for a repair after a mismatch, false for a catch-up.
        repair: bool,
    },
    /// An own log was given up: older than the lateness bound.
    Abandoned {
        /// Epoch of the log.
        epoch: u64,
        /// Log hash.
        hash: [u8; 32],
        /// Entries written to `_conflicts`.
        entries: usize,
    },
    /// A peer has another root hash after the same manifest (an alert).
    Mismatch {
        /// The peer.
        from: NodeId,
        /// Epoch.
        epoch: u64,
        /// This node's root.
        ours: NodeHash,
        /// The peer's root.
        theirs: NodeHash,
    },
}

// A catch-up or repair from one peer, in steps.
struct Adopt {
    peer: NodeId,
    repair: bool,
    last_progress_ms: u64,
    stage: Stage,
}

enum Stage {
    Image,
    Walk { image: BaseImage, walk: TreeWalk },
    Rows { image: BaseImage, removed: Vec<RowId>, rows: Vec<(u32, Row)>, waiting: BTreeSet<RowId> },
}

const TREE_BATCH: usize = 256;
const ROW_BATCH: usize = 512;

/// One node.
pub struct Peer {
    store: Store,
    cfg: PeerConfig,
    me: NodeId,
    // Manifests seen, by epoch and hash, with the node that sent them.
    candidates: BTreeMap<u64, BTreeMap<[u8; 32], (Manifest, NodeId)>>,
    // Manifests that failed to merge.
    bad: BTreeSet<[u8; 32]>,
    // (epoch, parent) pairs this node published a manifest for.
    published: BTreeSet<(u64, [u8; 32])>,
    // The epoch to merge next, and the local time it became next.
    ready: (u64, u64),
    // Root hashes reported for (epoch, manifest), by node.
    roots: BTreeMap<(u64, [u8; 32]), BTreeMap<NodeId, NodeHash>>,
    // Last status of each peer.
    peers: BTreeMap<NodeId, Status>,
    // When a log, a chain, or a status was last asked for.
    asked: BTreeMap<[u8; 32], u64>,
    adopt: Option<Adopt>,
    // (next merge epoch, open epoch) at the last check for old own logs.
    abandon_checked: (u64, u64),
    next_status_ms: u64,
    rng: u64,
    out: Vec<(NodeId, Msg)>,
    events: Vec<Event>,
    metrics: Metrics,
}

impl Peer {
    /// Starts a node on its store. It asks the other members for their
    /// status, to catch up.
    pub fn new(store: Store, cfg: PeerConfig) -> Result<Peer> {
        let me = store.config().node;
        if !cfg.members.contains(&me) || !cfg.members.is_sorted() {
            return Err(Error::Invalid(format!("node {me} is not in the sorted member list")));
        }
        let now = store.now_ms();
        let next = store.next_merge_epoch()?;
        let mut peer = Peer {
            me,
            candidates: BTreeMap::new(),
            bad: BTreeSet::new(),
            published: BTreeSet::new(),
            // Give the status answers time to come before leading.
            ready: (next, now + cfg.status_interval_ms),
            roots: BTreeMap::new(),
            peers: BTreeMap::new(),
            asked: BTreeMap::new(),
            adopt: None,
            abandon_checked: (u64::MAX, u64::MAX),
            next_status_ms: now,
            rng: 0x9E37_79B9_7F4A_7C15 ^ (me.0 as u64 + 1).wrapping_mul(0xBF58_476D_1CE4_E5B9),
            out: Vec::new(),
            events: Vec::new(),
            metrics: Metrics { merged_epoch: store.merged_epoch()?, ..Metrics::default() },
            store,
            cfg,
        };
        for m in peer.others() {
            peer.send(m, Msg::GetStatus);
        }
        Ok(peer)
    }

    /// Returns the store, for reads.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Returns the store, for local writes.
    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    /// Returns the store and ends the node.
    pub fn into_store(self) -> Store {
        self.store
    }

    /// Returns the settings.
    pub fn config(&self) -> &PeerConfig {
        &self.cfg
    }

    /// Returns the metrics.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// True while the node takes over a peer's base state.
    pub fn is_adopting(&self) -> bool {
        self.adopt.is_some()
    }

    /// Returns the messages to send, and empties the outbox.
    pub fn take_outbox(&mut self) -> Vec<(NodeId, Msg)> {
        self.metrics.messages_out += self.out.len() as u64;
        std::mem::take(&mut self.out)
    }

    /// Returns what happened since the last call.
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    /// Returns this node's status.
    pub fn status(&self) -> Result<Status> {
        let next = self.store.next_merge_epoch()?;
        let own_logs = self
            .store
            .own_logs(next.saturating_sub(self.lateness()))?
            .into_iter()
            .filter(|(_, merged)| !merged)
            .map(|(r, _)| r)
            .collect();
        let chain = self
            .store
            .merged_records()?
            .into_iter()
            .map(|r| Link { epoch: r.epoch, hash: r.hash, parent: r.manifest.parent, rank: r.manifest.rank })
            .collect();
        Ok(Status { open_epoch: self.store.open_epoch()?, chain, own_logs })
    }

    /// Runs the timed work: seal, lead, merge, give up old logs, and
    /// anti-entropy. Call it often (a tenth of an epoch or less).
    pub fn tick(&mut self) -> Result<()> {
        let now = self.store.now_ms();
        self.seal_due(now)?;
        if let Some(a) = &self.adopt
            && now > a.last_progress_ms + 4 * self.cfg.retry_ms
        {
            // The peer stopped answering; a later status starts again.
            self.adopt = None;
        }
        self.progress(now)?;
        if now >= self.next_status_ms {
            self.next_status_ms = now + self.cfg.status_interval_ms;
            self.anti_entropy()?;
        }
        Ok(())
    }

    /// Handles a message from a member. Merges and other timed work wait
    /// for the next [`tick`](Peer::tick).
    pub fn on_message(&mut self, from: NodeId, msg: Msg) -> Result<()> {
        self.metrics.messages_in += 1;
        if from == self.me || !self.cfg.members.contains(&from) {
            self.metrics.rejected += 1;
            return Ok(());
        }
        let now = self.store.now_ms();
        match msg {
            Msg::Log(bytes) => self.on_log(from, &bytes)?,
            Msg::Manifest(m) => {
                if self.accept_manifest(m.clone(), from)? && m.epoch > self.store.next_merge_epoch()? {
                    // A manifest ahead of this node: it is behind.
                    self.ask(from, Msg::GetStatus, now);
                }
            }
            Msg::Root { epoch, manifest, root } => {
                self.roots.entry((epoch, manifest)).or_default().insert(from, root);
                self.verify(epoch, manifest)?;
            }
            Msg::GetStatus => {
                let s = self.status()?;
                self.send(from, Msg::Status(s));
            }
            Msg::Status(s) => self.on_status(from, s, now)?,
            Msg::GetLogs(hashes) => {
                let (mut logs, mut missing) = (Vec::new(), Vec::new());
                for h in hashes {
                    match self.store.log(&h)? {
                        Some(b) => logs.push(b),
                        None => missing.push(h),
                    }
                }
                self.send(from, Msg::Logs { logs, missing });
            }
            Msg::Logs { logs, missing } => {
                for b in logs {
                    self.on_log(from, &b)?;
                }
                if !missing.is_empty() && self.adopt.is_none() {
                    // The peer no longer keeps logs that a manifest needs:
                    // take over its state instead, if it is ahead.
                    let next = self.store.next_merge_epoch()?;
                    if self.peers.get(&from).and_then(Status::head_epoch).is_some_and(|h| h >= next) {
                        self.start_adopt(from, false, now);
                    }
                }
            }
            Msg::GetChain(from_epoch) => {
                let chain = self
                    .store
                    .merged_records()?
                    .into_iter()
                    .filter(|r| r.epoch >= from_epoch)
                    .map(|r| r.manifest)
                    .collect();
                self.send(from, Msg::Chain(chain));
            }
            Msg::Chain(ms) => {
                for m in ms {
                    self.accept_manifest(m, from)?;
                }
            }
            Msg::GetImage => {
                let img = self.store.base_image()?;
                self.send(from, Msg::Image(img));
            }
            Msg::GetTree(hashes) => {
                let root = self.store.base_root()?;
                let nodes =
                    hashes.into_iter().map(|h| Ok((h, self.store.base_tree_node(&h)?))).collect::<Result<Vec<_>>>()?;
                self.send(from, Msg::Tree { root, nodes });
            }
            Msg::GetRows(ids) => {
                let root = self.store.base_root()?;
                let rows = self.store.base_rows(&ids)?;
                self.send(from, Msg::Rows { root, rows });
            }
            Msg::Image(img) => self.on_image(from, img, now)?,
            Msg::Tree { root, nodes } => self.on_tree(from, root, nodes, now)?,
            Msg::Rows { root, rows } => self.on_rows(from, root, rows, now)?,
        }
        Ok(())
    }

    // ------------------------------------------------------------ helpers

    fn others(&self) -> Vec<NodeId> {
        self.cfg.members.iter().copied().filter(|m| *m != self.me).collect()
    }

    fn send(&mut self, to: NodeId, msg: Msg) {
        self.out.push((to, msg));
    }

    fn broadcast(&mut self, msg: &Msg) {
        for m in self.others() {
            self.send(m, msg.clone());
        }
    }

    // Sends a request unless the same one went out less than `retry_ms`
    // ago.
    fn ask(&mut self, to: NodeId, msg: Msg, now: u64) {
        let mut key = *blake3::hash(&msg.encode()).as_bytes();
        key[..4].copy_from_slice(&to.0.to_be_bytes());
        if self.asked.get(&key).is_some_and(|&t| now < t + self.cfg.retry_ms) {
            return;
        }
        self.asked.insert(key, now);
        self.send(to, msg);
    }

    fn random(&mut self, n: usize) -> usize {
        // xorshift64*
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        (self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as usize
    }

    fn random_others(&mut self, k: usize, except: Option<NodeId>) -> Vec<NodeId> {
        let mut pool: Vec<NodeId> = self.others().into_iter().filter(|m| Some(*m) != except).collect();
        let mut out = Vec::new();
        while out.len() < k && !pool.is_empty() {
            let i = self.random(pool.len());
            out.push(pool.swap_remove(i));
        }
        out
    }

    fn rank_of(&self, node: NodeId, epoch: u64) -> u32 {
        let n = self.cfg.members.len() as u64;
        let idx = self.cfg.members.iter().position(|m| *m == node).unwrap() as u64;
        ((idx + n - epoch % n) % n) as u32
    }

    fn lateness(&self) -> u64 {
        self.store.config().lateness
    }

    // ------------------------------------------------------------ logs

    fn seal_due(&mut self, now: u64) -> Result<()> {
        loop {
            let open = self.store.open_epoch()?;
            if now < self.store.epoch_start_ms(open + 1) {
                return Ok(());
            }
            let log = self.store.seal()?;
            self.metrics.epochs_sealed += 1;
            self.metrics.own_log_bytes += log.bytes.len() as u64;
            let msg = Msg::Log(log.bytes.clone());
            match self.cfg.gossip {
                None => self.broadcast(&msg),
                Some(k) => {
                    for m in self.random_others(k, None) {
                        self.send(m, msg.clone());
                    }
                }
            }
            self.events.push(Event::Sealed {
                epoch: log.epoch,
                hash: log.hash,
                entries: log.entries,
                bytes: log.bytes,
            });
        }
    }

    fn on_log(&mut self, from: NodeId, bytes: &[u8]) -> Result<()> {
        let hash = synod_core::log::log_hash(bytes);
        if self.store.has_log(&hash)? {
            return Ok(());
        }
        match self.store.add_log(bytes) {
            Ok(r) => {
                if !self.cfg.members.contains(&r.node) {
                    self.metrics.rejected += 1;
                } else if let Some(k) = self.cfg.gossip {
                    for m in self.random_others(k, Some(from)) {
                        if m != r.node {
                            self.send(m, Msg::Log(bytes.to_vec()));
                        }
                    }
                }
            }
            Err(Error::Invalid(_)) => self.metrics.rejected += 1,
            Err(e) => return Err(e),
        }
        Ok(())
    }

    // Gives up own logs that no merge can take any more.
    fn abandon_old(&mut self) -> Result<()> {
        let next = self.store.next_merge_epoch()?;
        let open = self.store.open_epoch()?;
        // Nothing new to give up unless a merge, a catch-up, or a seal
        // moved since the last check.
        if self.abandon_checked == (next, open) {
            return Ok(());
        }
        self.abandon_checked = (next, open);
        let lateness = self.lateness();
        let old: Vec<LogRef> = self
            .store
            .own_logs(0)?
            .into_iter()
            .filter(|(r, merged)| !merged && r.epoch + lateness < next)
            .map(|(r, _)| r)
            .collect();
        for r in old {
            let entries = self.store.abandon_log(r.epoch)?;
            self.metrics.abandoned_logs += 1;
            self.metrics.stale_entries += entries as u64;
            self.events.push(Event::Abandoned { epoch: r.epoch, hash: r.hash, entries });
        }
        Ok(())
    }

    // Logs that a manifest of epoch `e` can take: not merged, not given up,
    // within the lateness bound, at most one per (node, epoch).
    fn mergeable_logs(&self, e: u64) -> Result<Vec<LogRef>> {
        let lateness = self.lateness();
        let mut by_slot: BTreeMap<(NodeId, u64), LogRef> = BTreeMap::new();
        for (r, merged) in self.store.known_logs(e.saturating_sub(lateness))? {
            if merged || r.epoch > e || !self.cfg.members.contains(&r.node) {
                continue;
            }
            by_slot.entry((r.node, r.epoch)).and_modify(|x| x.hash = x.hash.min(r.hash)).or_insert(r);
        }
        Ok(by_slot.into_values().collect())
    }

    // ------------------------------------------------------------ manifests

    // Keeps a manifest from a peer if it is well formed. Returns true if
    // it was kept.
    fn accept_manifest(&mut self, m: Manifest, from: NodeId) -> Result<bool> {
        let n = self.cfg.members.len();
        let valid = (m.rank as usize) < n
            && m.leader == leader(&self.cfg.members, m.epoch, m.rank)
            && m.logs.iter().all(|l| self.cfg.members.contains(&l.node));
        if !valid {
            self.metrics.rejected += 1;
            return Ok(false);
        }
        let oldest = self.store.next_merge_epoch()?.saturating_sub(self.lateness() + 2);
        if m.epoch < oldest {
            return Ok(false);
        }
        self.candidates.entry(m.epoch).or_default().entry(m.hash()).or_insert((m, from));
        Ok(true)
    }

    // The manifest of highest priority for `epoch` on `parent`.
    fn best(&self, epoch: u64, parent: [u8; 32]) -> Option<(Manifest, NodeId)> {
        self.candidates
            .get(&epoch)?
            .iter()
            .filter(|(h, (m, _))| m.parent == parent && !self.bad.contains(*h))
            .min_by_key(|(h, (m, _))| (m.rank, **h))
            .map(|(_, v)| v.clone())
    }

    fn progress(&mut self, now: u64) -> Result<()> {
        if self.adopt.is_some() {
            return Ok(());
        }
        self.abandon_old()?;
        loop {
            self.switch_if_better()?;
            if self.adopt.is_some() {
                return Ok(());
            }
            let next = self.store.next_merge_epoch()?;
            if self.ready.0 != next {
                self.ready = (next, now);
            }
            self.lead(now, next)?;
            if !self.try_merge(now, next)? {
                break;
            }
        }
        self.abandon_old()?;
        self.prune()
    }

    // The split-leader rule: if a manifest of higher priority than a merged
    // one appears on the same parent, undo the merges back to it; the merge
    // loop then takes the better one.
    fn switch_if_better(&mut self) -> Result<()> {
        let records = self.store.merged_records()?;
        let kept: BTreeSet<[u8; 32]> = records.iter().map(|r| r.hash).collect();
        for (i, r) in records.iter().enumerate() {
            let Some((best, src)) = self.best(r.epoch, r.manifest.parent) else { continue };
            let h = best.hash();
            if (best.rank, h) >= (r.manifest.rank, r.hash) {
                continue;
            }
            // Undo back to the better manifest if every merge since can be
            // undone and its parent is kept; else take over the state of a
            // peer that merged it.
            let parent_kept = r.manifest.parent == [0; 32] || kept.contains(&r.manifest.parent);
            if !parent_kept || !records[i..].iter().all(|x| x.undoable) {
                let has = |s: &Status| s.chain.iter().any(|l| l.hash == h);
                let peer = if self.peers.get(&src).is_some_and(has) {
                    Some(src)
                } else {
                    self.peers.iter().find(|(_, s)| has(s)).map(|(n, _)| *n)
                };
                let now = self.store.now_ms();
                match peer {
                    Some(p) if self.adopt.is_none() => self.start_adopt(p, false, now),
                    Some(_) => {}
                    None => self.ask(src, Msg::GetStatus, now),
                }
                return Ok(());
            }
            while self.store.merged_epoch()?.is_some_and(|e| e >= r.epoch) {
                let e = self.store.unmerge()?;
                self.metrics.unmerges += 1;
                self.events.push(Event::Unmerged(e));
            }
            self.metrics.merged_epoch = self.store.merged_epoch()?;
            return Ok(());
        }
        Ok(())
    }

    // Publishes the manifest of epoch `next` if this node leads it now.
    fn lead(&mut self, now: u64, next: u64) -> Result<()> {
        let parent = self.store.head()?;
        if self.published.contains(&(next, parent)) || self.best(next, parent).is_some() {
            return Ok(());
        }
        // A peer that merged further: this node is behind and must not lead.
        if self.peers.values().any(|s| s.head_epoch().is_some_and(|h| h >= next)) {
            return Ok(());
        }
        let end = self.store.epoch_start_ms(next + 1);
        if now < end {
            return Ok(());
        }
        let rank = self.rank_of(self.me, next);
        let n = self.cfg.members.len();
        let start = (end + self.cfg.log_deadline_ms).max(self.ready.1);
        let due = now >= start + rank as u64 * self.cfg.leader_slot_ms;
        // Rank 0 publishes early when the logs of all members are here.
        if !due && (rank != 0 || self.store.log_nodes(next)? < n) {
            return Ok(());
        }
        let logs = self.mergeable_logs(next)?;
        let with_log = logs.iter().filter(|l| l.epoch == next).count();
        if self.cfg.quorum && with_log <= n / 2 {
            return Ok(());
        }
        let gc = next.checked_sub(self.lateness());
        let m = Manifest::new(next, rank, self.me, parent, gc, logs);
        self.published.insert((next, parent));
        self.metrics.manifests_published += 1;
        self.candidates.entry(next).or_default().insert(m.hash(), (m.clone(), self.me));
        self.broadcast(&Msg::Manifest(m.clone()));
        self.events.push(Event::Published(m));
        Ok(())
    }

    // Merges the best manifest of epoch `next`, if all its logs are here.
    // Returns true if it merged or dropped a manifest.
    fn try_merge(&mut self, now: u64, next: u64) -> Result<bool> {
        let parent = self.store.head()?;
        let Some((m, src)) = self.best(next, parent) else { return Ok(false) };
        let mut missing = Vec::new();
        let mut bytes = 0;
        for l in &m.logs {
            match self.store.log(&l.hash)? {
                Some(b) => bytes += b.len() as u64,
                None => missing.push(*l),
            }
        }
        if !missing.is_empty() {
            let hashes: Vec<[u8; 32]> = missing.iter().map(|l| l.hash).collect();
            if src != self.me {
                self.ask(src, Msg::GetLogs(hashes.clone()), now);
            }
            for l in &missing {
                if l.node != self.me && l.node != src {
                    self.ask(l.node, Msg::GetLogs(vec![l.hash]), now);
                }
            }
            if let Some(&other) = self.random_others(1, Some(src)).first() {
                self.ask(other, Msg::GetLogs(hashes), now);
            }
            return Ok(false);
        }
        let started = Instant::now();
        match self.store.merge_manifest(&m) {
            Ok(report) => {
                let ms = started.elapsed().as_millis() as u64;
                let end = self.store.epoch_start_ms(m.epoch + 1);
                let mt = &mut self.metrics;
                mt.merges += 1;
                mt.merge_time_ms = ms;
                mt.merge_time_ms_total += ms;
                mt.epoch_duration_ms = now.saturating_sub(end);
                mt.epoch_duration_ms_total += mt.epoch_duration_ms;
                mt.merged_log_bytes += bytes;
                mt.late_logs += m.logs.iter().filter(|l| l.epoch < m.epoch).count() as u64;
                mt.conflicts += report.conflicts.len() as u64;
                mt.merged_epoch = Some(m.epoch);
                let mh = report.manifest_hash;
                self.roots.entry((m.epoch, mh)).or_default().insert(self.me, report.root);
                self.broadcast(&Msg::Root { epoch: m.epoch, manifest: mh, root: report.root });
                self.events.push(Event::Merged {
                    manifest: m.clone(),
                    root: report.root,
                    conflicts: report.conflicts.len(),
                });
                self.verify(m.epoch, mh)?;
                Ok(true)
            }
            Err(Error::Invalid(_)) => {
                self.bad.insert(m.hash());
                self.metrics.rejected += 1;
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    // The verify step: compares the root hashes reported for one manifest.
    // A node in the minority repairs itself from a node of the majority.
    fn verify(&mut self, epoch: u64, manifest: [u8; 32]) -> Result<()> {
        let Some(reports) = self.roots.get(&(epoch, manifest)) else { return Ok(()) };
        let Some(&ours) = reports.get(&self.me) else { return Ok(()) };
        // Our root is from the merge, and the base may have moved on;
        // compare with the record of that merge.
        if self.store.merged_record(epoch)?.is_none_or(|r| r.hash != manifest) {
            return Ok(());
        }
        let mut count: BTreeMap<NodeHash, usize> = BTreeMap::new();
        for r in reports.values() {
            *count.entry(*r).or_default() += 1;
        }
        let mismatched: Vec<(NodeId, NodeHash)> =
            reports.iter().filter(|(n, r)| **r != ours && **n != self.me).map(|(n, r)| (*n, *r)).collect();
        if mismatched.is_empty() {
            return Ok(());
        }
        for &(from, theirs) in &mismatched {
            self.metrics.hash_mismatches += 1;
            self.events.push(Event::Mismatch { from, epoch, ours, theirs });
        }
        // Reported once per peer and root: forget them.
        let reports = self.roots.get_mut(&(epoch, manifest)).unwrap();
        for (n, _) in &mismatched {
            reports.remove(n);
        }
        let (best_root, best_count) = count.iter().max_by_key(|(r, c)| (**c, std::cmp::Reverse(**r))).unwrap();
        if *best_root != ours && *best_count > count[&ours] && self.adopt.is_none() {
            let from = mismatched.iter().find(|(_, r)| r == best_root).unwrap().0;
            let now = self.store.now_ms();
            self.start_adopt(from, true, now);
        }
        Ok(())
    }

    // ------------------------------------------------------------ status

    fn anti_entropy(&mut self) -> Result<()> {
        let status = self.status()?;
        for m in self.random_others(2, None) {
            self.send(m, Msg::Status(status.clone()));
        }
        // Send again own logs that no merge took, to the next leaders.
        let next = self.store.next_merge_epoch()?;
        let open = self.store.open_epoch()?;
        let lateness = self.lateness();
        let own: Vec<LogRef> = self
            .store
            .own_logs(next.saturating_sub(lateness))?
            .into_iter()
            .filter(|(r, merged)| !merged && r.epoch + 1 < open)
            .map(|(r, _)| r)
            .collect();
        if own.is_empty() {
            return Ok(());
        }
        // The leaders of the epoch that the cluster merges now.
        let current = open.saturating_sub(1).max(next);
        let n = self.cfg.members.len() as u32;
        let leaders: BTreeSet<NodeId> =
            (0..n.min(3)).map(|r| leader(&self.cfg.members, current, r)).filter(|l| *l != self.me).collect();
        for r in own {
            let bytes = self.store.log(&r.hash)?.expect("own log");
            for &l in &leaders {
                self.send(l, Msg::Log(bytes.clone()));
            }
        }
        Ok(())
    }

    // Compares a peer's chain with this node's, and catches up or switches
    // when the peer's chain wins.
    fn on_status(&mut self, from: NodeId, theirs: Status, now: u64) -> Result<()> {
        let mut want = Vec::new();
        for r in &theirs.own_logs {
            if r.node == from && !self.store.has_log(&r.hash)? && self.store.log_merged_in(&r.hash)?.is_none() {
                want.push(r.hash);
            }
        }
        if !want.is_empty() {
            self.ask(from, Msg::GetLogs(want), now);
        }
        self.peers.insert(from, theirs.clone());
        if self.adopt.is_some() {
            return Ok(());
        }
        let ours = self.status()?;
        let mine: BTreeMap<u64, Link> = ours.chain.iter().map(|l| (l.epoch, *l)).collect();
        let our_head = ours.chain.last().copied();
        let Some(their_head) = theirs.chain.last().copied() else { return Ok(()) };
        let fork = theirs.chain.iter().find(|t| mine.get(&t.epoch).is_some_and(|o| o.hash != t.hash));
        if let Some(t) = fork {
            let o = mine[&t.epoch];
            if t.parent == o.parent {
                // Sibling manifests: the priority rule decides.
                if (t.rank, t.hash) < (o.rank, o.hash) {
                    self.ask(from, Msg::GetChain(t.epoch), now);
                } else {
                    self.send(from, Msg::Status(ours));
                }
                return Ok(());
            }
            // The chains split before the epochs both keep: the longer
            // chain wins, then the lower head hash.
            let ours_wins = our_head.is_some_and(|h| {
                (h.epoch, std::cmp::Reverse(h.hash)) >= (their_head.epoch, std::cmp::Reverse(their_head.hash))
            });
            if ours_wins {
                self.send(from, Msg::Status(ours));
            } else {
                self.start_adopt(from, false, now);
            }
            return Ok(());
        }
        let next = self.store.next_merge_epoch()?;
        if their_head.epoch < next {
            if our_head.is_some_and(|h| h.epoch > their_head.epoch) {
                self.send(from, Msg::Status(ours));
            }
            return Ok(());
        }
        // The peer is ahead. Follow its chain if it goes on from this
        // node's head; else take over its state.
        let head = self.store.head()?;
        let follows = theirs.chain.iter().any(|t| t.epoch == next && t.parent == head);
        if follows {
            self.ask(from, Msg::GetChain(next), now);
        } else {
            self.start_adopt(from, false, now);
        }
        Ok(())
    }

    // ------------------------------------------------------------ adopt

    fn start_adopt(&mut self, peer: NodeId, repair: bool, now: u64) {
        self.adopt = Some(Adopt { peer, repair, last_progress_ms: now, stage: Stage::Image });
        self.send(peer, Msg::GetImage);
    }

    fn on_image(&mut self, from: NodeId, image: BaseImage, now: u64) -> Result<()> {
        match &self.adopt {
            Some(a) if a.peer == from && matches!(a.stage, Stage::Image) => {}
            _ => return Ok(()),
        }
        let mut walk = TreeWalk::new(image.root);
        let store = &self.store;
        let Ok(fetch) = walk.step(|h| store.base_tree_node(h).ok().flatten()) else {
            self.restart_adopt();
            return Ok(());
        };
        let a = self.adopt.as_mut().unwrap();
        a.last_progress_ms = now;
        a.stage = Stage::Walk { image, walk };
        self.after_walk_step(fetch, now)
    }

    fn on_tree(
        &mut self,
        from: NodeId,
        root: NodeHash,
        nodes: Vec<(NodeHash, Option<Vec<u8>>)>,
        now: u64,
    ) -> Result<()> {
        let store = &self.store;
        let Some(a) = &mut self.adopt else { return Ok(()) };
        let Stage::Walk { image, walk } = &mut a.stage else { return Ok(()) };
        if a.peer != from {
            return Ok(());
        }
        let mut ok = root == image.root;
        for (h, node) in nodes {
            ok = ok && node.is_some_and(|n| walk.receive(h, n).is_ok());
        }
        if !ok {
            self.restart_adopt();
            return Ok(());
        }
        a.last_progress_ms = now;
        if walk.open_fetches() > 0 {
            return Ok(());
        }
        let Ok(fetch) = walk.step(|h| store.base_tree_node(h).ok().flatten()) else {
            self.restart_adopt();
            return Ok(());
        };
        self.after_walk_step(fetch, now)
    }

    fn after_walk_step(&mut self, fetch: Vec<NodeHash>, now: u64) -> Result<()> {
        let Some(a) = &self.adopt else { return Ok(()) };
        let Stage::Walk { walk, .. } = &a.stage else { return Ok(()) };
        let peer = a.peer;
        if !fetch.is_empty() {
            for chunk in fetch.chunks(TREE_BATCH) {
                self.send(peer, Msg::GetTree(chunk.to_vec()));
            }
            return Ok(());
        }
        if walk.open_fetches() > 0 {
            return Ok(());
        }
        let Some(Adopt { repair, stage: Stage::Walk { image, walk }, .. }) = self.adopt.take() else { unreachable!() };
        let Ok(theirs) = walk.finish(image.root) else {
            self.start_adopt(peer, repair, now);
            return Ok(());
        };
        let ours = self.store.base_hashes(None, None)?;
        let (changed, removed) = diff_hashes(&ours, &theirs);
        if changed.is_empty() {
            return self.finish_adopt(peer, repair, image, Vec::new(), removed, now);
        }
        for chunk in changed.chunks(ROW_BATCH) {
            self.send(peer, Msg::GetRows(chunk.to_vec()));
        }
        let stage = Stage::Rows { image, removed, rows: Vec::new(), waiting: changed.into_iter().collect() };
        self.adopt = Some(Adopt { peer, repair, last_progress_ms: now, stage });
        Ok(())
    }

    fn on_rows(
        &mut self,
        from: NodeId,
        root: NodeHash,
        got: Vec<(RowId, synod_store::BaseRow)>,
        now: u64,
    ) -> Result<()> {
        let Some(a) = &mut self.adopt else { return Ok(()) };
        let Stage::Rows { image, rows, waiting, .. } = &mut a.stage else { return Ok(()) };
        if a.peer != from {
            return Ok(());
        }
        let mut ok = root == image.root;
        for (id, row) in got {
            if waiting.remove(&id) {
                match row {
                    Some(r) if ok => rows.push(r),
                    _ => ok = false,
                }
            }
        }
        if !ok {
            self.restart_adopt();
            return Ok(());
        }
        a.last_progress_ms = now;
        if !waiting.is_empty() {
            return Ok(());
        }
        let Some(Adopt { peer, repair, stage: Stage::Rows { image, removed, rows, .. }, .. }) = self.adopt.take()
        else {
            unreachable!()
        };
        self.finish_adopt(peer, repair, image, rows, removed, now)
    }

    fn restart_adopt(&mut self) {
        if let Some(a) = &mut self.adopt {
            a.stage = Stage::Image;
            let peer = a.peer;
            self.send(peer, Msg::GetImage);
        }
    }

    fn finish_adopt(
        &mut self,
        peer: NodeId,
        repair: bool,
        image: BaseImage,
        rows: Vec<(u32, Row)>,
        removed: Vec<RowId>,
        now: u64,
    ) -> Result<()> {
        match self.store.adopt_base(&image, &rows, &removed) {
            Ok(()) => {}
            // The base changed under the walk: a later status starts again.
            Err(Error::Corrupt(_) | Error::Invalid(_)) => return Ok(()),
            Err(e) => return Err(e),
        }
        if repair {
            self.metrics.repairs += 1;
        } else {
            self.metrics.catch_ups += 1;
        }
        self.metrics.merged_epoch = image.epoch;
        self.events.push(Event::Adopted { from: peer, epoch: image.epoch, root: image.root, repair });
        self.ready = (self.store.next_merge_epoch()?, now);
        self.abandon_old()
    }

    fn prune(&mut self) -> Result<()> {
        let next = self.store.next_merge_epoch()?;
        let keep = next.saturating_sub(self.lateness() + 2);
        self.candidates = self.candidates.split_off(&keep);
        self.roots = self.roots.split_off(&(keep, [0; 32]));
        self.published = self.published.split_off(&(keep, [0; 32]));
        let now = self.store.now_ms();
        let retry = self.cfg.retry_ms;
        self.asked.retain(|_, t| now < *t + retry);
        if self.bad.len() > 10_000 {
            self.bad.clear();
        }
        Ok(())
    }
}
