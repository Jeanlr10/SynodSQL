# SynodSQL: phased implementation plan

Oct 12, 2026 · @Jean-Luc Robitaille

SynodSQL ships in six phases, and each phase ends with a measurable gate. Phases 0 to 3 build a working multi-node database on top of SQLite. Phases 4 and 5 replace the storage layer and tune the indexes, but Phase 4 starts only if benchmarks show that SQLite is the bottleneck.

## At a glance

| Phase | Goal | Status |
| --- | --- | --- |
| 0. Foundations | Formats, HLC, row IDs, simulator, CI | **Done** |
| 1. Single-node core on SQLite | Record every write in a form the merge can use | **Done** |
| 2. Deterministic merge | `M(e) = merge(M(e−1), logs)`; offline file sync | **Done** |
| 3. Sync and multi-node operation | Automatic epochs over a network | In progress: gates 1 and 2 met |
| 4. Custom storage engine | Only if Phase 3 benchmarks show SQLite is the bottleneck | Conditional |
| 5. Hot indexes and hardening | Read optimizations and long-lived deployments | |

Phase 3 is the main milestone: a complete multi-node database on SQLite. Phase 4 runs only if the Phase 3 benchmarks justify it; they do not, so the plan is to skip it (see the Phase 3 results). Phase 3 still needs gate 3 on real machines.

## Architecture decisions

These decisions come from the white paper and the follow-up design discussion. Later phases depend on them, so change them only with a reason.

| Area | Decision |
| --- | --- |
| Name | SynodSQL |
| Replication | Full copy on every node; local writes; one N-way merge per epoch |
| Merge base | The previous merged state M(e−1), the same on all nodes |
| Row ID | 128 bits: origin node (32) + counter (64) + reserved (32); never reused |
| Clock | Hybrid logical clock (HLC), 64 bits |
| Conflicts | Per-column merge policies in the schema; unresolved cases go to a replicated \_conflicts table |
| State hash | Prolly tree over (row\_id, row hash); root hash proves agreement |
| Manifest | Rotating leader per epoch; Raft only if needed |
| Local engine, Phases 0 to 3 | SQLite |
| Local engine, Phases 4 and 5 | Heap pages, a radix row map for O(1) lookup by row\_id, B+tree secondary indexes that store (page, slot) |
| Hot index layer | Open-addressing hash table for exact lookups; Eytzinger array where hot queries need order; weighted tree only if measurements show a gain |

## Phase 0: Foundations

Fix the data formats and build the test harness before any database code. Every later phase depends on these parts, and format errors found later are expensive.

**Deliverables:**

- [x] Specification v0.1: row format, log entry format, manifest format, and merge rules, taken from the white paper — [`spec/v0.1.md`](spec/v0.1.md)
- [x] HLC library: send, receive, compare, and a check that rejects peers with too much clock drift — `synod-core/src/hlc.rs`
- [x] Row ID allocator: (node ID, counter), with the counter made durable before use — `synod-core/src/row_id.rs`
- [x] Canonical row encoding for hashes: fixed column order, fixed integer and decimal formats, floats as raw IEEE bits, text as UTF-8 bytes — `synod-core/src/encoding.rs`
- [x] Deterministic simulator: virtual clock, virtual network (delay, loss, partition), and virtual disk with crash injection; one seed replays a run exactly — `crates/synod-sim`
- [x] CI pipeline with property-based tests — `.github/workflows/ci.yml`

**Exit gate:** HLC and row ID property tests pass under simulated clock drift, and the simulator replays any failing seed byte for byte.

**Gate result:** met. `synod-sim/tests/cluster.rs` runs 3 to 8 nodes with clock offset and drift, message loss, partitions, and crashes at any disk write. It checks HLC order and row ID uniqueness, replays a seed byte for byte, and finds a planted bug (a counter store with no sync). The pinned hash vectors run on x86-64 and ARM64 in CI.

**Main risk:** the canonical encoding. If two nodes encode one row differently, their root hashes differ even when the data agrees. Fix the encoding in this phase, and test it on more than one CPU architecture.

## Phase 1: Single-node core on SQLite

One node records every write in a form that the merge can use. No merge and no network yet.

**Deliverables:**

- [x] Schema extension: a MERGE policy for each column, parsed and stored in a \_synod\_schema table — `synod-store/src/schema.rs`, [spec 4.6](spec/v0.1.md)
- [x] Managed tables use SQLite STRICT mode, so that a value has one type on every node (no '1' against 1)
- [x] Shadow columns: row\_id, write stamps, and a tombstone flag — [spec 4.5](spec/v0.1.md)
- [x] Write API: assigns row IDs and HLC stamps, and writes one epoch log entry per change, with before-image, after-image, and transaction ID — `Txn` in `synod-store/src/store.rs`
- [x] Epoch seal: close the log at the HLC boundary and hash it — `Store::seal`, [spec 5.2](spec/v0.1.md)
- [x] Prolly tree over (row\_id, row hash), updated in one batch at seal time — `synod-core/src/prolly.rs`, [spec 8](spec/v0.1.md)
- [x] Base-state switch: write the merged state to a new file, then swap it in with an atomic rename — `Store::rewrite` and `Store::install`, [spec 5.3](spec/v0.1.md). Since Phase 3, a merge switches inside one SQLite transaction instead (`Store::rebuild`): the new pages go to the WAL beside the old ones, and the commit is the atomic switch. The file copy stays for `rewrite`.
- [x] Read path: normal SQL reads on the local state, with tombstones hidden by views — `Store::query`

**Exit gate:**

1. Every committed write appears exactly once in the epoch log, with correct images.
2. The same set of rows, inserted in two different orders, gives the same root hash.
3. A simulated crash at any point loses no committed write.

**Note:** measure the size cost of per-column stamps here. If it is too high, change to one stamp per row plus a short version number per column, before Phase 2 depends on the format.

**Gate result:** met. `synod-store/tests/gate.rs`:

1. A workload of inserts, updates, deletes, seals, base switches, and restarts runs against an independent model. The log must equal the model's expected entries exactly, and a replay of the log must give every stored row, stamps included. Every sealed log must hold exactly the entries of its epoch.
2. The same rows, written in a random order with older versions first, give the same root hash on two nodes. The hash also equals an independent tree build and survives a base switch. A second node that applies the first node's log gets the same root hash.
3. A crash VFS for SQLite (`synod-sim/src/sqlite.rs`) loses unsynced writes, tears sectors, and can stop at any disk operation. The test crashes a short run at **every** disk operation, and longer runs at random ones. After each crash, nothing that committed is lost, and the in-flight transaction is either fully present or fully absent.

Planted bugs check the tests: `synchronous = NORMAL` loses committed writes, and counters written outside the transaction reuse a row ID. The gate tests catch both. A differential test (`tests/differential.rs`) runs the same random writes on plain SQLite: each write is accepted or rejected the same way, and the rows stay equal.

**Stamp cost, measured** (`cargo run --release -p synod-store --example stamp_cost`, 50,000 rows of the white paper's inventory table):

| Layout | Bytes per row |
| --- | --- |
| Plain SQLite table | 73.5 |
| One 12-byte stamp per column, stored as is | 179.1 |
| **Built: per-column stamps, each distinct stamp stored once plus a 1-byte index per column** | **123.5** |
| Alternative: one stamp per row plus a 2-byte version per column | 117.4 |

**Decision:** keep per-column stamps. The compact storage removes most of the cost, and the alternative would save only 6 more bytes per row but lose the per-column HLC that a late `lww` write needs (white paper, Section XI-A). The storage form is local ([spec 4.5](spec/v0.1.md)); the row hash and the log do not depend on it.

**Not done in Phase 1:** flipped-bit tests (test strategy table). SQLite has no page checksums by default, so a flipped bit in a page is not detected. The root hash check of Phase 3 detects a bad row between nodes; SQLite's `cksumvfs` extension or the Phase 4 engine can add page checksums.

## Phase 2: Deterministic merge

Build a pure merge function that turns M(e−1) and N logs into one M(e), the same on every machine. This phase gives the first usable version: an offline, file-based sync.

**Deliverables:**

- [x] Per-row merge, column by column (white paper, Section 6.1) — `synod-store/src/merge/`, [spec 7.1](spec/v0.1.md)
- [x] Column policies: lww, lww\_flag, delta, max, min, union, text3, flag — `merge/policy.rs`, `merge/text3.rs`
- [x] Delete rules: delete wins, update wins, flag
- [x] Constraint pass: unique keys (earliest insert wins), foreign keys (undo the delete), check constraints (record) — `merge/constraints.rs`
- [x] \_conflicts table with a deterministic conflict\_id — table 0 of every store, [spec 7.5](spec/v0.1.md)
- [x] Late-log handling with each entry's own before-image and HLC time
- [x] Offline command: `synod merge base.db logs/*.log` — `crates/synod-cli`
- [x] Two-node file sync: export a log, copy it by any means, import it, merge — `synod export`, `synod merge`, and `synod clone` for a new node

**Exit gate:**

1. A property test runs 100,000 random histories with 2 to 10 nodes. Every node order and every arrival order gives the same M(e), the same \_conflicts rows, and the same root hash.
2. The worked example in the white paper gives exactly its published result.

**Main risk:** hidden nondeterminism. Hash map iteration order, float sums, and locale-dependent text comparison can all change the result between machines. Use sorted iteration, decimal or integer types, and byte-wise comparison inside the merge.

**Gate result:** met.

1. `synod-store/tests/determinism.rs` runs 100,000 random histories in CI (1,000 by default): 2 to 10 nodes with random IDs, 1 to 3 epochs, late logs, two tables with every policy, a random delete rule and CASCADE. Each epoch merges with the logs in five orders, on separate copies of the base; all outputs (rows, `_conflicts` rows, root hash) are equal. The test also checks unique keys and foreign keys in every M(e), and that every kind of conflict and rule occurs. A run takes about 4 minutes in release mode.
2. `synod-store/tests/merge.rs` runs the worked example on three stores at the white paper's HLC times. M(e) is Table VIII, `_conflicts` is Table IX (three rows), and the three nodes have one root hash.

More tests: stores that write during the sync converge, and their M(e) equals the pure merge of the same logs (`tests/sync.rs`); a crash at each of the 94 disk operations of a merge leaves the old state (the merge then runs again to the same root) or the merged one; the command-line file sync works (`synod-cli/tests/cli.rs`).

**Found by the gate:** at 100,000 histories, seed 8344 left a live child without a parent and without a conflict. A parent restored from M(e−1) gave up a key value that a new child used, and the next round did not look at that value. The constraint pass now keeps every row version that it replaces. A parent whose key changed now keeps the change, and the orphan child is recorded instead of undoing the parent's other columns.

**Known limits, for the Phase 3 benchmarks:** the constraint pass scans the merged rows for each key lookup (cost Δ² in the number of changed rows). The state hash rebuilt the tree in one pass over all rows. Phase 3 removed both limits (see below).

## Phase 3: Sync and multi-node operation

Nodes run epochs automatically over a network. At the end of this phase, SynodSQL is a complete system on SQLite, with a fixed set of member nodes.

**Deliverables:**

- [x] Transport: HTTP/2 with mutual TLS; a node takes a message only if the sender's certificate names that node — `crates/synod-net`, `synod serve`, `synod remote`, `synod certs`
- [x] Epoch scheduler: seal at the HLC boundary, with a deadline for log arrival — `Peer` in `synod-node/src/peer.rs`, a state machine with no I/O that the simulator and the transport both drive
- [x] Log exchange: all-to-all first, gossip as an option (`PeerConfig::gossip`)
- [x] Manifest: rotating leader, fallback order, and the priority rule for split leaders
- [x] Verify and switch: exchange root hashes; a node with a mismatch repairs itself by a Prolly tree diff and raises an alert
- [x] Catch-up: a returning node fetches changed subtrees, and its old logs go into the next manifest as late logs — `Store::adopt_base`, `TreeWalk`
- [x] Lateness bound L: a node more than L epochs behind clones a peer, and its stale entries go to \_conflicts — `Store::abandon_log`
- [x] Garbage collection of tombstones, logs, manifests, and resolved conflicts after L epochs — `gc_through` in the manifest, `Store::gc_local`
- [x] Revert tool: write a revert log that undoes a chosen node's changes — `Txn::revert`
- [x] Metrics: epoch duration, log sizes, conflict counts, hash mismatches — `synod-node/src/metrics.rs`
- [x] Merges by manifest and unmerge, for a manifest of higher priority — `Store::merge_manifest`, `Store::unmerge`
- [x] Libraries: `synodsql::Database` in Rust, C and C++ (`libsynod`, `synod.h`, `synod.hpp`), and Python (`synodsql`), with a node that serves in the background while the program uses it (`synod_net::start`); each tag builds wheels and C libraries for Linux, macOS and Windows — `crates/synodsql*`, `.github/workflows/release.yml`
- [x] Client API for any language: JSON over HTTPS (HTTP/1.1 or HTTP/2, mutual TLS) on the node's port — query, write batches as one transaction, tables, status, conflicts; typed values; queries limited to reads (SQLite authorizer), a time limit and a result limit, as requests run on the node's thread; tested with `curl` and the Python standard library only — spec Section 10, `synod-net/src/api.rs`, [`docs/api.md`](api.md)

**Addition to the white paper's rule:** by default, a leader publishes a manifest only when it has the logs of a majority of the nodes (`PeerConfig::quorum`). Without it, a minority side of a partition makes its own chain of merges and loses it later, when the majority's chain wins. The rule can be turned off.

**Exit gate:**

1. The simulator runs 5 to 50 nodes for 10,000 epochs with crashes, partitions, clock drift, and late nodes. Nodes on the same manifest have zero root hash mismatches, and no committed write is lost.
2. Measured conflict counts follow the model in the white paper, Section 9.
3. A real deployment on at least 3 machines runs the full epoch cycle with no manual work.

**Gate results:**

1. Met. `synod-node/tests/cluster.rs` runs the epoch cycle in the simulator with crashes (also inside disk writes), partitions, message loss, clock offset and drift, and late nodes that stay down longer than L. An oracle checks that every merge of one manifest has one root hash, that all nodes converge, and that no committed write is lost. Twenty independent seeds ran for 10,000 epochs each (`scripts/gate.sh`):

   | Nodes | Seeds | Merges per run | Catch-ups per run | Root hash mismatches |
   | --- | --- | --- | --- | --- |
   | 5 | 1 to 12 | 48,881 to 49,740 | 282 (seed 1) | 0 |
   | 50 | 2 to 9 | 500,109 to 506,257 | 1,990 to 2,370 | 0 |

   One 5-node run has about 1,800 crashes and 450 partitions. More than 500 random clusters of 5 to 12 nodes (`random_clusters`) also pass; their seeds found several real bugs, all fixed.
2. Met. `conflict_counts_follow_the_model` counts conflicts in 200 epochs of 5 nodes on 1,000 rows. The exact expected value is Σᵢ 1 − (1−qᵢ)ᴺ − N qᵢ (1−qᵢ)ᴺ⁻¹, with qᵢ = 1 − (1−pᵢ)ʷ:

   | Access | w | Measured per epoch | Exact model | White paper, equation 1 |
   | --- | --- | --- | --- | --- |
   | Uniform | 20 | 3.77 | 3.77 | 4.00 |
   | Zipf (s = 1) | 4 | 1.92 | 1.93 | 4.69 |
   | Tiered (1% / 9% / 90%) | 4 | 5.12 | 5.02 | 12.97 |

   Equation 1 assumes that w pᵢ is small. On skewed access it is not, for the hot rows, and the equation overestimates by 2 to 3 times; the exact formula fits.
3. Open. `synod-cli/tests/deploy.rs` runs three `synod serve` processes on one machine, over HTTP/2 with mutual TLS, through a restart and a catch-up, with no manual work. The gate asks for at least 3 real machines.

**Found in the gate runs:** the state hash rebuilt the whole tree several times per epoch, so an epoch got slower as the state grew: 6.8 ms per epoch over the first 1,000 epochs of the 5-node run, and 23 ms over 10,000. The tree is now kept level by level and updated in place (`synod-store/src/tree.rs`, store format 4); a 4,000-epoch run takes 24 s instead of 50 s. Also fixed: garbage collection kept the sealed copy of every given-up log.

**Benchmarks for the Phase 4 decision:** record lookup latency (p50, p99) by row\_id and by secondary index, merge time against log size and database size, and bytes on disk per row. Use uniform, Zipf, and tiered (1% / 9% / 90%) workloads.

**Benchmark results** (`cargo run --release -p synod-store --example phase4_bench`): three nodes, epochs of 60 s, L = 16, on an NVMe disk. Each epoch the nodes together write Δ entries (90% updates, 5% inserts, 5% deletes), seal, and merge one manifest with GC; 65 empty epochs after the load come first, so that the load's GC is not measured. Merge time is the slowest node, median of 3 epochs (uniform / Zipf / tiered), store format 6; the 1M row is after the merge speedups below, the 10k and 100k rows before them:

| Database rows | Δ = 100 | Δ = 1,000 | Δ = 10,000 |
| --- | --- | --- | --- |
| 10k | 27 / 23 / 22 ms | 39 / 50 / 49 ms | 148 / 93 / 132 ms |
| 100k | 18 / 24 / 37 ms | 102 / 86 / 94 ms | 353 / 248 / 191 ms |
| 1M | 19 / 23 / 29 ms | 74 / 61 / 80 ms | 469 / 324 / 398 ms |

Times under 60 ms vary by 2 times between runs: a merge has a fixed cost of about 10 ms, the fsync of its commit (synchronous = FULL). At 1M rows and Δ = 10,000, a merge writes 100 to 160 MB, about 16 KB per changed row: each changed row dirties a page in each of the rows, the row hashes and the unique index, and the WAL writes each page again at checkpoint. Of the pages a merge changes, the rows are 43%, the row hashes 38%, and the unique index 14%. A 256 MB page cache changes merge time by under 10%.

Lookups at 1M rows, p50 / p99 in µs (the three workloads are within 0.2 µs):

| Path | SynodSQL | Plain SQLite |
| --- | --- | --- |
| By row\_id, SQL through the view | 2.8 / 4.3 | 2.1 / 2.6 (rowid) |
| By row\_id, `Store::get` | 3.1 / 3.7 | |
| By unique index (`sku`) | 4.1 / 4.8 | 3.4 / 4.1 |

Bytes on disk per row at 1M rows: rows 94 (plain SQLite 50), unique index 39 (24), state hash 64, undo and merge records 8. Logs are kept 4L epochs: 21 bytes per row once the load's logs are collected. Without logs and free pages, a node holds 200 to 205 bytes per row, against 74 for plain SQLite.

**Decision: skip Phase 4.** None of the three entry conditions shows a limit from SQLite's layout. Lookups are 30 to 40% slower than plain SQLite, at a few µs, because of the view and the wider row. Merge cost is writes to SynodSQL's own metadata trees, which a heap engine would also need. The extra space is row IDs, stamps, and hash caches. The next work is to reduce write amplification and space inside the SQLite build, then Phase 5. Done so far: the base tree copy is gone (store format 6, below), and merges at 1M rows take half the time (merge speedups, below). Phase 4 stays here in case a later workload shows SQLite as the bottleneck.

**Found by the benchmarks:** the constraint pass was Δ² (a merge of Δ = 10,000 on 10k rows took 1,322 ms; now 163 ms, with a key index over the merge overlay); the rebase after a merge re-read every sealed own log (a Δ = 100 merge on 1M rows took 145 ms; now 66 ms); sealed logs were stored twice (store format 5); and a clone kept the source's free pages (`clone_base` now vacuums the copy).

**Store format 6.** The store kept the state hash tree twice: the local state's, and a copy of the base tree for catch-up, written again item by item at each merge (with an index on node hash). In a merge of 10 changes at 1M rows, half the changed pages were the copy and its index; in a merge of 10,000, a third. The stored tree is now the base tree only; a merge writes it, and local writes stay out of it. The root of the local state, at a seal, is the base tree updated for the rows in `_synod_undo`, in a savepoint that is rolled back. A refresh no longer writes a row hash that did not change, and a tree update finds the boundary before a key from the parent level. Same runs at 1M rows, format 5 against 6: a merge of Δ = 10,000 changes 14,200 pages against 10,000 and takes 981 ms against 802 ms; a seal computes the local root in the savepoint, and the merge does not reuse it: 3 ms at Δ = 100, 100 to 250 ms at Δ = 10,000. Space drops by 64 bytes per row.

**Merge speedups.** At 1M rows and Δ = 10,000 (uniform / Zipf / tiered), each step on top of the last:

| Change | Merge |
| --- | --- |
| Store format 6 | 1,006 / 696 / 844 ms |
| WAL checkpoint in idle time, not in the merge | 628 / 412 / 508 ms |
| Rows whose deleted flag and indexed columns are unchanged updated in place | 515 / 350 / 436 ms |
| BLAKE3 derive-key hashers made once | 501 / 324 / 406 ms |
| Row hashes of the rows just written taken from memory | 469 / 324 / 398 ms |

The WAL checkpoints every 65,536 pages, and a node checkpoints (PASSIVE) at the end of a tick with nothing to send. An update in place writes only the stamps and the unindexed columns, so no index entry changes. A merge now spends about 50 ms restoring the base, 400 ms changing rows, 110 to 150 ms updating the tree, and 60 ms on row hashes. Most of what is left is SQLite writing rows, row hashes and index entries at random places; the next step would be fewer pages per changed row, for example the row hash in the row (38% of the pages a merge changes).

**Large merges.** One merge of Δ = 1,000,000 entries on 1M rows: uniform changes 663k rows, merge 16.9 s, seal 2.8 s, 832 MB written; Zipf 302k rows, 9.7 s and 2.3 s, 602 MB; tiered 173k rows, 7.8 s and 1.2 s, 491 MB. That is 8 to 17 µs per entry, less than at Δ = 10,000, since changed rows share pages.

**10M rows** (store format 6, before the merge speedups). Merge at Δ = 100: 68 to 190 ms; at Δ = 1,000: 320 to 730 ms; at Δ = 10,000: 2.1 / 2.9 / 4.3 s (Zipf / tiered / uniform), 40 to 50 KB written per changed row. A 512 MB page cache changes merge time by under 15%. With the process capped at 3 GB of memory (a database of 4 GB, larger than memory), merges at Δ = 10,000 take 1.4 to 1.7 times as long (3.5 to 5.9 s). Lookups by `Store::get` stay at 2 to 4 µs at the p50; with less of the database cached, SQL lookups go to disk for SynodSQL and plain SQLite alike (p50 of 80 to 250 µs under the cap, p99 up to 2 ms). Space per row is as at 1M rows. None of this shows a limit from SQLite's layout that a heap engine would remove: the decision to skip Phase 4 stands.

**White paper experiments** (Table X; `cargo test --release -p synod-node --test experiments -- --ignored --nocapture`). These run in the simulator with no faults. The index rebuild experiment belongs to Phase 5.

- **Conflict model.** The exact formula fits measured conflicts in all 75 cases: N from 2 to 20, w of 2, 5 and 20, and uniform, Zipf (s = 0.8, 1, 1.2) and tiered access, 3 seeds each. At N = 20 and w = 20, measured against exact: uniform 58.5 against 58.9, Zipf (s = 1) 50.5 against 50.3, tiered 15.0 against 15.3. Equation 1 gives 76, 2,230 and 6,163: it fits uniform access and overestimates skewed access by up to 400 times.
- **In-flight writes.** The model assumes that each write sees the previous merge. A write made while the previous epoch syncs does not, and can overwrite a value it never saw. With every write in flight, conflicts rise 2.5 to 2.8 times on uniform access, 1.7 to 2 times on Zipf, and 1.05 to 1.6 times on tiered. In a deployment, only the writes in the sync window are in flight. The conflict model tests now write after the merge (`Model::after_merge`).
- **Sync cost.** All-to-all sends a node's log to each other node, about (N−1)Δ, plus 1 to 6 KB per epoch of status, root and manifest messages; a log costs about 120 bytes per entry. With 100 entries per node, a node sends 30 KB per epoch at N = 3, 116 KB at N = 10, and 625 KB at N = 50. Gossip as built sends more: every node forwards every log that is new to it to k peers, about N·Δ·k, which is k·N/(N−1) times all-to-all (253 KB for k = 2 and 340 KB for k = 3 at N = 10). The white paper's "Δ × fanout" counts only a node's own log. Gossip spreads the load off a log's origin; it does not save bytes.
- **Value of column policies.** The same workload with the schema's policies (`price` lww\_flag, `qty` delta), then with `lww` on every column. With the policies, no `qty` update is lost. With `lww` only, 3.8% (uniform, N = 5, w = 20) to 81% (tiered, N = 10, w = 20) of them are lost. The policies cost one conflict for review per price contest, which `lww` decides silently: 364 to 2,470 conflicts in 100 epochs.
- **Merge cost against n.** Merge time follows Δ, as the paper expects, plus a fixed cost of about 10 ms, the fsync of the commit, at any n: a merge of 10 changes takes 11.5 ms at 10k rows and 12.5 to 13 ms at 1M rows. Earlier runs at 1M rows gave 25 to 100 ms: the first merges after the load collected the load's logs. The cost per changed row grows with n, by the depth of the B-trees and fewer changed rows per page.

**Found by the experiments:** under `lww_flag`, a late or in-flight write that won against a value it never saw overwrote that value with no conflict. The merge recorded only the losing writers, but the white paper's rule is that losing values go to `_conflicts`. In a run with late logs, 177 conflicts should have been recorded and 130 were. The merge now records the overwritten value too ([spec 7.2](spec/v0.1.md)).

## Phase 4: Custom storage engine (conditional)

Replace SQLite's table storage with a heap engine, but only if the Phase 3 benchmarks show that SQLite's layout is the bottleneck. If they do not, skip to Phase 5 and stay on SQLite.

**Entry condition:** a Phase 3 benchmark shows a clear limit from SQLite in index lookups, merge-time bulk updates, or space per row.

**Deliverables:**

- [ ] Page manager: fixed-size pages, a checksum on each page, a free-space map
- [ ] Heap pages: slotted pages with free space kept in reserve for updates, and forward pointers for rows that move
- [ ] Radix row map: row\_map\[node\]\[counter\] → (page, slot), O(1) in the worst case
- [ ] B+tree secondary indexes that store (page, slot), with bulk load
- [ ] Write-ahead log, crash recovery, and the copy-on-write base-state switch
- [ ] Vacuum and tombstone cleanup for heap pages
- [ ] SQL front end: an SQLite virtual table module over the new storage, so SQLite's parser and query planner stay in use
- [ ] Migration tool from the Phase 3 file format

**Exit gate:**

1. Crash-injection tests pass in the simulator at every write, including torn pages.
2. The new engine beats the Phase 3 build on the benchmark that started this phase, with no regression on the others.
3. File format v1 is frozen and documented.

**Main risk:** scope. A storage engine is a large project, and a SQL layer is a larger one. The virtual table approach keeps the SQL layer out of scope.

## Phase 5: Hot indexes and hardening

Add the read optimizations and the features a long-lived deployment needs. This phase runs on SQLite or on the Phase 4 engine.

**Hot index layer:**

- [ ] Per-thread read counters, with sampling to keep the cost low
- [ ] Hot layer: open-addressing hash table over the top k keys, rebuilt at a merge only when the access pattern changes enough (KL threshold)
- [ ] Eytzinger-ordered hot layer for tables whose hot queries need order
- [ ] Optional experiment: weighted tree from the epoch counts; keep it only if it beats the hash and Eytzinger layers
- [ ] Invalidate or update hot entries when a merge changes or deletes them

**Hardening:**

- [ ] Schema epochs: a schema change is a manifest entry that all nodes acknowledge before it applies
- [ ] Dynamic membership: add and remove nodes; a new node clones a peer
- [ ] Escrow helper for bounded counters such as stock levels
- [ ] Opt-in transaction-level conflict resolution
- [ ] Conflict review tool: list, compare, and resolve \_conflicts rows
- [ ] Operations: backup, restore, clone, certificate rotation, operator documentation

**Exit gate:**

1. The hot layer improves p50 and p99 read latency on the Zipf and tiered benchmarks, or it is removed.
2. In the simulator, a schema change and a membership change each complete on all nodes with no lost data.
3. An operator can run a deployment from the documentation alone.

## Test strategy

Deterministic simulation is the main test tool from Phase 0 onward, in the style that FoundationDB made well known. Every bug found gets a seed that replays it.

| Test type | What it checks | Starts in |
| --- | --- | --- |
| Deterministic simulation | Whole-cluster behavior under crashes, partitions, drift, and late nodes | Phase 0 |
| Property-based tests | HLC order, row ID uniqueness, canonical encoding, merge determinism | Phase 0 |
| Differential tests | SynodSQL on one node returns the same query results as plain SQLite | Phase 1 |
| Fault injection | Crash at every write, torn pages, flipped bits (checksums must catch them) | Phase 1 |
| Cross-platform hash test | The same data gives the same root hash on x86-64 and ARM64 | Phase 1 |
| Benchmarks | Latency, merge time, bytes on disk, bytes on the network; uniform, Zipf, and tiered workloads, with drift | Phase 3 |
| Soak test | A real cluster runs for a long period with random restarts | Phase 3 |

## Risks and mitigations

The largest risks are silent divergence between nodes and scope growth in Phase 4.

| Risk | Effect | Mitigation | Phase |
| --- | --- | --- | --- |
| Encoding differs between nodes | Root hashes differ although the data agrees | Canonical encoding; cross-platform hash test | 0 |
| Dynamic typing in SQLite | One value merges differently on two nodes | STRICT tables for all managed data | 1 |
| Nondeterminism in the merge | Nodes compute different states | Sorted iteration, no floats in merged sums, byte-wise comparison | 2 |
| Clock skew past the HLC limit | lww favors the node with a fast clock | Reject peers above the drift limit; alert | 3 |
| Too many conflicts to review | People ignore \_conflicts | Shorter epochs, mergeable policies, review tool | 3, 5 |
| Bugs in the custom engine | Data loss | Start Phase 4 only on measured need; crash tests at every write | 4 |
| Scope growth in the SQL layer | The project does not finish | Keep SQLite as the front end through virtual tables | 4 |

## Open decisions

These choices are still open. Each one has a recommendation and the phase by which it must be settled.

| Decision | Options | Recommendation | Settle by |
| --- | --- | --- | --- |
| Core language | Rust, Go, C, Python prototype | **Settled: Rust.** Memory safety, good SQLite bindings, and a fit for the Phase 4 engine | Phase 0 |
| Write capture | Own write API, SQLite triggers, SQLite session extension | **Settled: own write API.** It reads the before-image in the write's transaction, so it needs no extension. The session extension was not evaluated | Phase 1 |
| Stamp size | Per-column stamps, or one per-row stamp plus per-column versions | **Settled: per-column stamps, stored compactly.** See the Phase 1 measurement | Phase 1 |
| Default epoch length | A fixed value, or one derived from the conflict model | Configurable; derive the default from the Section 9 model and a target conflict rate | Phase 3 |
| Manifest agreement | Rotating leader, Raft | **Settled for Phase 3: rotating leader**, with the majority rule above; Raft only if split leaders cause trouble in practice | Phase 3 |
| Transport | HTTP/2 with mutual TLS, QUIC | **Settled: HTTP/2 with mutual TLS** | Phase 3 |
| SQL front end on the custom engine | SQLite virtual tables, own parser, another embedded engine | SQLite virtual tables | Phase 4 |
