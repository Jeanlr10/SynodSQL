# SynodSQL: phased implementation plan

Oct 12, 2026 · @Jean-Luc Robitaille

SynodSQL ships in six phases, and each phase ends with a measurable gate. Phases 0 to 3 build a working multi-node database on top of SQLite. Phases 4 and 5 replace the storage layer and tune the indexes, but Phase 4 starts only if benchmarks show that SQLite is the bottleneck.

## At a glance

| Phase | Goal | Status |
| --- | --- | --- |
| 0. Foundations | Formats, HLC, row IDs, simulator, CI | **Done** |
| 1. Single-node core on SQLite | Record every write in a form the merge can use | Next |
| 2. Deterministic merge | `M(e) = merge(M(e−1), logs)`; offline file sync | |
| 3. Sync and multi-node operation | Automatic epochs over a network | |
| 4. Custom storage engine | Only if Phase 3 benchmarks show SQLite is the bottleneck | Conditional |
| 5. Hot indexes and hardening | Read optimizations and long-lived deployments | |

Phase 3 is the main milestone: a complete multi-node database on SQLite. Phase 4 runs only if the Phase 3 benchmarks justify it.

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

- [ ] Schema extension: a MERGE policy for each column, parsed and stored in a \_synod\_schema table
- [ ] Managed tables use SQLite STRICT mode, so that a value has one type on every node (no '1' against 1)
- [ ] Shadow columns: row\_id, write stamps, and a tombstone flag
- [ ] Write API: assigns row IDs and HLC stamps, and writes one epoch log entry per change, with before-image, after-image, and transaction ID
- [ ] Epoch seal: close the log at the HLC boundary and hash it
- [ ] Prolly tree over (row\_id, row hash), updated in one batch at seal time
- [ ] Base-state switch: write the merged state to a new file, then swap it in with an atomic rename
- [ ] Read path: normal SQL reads on the local state, with tombstones hidden by views

**Exit gate:**

1. Every committed write appears exactly once in the epoch log, with correct images.
2. The same set of rows, inserted in two different orders, gives the same root hash.
3. A simulated crash at any point loses no committed write.

**Note:** measure the size cost of per-column stamps here. If it is too high, change to one stamp per row plus a short version number per column, before Phase 2 depends on the format.

## Phase 2: Deterministic merge

Build a pure merge function that turns M(e−1) and N logs into one M(e), the same on every machine. This phase gives the first usable version: an offline, file-based sync.

**Deliverables:**

- [ ] Per-row merge, column by column (white paper, Section 6.1)
- [ ] Column policies: lww, lww\_flag, delta, max, min, union, text3, flag
- [ ] Delete rules: delete wins, update wins, flag
- [ ] Constraint pass: unique keys (earliest insert wins), foreign keys (undo the delete), check constraints (record)
- [ ] \_conflicts table with a deterministic conflict\_id
- [ ] Late-log handling with each entry's own before-image and HLC time
- [ ] Offline command: `synod merge base.db logs/*.log`
- [ ] Two-node file sync: export a log, copy it by any means, import it, merge

**Exit gate:**

1. A property test runs 100,000 random histories with 2 to 10 nodes. Every node order and every arrival order gives the same M(e), the same \_conflicts rows, and the same root hash.
2. The worked example in the white paper gives exactly its published result.

**Main risk:** hidden nondeterminism. Hash map iteration order, float sums, and locale-dependent text comparison can all change the result between machines. Use sorted iteration, decimal or integer types, and byte-wise comparison inside the merge.

## Phase 3: Sync and multi-node operation

Nodes run epochs automatically over a network. At the end of this phase, SynodSQL is a complete system on SQLite, with a fixed set of member nodes.

**Deliverables:**

- [ ] Transport: HTTP/2 or QUIC with mutual TLS
- [ ] Epoch scheduler: seal at the HLC boundary, with a deadline for log arrival
- [ ] Log exchange: all-to-all first, gossip as an option
- [ ] Manifest: rotating leader, fallback order, and the priority rule for split leaders
- [ ] Verify and switch: exchange root hashes; a node with a mismatch repairs itself by a Prolly tree diff and raises an alert
- [ ] Catch-up: a returning node fetches changed subtrees, and its old logs go into the next manifest as late logs
- [ ] Lateness bound L: a node more than L epochs behind clones a peer, and its stale entries go to \_conflicts
- [ ] Garbage collection of tombstones, logs, manifests, and resolved conflicts after L epochs
- [ ] Revert tool: write a revert log that undoes a chosen node's changes
- [ ] Metrics: epoch duration, log sizes, conflict counts, hash mismatches

**Exit gate:**

1. The simulator runs 5 to 50 nodes for 10,000 epochs with crashes, partitions, clock drift, and late nodes. Nodes on the same manifest have zero root hash mismatches, and no committed write is lost.
2. Measured conflict counts follow the model in the white paper, Section 9.
3. A real deployment on at least 3 machines runs the full epoch cycle with no manual work.

**Benchmarks for the Phase 4 decision:** record lookup latency (p50, p99) by row\_id and by secondary index, merge time against log size and database size, and bytes on disk per row. Use uniform, Zipf, and tiered (1% / 9% / 90%) workloads.

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
| Write capture | Own write API, SQLite triggers, SQLite session extension | Own write API; test the session extension as a way to collect before- and after-images | Phase 1 |
| Stamp size | Per-column stamps, or one per-row stamp plus per-column versions | Measure in Phase 1, then choose | Phase 1 |
| Default epoch length | A fixed value, or one derived from the conflict model | Configurable; derive the default from the Section 9 model and a target conflict rate | Phase 3 |
| Manifest agreement | Rotating leader, Raft | Rotating leader first; Raft if split leaders happen in practice | Phase 3 |
| Transport | HTTP/2 with mutual TLS, QUIC | HTTP/2 first for simple debugging | Phase 3 |
| SQL front end on the custom engine | SQLite virtual tables, own parser, another embedded engine | SQLite virtual tables | Phase 4 |
