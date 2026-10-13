# SynodSQL

**A git-style replicated SQL database for a small group of trusted servers.**

Every node holds a full copy of the data and writes locally, with no network wait. At fixed intervals (epochs), all nodes merge their changes into one shared state. The merge is an N-way, git-style merge with one common base: the state at the end of the previous epoch. Because the merge is deterministic, every node computes the same result by itself, and a Merkle root hash proves that they agree. Conflicts that column rules cannot resolve become rows in a replicated `_conflicts` table. They never stop the merge.

```sql
CREATE TABLE inventory (
  row_id  ROWID,
  sku     TEXT UNIQUE                MERGE lww,
  name    TEXT                       MERGE lww,
  price   DECIMAL(10,2)              MERGE lww_flag,
  qty     INTEGER CHECK (qty >= 0)   MERGE delta,
  tags    SET<TEXT>                  MERGE union,
  notes   TEXT                       MERGE text3
);
```

Two sales on two nodes in one epoch (`qty` 800 → 650 and 800 → 700) merge to 550 with `delta`, and no sale is lost. Two different prices merge by last-writer-wins, and the losing price goes to `_conflicts` for review.

| Git | SynodSQL |
| --- | --- |
| Commit | Local transaction |
| Branch | A node's epoch log |
| Merge base | Previous merged state M(e−1) |
| Octopus merge | The N-way epoch merge |
| Merge driver | Column merge policy in the schema |
| Conflict markers | Rows in `_conflicts` |
| Tree hash | Prolly tree root hash |

Read the [white paper](docs/whitepaper.pdf) for the full model, and the [roadmap](docs/roadmap.md) for the plan.

## Status

**Phase 3 (sync and multi-node operation) is in progress.** Nodes run as a cluster over HTTP/2 with mutual TLS. Each epoch, every node seals its log and sends it to the others; a rotating leader publishes the manifest (the list of logs to merge); every node merges it and checks that all nodes have the same root hash. A node that was away catches up by comparing Prolly trees, and fetches only the rows that differ. In simulation, runs of 10,000 epochs with 5 and 50 nodes, crashes, partitions, and clock drift end with no root hash mismatch (gates 1 and 2 of the [roadmap](docs/roadmap.md)). Gate 3, a run on three real machines, is still open.

Three nodes on one machine:

```sh
synod certs certs --nodes 1,2,3                # a cluster CA, and certificates node-1 ... and admin
synod init n1.db --node 1 --epoch-ms 2000
synod create-table n1.db "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, qty INTEGER MERGE delta)"
synod clone n1.db n2.db --node 2 && synod clone n1.db n3.db --node 3
peers=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
for n in 1 2 3; do synod serve n$n.db --listen 127.0.0.1:700$n --peers $peers --certs certs & done

synod remote 127.0.0.1:7001 --to 1 --certs certs insert stock sku=A-100 qty=800
synod remote 127.0.0.1:7002 --to 2 --certs certs insert stock sku=B-200 qty=5
# A few epochs later, every node has both rows and the same base root:
synod remote 127.0.0.1:7003 --to 3 --certs certs query "SELECT sku, qty FROM stock"
synod remote 127.0.0.1:7003 --to 3 --certs certs status
```

Nodes can also sync by files, with no network: each node seals its epoch, exports its log, and merges the logs of the others.

```sh
synod init a.db --node 10 --epoch-ms 60000
synod create-table a.db "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, qty INTEGER MERGE delta)"
synod insert a.db stock sku=A-100 qty=800
# ... at the end of the epoch:
synod seal a.db && synod merge a.db
synod clone a.db b.db --node 11            # a second node from a's base state

# Each node writes, seals, and exports its log for epoch 1.
synod seal a.db && synod export a.db 1 a-1.log
synod seal b.db && synod export b.db 1 b-1.log

# Swap the files by any means, then merge. Both print the same root.
synod merge a.db b-1.log
synod merge b.db a-1.log
synod conflicts a.db
```

In Rust, the same node is a `synod_store::Store`:

```rust
let mut store = Store::open("node.db", config, Env::os())?;
store.create_table("CREATE TABLE stock (sku TEXT UNIQUE, qty INTEGER CHECK (qty >= 0) MERGE delta)")?;

let mut txn = store.begin()?;                      // one HLC time, one transaction ID
let id = txn.insert("stock", &[("sku", Value::Text("A-100".into())), ("qty", Value::Integer(500))])?;
txn.update("stock", id, &[("qty", Value::Integer(350))])?;
txn.commit()?;                                     // rows, log entries, counters: one SQLite commit

store.seal()?;                                     // at the epoch's end
let report = store.merge(epoch, &other_logs)?;     // M(e), conflicts, root hash
```

| Part | Where |
| --- | --- |
| Specification v0.1: formats, schema language, log, merge rules, state hash | [`docs/spec/v0.1.md`](docs/spec/v0.1.md) |
| HLC, row IDs, canonical encoding, epoch log format, Prolly tree | [`crates/synod-core`](crates/synod-core) |
| Store on SQLite: schema with `MERGE`, write API, seal, merge, conflicts, catch-up, revert | [`crates/synod-store`](crates/synod-store) |
| The node state machine: leaders, manifests, verify, catch-up, repair (no I/O) | [`crates/synod-node`](crates/synod-node) |
| The network transport: HTTP/2 with mutual TLS | [`crates/synod-net`](crates/synod-net) |
| The `synod` command | [`crates/synod-cli`](crates/synod-cli) |
| Deterministic simulator, and a SQLite VFS with crash injection | [`crates/synod-sim`](crates/synod-sim) |

## Build and test

You need Rust 1.88 or later.

```sh
cargo test --workspace
```

The simulation tests run many seeds. On a failure they print the seed and a command that replays the run exactly:

```sh
SYNOD_SEED=1234 cargo test -- --exact log_is_exact_and_crashes_lose_nothing
```

The crash tests run real SQLite on an in-memory disk that loses unsynced writes at a crash. They need no setup.

To run more seeds, property cases, or merge histories:

```sh
SYNOD_SIM_RUNS=1000 PROPTEST_CASES=10000 SYNOD_MERGE_RUNS=100000 cargo test --workspace --release
```

The cluster tests of gate 1 run short by default (300 epochs at 5 nodes, 10 at 50). `scripts/gate.sh` runs them long, for many seeds in parallel, one seed per CPU:

```sh
scripts/gate.sh five 10000 1 12      # 5 nodes, 10,000 epochs, seeds 1 to 12
scripts/gate.sh fifty 10000 2 9      # 50 nodes
```

To build the `synod` command: `cargo install --path crates/synod-cli`.

## Contributing

The project is new, and contributions are welcome. Read the roadmap first: each phase has deliverables and an exit gate. Two rules matter most:

1. **Determinism.** The merge and the encodings must give the same bytes on every machine. No hash map iteration order, no float sums, no locale-dependent comparison, no clock or random reads inside the merge.
2. **Simulation first.** A bug report or a fix comes with a simulator seed or a property test that shows it.

Before you send a change, run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.

Unless you state otherwise, any contribution that you submit for inclusion in this work, as defined in the Apache-2.0 license, is dual licensed as above, with no additional terms or conditions.
