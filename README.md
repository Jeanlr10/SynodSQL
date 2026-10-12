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

**Phase 1 (single-node core on SQLite) is done.** One node records every write in a form that the merge can use. There is no merge and no network yet. Phase 2 (the deterministic merge) is next.

```rust
let mut store = Store::open("node.db", config, Env::os())?;
store.create_table("CREATE TABLE stock (sku TEXT UNIQUE, qty INTEGER CHECK (qty >= 0) MERGE delta)")?;

let mut txn = store.begin()?;                       // one HLC time, one transaction ID
let id = txn.insert("stock", &[("sku", Value::Text("A-100".into())), ("qty", Value::Integer(500))])?;
txn.update("stock", id, &[("qty", Value::Integer(350))])?;
txn.commit()?;                                      // rows, log entries, counters: one SQLite commit

store.query("SELECT sku, qty FROM stock", &[])?;    // a view; tombstones are hidden
let sealed = store.seal()?;                         // at the epoch's end: log + hash + state hash
```

| Part | Where |
| --- | --- |
| Specification v0.1: formats, schema language, log, state hash, merge rules | [`docs/spec/v0.1.md`](docs/spec/v0.1.md) |
| HLC, row IDs, canonical encoding, epoch log format, Prolly tree | [`crates/synod-core`](crates/synod-core) |
| Store on SQLite: schema with `MERGE`, write API, seal, base switch | [`crates/synod-store`](crates/synod-store) |
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

To run more seeds or more property cases:

```sh
SYNOD_SIM_RUNS=1000 PROPTEST_CASES=10000 cargo test --workspace --release
```

## Contributing

The project is new, and contributions are welcome. Read the roadmap first: each phase has deliverables and an exit gate. Two rules matter most:

1. **Determinism.** The merge and the encodings must give the same bytes on every machine. No hash map iteration order, no float sums, no locale-dependent comparison, no clock or random reads inside the merge.
2. **Simulation first.** A bug report or a fix comes with a simulator seed or a property test that shows it.

Before you send a change, run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.

Unless you state otherwise, any contribution that you submit for inclusion in this work, as defined in the Apache-2.0 license, is dual licensed as above, with no additional terms or conditions.
