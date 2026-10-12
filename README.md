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

**Phase 0 (Foundations) is done.** Nothing here is a database yet. Phase 1 (a single-node core on SQLite) is next.

| Part | Where |
| --- | --- |
| Specification v0.1: row, log, and manifest formats; merge rules | [`docs/spec/v0.1.md`](docs/spec/v0.1.md) |
| Hybrid logical clock with a drift check | [`crates/synod-core/src/hlc.rs`](crates/synod-core/src/hlc.rs) |
| 128-bit row IDs, durable before use | [`crates/synod-core/src/row_id.rs`](crates/synod-core/src/row_id.rs) |
| Canonical value and row encoding, row hash | [`crates/synod-core/src/encoding.rs`](crates/synod-core/src/encoding.rs) |
| Deterministic simulator: virtual clocks, network, and disk with crash injection | [`crates/synod-sim`](crates/synod-sim) |

## Build and test

You need Rust 1.88 or later.

```sh
cargo test --workspace
```

The simulation tests run many seeds. On a failure they print the seed and a command that replays the run exactly:

```sh
SYNOD_SEED=1234 cargo test -p synod-sim -- --exact hlc_and_row_ids_under_faults
```

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
