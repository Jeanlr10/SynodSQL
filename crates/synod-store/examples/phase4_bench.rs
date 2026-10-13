//! The benchmarks for the Phase 4 decision (roadmap, end of Phase 3):
//! lookup latency by row ID and by secondary index, merge time against log
//! size and database size, and bytes on disk per row, on uniform, Zipf, and
//! tiered (1% / 9% / 90%) workloads.
//!
//! For each database size n, three nodes start from one base state of n
//! rows. In each epoch the three nodes together make Δ writes (90%
//! updates, 5% inserts, 5% deletes) on rows that the workload picks, seal,
//! exchange their logs, and merge one manifest each, with garbage
//! collection on, as a leader sets it. A plain SQLite file with the same
//! columns and the same updates is the baseline for lookups and space.
//!
//! Run with
//! `cargo run --release -p synod-store --example phase4_bench -- [--rows 10000,100000,1000000] [--deltas 100,1000,10000] [--epochs 3] [--lookups 200000] [--dir target/bench]`.
//! The files go to `--dir`, which should be on a real disk.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use synod_core::manifest::{LogRef, Manifest};
use synod_core::{Decimal, NodeId, RowId, Value};
use synod_store::{Env, ManualClock, Store, StoreConfig};

const TABLE: &str = "CREATE TABLE inventory (
    row_id ROWID, sku TEXT UNIQUE, name TEXT, price DECIMAL(10,2) MERGE lww, qty INTEGER MERGE delta)";
const GENESIS: u64 = 1_791_763_200_000;
const EPOCH_MS: u64 = 60_000;
const LATENESS: u64 = 16;
const LOAD_BATCH: u64 = 100_000;

#[derive(Clone, Copy, Debug)]
enum Dist {
    Uniform,
    Zipf,
    Tiered,
}

impl Dist {
    fn name(self) -> &'static str {
        match self {
            Dist::Uniform => "uniform",
            Dist::Zipf => "zipf",
            Dist::Tiered => "tiered",
        }
    }

    // Cumulative weights by rank: Zipf with s = 1; tiered: the hot 1% of
    // rows take 90% of accesses, the warm 9% take 9%, the cold 90% take 1%.
    fn cdf(self, n: usize) -> Vec<f64> {
        let (hot, warm) = ((n / 100).max(1), (n * 9 / 100).max(1));
        let w = |k: usize| match self {
            Dist::Uniform => 1.0,
            Dist::Zipf => 1.0 / (k + 1) as f64,
            Dist::Tiered if k < hot => 0.90 / hot as f64,
            Dist::Tiered if k < hot + warm => 0.09 / warm as f64,
            Dist::Tiered => 0.01 / (n - hot - warm) as f64,
        };
        let mut sum = 0.0;
        let mut out: Vec<f64> = (0..n)
            .map(|k| {
                sum += w(k);
                sum
            })
            .collect();
        out.iter_mut().for_each(|x| *x /= sum);
        out
    }
}

// SplitMix64: a small, seeded generator, so that every run is the same.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// Picks rows by a workload: rank k of the distribution is row perm[k], so
// that the hot rows are spread over the table, not the first ones.
struct Picker {
    cdf: Vec<f64>,
    perm: Vec<usize>,
}

impl Picker {
    fn new(dist: Dist, n: usize, rng: &mut Rng) -> Picker {
        let mut perm: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            perm.swap(i, rng.below(i as u64 + 1) as usize);
        }
        Picker { cdf: dist.cdf(n), perm }
    }
    fn pick(&self, rng: &mut Rng) -> usize {
        let u = rng.unit();
        self.perm[self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1)]
    }
}

fn sku(i: usize) -> String {
    format!("SKU-{i:08}")
}

fn values(i: usize) -> [(&'static str, Value); 4] {
    [
        ("sku", Value::Text(sku(i))),
        ("name", Value::Text(format!("Part number {i}"))),
        ("price", Value::Decimal(Decimal::new((i % 100_000) as i128, 2).unwrap())),
        ("qty", Value::Integer((i * 37 % 5_000) as i64)),
    ]
}

struct Args {
    rows: Vec<usize>,
    deltas: Vec<usize>,
    epochs: usize,
    lookups: usize,
    dir: String,
}

fn args() -> Args {
    let mut a = Args {
        rows: vec![10_000, 100_000, 1_000_000],
        deltas: vec![100, 1_000, 10_000],
        epochs: 3,
        lookups: 200_000,
        dir: "target/bench".into(),
    };
    let list = |s: &str| s.split(',').map(|x| x.replace('_', "").parse().expect("a number")).collect();
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let v = it.next().unwrap_or_else(|| panic!("{k} needs a value"));
        match k.as_str() {
            "--rows" => a.rows = list(&v),
            "--deltas" => a.deltas = list(&v),
            "--epochs" => a.epochs = v.parse().expect("--epochs"),
            "--lookups" => a.lookups = v.parse().expect("--lookups"),
            "--dir" => a.dir = v,
            _ => panic!("unknown option {k}"),
        }
    }
    a
}

struct Cluster {
    clock: Arc<ManualClock>,
    nodes: Vec<Store>,
    // Row IDs of the n base rows, by index. They stay live: deletes take
    // rows that the benchmark inserted in earlier epochs.
    ids: Vec<RowId>,
    inserted: Vec<RowId>,
    plain: Connection,
    next_insert: usize,
}

// A point of the merge benchmark.
struct MergePoint {
    merge: Vec<Duration>,
    seal: Vec<Duration>,
    entries: usize,
    rows: usize,
    conflicts: usize,
    wrote: u64,
}

// Bytes this process has sent to the storage layer (Linux), or 0.
fn written() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| s.lines().find_map(|l| l.strip_prefix("write_bytes: ").map(|v| v.trim().parse().unwrap_or(0))))
        .unwrap_or(0)
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

impl Cluster {
    fn new(dir: &str, n: usize) -> Cluster {
        let dir = format!("{dir}/n{n}");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let clock = Arc::new(ManualClock::new(GENESIS + 1));
        let env = Env { clock: clock.clone(), ..Env::os() };
        let config = StoreConfig {
            node: NodeId(1),
            genesis_ms: GENESIS,
            epoch_len_ms: EPOCH_MS,
            max_drift_ms: 500,
            lateness: LATENESS,
        };
        let mut a = Store::open(&format!("{dir}/a.db"), config, env).unwrap();
        a.create_table(TABLE).unwrap();
        let plain = Connection::open(format!("{dir}/plain.db")).unwrap();
        plain
            .execute_batch(
                "PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;
                 CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT UNIQUE, name TEXT, price TEXT, qty INTEGER) STRICT;",
            )
            .unwrap();

        // Load in epochs of LOAD_BATCH rows: each epoch seals and merges.
        let started = Instant::now();
        let mut ids = Vec::with_capacity(n);
        let mut i = 0;
        while i < n {
            let end = (i + LOAD_BATCH as usize).min(n);
            let mut t = a.begin().unwrap();
            for k in i..end {
                ids.push(t.insert("inventory", &values(k)).unwrap());
            }
            t.commit().unwrap();
            plain.execute_batch("BEGIN").unwrap();
            for k in i..end {
                let v = values(k).map(|(_, v)| synod_store::values::to_sql(&v));
                plain
                    .prepare_cached("INSERT INTO inventory VALUES (?1, ?2, ?3, ?4, ?5)")
                    .unwrap()
                    .execute(rusqlite::params![k as i64 + 1, v[0], v[1], v[2], v[3]])
                    .unwrap();
            }
            plain.execute_batch("COMMIT").unwrap();
            clock.advance(EPOCH_MS);
            let e = a.seal().unwrap().epoch;
            a.merge(e, &[]).unwrap();
            i = end;
        }
        eprintln!("n = {n}: loaded in {:.1} s", started.elapsed().as_secs_f64());
        let b = a.clone_base(&format!("{dir}/b.db"), NodeId(2)).unwrap();
        let c = a.clone_base(&format!("{dir}/c.db"), NodeId(3)).unwrap();
        Cluster { clock, nodes: vec![a, b, c], inserted: Vec::new(), ids, plain, next_insert: n }
    }

    // One epoch: `delta` writes split over the nodes, seal, and merge.
    fn epoch(&mut self, picker: &Picker, rng: &mut Rng, delta: usize) -> (Duration, Vec<Duration>, MergeOutcome) {
        let k = self.nodes.len();
        self.plain.execute_batch("BEGIN").unwrap();
        let mut touched = BTreeMap::new();
        let mut fresh = Vec::new();
        for (j, node) in self.nodes.iter_mut().enumerate() {
            let writes = delta / k + usize::from(j < delta % k);
            let mut done = 0;
            while done < writes {
                let mut t = node.begin().unwrap();
                for _ in 0..100.min(writes - done) {
                    let roll = rng.below(100);
                    if roll < 5 {
                        let i = self.next_insert;
                        self.next_insert += 1;
                        let mut v = values(i);
                        v[0].1 = Value::Text(format!("NEW-{j}-{i}"));
                        let id = t.insert("inventory", &v).unwrap();
                        touched.insert(id, ());
                        fresh.push(id);
                    } else if roll < 10 && !self.inserted.is_empty() {
                        let id = self.inserted.swap_remove(rng.below(self.inserted.len() as u64) as usize);
                        t.delete("inventory", id).unwrap();
                        touched.insert(id, ());
                    } else {
                        let i = picker.pick(rng);
                        let id = self.ids[i];
                        touched.insert(id, ());
                        {
                            let qty = match t.get("inventory", id).unwrap().expect("live")[3] {
                                Value::Integer(q) => q,
                                _ => 0,
                            };
                            let mut change = vec![("qty", Value::Integer(qty + 1))];
                            if roll < 30 {
                                change.push((
                                    "price",
                                    Value::Decimal(Decimal::new(rng.below(100_000) as i128, 2).unwrap()),
                                ));
                            }
                            t.update("inventory", id, &change).unwrap();
                            self.plain
                                .prepare_cached("UPDATE inventory SET qty = qty + 1 WHERE id = ?1")
                                .unwrap()
                                .execute([i as i64 + 1])
                                .unwrap();
                        }
                    }
                }
                done += 100.min(writes - done);
                t.commit().unwrap();
            }
        }
        self.plain.execute_batch("COMMIT").unwrap();
        self.clock.advance(EPOCH_MS);
        let mut seal = Duration::ZERO;
        let mut logs = Vec::new();
        for node in &mut self.nodes {
            let started = Instant::now();
            let log = node.seal().unwrap();
            seal = seal.max(started.elapsed());
            logs.push(log);
        }
        let epoch = logs[0].epoch;
        let entries = logs.iter().map(|l| l.entries as usize).sum();
        let mut merges = Vec::new();
        let mut conflicts = 0;
        let mut wrote = 0;
        for (j, node) in self.nodes.iter_mut().enumerate() {
            let mut refs: Vec<LogRef> = Vec::new();
            for (m, log) in logs.iter().enumerate() {
                if m == j {
                    refs.push(LogRef { node: node.config().node, epoch, hash: log.hash });
                } else {
                    refs.push(node.add_log(&log.bytes).unwrap());
                }
            }
            let manifest = Manifest::new(epoch, 0, NodeId(1), node.head().unwrap(), epoch.checked_sub(LATENESS), refs);
            let (started, bytes) = (Instant::now(), written());
            let report = node.merge_manifest(&manifest).unwrap();
            merges.push(started.elapsed());
            wrote = wrote.max(written() - bytes);
            conflicts = report.conflicts.len();
        }
        self.inserted.extend(fresh);
        let roots: Vec<_> = self.nodes.iter().map(|s| s.base_root().unwrap()).collect();
        assert!(roots.windows(2).all(|w| w[0] == w[1]), "root hash mismatch");
        (seal, merges, MergeOutcome { entries, rows: touched.len(), conflicts, wrote })
    }
}

struct MergeOutcome {
    // Bytes written by the merge of the node that wrote most.
    wrote: u64,
    entries: usize,
    rows: usize,
    conflicts: usize,
}

fn percentiles(mut v: Vec<Duration>) -> (f64, f64, f64) {
    v.sort();
    let at = |p: f64| v[((v.len() as f64 * p) as usize).min(v.len() - 1)].as_secs_f64() * 1e6;
    let mean = v.iter().sum::<Duration>().as_secs_f64() * 1e6 / v.len() as f64;
    (at(0.50), at(0.99), mean)
}

fn time_lookups(count: usize, mut f: impl FnMut() -> usize) -> (f64, f64, f64) {
    let mut times = Vec::with_capacity(count);
    let mut found = 0;
    for _ in 0..count {
        let started = Instant::now();
        found += f();
        times.push(started.elapsed());
    }
    assert!(found == count, "a lookup found {found} of {count} rows");
    percentiles(times)
}

fn plain_query(c: &Connection, sql: &str, p: SqlValue) -> usize {
    let mut stmt = c.prepare_cached(sql).unwrap();
    let n = stmt.column_count();
    let rows: Vec<Vec<SqlValue>> =
        stmt.query_map([p], |r| (0..n).map(|i| r.get(i)).collect()).unwrap().collect::<rusqlite::Result<_>>().unwrap();
    rows.len()
}

fn space(path: &str) -> BTreeMap<String, u64> {
    let c = Connection::open(path).unwrap();
    let mut stmt = c.prepare("SELECT name, sum(pgsize) FROM dbstat GROUP BY name").unwrap();
    let mut out: BTreeMap<String, u64> =
        stmt.query_map([], |r| Ok((r.get(0)?, r.get::<_, i64>(1)? as u64))).unwrap().map(|r| r.unwrap()).collect();
    let free: i64 = c.query_row("PRAGMA freelist_count", [], |r| r.get(0)).unwrap();
    let page: i64 = c.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap();
    out.insert("(free pages)".into(), (free * page) as u64);
    out
}

fn group(name: &str) -> &'static str {
    match name {
        "_synod_t_inventory" | "inventory" => "rows",
        "_synod_u_inventory_0" | "sqlite_autoindex_inventory_1" => "unique index (sku)",
        "_synod_rowhash" | "_synod_level" | "_synod_level_hash" | "_synod_dirty" => "state hash (row hashes, tree)",
        n if n.starts_with("_synod_log") || n == "_synod_sealed" => "logs (kept 4 L epochs)",
        "_synod_undo" | "_synod_merge_undo" | "_synod_merged" | "_synod_merged_log" => "undo and merge records",
        n if n.contains("conflicts") => "_conflicts",
        "(free pages)" => "free pages",
        _ => "other",
    }
}

fn main() {
    let a = args();
    let dists = [Dist::Uniform, Dist::Zipf, Dist::Tiered];
    println!("# Phase 4 decision benchmarks\n");
    println!(
        "Three nodes; each epoch the nodes together write Δ entries (90% updates, 5% inserts, 5% deletes), seal, and merge one manifest with GC (L = {LATENESS}). Times are medians over {} epochs; merge is the slowest of the three nodes, and bytes written are those of the node that wrote most (WAL and checkpoints).\n",
        a.epochs
    );
    let mut merge_rows = Vec::new();
    let mut lookup_rows = Vec::new();
    let mut space_rows = Vec::new();
    for &n in &a.rows {
        let mut cluster = Cluster::new(&a.dir, n);
        let mut rng = Rng(n as u64);
        // Empty epochs until GC has dropped what the load left (its logs
        // are kept 4 L epochs), so that no measured merge pays for it.
        let warm = Picker::new(Dist::Uniform, n, &mut rng);
        for _ in 0..=4 * LATENESS {
            cluster.epoch(&warm, &mut rng, 0);
        }
        for dist in dists {
            let picker = Picker::new(dist, n, &mut rng);
            for &delta in &a.deltas {
                let mut p =
                    MergePoint { merge: Vec::new(), seal: Vec::new(), entries: 0, rows: 0, conflicts: 0, wrote: 0 };
                for _ in 0..a.epochs {
                    let (seal, merges, out) = cluster.epoch(&picker, &mut rng, delta);
                    p.seal.push(seal);
                    p.merge.push(merges.into_iter().max().unwrap());
                    p.entries += out.entries;
                    p.rows += out.rows;
                    p.conflicts += out.conflicts;
                    p.wrote += out.wrote;
                }
                let e = a.epochs;
                let line = format!(
                    "| {n} | {} | {delta} | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} |",
                    dist.name(),
                    p.rows / e,
                    p.conflicts / e,
                    ms(median(p.seal)),
                    ms(median(p.merge)),
                    p.wrote as f64 / e as f64 / 1e6,
                    p.wrote as f64 / p.rows.max(1) as f64 / 1e3,
                );
                eprintln!("{line}");
                merge_rows.push(line);
            }
        }

        // Lookups on node A after the merges, and on plain SQLite.
        let store = &cluster.nodes[0];
        for dist in dists {
            let picker = Picker::new(dist, n, &mut rng);
            let live: Vec<usize> = (0..a.lookups).map(|_| picker.pick(&mut rng)).collect();
            let mut it = live.iter().cycle();
            let ids = &cluster.ids;
            let s_id = time_lookups(live.len(), || {
                let i = *it.next().unwrap();
                store
                    .query(
                        "SELECT sku, name, price, qty FROM inventory WHERE row_id = ?1",
                        &[Value::Blob(ids[i].to_be_bytes().to_vec())],
                    )
                    .unwrap()
                    .len()
            });
            let s_ix = time_lookups(live.len(), || {
                let i = *it.next().unwrap();
                store
                    .query("SELECT row_id, name, price, qty FROM inventory WHERE sku = ?1", &[Value::Text(sku(i))])
                    .unwrap()
                    .len()
            });
            let s_api = time_lookups(live.len(), || {
                let i = *it.next().unwrap();
                usize::from(store.get("inventory", ids[i]).unwrap().is_some())
            });
            let plain = &cluster.plain;
            let p_id = time_lookups(live.len(), || {
                let i = *it.next().unwrap();
                plain_query(
                    plain,
                    "SELECT sku, name, price, qty FROM inventory WHERE id = ?1",
                    SqlValue::Integer(i as i64 + 1),
                )
            });
            let p_ix = time_lookups(live.len(), || {
                let i = *it.next().unwrap();
                plain_query(plain, "SELECT id, name, price, qty FROM inventory WHERE sku = ?1", SqlValue::Text(sku(i)))
            });
            for (what, (p50, p99, mean)) in [
                ("SynodSQL, row_id (SQL)", s_id),
                ("SynodSQL, row_id (Store::get)", s_api),
                ("plain SQLite, rowid", p_id),
                ("SynodSQL, sku index", s_ix),
                ("plain SQLite, sku index", p_ix),
            ] {
                let line = format!("| {n} | {} | {what} | {p50:.1} | {p99:.1} | {mean:.1} |", dist.name());
                eprintln!("{line}");
                lookup_rows.push(line);
            }
        }

        // Space, on node A (which also keeps the load logs) and node B.
        let paths = [cluster.nodes[0].path().to_owned(), cluster.nodes[1].path().to_owned()];
        let live_rows = n + cluster.inserted.len();
        drop(cluster);
        let plain_path = format!("{}/n{n}/plain.db", a.dir);
        let mut groups: BTreeMap<&str, [u64; 3]> = BTreeMap::new();
        for (k, path) in [paths[0].as_str(), paths[1].as_str(), plain_path.as_str()].into_iter().enumerate() {
            for (name, bytes) in space(path) {
                groups.entry(group(&name)).or_default()[k] += bytes;
            }
        }
        let per = |b: u64| b as f64 / n as f64;
        let mut total = [0u64; 3];
        for (g, b) in &groups {
            (0..3).for_each(|k| total[k] += b[k]);
            space_rows.push(format!("| {n} | {g} | {:.1} | {:.1} | {:.1} |", per(b[0]), per(b[1]), per(b[2])));
        }
        space_rows.push(format!(
            "| {n} | **total** | **{:.1}** | **{:.1}** | **{:.1}** |",
            per(total[0]),
            per(total[1]),
            per(total[2])
        ));
        eprintln!("n = {n}: {live_rows} live rows at the end; space measured");
    }

    println!("## Merge time\n");
    println!(
        "| n | workload | Δ entries | rows changed | conflicts | seal ms | merge ms | MB written by a merge | KB written per changed row |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    merge_rows.iter().for_each(|l| println!("{l}"));
    println!("\n## Lookup latency (µs)\n");
    println!("| n | workload | path | p50 | p99 | mean |");
    println!("| --- | --- | --- | --- | --- | --- |");
    lookup_rows.iter().for_each(|l| println!("{l}"));
    println!("\n## Bytes on disk per base row\n");
    println!("| n | part | SynodSQL node A | SynodSQL node B | plain SQLite |");
    println!("| --- | --- | --- | --- | --- |");
    space_rows.iter().for_each(|l| println!("{l}"));
}
