//! The white paper's planned experiments (Table X) that run in the
//! simulator: sync cost, the conflict model over N and w, and the value of
//! column policies. Each prints a Markdown table.
//!
//! `cargo test --release -p synod-node --test experiments -- --ignored --nocapture --test-threads 1`

mod common;

use std::collections::BTreeMap;

use common::*;

const LWW_ONLY: &str = "CREATE TABLE stock (
    sku   TEXT UNIQUE               MERGE lww,
    name  TEXT                      MERGE lww,
    price DECIMAL(10,2)             MERGE lww,
    qty   INTEGER CHECK (qty >= 0)  MERGE lww,
    tags  SET<TEXT>                 MERGE lww
)";

/// No crashes, partitions, or message loss.
fn quiet(nodes: usize, epochs: u64, model: Model) -> Params {
    let mut p = params(nodes, epochs);
    p.sim.faults.crash_interval_ms = None;
    p.sim.faults.partition_interval_ms = None;
    p.sim.net.loss_per_million = 0;
    p.model = Some(model);
    p
}

fn sent(r: &Run) -> BTreeMap<&'static str, (u64, u64)> {
    r.oracle.borrow().sent.clone()
}

/// Bytes sent per node per epoch, by message kind, against N, fanout, and
/// the entries each node writes per epoch.
#[test]
#[ignore]
fn sync_cost() {
    const WARM: u64 = 3;
    const EPOCHS: u64 = 10;
    let mut cases = Vec::new();
    for n in [3usize, 5, 10, 20, 50] {
        for w in [10u32, 100, 1000] {
            if n * w as usize > 20_000 {
                continue;
            }
            cases.push((n, w, None));
            if n >= 10 && w == 100 {
                cases.push((n, w, Some(2)));
                cases.push((n, w, Some(3)));
            }
        }
    }
    eprintln!(
        "| N | fanout | entries per node | own log KB | sent KB per node per epoch | logs | manifests | roots | status | other | sent ÷ own log |"
    );
    eprintln!("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for (n, w, gossip) in cases {
        let mut p = quiet(n, WARM + EPOCHS + 2, Model::new(10_000, w, Dist::Uniform));
        p.gossip = gossip;
        let mut r = start(1, p);
        let epoch_ms = r.params.epoch_ms;
        r.sim.run_for(WARM * epoch_ms);
        let (before, log_before) = (sent(&r), own_log_bytes(&r));
        r.sim.run_for(EPOCHS * epoch_ms);
        let (after, log_after) = (sent(&r), own_log_bytes(&r));
        let per = (n as u64 * EPOCHS) as f64 * 1024.0;
        let mut kinds: BTreeMap<&str, f64> = BTreeMap::new();
        for (k, (_, bytes)) in &after {
            let b = bytes - before.get(k).map_or(0, |x| x.1);
            let group = match *k {
                "log" | "logs" | "get_logs" => "logs",
                "manifest" | "chain" | "get_chain" => "manifests",
                "root" => "roots",
                "status" | "get_status" => "status",
                _ => "other",
            };
            *kinds.entry(group).or_default() += b as f64 / per;
        }
        let total: f64 = kinds.values().sum();
        let own = (log_after - log_before) as f64 / per;
        let g = |k| kinds.get(k).copied().unwrap_or(0.0);
        eprintln!(
            "| {n} | {} | {w} | {own:.1} | {total:.1} | {:.1} | {:.2} | {:.2} | {:.2} | {:.2} | {:.1} |",
            gossip.map_or("all".to_string(), |k| format!("gossip {k}")),
            g("logs"),
            g("manifests"),
            g("roots"),
            g("status"),
            g("other"),
            total / own
        );
        r.finish();
        r.check();
    }
}

fn own_log_bytes(r: &Run) -> u64 {
    (0..r.params.nodes).map(|i| r.peer(i).metrics().own_log_bytes).sum()
}

/// Exact expected conflicting rows per epoch (white paper, Section IX):
/// rows that two or more nodes write in one epoch.
fn exact(dist: Dist, rows: usize, n: usize, w: u32) -> (f64, f64) {
    let (nf, wf) = (n as f64, w as f64);
    let probs = dist.probs(rows);
    let mut exact = 0.0;
    for p in &probs {
        let q = 1.0 - (1.0 - p).powf(wf);
        exact += 1.0 - (1.0 - q).powf(nf) - nf * q * (1.0 - q).powf(nf - 1.0);
    }
    let approx = nf * (nf - 1.0) / 2.0 * wf * wf * probs.iter().map(|p| p * p).sum::<f64>();
    (exact, approx)
}

/// Measured conflicts per epoch against the model, over N, w (the writes of
/// a node in one epoch, so the epoch length T at a fixed write rate), and
/// skew. Three seeds each.
#[test]
#[ignore]
fn conflict_model_sweep() {
    let rows = 1_000;
    let epochs = 100u64;
    let seeds = 1..=3u64;
    eprintln!("| access | N | w | measured per epoch | exact model | equation 1 |");
    eprintln!("| --- | --- | --- | --- | --- | --- |");
    for dist in [Dist::Uniform, Dist::Zipf(0.8), Dist::Zipf(1.0), Dist::Zipf(1.2), Dist::Tiered] {
        for n in [2usize, 3, 5, 10, 20] {
            for w in [2u32, 5, 20] {
                let mut sum = 0;
                for seed in seeds.clone() {
                    let r = run(seed, quiet(n, epochs + 1, Model::new(rows, w, dist)));
                    r.check();
                    sum += r.peer(0).store().conflicts().unwrap().len();
                }
                let measured = sum as f64 / (epochs * seeds.clone().count() as u64) as f64;
                let (ex, approx) = exact(dist, rows, n, w);
                eprintln!("| {dist:?} | {n} | {w} | {measured:.2} | {ex:.2} | {approx:.2} |");
            }
        }
    }
}

/// Writes made while the previous epoch syncs (in flight) do not see the
/// other nodes' writes of that epoch: conflicts per epoch with every write
/// in flight, against every write after the merge.
#[test]
#[ignore]
fn in_flight_writes() {
    let rows = 1_000;
    let epochs = 100u64;
    eprintln!("| access | N | w | after the merge | in flight | exact model |");
    eprintln!("| --- | --- | --- | --- | --- | --- |");
    for dist in [Dist::Uniform, Dist::Zipf(1.0), Dist::Tiered] {
        for (n, w) in [(3usize, 5u32), (5, 5), (5, 20), (10, 5)] {
            let mut per = [0.0; 2];
            for (i, after_merge) in [true, false].into_iter().enumerate() {
                let r = run(1, quiet(n, epochs + 1, Model { after_merge, ..Model::new(rows, w, dist) }));
                r.check();
                per[i] = r.peer(0).store().conflicts().unwrap().len() as f64 / epochs as f64;
            }
            let (ex, _) = exact(dist, rows, n, w);
            eprintln!("| {dist:?} | {n} | {w} | {:.2} | {:.2} | {ex:.2} |", per[0], per[1]);
        }
    }
}

/// The same workload with all column policies, then with lww on every
/// column: lost updates, and conflicts that need review.
#[test]
#[ignore]
fn value_of_column_policies() {
    let rows = 1_000;
    let epochs = 100u64;
    eprintln!(
        "| access | N | w | schema | writes | lost qty updates | lost, % of writes | conflicts for review | price contests decided silently |"
    );
    eprintln!("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for dist in [Dist::Uniform, Dist::Zipf(1.0), Dist::Tiered] {
        for (n, w) in [(5usize, 4u32), (5, 20), (10, 20)] {
            for (name, schema) in [("all policies", SCHEMA), ("lww only", LWW_ONLY)] {
                let mut p = quiet(n, epochs + 1, Model::new(rows, w, dist));
                p.schema = schema;
                let r = run(1, p);
                r.check();
                let writes = r.oracle.borrow().committed.len() as i64;
                let store = r.peer(0).store();
                let q = store.query("SELECT sum(qty) FROM stock", &[]).unwrap();
                let sum = match &q[0][0] {
                    rusqlite::types::Value::Integer(v) => *v,
                    v => panic!("{v:?}"),
                };
                // Every write sells one unit.
                let lost = sum - (rows as i64 * 1_000_000 - writes);
                let review = store.conflicts().unwrap().len();
                // With lww on price, the contests that lww_flag would flag
                // are decided by HLC order; the model gives their number.
                let silent = if schema == LWW_ONLY { exact(dist, rows, n, w).0 * epochs as f64 } else { 0.0 };
                eprintln!(
                    "| {dist:?} | {n} | {w} | {name} | {writes} | {lost} | {:.1}% | {review} | {} |",
                    100.0 * lost as f64 / writes as f64,
                    if silent > 0.0 { format!("about {silent:.0}") } else { "0".into() }
                );
            }
        }
    }
}

