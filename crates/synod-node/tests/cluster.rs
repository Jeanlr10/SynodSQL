//! Phase 3 gate: clusters of SynodSQL nodes in the simulator.

mod common;

use common::*;

#[test]
fn three_nodes_no_faults() {
    let mut p = params(3, 30);
    p.sim.faults.crash_interval_ms = None;
    p.sim.faults.partition_interval_ms = None;
    p.sim.net.loss_per_million = 0;
    let r = run(1, p);
    r.check();
    let o = r.oracle.borrow();
    eprintln!("merges {} published {} unmerges {} catch-ups {}", o.merges, o.published, o.unmerges, o.catch_ups);
    eprintln!("{}", r.positions());
}

#[test]
fn five_nodes_with_faults() {
    synod_sim::check_seeds("five_nodes_with_faults", 5, |seed| {
        let r = run(seed, params(5, 100));
        r.check();
        let o = r.oracle.borrow();
        eprintln!(
            "seed {seed}: merges {} published {} unmerges {} catch-ups {} abandoned {} crashes {} partitions {}",
            o.merges,
            o.published,
            o.unmerges,
            o.catch_ups,
            o.abandoned.len(),
            r.sim.stats().crashes,
            r.sim.stats().partitions
        );
    });
}

#[test]
#[ignore]
fn debug_seed() {
    let var = |k: &str| std::env::var(k).unwrap().parse::<u64>().unwrap();
    let (seed, node, epoch) = (var("SEED"), var("NODE") as u32, var("EPOCH"));
    let p = if std::env::var("RANDOM").is_ok() { random_params(seed, 12, 150) } else { params(5, 100) };
    let (epochs, lateness) = (p.epochs, p.lateness);
    let mut r = start(seed, p);
    r.sim.run_for(epochs * 1_000);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| r.finish()));
    let o = r.oracle.borrow();
    let mut h = r.peer(0).store().head().unwrap();
    while let Some(m) = o.manifests.get(&h) {
        if m.epoch + 2 >= epoch && m.epoch <= epoch + lateness + 2 {
            let logs: Vec<String> = m.logs.iter().map(|l| format!("{}@{}", l.node.0, l.epoch)).collect();
            eprintln!("chain e{} rank {} by {} gc {:?}: {}", m.epoch, m.rank, m.leader, m.gc_through, logs.join(" "));
        }
        h = m.parent;
    }
    for (hash, m) in &o.manifests {
        if m.logs.iter().any(|l| l.node.0 == node && l.epoch == epoch) {
            eprintln!("manifest {:02x?} e{} rank {} by {} has it", &hash[..4], m.epoch, m.rank, m.leader);
        }
    }
    eprintln!(
        "sealed: {:?}; abandoned: {}",
        o.sealed.get(&(node, epoch)).map(|x| x.0[0]),
        o.abandoned.contains(&(node, epoch))
    );
    eprintln!("{}", r.positions());
    let trace = String::from_utf8_lossy(r.sim.trace()).to_string();
    let me = format!("n{}", node - 1);
    let lo = 1_791_763_200_000 + epoch.saturating_sub(3) * 1_000;
    let hi = 1_791_763_200_000 + (epoch + lateness + 3) * 1_000;
    for line in trace.lines() {
        let t: u64 = line.split(' ').next().unwrap().parse().unwrap_or(0);
        let ev = line.contains("crash") || line.contains("boot") || line.contains("partition") || line.contains("heal");
        if ev && (line.contains(&me) || !line.contains(" n")) && t >= lo && t <= hi {
            eprintln!("{line}");
        }
    }
}

/// A node whose state differs from the others after the same manifest (as
/// after a faulty merge) finds it in the verify step, raises an alert, and
/// repairs itself from the majority by a Prolly tree diff.
#[test]
fn a_diverged_node_is_found_and_repaired() {
    let mut p = params(5, 0);
    p.sim.faults.crash_interval_ms = None;
    p.sim.faults.partition_interval_ms = None;
    let mut r = start(21, p);
    r.sim.run_for(20 * 1_000);
    let peer = r.peer_mut(2).unwrap();
    let store = peer.store_mut();
    let (id, _) = store
        .base_hashes(None, None)
        .unwrap()
        .into_iter()
        .find(|(id, _)| {
            store.base_rows(&[*id]).unwrap()[0].1.as_ref().is_some_and(|(t, row)| *t == 1 && !row.is_tombstone())
        })
        .unwrap();
    store.diverge_for_tests("stock", id, 1, synod_core::Value::Text("BAD".into())).unwrap();
    r.sim.run_for(10 * 1_000);
    r.finish();
    let o = r.oracle.borrow();
    assert!(o.mismatches.iter().any(|m| m.contains("mismatch")), "{:?}", o.mismatches);
    assert!(r.peer(2).metrics().hash_mismatches > 0);
    assert_eq!(r.peer(2).metrics().repairs, 1);
    for i in [0, 1, 3, 4] {
        assert_eq!(r.peer(i).metrics().repairs, 0, "node {i}");
    }
    let row = r.peer(2).store().row("stock", id).unwrap().unwrap();
    assert_ne!(row.cells[1].value, synod_core::Value::Text("BAD".into()));
}

/// Random clusters: every merge of one manifest has one root hash, all
/// nodes converge, and no committed write is lost. SYNOD_CLUSTER_RUNS sets
/// the number of seeds.
#[test]
fn random_clusters() {
    synod_sim::check_seeds_with("random_clusters", "SYNOD_CLUSTER_RUNS", 2, |seed| {
        let p = random_params(seed, 12, 150);
        let desc = format!("{} nodes, L {}, quorum {}, gossip {:?}", p.nodes, p.lateness, p.quorum, p.gossip);
        let r = run(seed, p);
        r.check();
        let o = r.oracle.borrow();
        eprintln!(
            "seed {seed} ({desc}): merges {} unmerges {} catch-ups {} abandoned {} crashes {}",
            o.merges,
            o.unmerges,
            o.catch_ups,
            o.abandoned.len(),
            r.sim.stats().crashes
        );
    });
}

#[test]
#[ignore]
fn profile_run() {
    let seed = std::env::var("SEED").map_or(1, |s| s.parse().unwrap());
    let mut p = if std::env::var("RANDOM").is_ok() { random_params(seed, 12, 150) } else { params(5, 30) };
    if let Ok(n) = std::env::var("NODES") {
        p = gate_params(n.parse().unwrap(), 20);
    }
    let t = std::time::Instant::now();
    let r = run(seed, p);
    eprintln!("total {:.0} ms", t.elapsed().as_secs_f64() * 1000.0);
    for (k, (n, ms)) in &r.oracle.borrow().time_ms {
        eprintln!("{k:12} {n:8} calls {ms:10.0} ms");
    }
}

/// Phase 3 gate 2: conflict counts follow the model of the white paper,
/// Section IX. N nodes each write w rows per epoch, drawn from an access
/// pattern p over R rows. The expected number of rows that two or more
/// nodes write in one epoch is, exactly,
/// sum_i 1 - (1-q_i)^N - N q_i (1-q_i)^(N-1), with q_i = 1 - (1-p_i)^w,
/// and about C(N, 2) w^2 sum_i p_i^2 (equation 1) when w p_i is small.
/// Each such row gives one lww_flag conflict on `price`; the delta column
/// `qty` merges every write with no record.
#[test]
fn conflict_counts_follow_the_model() {
    let n = 5usize;
    let rows = 1_000;
    for (dist, w) in [(Dist::Uniform, 20u32), (Dist::Zipf(1.0), 4), (Dist::Tiered, 4)] {
        let mut p = params(n, 201);
        p.sim.faults.crash_interval_ms = None;
        p.sim.faults.partition_interval_ms = None;
        p.sim.net.loss_per_million = 0;
        p.model = Some(Model { rows, w, dist });
        let epochs = (p.epochs - 1) as f64;
        let r = run(7, p);
        r.check();
        let conflicts = r.peer(0).store().conflicts().unwrap();
        let price = conflicts.iter().filter(|(_, c, _)| c.column_name.as_deref() == Some("price")).count() as f64;
        assert_eq!(price as usize, conflicts.len(), "only price conflicts: {conflicts:?}");
        let probs = dist.probs(rows);
        let (nf, wf) = (n as f64, w as f64);
        let mut exact = 0.0;
        let mut var = 0.0;
        for p in &probs {
            let q = 1.0 - (1.0 - p).powf(wf);
            let v = 1.0 - (1.0 - q).powf(nf) - nf * q * (1.0 - q).powf(nf - 1.0);
            exact += v;
            var += v * (1.0 - v);
        }
        let approx = nf * (nf - 1.0) / 2.0 * wf * wf * probs.iter().map(|p| p * p).sum::<f64>();
        let (expected, sigma) = (exact * epochs, (var * epochs).sqrt());
        eprintln!(
            "{dist:?}, w {w}: measured {price} conflicts in {epochs} epochs ({:.2}/epoch); \
             exact model {:.2}/epoch; equation 1: {approx:.2}/epoch",
            price / epochs,
            exact
        );
        assert!(
            (price - expected).abs() <= 4.0 * sigma + 0.02 * expected,
            "{dist:?}: {price} vs {expected:.1} ± {sigma:.1}"
        );
    }
}

/// Settings of the gate runs: crashes (also inside disk writes), network
/// partitions, message loss, clock offset and drift, and late nodes that
/// stay down for 10 to 30 epochs, more than L = 8. The cluster writes
/// about 7.5 transactions per epoch, on 200 keys.
fn gate_params(nodes: usize, epochs: u64) -> Params {
    let mut p = params(nodes, epochs);
    p.lateness = 8;
    p.keys = 200;
    // The same write load on the cluster for any number of nodes.
    p.write_per_million = (1_500_000 / nodes as u32).min(300_000);
    p.sim.clocks.max_offset_ms = 100;
    p.sim.clocks.max_drift_ppm = 50;
    p.sim.faults.long_downtime_per_million = 50_000;
    p.sim.faults.long_downtime_ms = (10_000, 30_000);
    p
}

fn number_from_env(var: &str, default: u64) -> u64 {
    std::env::var(var).map_or(default, |v| v.parse().unwrap_or_else(|_| panic!("{var} is not a number")))
}

fn report(name: &str, seed: u64, r: &Run) {
    let o = r.oracle.borrow();
    let st = r.sim.stats();
    let m = |f: fn(&synod_node::Metrics) -> u64| (0..r.params.nodes).map(|i| f(r.peer(i).metrics())).sum::<u64>();
    eprintln!(
        "{name}, seed {seed}: {} nodes, {} epochs: {} merges ({} distinct), {} unmerges, {} catch-ups, {} repairs, \
         {} own logs given up, {} conflicts recorded, {} late logs; {} crashes ({} inside a disk write), \
         {} partitions, {} messages lost; root hash mismatches: {}",
        r.params.nodes,
        r.params.epochs,
        o.merges,
        o.roots.len(),
        o.unmerges,
        o.catch_ups,
        o.repairs,
        o.abandoned.len(),
        m(|x| x.conflicts),
        m(|x| x.late_logs),
        st.crashes,
        st.crashes_at_write,
        st.partitions,
        st.dropped,
        m(|x| x.hash_mismatches),
    );
}

/// Phase 3 gate 1: 5 nodes for SYNOD_GATE_EPOCHS epochs (10,000 in the CI
/// simulation job). Nodes on the same manifest have one root hash, all
/// nodes converge, and no committed write is lost. SYNOD_GATE_SEED picks
/// the seed (default 1); `scripts/gate.sh` runs many seeds in parallel.
#[test]
fn gate_five_nodes_long_run() {
    let seed = number_from_env("SYNOD_GATE_SEED", 1);
    let r = run(seed, gate_params(5, number_from_env("SYNOD_GATE_EPOCHS", 300)));
    r.check();
    report("gate_five_nodes_long_run", seed, &r);
}

/// Phase 3 gate 1 at 50 nodes, for SYNOD_GATE50_EPOCHS epochs, with seed
/// SYNOD_GATE_SEED (default 2).
#[test]
fn gate_fifty_nodes() {
    let seed = number_from_env("SYNOD_GATE_SEED", 2);
    let r = run(seed, gate_params(50, number_from_env("SYNOD_GATE50_EPOCHS", 10)));
    r.check();
    report("gate_fifty_nodes", seed, &r);
}
