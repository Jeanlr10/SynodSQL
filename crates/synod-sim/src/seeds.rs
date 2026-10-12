//! Running a simulation over many seeds.

use std::env;
use std::panic::{self, AssertUnwindSafe};

/// Runs `test` once for each of many seeds.
///
/// Environment variables change the seeds:
///
/// - `SYNOD_SEED=<n>` runs only seed `n`, to replay a failure;
/// - `SYNOD_SIM_RUNS=<n>` sets the number of seeds (default
///   `default_runs`);
/// - `SYNOD_SIM_BASE_SEED=<n>` sets the first seed (default 0).
///
/// On a failure, the seed and a replay command go to standard error, and
/// the panic continues.
pub fn check_seeds(name: &str, default_runs: u64, test: impl Fn(u64)) {
    check_seeds_with(name, "SYNOD_SIM_RUNS", default_runs, test)
}

/// As [`check_seeds`], but the number of seeds comes from the environment
/// variable `runs_var` instead of `SYNOD_SIM_RUNS`.
pub fn check_seeds_with(name: &str, runs_var: &str, default_runs: u64, test: impl Fn(u64)) {
    let var = |key: &str| {
        env::var(key).ok().map(|v| v.parse::<u64>().unwrap_or_else(|_| panic!("{key} is not a number: {v:?}")))
    };
    if let Some(seed) = var("SYNOD_SEED") {
        test(seed);
        return;
    }
    let runs = var(runs_var).unwrap_or(default_runs);
    let base = var("SYNOD_SIM_BASE_SEED").unwrap_or(0);
    for i in 0..runs {
        let seed = base.wrapping_add(i);
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| test(seed))) {
            eprintln!("\nsimulation `{name}` failed with seed {seed}");
            eprintln!("replay: SYNOD_SEED={seed} cargo test -- --exact {name}\n");
            panic::resume_unwind(payload);
        }
    }
}
