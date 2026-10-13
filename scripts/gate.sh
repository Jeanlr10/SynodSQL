#!/usr/bin/env bash
# Runs the Phase 3 gate 1 simulation for many seeds in parallel.
#
#   scripts/gate.sh five|fifty EPOCHS FIRST_SEED LAST_SEED [JOBS]
#
# Each seed is one independent, single-threaded, deterministic run; JOBS
# runs (default: the number of CPUs) go at once. Logs go to
# target/gate/<test>-<epochs>/seed-<n>.log. A failed seed replays alone with
# the command that the summary prints.
set -euo pipefail

usage() {
    echo "usage: $0 five|fifty EPOCHS FIRST_SEED LAST_SEED [JOBS]" >&2
    exit 2
}
[ $# -ge 4 ] || usage
case "$1" in
    five) test=gate_five_nodes_long_run; epochs_var=SYNOD_GATE_EPOCHS ;;
    fifty) test=gate_fifty_nodes; epochs_var=SYNOD_GATE50_EPOCHS ;;
    *) usage ;;
esac
epochs=$2 first=$3 last=$4 jobs=${5:-$(nproc)}

cd "$(dirname "$0")/.."
bin=$(cargo test --release --locked -p synod-node --test cluster --no-run --message-format=json 2>/dev/null |
    grep -o '"executable":"[^"]*"' | cut -d'"' -f4 | tail -n 1)
[ -x "$bin" ] || { echo "could not build the cluster test" >&2; exit 1; }

out=target/gate/$test-$epochs
mkdir -p "$out"
echo "$test: $epochs epochs, seeds $first..$last, $jobs at once; logs in $out"

export bin test epochs_var epochs out
seq "$first" "$last" | xargs -P "$jobs" -I{} sh -c '
    start=$(date +%s)
    if env "$epochs_var=$epochs" SYNOD_GATE_SEED={} "$bin" "$test" --exact --nocapture >"$out/seed-{}.log" 2>&1; then
        result=ok
    else
        result=FAILED
    fi
    echo "seed {}: $result in $(( $(date +%s) - start )) s"
'

failed=$(grep -L "test result: ok" "$out"/seed-*.log | sed 's/.*seed-\([0-9]*\)\.log/\1/' | sort -n | tr '\n' ' ' || true)
for s in $(seq "$first" "$last"); do
    grep -h "^$test, seed" "$out/seed-$s.log" || true
done
if [ -n "$failed" ]; then
    echo "FAILED seeds: $failed"
    echo "replay one: $epochs_var=$epochs SYNOD_GATE_SEED=<seed> cargo test --release -p synod-node --test cluster $test -- --exact --nocapture"
    exit 1
fi
echo "all seeds passed"
