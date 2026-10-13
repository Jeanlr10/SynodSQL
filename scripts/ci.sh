#!/usr/bin/env bash
# Runs the CI jobs of .github/workflows/ci.yml on this machine, in the
# order of their cost, and stops at the first failure.
#
#   scripts/ci.sh          lint, msrv, test, bindings
#   scripts/ci.sh --full   also the simulation job (release, many seeds; long)
#   scripts/ci.sh --update first update the stable toolchain, as CI uses the
#                          latest stable
#
# The test job also runs on ARM64 and macOS in CI, and the bindings on
# macOS and Windows; this script runs them on this machine only.
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"

full=0
for arg in "$@"; do
    case "$arg" in
        --full) full=1 ;;
        --update) rustup update stable ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done

msrv=$(sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml)

step() { printf '\n== %s\n' "$*"; }

step "lint: cargo fmt --all --check"
cargo fmt --all --check

step "lint: cargo clippy --workspace --all-targets --locked"
cargo clippy --workspace --all-targets --locked

step "msrv: cargo +$msrv check --workspace --all-targets --locked"
if ! rustup run "$msrv" rustc --version >/dev/null 2>&1; then
    rustup toolchain install "$msrv" --profile minimal
fi
cargo +"$msrv" check --workspace --all-targets --locked

step "test: cargo test --workspace --locked"
cargo test --workspace --locked

step "bindings: scripts/test-bindings.sh"
scripts/test-bindings.sh

if [ "$full" = 1 ]; then
    step "simulation: cargo test --workspace --release --locked (CI seeds)"
    SYNOD_SIM_RUNS=300 PROPTEST_CASES=2000 SYNOD_MERGE_RUNS=100000 \
        SYNOD_GATE_EPOCHS=10000 SYNOD_GATE50_EPOCHS=300 SYNOD_CLUSTER_RUNS=40 \
        cargo test --workspace --release --locked
fi

step "all CI checks passed"
