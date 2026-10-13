#!/usr/bin/env bash
# Builds the C library (libsynod) and the Python module, and runs their
# smoke tests: the CI "bindings" job.
#
#   scripts/test-bindings.sh            C, C++ and Python
#   scripts/test-bindings.sh --no-python
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
python=1
for arg in "$@"; do
    case "$arg" in
        --no-python) python=0 ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done

step() { printf '\n== %s\n' "$*"; }
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
inc=crates/synodsql-ffi/include
lib=target/release

step "C library: cargo build --release -p synodsql-ffi --locked"
cargo build --release -p synodsql-ffi --locked

# Link libsynod.a, with the system libraries Rust's static libraries need.
case "$(uname -s)" in
    Darwin) syslibs="-framework Security -framework CoreFoundation -liconv -lm" ;;
    *) syslibs="-lpthread -ldl -lm" ;;
esac

step "C: static link"
mkdir -p "$work/c"
cc -std=c11 -Wall -Wextra -Werror -I"$inc" crates/synodsql-ffi/tests/smoke.c "$lib/libsynod.a" $syslibs -o "$work/smoke-c"
"$work/smoke-c" "$work/c"

step "C++: dynamic link"
mkdir -p "$work/cpp"
c++ -std=c++17 -Wall -Wextra -Werror -I"$inc" crates/synodsql-ffi/tests/smoke.cpp -L"$lib" -lsynod \
    -Wl,-rpath,"$PWD/$lib" -o "$work/smoke-cpp"
"$work/smoke-cpp" "$work/cpp"

if [ "$python" = 1 ]; then
    step "Python: maturin build, install, and test"
    # The build tools stay in target/ between runs; the module is
    # installed fresh each time.
    venv=target/bindings-venv
    if [ ! -x "$venv/bin/maturin" ] || [ ! -x "$venv/bin/pytest" ]; then
        python3 -m venv "$venv"
        "$venv/bin/pip" install --quiet --upgrade pip maturin pytest
    fi
    "$venv/bin/maturin" build --release --locked -m crates/synodsql-py/Cargo.toml -o "$work/wheels"
    "$venv/bin/pip" install --quiet --force-reinstall --no-deps "$work"/wheels/*.whl
    (cd "$work" && "$OLDPWD/$venv/bin/python" -m pytest -q -p no:cacheprovider "$OLDPWD/crates/synodsql-py/tests")
fi

step "bindings ok"
