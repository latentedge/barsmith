#!/usr/bin/env bash
set -euo pipefail

# Simple orchestrator to exercise the main Rust validation surfaces.
# Run from the barsmith workspace root:
#   ./scripts/run_parity_all.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "== Rust: core tests =="
cargo test -p barsmith_rs --tests
cargo test -p custom_rs --tests

echo
echo "== Rust: full parity regression harness (ignored by default) =="
cargo test -p custom_rs --test parity_regression -- --ignored

echo
echo "All parity checks completed."
