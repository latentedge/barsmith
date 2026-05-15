#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV="${BARSMITH_BENCH_CSV:-tests/data/ohlcv_tiny.csv}"
SUITE="${BARSMITH_BENCH_SUITE:-comb-cli}"
SAFE_SUITE="$(printf '%s' "$SUITE" | tr -c 'A-Za-z0-9._-' '_')"
if [[ "$SUITE" == "comb-cli" ]]; then
  DEFAULT_OUT_DIR="tmp/benchmark-smoke"
  DEFAULT_REPORT="target/barsmith-bench/benchmark-smoke.json"
else
  DEFAULT_OUT_DIR="tmp/benchmark-smoke/$SAFE_SUITE"
  DEFAULT_REPORT="target/barsmith-bench/benchmark-smoke-$SAFE_SUITE.json"
fi
OUT_DIR="${BARSMITH_BENCH_OUT:-$DEFAULT_OUT_DIR}"
MAX_DEPTH="${BARSMITH_BENCH_MAX_DEPTH:-2}"
MIN_SAMPLES="${BARSMITH_BENCH_MIN_SAMPLES:-25}"
MAX_COMBOS="${BARSMITH_BENCH_MAX_COMBOS:-200}"
BATCH_SIZE="${BARSMITH_BENCH_BATCH_SIZE:-200}"
WORKERS="${BARSMITH_BENCH_WORKERS:-1}"
SAMPLES="${BARSMITH_BENCH_SAMPLES:-7}"
WARMUPS="${BARSMITH_BENCH_WARMUPS:-5}"
REPORT="${BARSMITH_BENCH_REPORT:-$DEFAULT_REPORT}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BENCH_BIN="${BARSMITH_BENCH_BIN:-$TARGET_DIR/release/barsmith_bench}"
CLI_BIN="${BARSMITH_CLI_BIN:-$TARGET_DIR/release/barsmith_cli}"

cd "$ROOT"

if [[ ! -f "$CSV" ]]; then
  echo "Benchmark CSV not found: $CSV" >&2
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$(dirname "$OUT_DIR")"
mkdir -p "$(dirname "$REPORT")"

echo "== Barsmith benchmark smoke =="
echo "csv=$CSV"
echo "suite=$SUITE"
echo "out=$REPORT"
echo "work_dir=$OUT_DIR/work"
echo "max_depth=$MAX_DEPTH min_samples=$MIN_SAMPLES max_combos=$MAX_COMBOS batch_size=$BATCH_SIZE workers=$WORKERS samples=$SAMPLES warmups=$WARMUPS"

build_args=(--release -p barsmith_bench)
bench_args=(
  run
  --suite "$SUITE"
  --samples "$SAMPLES"
  --warmups "$WARMUPS"
  --fixture-csv "$CSV"
  --work-dir "$OUT_DIR/work"
  --max-depth "$MAX_DEPTH"
  --min-samples "$MIN_SAMPLES"
  --batch-size "$BATCH_SIZE"
  --workers "$WORKERS"
  --max-combos "$MAX_COMBOS"
  --out "$REPORT"
)

case "$SUITE" in
  all|comb-cli|results-cli|strict-eval|formula-eval|select-validate|selection-workflow)
    build_args+=(-p barsmith_cli)
    bench_args+=(--barsmith-bin "$CLI_BIN")
    ;;
esac

cargo build "${build_args[@]}"

"$BENCH_BIN" "${bench_args[@]}"

echo "Benchmark smoke complete. Structured report: $REPORT"
