#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV="${BARSMITH_BENCH_CSV:-tests/data/ohlcv_tiny.csv}"
OUT_DIR="${BARSMITH_BENCH_OUT:-tmp/benchmark-smoke}"
MAX_DEPTH="${BARSMITH_BENCH_MAX_DEPTH:-2}"
MIN_SAMPLES="${BARSMITH_BENCH_MIN_SAMPLES:-25}"
MAX_COMBOS="${BARSMITH_BENCH_MAX_COMBOS:-200}"
BATCH_SIZE="${BARSMITH_BENCH_BATCH_SIZE:-200}"
WORKERS="${BARSMITH_BENCH_WORKERS:-1}"
SAMPLES="${BARSMITH_BENCH_SAMPLES:-7}"
SUITE="${BARSMITH_BENCH_SUITE:-comb-cli}"
REPORT="${BARSMITH_BENCH_REPORT:-target/barsmith-bench/benchmark-smoke.json}"

cd "$ROOT"

if [[ ! -f "$CSV" ]]; then
  echo "Benchmark CSV not found: $CSV" >&2
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$(dirname "$OUT_DIR")"

echo "== Barsmith benchmark smoke =="
echo "csv=$CSV"
echo "suite=$SUITE"
echo "out=$REPORT"
echo "work_dir=$OUT_DIR/work"
echo "max_depth=$MAX_DEPTH min_samples=$MIN_SAMPLES max_combos=$MAX_COMBOS batch_size=$BATCH_SIZE workers=$WORKERS samples=$SAMPLES"

cargo test -p barsmith_rs --test unranking

cargo run --release -p barsmith_bench -- run \
  --suite "$SUITE" \
  --samples "$SAMPLES" \
  --fixture-csv "$CSV" \
  --work-dir "$OUT_DIR/work" \
  --max-depth "$MAX_DEPTH" \
  --min-samples "$MIN_SAMPLES" \
  --batch-size "$BATCH_SIZE" \
  --workers "$WORKERS" \
  --max-combos "$MAX_COMBOS" \
  --out "$REPORT"

echo "Benchmark smoke complete. Structured report: $REPORT"
