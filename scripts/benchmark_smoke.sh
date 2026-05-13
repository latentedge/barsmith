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

cd "$ROOT"

if [[ ! -f "$CSV" ]]; then
  echo "Benchmark CSV not found: $CSV" >&2
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$(dirname "$OUT_DIR")"

echo "== Barsmith benchmark smoke =="
echo "csv=$CSV"
echo "out=$OUT_DIR"
echo "max_depth=$MAX_DEPTH min_samples=$MIN_SAMPLES max_combos=$MAX_COMBOS batch_size=$BATCH_SIZE workers=$WORKERS"

cargo test -p barsmith_rs --test unranking

cargo build --release -p barsmith_cli

/usr/bin/time -p target/release/barsmith_cli \
  comb \
  --csv "$CSV" \
  --direction long \
  --target next_bar_color_and_wicks \
  --position-sizing fractional \
  --output-dir "$OUT_DIR" \
  --max-depth "$MAX_DEPTH" \
  --min-samples "$MIN_SAMPLES" \
  --batch-size "$BATCH_SIZE" \
  --workers "$WORKERS" \
  --max-combos "$MAX_COMBOS" \
  --stats-detail core \
  --report off \
  --force

echo "Benchmark smoke complete. Record the /usr/bin/time output with the fixture, command, git SHA, and machine."
