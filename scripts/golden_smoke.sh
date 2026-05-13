#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV="${BARSMITH_GOLDEN_CSV:-tests/data/ohlcv_tiny.csv}"
OUT_ROOT="${BARSMITH_GOLDEN_OUT:-tmp/golden-smoke}"
DRY_OUT="$OUT_ROOT/dry-run"
RUN_OUT="$OUT_ROOT/real-run"

cd "$ROOT"

if [[ ! -f "$CSV" ]]; then
  echo "Golden CSV not found: $CSV" >&2
  exit 1
fi

rm -rf "$OUT_ROOT"
mkdir -p "$OUT_ROOT"

echo "== Barsmith golden smoke =="
echo "csv=$CSV"
echo "out=$OUT_ROOT"

cargo build --release -p barsmith_cli

target/release/barsmith_cli \
  comb \
  --csv "$CSV" \
  --direction long \
  --target next_bar_color_and_wicks \
  --position-sizing fractional \
  --output-dir "$DRY_OUT" \
  --max-depth 2 \
  --min-samples 25 \
  --batch-size 50 \
  --workers 1 \
  --max-combos 50 \
  --dry-run \
  --report off

test -f "$DRY_OUT/barsmith_prepared.csv"

target/release/barsmith_cli \
  comb \
  --csv "$CSV" \
  --direction long \
  --target next_bar_color_and_wicks \
  --position-sizing fractional \
  --output-dir "$RUN_OUT" \
  --max-depth 2 \
  --min-samples 25 \
  --min-samples-report 25 \
  --batch-size 50 \
  --workers 1 \
  --max-combos 200 \
  --stats-detail core \
  --report formula \
  --top-k 3 \
  --force

test -f "$RUN_OUT/barsmith_prepared.csv"
test -f "$RUN_OUT/run_manifest.json"
test -f "$RUN_OUT/cumulative.duckdb"
find "$RUN_OUT/results_parquet" -name 'part-*.parquet' -type f | grep -q .
grep -q '"run_identity_hash"' "$RUN_OUT/run_manifest.json"

target/release/barsmith_cli \
  comb \
  --csv "$CSV" \
  --direction long \
  --target next_bar_color_and_wicks \
  --position-sizing fractional \
  --output-dir "$RUN_OUT" \
  --max-depth 2 \
  --min-samples 25 \
  --min-samples-report 25 \
  --batch-size 50 \
  --workers 1 \
  --max-combos 250 \
  --stats-detail core \
  --report formula \
  --top-k 3 \
  --ack-new-df

prepared_sha="$(shasum -a 256 "$RUN_OUT/barsmith_prepared.csv" | awk '{print $1}')"
echo "prepared_sha256=$prepared_sha"
echo "Golden smoke complete."
