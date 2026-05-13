#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV="${BARSMITH_GOLDEN_CSV:-tests/data/ohlcv_tiny.csv}"
OUT_ROOT="${BARSMITH_GOLDEN_OUT:-tmp/golden-smoke}"
FORMULA_PREPARED="${BARSMITH_GOLDEN_FORMULA_PREPARED:-barsmith_rs/tests/fixtures/formula_eval_prepared.csv}"
FORMULA_FILE="${BARSMITH_GOLDEN_FORMULAS:-barsmith_rs/tests/fixtures/formula_eval_formulas.txt}"
RUNS_ROOT="$OUT_ROOT/artifacts"
REGISTRY_DIR="$OUT_ROOT/registry"
DRY_OUT="$RUNS_ROOT/comb/next_bar_color_and_wicks/long/tiny_sample/golden_dry"
RUN_OUT="$RUNS_ROOT/comb/next_bar_color_and_wicks/long/tiny_sample/golden_real"
FORMULA_OUT="$RUNS_ROOT/forward-test/2x_atr_tp_atr_stop/formula_fixture/2024-12-31/golden_forward"

cd "$ROOT"

if [[ ! -f "$CSV" ]]; then
  echo "Golden CSV not found: $CSV" >&2
  exit 1
fi
if [[ ! -f "$FORMULA_PREPARED" ]]; then
  echo "Formula prepared CSV not found: $FORMULA_PREPARED" >&2
  exit 1
fi
if [[ ! -f "$FORMULA_FILE" ]]; then
  echo "Formula file not found: $FORMULA_FILE" >&2
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
  --runs-root "$RUNS_ROOT" \
  --dataset-id tiny_sample \
  --run-id golden_dry \
  --registry-dir "$REGISTRY_DIR" \
  --max-depth 2 \
  --min-samples 25 \
  --batch-size 50 \
  --workers 1 \
  --max-combos 50 \
  --dry-run \
  --report off

test -f "$DRY_OUT/barsmith_prepared.csv"
test -f "$DRY_OUT/command.json"

target/release/barsmith_cli \
  comb \
  --csv "$CSV" \
  --direction long \
  --target next_bar_color_and_wicks \
  --position-sizing fractional \
  --runs-root "$RUNS_ROOT" \
  --dataset-id tiny_sample \
  --run-id golden_real \
  --registry-dir "$REGISTRY_DIR" \
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
test -f "$RUN_OUT/command.txt"
test -f "$RUN_OUT/command.json"
test -f "$RUN_OUT/checksums.sha256"
test -f "$RUN_OUT/reports/summary.md"
test -f "$REGISTRY_DIR/comb/next_bar_color_and_wicks/long/tiny_sample/golden_real.json"
test -f "$RUN_OUT/cumulative.duckdb"
find "$RUN_OUT/results_parquet" -name 'part-*.parquet' -type f | grep -q .
grep -q '"run_identity_hash"' "$RUN_OUT/run_manifest.json"

target/release/barsmith_cli \
  results \
  --output-dir "$RUN_OUT" \
  --direction long \
  --target next_bar_color_and_wicks \
  --min-samples 25 \
  --limit 3 >/dev/null

target/release/barsmith_cli \
  comb \
  --csv "$CSV" \
  --direction long \
  --target next_bar_color_and_wicks \
  --position-sizing fractional \
  --runs-root "$RUNS_ROOT" \
  --dataset-id tiny_sample \
  --run-id golden_real \
  --registry-dir "$REGISTRY_DIR" \
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

target/release/barsmith_cli \
  eval-formulas \
  --prepared "$FORMULA_PREPARED" \
  --formulas "$FORMULA_FILE" \
  --target 2x_atr_tp_atr_stop \
  --position-sizing fractional \
  --stacking-mode no-stacking \
  --cutoff 2024-12-31 \
  --report-top 2 \
  --runs-root "$RUNS_ROOT" \
  --dataset-id formula_fixture \
  --run-id golden_forward \
  --registry-dir "$REGISTRY_DIR" \
  --checksum-artifacts \
  --plot \
  --plot-mode combined \
  >/dev/null

test -s "$FORMULA_OUT/command.txt"
test -s "$FORMULA_OUT/command.json"
test -s "$FORMULA_OUT/run_manifest.json"
test -s "$FORMULA_OUT/checksums.sha256"
test -s "$FORMULA_OUT/reports/summary.md"
test -s "$FORMULA_OUT/formula_results.csv"
test -s "$FORMULA_OUT/formula_results.json"
test -s "$FORMULA_OUT/frs_summary.csv"
test -s "$FORMULA_OUT/frs_windows.csv"
test -s "$FORMULA_OUT/equity_curves.csv"
test -s "$FORMULA_OUT/plots/equity_curves.png"
test -s "$REGISTRY_DIR/forward-test/2x_atr_tp_atr_stop/formula_fixture/2024-12-31/golden_forward.json"
grep -q 'formula_results.csv' "$FORMULA_OUT/checksums.sha256"
grep -q 'plots/equity_curves.png' "$FORMULA_OUT/checksums.sha256"

prepared_sha="$(shasum -a 256 "$RUN_OUT/barsmith_prepared.csv" | awk '{print $1}')"
echo "prepared_sha256=$prepared_sha"
echo "Golden smoke complete."
