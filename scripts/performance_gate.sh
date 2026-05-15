#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUITE="${BARSMITH_PERF_SUITE:-smoke}"
SAMPLES="${BARSMITH_PERF_SAMPLES:-21}"
WARMUPS="${BARSMITH_PERF_WARMUPS:-2}"
REPORT="${BARSMITH_PERF_REPORT:-target/barsmith-bench/performance-gate.json}"
if [[ "$SUITE" == "smoke" ]]; then
  DEFAULT_BASELINE="target/barsmith-bench/baseline.json"
else
  DEFAULT_BASELINE="target/barsmith-bench/${SUITE}-baseline.json"
fi
BASELINE="${BARSMITH_PERF_BASELINE:-}"
COMPARISON="${BARSMITH_PERF_COMPARISON:-target/barsmith-bench/performance-gate-comparison.json}"
COMPARISON_MD="${BARSMITH_PERF_COMPARISON_MD:-target/barsmith-bench/performance-gate-comparison.md}"
MEDIAN_BUDGET="${BARSMITH_PERF_MEDIAN_BUDGET_PCT:-3.0}"
P95_BUDGET="${BARSMITH_PERF_P95_BUDGET_PCT:-5.0}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BENCH_BIN="${BARSMITH_BENCH_BIN:-$TARGET_DIR/release/barsmith_bench}"
CLI_BIN="${BARSMITH_CLI_BIN:-$TARGET_DIR/release/barsmith_cli}"
CSV="${BARSMITH_BENCH_CSV:-tests/data/ohlcv_tiny.csv}"
OUT_DIR="${BARSMITH_PERF_OUT:-tmp/performance-gate}"
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

case "$BASELINE" in
  "" )
    if [[ -f "$DEFAULT_BASELINE" ]]; then
      BASELINE="$DEFAULT_BASELINE"
    fi
    ;;
  none|off|false|0 )
    BASELINE=""
    ;;
esac

mkdir -p "$(dirname "$REPORT")"
rm -rf "$OUT_DIR"

echo "== Barsmith performance gate =="
echo "suite=$SUITE samples=$SAMPLES warmups=$WARMUPS"
echo "report=$REPORT"
if [[ -n "$BASELINE" ]]; then
  echo "baseline=$BASELINE"
else
  echo "baseline=<none; run-only mode>"
fi

build_args=(--release -p barsmith_bench)
needs_cli=false
case "$SUITE" in
  all|comb-cli|results-cli|strict-eval|formula-eval|select-validate|selection-workflow)
    build_args+=(-p barsmith_cli)
    needs_cli=true
    ;;
esac

cargo build "${build_args[@]}"

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
  --median-budget-pct "$MEDIAN_BUDGET"
  --p95-budget-pct "$P95_BUDGET"
  --out "$REPORT"
)

if [[ "$needs_cli" == true ]]; then
  bench_args+=(--barsmith-bin "$CLI_BIN")
fi

"$BENCH_BIN" "${bench_args[@]}"

if [[ -n "$BASELINE" ]]; then
  if [[ ! -f "$BASELINE" ]]; then
    echo "Performance baseline not found: $BASELINE" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$COMPARISON")" "$(dirname "$COMPARISON_MD")"
  "$BENCH_BIN" compare \
    --baseline "$BASELINE" \
    --candidate "$REPORT" \
    --median-budget-pct "$MEDIAN_BUDGET" \
    --p95-budget-pct "$P95_BUDGET" \
    --fail-on-regression \
    --out "$COMPARISON" \
    --markdown-out "$COMPARISON_MD"
  echo "Performance comparison written: $COMPARISON"
  echo "Performance comparison markdown written: $COMPARISON_MD"
else
  echo "Performance gate run complete. Create $DEFAULT_BASELINE or set BARSMITH_PERF_BASELINE to enforce a same-machine comparison."
fi
