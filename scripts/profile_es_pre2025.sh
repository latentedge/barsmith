#!/usr/bin/env bash
set -euo pipefail

# Lightweight harness to exercise Barsmith on the ES 30m desktop slice for both
# directions and a couple of depths. This is intended for manual CPU/memory
# profiling rather than automated tests.
#
# Usage (from barsmith/):
#   ./scripts/profile_es_pre2025.sh

DATA="${DATA:-$HOME/Desktop/es_30m_pre2025.csv}"
OUT_BASE="${OUT_BASE:-tmp/profile_runs}"

mkdir -p "$OUT_BASE"

run_case () {
  local direction="$1"
  local depth="$2"
  local label="${direction}_d${depth}"
  local runs_root="$OUT_BASE/artifacts"
  local registry_dir="$OUT_BASE/registry"

  echo
  echo "== Running Barsmith: direction=${direction}, max-depth=${depth}, run-id=${label} =="
  time cargo run -p barsmith_cli -- \
    comb \
    --csv "$DATA" \
    --direction "$direction" \
    --target next_bar_color_and_wicks \
    --runs-root "$runs_root" \
    --registry-dir "$registry_dir" \
    --dataset-id es_30m_pre2025 \
    --run-id "$label" \
    --max-depth "$depth" \
    --min-sample-size 500 \
    --date-start 2024-01-01 \
    --date-end 2024-12-31 \
    --batch-size 5000 \
    --max-combos 500 \
    --report-metrics top10
}

run_case long 1
run_case short 1
run_case long 2
run_case short 2

echo
echo "Profile runs complete. Inspect $OUT_BASE and your system profiler for CPU/memory behaviour."
