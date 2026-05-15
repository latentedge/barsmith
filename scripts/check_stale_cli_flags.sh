#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

doc_terms=(
  "--min-sample-size"
  "--report-metrics"
  "--logic"
  "--logic-mode"
  "--early-exit-when-reused"
  "--n-jobs"
  "--resume-offset"
  "--force-recompute"
  "--enable-feature-pairs"
  "--feature-pairs-max"
  "--enable-subset-pruning"
)

doc_paths=(
  README.md
  docs
  scripts
  .github
)

rg_args=(
  --fixed-strings
  --line-number
  --glob '!docs/migration.md'
  --glob '!docs/cli.md'
  --glob '!scripts/check_stale_cli_flags.sh'
)

failed=0
for term in "${doc_terms[@]}"; do
  if rg "${rg_args[@]}" -- "$term" "${doc_paths[@]}"; then
    failed=1
  fi
done

source_aliases=(
  'alias = "min-sample-size"'
  'alias = "report-metrics"'
  'alias = "logic"'
  'alias = "logic-mode"'
  'alias = "early-exit-when-reused"'
  'alias = "n-jobs"'
  'alias = "resume-offset"'
  'alias = "limit"'
  'alias = "force-recompute"'
  'alias = "enable-feature-pairs"'
  'alias = "feature-pairs-max"'
  'alias = "enable-subset-pruning"'
)

for alias_expr in "${source_aliases[@]}"; do
  if rg --fixed-strings --line-number -- "$alias_expr" barsmith_cli/src; then
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  echo "Found removed or legacy CLI flags in supported docs, scripts, CI, or CLI source." >&2
  echo "Use the canonical flags documented in docs/cli.md; keep historical notes in docs/migration.md only." >&2
  exit 1
fi

echo "No stale CLI flags found."
