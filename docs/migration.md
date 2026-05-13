# Migration Notes

Barsmith is currently unstable, so breaking changes can happen before a stable release.

## Unreleased

### Rust-native formula evaluation and result queries

Ranked formula evaluation now lives in `barsmith_cli eval-formulas`. The command reads an existing `barsmith_prepared.csv`, evaluates AND-only formula clauses with the same stacking, reward, sizing, cost, and equity semantics used by Barsmith, computes optional Forward Robustness Score outputs, and can export equity curves and PNG plots.

`eval-formulas` now supports the same standard output contract as `comb` through
`--runs-root`, `--output-dir`, `--dataset-id`, `--run-id`, and `--registry-dir`.
When a standard run folder is selected, formula CSV/JSON, FRS outputs,
equity-curve exports, optional plots, command metadata, manifest, checksums, and
`reports/summary.md` are written into that folder by default. Explicit
`--csv-out`, `--json-out`, `--frs-out`, `--frs-windows-out`,
`--equity-curves-out`, `--plot-out`, and `--plot-dir` still override individual
artifact paths.

The old external result-query script has been removed. Use `barsmith_cli results` for routine top-result queries against `cumulative.duckdb` and `results_parquet/`.

Unsupported formula modes are intentionally not carried forward. Translate any formula syntax outside boolean flags, feature-vs-constant comparisons, feature-vs-feature comparisons, and `&&` conjunctions before running.

### AND-only combination logic

The `--logic` / `--logic-mode` CLI surface and the internal `LogicMode` config enum have been removed. Barsmith currently supports AND-only feature combinations. Existing scripts that pass `--logic and`, `--logic or`, or `--logic both` must remove that flag.

### Removed unused early-exit flag

`--early-exit-when-reused` has been removed because it was accepted by the CLI but not implemented by the evaluator. This avoids a false safety/performance contract.

### Run manifest required for resume

Run folders now include `run_manifest.json`. Existing output directories that contain Parquet/DuckDB state but no manifest are rejected unless you pass `--force` or choose a fresh `--output-dir`.

The manifest binds resume to the CSV fingerprint and resume-sensitive settings such as target, direction, date window, catalog hash, pruning settings, cost model, sizing mode, and required-feature gate. Increasing `--max-depth` is intentionally allowed because deeper runs extend the deterministic enumeration stream.

### Reporting sample threshold

`--min-samples-report` is now applied to final top-result queries. `--min-samples` still controls what gets persisted to cumulative results.

### Storage writes

Parquet result batches are written through a temporary file and renamed into place after the writer flushes, so interrupted writes are less likely to leave a partial `part-*.parquet` file.
