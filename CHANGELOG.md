# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project aims to follow Semantic Versioning.

## Unreleased

- Removed unsupported `--logic` / `--logic-mode` options and the internal `LogicMode` enum; feature combinations are AND-only.
- Removed unused `--early-exit-when-reused` CLI/config surface.
- Added `run_manifest.json` and run-identity checks to reject unsafe resume across incompatible CSV/config/catalog changes.
- Allowed deterministic `--max-depth` extension for matching run identities.
- Applied `--min-samples-report` to final top-result queries.
- Wrote Parquet result batches through a temporary file before atomic rename.
- Added fixture-tier docs, golden smoke, and benchmark smoke scripts.
- Refactored bitset scanning, stats metrics, batch/subset pruning, and custom target geometry into dedicated modules with focused tests.
- Added Rust-native ranked formula evaluation with FRS summaries, equity-curve exports, and optional PNG plots.
- Added a Rust-native `results` command for querying cumulative run folders and removed the legacy external result-query script.
- Added standardized run-folder construction via `--runs-root`, command metadata files, closeout summaries, optional registry JSON, and `total_return`/`calmar_r` result columns for future audits.
- Standardized forward-test outputs so `eval-formulas` can write the same command metadata, manifest, checksums, summary, plots, and Git-safe registry records as combination runs.
- Added holdout-aware formula selection artifacts (`selection_report.json`, `selection_decisions.csv`, `selected_formulas.txt`, and `reports/selection.md`) with pre-ranked/post-confirmed defaults.
- Added `results --export-formulas` for feeding stored combination results directly into `eval-formulas`, with metadata comments that call out the discovery/pre-only requirement for holdout-safe research.
- Added strict research protocol manifests, formula-export provenance sidecars, validation/lockbox/live-shadow stages, lockbox rerun tracking, overfit diagnostics, and execution stress reports.
- Added `comb --engine auto|builtin|custom`, with `auto` routing richer Rust targets such as `2x_atr_tp_atr_stop` to the custom engine.
- Split large private unit-test modules out of production files to improve maintainability without changing runtime behavior.
- Expanded open-source documentation for stability, testing, performance review, migration, and contributor workflows.
- Documented first-party unsafe Rust usage and review policy.
- Upgraded the Polars/DuckDB dependency stack, removed the transitive `fast-float` RustSec warning, tightened Polars features, and documented the remaining cargo-audit exception for inactive optional Polars `bincode` metadata.
- Runs the RustSec security workflow on push and pull requests in addition to the weekly schedule.
- Added the Rust-native `barsmith_bench` runner and comparison gate for structured performance evidence.
- Added hard-gate comparison checks for missing benchmark baselines/candidates, mean-corroborated p95 regression policy, and benchmarked the release profile while keeping the existing ThinLTO default.
- Kept benchmark regression policies and statuses typed internally while preserving the same JSON strings.
- Split standard-output planning, record schemas, closeout writing, checksum generation, helpers, and Markdown reports into focused modules.
- Hardened `protocol validate` so it enforces strict schema and non-overlapping research windows.
- Versioned formula-export manifests to schema `2` and renamed the source path fingerprint to `source_output_dir_path_sha256`.
- Added the `comb-eval` hard-gate benchmark and optimized combination search with faster rank/unrank arithmetic, inline index storage, reusable batch buffers, and a precomputed trade gate for evaluator scans.

## 0.1.0

- Initial public-ready repository structure (CLI, core library, built-in feature engineering, CI).
