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
- Added standardized run-folder construction via `--runs-root`, command metadata files, closeout summaries, registry JSON, and `total_return`/`calmar_r` result columns for future audits.
- Made standard run folders and registry records the default for `comb` and `eval-formulas`; removed legacy run-producing `--output-dir` flags while keeping `results --output-dir` for querying existing runs.
- Standardized forward-test outputs so `eval-formulas` can write the same command metadata, manifest, checksums, summary, plots, and Git-safe registry records as combination runs.
- Added holdout-aware formula selection artifacts (`selection_report.json`, `selection_decisions.csv`, `selected_formulas.txt`, and `reports/selection.md`) with pre-ranked/post-confirmed defaults.
- Added `results --export-formulas` for feeding stored combination results directly into `eval-formulas`, with metadata comments that call out the discovery/pre-only requirement for holdout-safe research.
- Added strict research protocol manifests, formula-export provenance sidecars, validation/lockbox/live-shadow stages, lockbox rerun tracking, overfit diagnostics, and execution stress reports.
- Removed the split preparation model and made `custom_rs` the single supported `comb` strategy/target path.
- Added `barsmith_indicators` for reusable Polars-free indicator and rolling-window math shared by `custom_rs` targets.
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
- Added `scripts/performance_gate.sh` as the default synthetic hard-gate benchmark wrapper and made `benchmark_smoke.sh` a suite-aware, build-once review-only throughput check.
- Removed the benchmark runner's internal Cargo-build fallback for CLI suites; scripts now build release binaries explicitly before timing.
- Moved the larger CLI dry-run parity test into the CLI test package to avoid nested `cargo run` rebuilds from `custom_rs` tests.
- Hardened several non-hot-path runtime boundaries by returning contextual errors for writer/subset-saver thread failures and removing avoidable unwraps from storage, subset-cache decoding, and finite-float sorting helpers.
- Added targeted `comb-depth5` and `target-generation` benchmark suites, depth-5 SIMD scan dispatch, fixed-depth scalar bitset coverage, small-mask support sorting, and target-generation parity tests for performance-sensitive refactors.
- Added the strict `select validate`, `select lockbox`, and `select explain` workflow for protocol-bound formula selection, including institutional/exploratory presets, source search accounting, workflow statuses, docs, smoke tests, and a review-only `select-validate` benchmark suite.
- Made `runs/registry/**/*.json` Git-trackable by default while keeping full run artifacts ignored.
- Removed remaining legacy CLI aliases such as `--min-sample-size`, `--report-metrics`, `--resume-offset`, and other compatibility spellings; supported docs and scripts now use canonical flags only.
- Added stale CLI flag and registry schema checks to local validation and CI.
- Hardened no-stacking evaluator state so hot-path scans use context-validated exit indices instead of panic-based invariants.
- Split top-result rendering out of the main pipeline module to keep orchestration easier to review without changing evaluator behavior.
- Made PNG plotting an optional default feature for `barsmith_cli`, so lean builds can use `--no-default-features` without pulling plot dependencies.
- Specialized the core statistics accumulator and full-length trade-gate scan path to preserve hot-path performance after the readability refactor.
- Lengthened the core statistics benchmark sample so the hard performance gate is less sensitive to sub-millisecond host jitter.

## 0.1.0

- Initial public-ready repository structure (CLI, core library, feature engineering, CI).
