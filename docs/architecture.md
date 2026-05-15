# Architecture

This page describes Barsmith’s supported Rust workflows at a high level.

## Crates

- `barsmith_cli`: CLI entrypoint (`comb`, `eval-formulas`, `results`, `select`, `protocol`)
- `barsmith_bench`: Rust-native benchmark runner and regression comparison gate
- `barsmith_indicators`: reusable Polars-free indicator and rolling-window math
- `barsmith_rs`: core library (dataset loading, combination enumeration, evaluation, storage)
- `custom_rs`: feature engineering, target catalog, and strategy preparation used by `comb`

## High-level flow

1. CLI parses flags and builds a `Config`.
2. `custom_rs` prepares the run:
   - reads the raw OHLCV CSV,
   - computes reusable indicator features,
   - labels the requested target from the target catalog,
   - writes `barsmith_prepared.csv` into the resolved run folder,
   - builds a feature catalog (`FeatureDescriptor`s) plus optional comparison predicates.
3. The core pipeline (`barsmith_rs`) runs:
   - loads the prepared dataset columnar (and applies optional date filters),
   - prunes to required columns,
   - builds caches/bitsets for fast evaluation,
   - enumerates combinations in a deterministic order (supports resume offsets),
   - evaluates each combination and persists qualifying results to Parquet + DuckDB,
   - optionally uploads batches to S3 (via AWS CLI).

The README contains a detailed flowchart: see `README.md` (“How `comb` works”).

## Formula evaluation flow

`eval-formulas` evaluates an existing prepared dataset and writes standard forward-test metadata by default. `select validate` and `select lockbox` orchestrate the strict certification path on top of the same result-query and formula-evaluation internals.

1. The CLI parses the ranked formula file into boolean flags and comparison clauses.
2. `formula_eval` loads `barsmith_prepared.csv`, validates target/RR/exit/sizing columns, and splits pre/post windows around the cutoff.
3. Distinct formula clauses are converted into bitsets once per window.
4. Formula clause indices are evaluated through the same `stats::evaluate_combination_indices` path used by combination search.
5. Optional FRS runs the same formulas across annual windows, then scores return consistency, drawdown shape, stability, and trade count.
6. Selection builds a candidate decision report from the pre/post rankings. The default `holdout-confirm` mode chooses by pre rank and treats post metrics as gates.
7. Strict protocol validation checks stage windows and formula-export provenance before a run can be treated as validation or lockbox evidence.
8. Optional overfit diagnostics compute bounded CSCV/PBO, PSR, DSR, effective-trials, and positive-window evidence outside the search hot path.
9. Optional stress diagnostics rerun the selected formula under cost, slippage, and sizing scenarios.
10. Optional equity-curve exports replay selected top formulas through the shared selection logic and keep plotting outside the evaluator path.
11. `select` records workflow status and source search accounting so reviewers can audit the path without terminal logs.

## Durability model

Barsmith writes incremental Parquet parts under `runs/artifacts/.../results_parquet/` and maintains a DuckDB catalog (`runs/artifacts/.../cumulative.duckdb`) that provides a stable `results` view across all parts.

Resume is index-based and protected by `run_manifest.json`, which binds the run folder to the CSV fingerprint and resume-sensitive configuration. This avoids silently resuming with a different dataset, catalog, target, direction, date window, sizing/cost model, or pruning mode. Increasing `--max-depth` is allowed because deeper combinations extend the same deterministic stream.

## Core module responsibilities

- `config`: runtime configuration and domain enums.
- `pipeline`: orchestration for dataset loading, catalog pruning, enumeration, evaluation, reporting, and uploads.
- `run_identity`: run-folder compatibility contract and `run_manifest.json` handling.
- `batch_tuning`: pure auto-batch heuristic and tests.
- `subset_pruning`: depth-2 dead-pair cache and background snapshot saver.
- `storage`: Parquet/DuckDB persistence, resume metadata, and top-results queries.
- `barsmith_indicators`: shared indicator kernels that remain independent from Polars and CLI/storage code.
- `custom_rs`: strategy-facing feature engineering, target labeling, and feature-catalog generation.
- `custom_rs::engineer`: preparation orchestration and DataFrame mutation.
- `custom_rs::engineer::feature_blocks`: a small facade over feature
  assembly modules for candles, derived metrics, oscillators, trend/Kalman
  state, volatility, price extraction, and warmup policy.
- `custom_rs::engineer::prepare`, `backtest`, `io`, and `hashing`: persistence, legacy backtest glue, column extraction, and prepared-CSV hash checks.
- `custom_rs::features`: feature-catalog orchestration, with focused modules for curated definitions, pairwise predicates, audit logging, pruning, and series-kind detection.
- `custom_rs::targets::registry`: canonical target metadata, default target risk columns, and target-output column detection.
- `barsmith_cli::standard_output`: public entrypoints for standard run folders, registry records, and closeout artifacts.
- `barsmith_cli::standard_output::checksums`: artifact checksum generation.
- `barsmith_cli::standard_output::closeout`: run manifests, summaries, registry records, and lockbox attempt tracking.
- `barsmith_cli::standard_output::helpers`: path, timestamp, JSON, and metric serialization helpers.
- `barsmith_cli::standard_output::plan`: run-path planning and default output-path wiring.
- `barsmith_cli::standard_output::records`: typed JSON record schemas for command, manifest, and registry files.
- `barsmith_cli::standard_output::reports`: human-readable forward-test, selection, overfit, stress, and lockbox Markdown reports.
- `formula`: ranked formula parsing and clause normalization.
- `formula_eval`: prepared-dataset formula evaluation, FRS wiring, and equity-curve row generation.
- `selection`: pre/post candidate selection policy and decision reports.
- `protocol`: strict research protocol, stage, provenance, and formula-export manifest contracts.
- `overfit`: PBO/CSCV and Sharpe-based overfit diagnostics.
- `stress`: execution stress report types.
- `frs`: Forward Robustness Score component calculation.
- `bitset`: compact mask storage, tiny support-ordering helpers, and scalar/SIMD scan loops used by the evaluator.
- `stats`: evaluation context and bitset-backed combination evaluation.
- `stats/metrics`: core/full metric accumulation and sample-quality helpers.
- `combinator`: deterministic combination counting, ranking, unranking, and batching.

First-party unsafe Rust is inventoried in `docs/unsafe.md`.

## Hot-path ownership

Barsmith keeps the performance-sensitive path narrow and explicit:

- `pipeline` decides what should be evaluated, but does not own per-row math.
- `combinator` owns deterministic index streams and reusable batch filling.
- `bitset` owns mask representation, fixed-depth scanner dispatch for common depth-1 through depth-5 combinations, and the generic fallback. Unsafe SIMD is isolated behind compile-time gates and safety comments.
- `stats` owns per-combination evaluation, the precomputed trade gate, tiny mask ordering by support, and no-stacking traversal.
- `stats/metrics` owns core/full metric accumulation so evaluator control flow stays easier to review.
- `storage` owns writer-side durability. Parquet parts are written to a temporary path and renamed only after a successful write.

Keep expensive work out of the per-combination loop: avoid string formatting, heap allocation, locking, hashing, filesystem work, and dynamic schema inspection while evaluating a candidate. Normal index combinations are stored inline, and the pipeline reuses batch buffers between evaluation rounds.

## Resume invariants

Resume offsets are safe only when the deterministic enumeration stream is unchanged for the already processed prefix.

`run_manifest.json` binds a run folder to:

- source CSV fingerprint,
- target and direction,
- date window,
- feature catalog hash,
- pruning and feature-pair settings,
- required feature gate,
- stats detail,
- sizing, cost, asset, and stacking settings.

`--max-depth` is intentionally excluded from the identity because increasing it extends the deterministic stream after the previously processed lower-depth prefix. Other settings that change result semantics must be treated as resume-sensitive.

## Test layout

Large test modules live beside their production modules instead of inside the production file:

- `barsmith_rs/src/stats/tests.rs`
- `custom_rs/src/engineer/tests.rs`
- `custom_rs/src/features/tests.rs`

This keeps production code easier to scan while preserving access to private module helpers for focused unit tests.

Target metadata lives in `custom_rs/src/targets/<target-id>/`, with the
central registry in `custom_rs/src/targets/registry.rs`. Shared stop/target
geometry is exposed through `custom_rs/src/targets/common/barrier.rs`; its
implementation is split into `targets/common/barrier/` modules for tick
rounding, target-resolution storage, next-bar targets, and high-low/ATR
geometry. Reusable indicator math belongs in `barsmith_indicators` so new
targets can reuse it without depending on Polars, CLI, or storage APIs.

The preparation layer is split by responsibility: `custom_rs/src/engineer.rs`
keeps the orchestration and final DataFrame mutations, while the
`custom_rs/src/engineer/` submodules hold feature blocks, IO helpers, hashing,
standard dataset preparation, and legacy backtest support. Feature assembly is
split again under `custom_rs/src/engineer/feature_blocks/` so indicator groups
can be reviewed without scrolling through one large preparation file.

The feature catalog follows the same pattern. `custom_rs/src/features.rs`
stays as the small public entrypoint, while `custom_rs/src/features/` owns the
curated definitions, pairwise catalog rules, dataset-audit logs, duplicate and
constant pruning, and boolean/numeric series classification.
