# Architecture

This page describes Barsmith’s supported Rust workflows at a high level.

## Crates

- `barsmith_cli`: CLI entrypoint (`comb`, `eval-formulas`, `results`)
- `barsmith_bench`: Rust-native benchmark runner and regression comparison gate
- `barsmith_builtin`: minimal built-in feature engineering + target labeling used by the default CLI
- `barsmith_rs`: core library (dataset loading, combination enumeration, evaluation, storage)
- `custom_rs`: example/advanced engine (not required by the default CLI)

## High-level flow

1. CLI parses flags and builds a `Config`.
2. The builtin engine:
   - reads the raw OHLCV CSV,
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

`eval-formulas` evaluates an existing prepared dataset and writes standard forward-test metadata by default.

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
- `bitset`: compact mask storage plus scalar/SIMD scan loops used by the evaluator.
- `stats`: evaluation context and bitset-backed combination evaluation.
- `stats/metrics`: core/full metric accumulation and sample-quality helpers.
- `combinator`: deterministic combination counting, ranking, unranking, and batching.

First-party unsafe Rust is inventoried in `docs/unsafe.md`.

## Hot-path ownership

Barsmith keeps the performance-sensitive path narrow and explicit:

- `pipeline` decides what should be evaluated, but does not own per-row math.
- `combinator` owns deterministic index streams and reusable batch filling.
- `bitset` owns mask representation and scanner dispatch. Unsafe SIMD is isolated behind compile-time gates and safety comments.
- `stats` owns per-combination evaluation, the precomputed trade gate, and no-stacking traversal.
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

This keeps production code easier to scan while preserving access to private module helpers for focused unit tests.

The advanced example engine also keeps target/RR geometry in `custom_rs/src/engineer/targets.rs`, separate from CSV loading, feature generation, and prepared-dataset persistence.
