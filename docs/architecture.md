# Architecture

This page describes Barsmith’s `comb` pipeline at a high level.

## Crates

- `barsmith_cli`: CLI entrypoint (`comb`)
- `barsmith_builtin`: minimal built-in feature engineering + target labeling used by the default CLI
- `barsmith_rs`: core library (dataset loading, combination enumeration, evaluation, storage)
- `custom_rs`: example/advanced engine (not required by the default CLI)

## High-level flow

1. CLI parses flags and builds a `Config`.
2. The builtin engine:
   - reads the raw OHLCV CSV,
   - writes `output-dir/barsmith_prepared.csv`,
   - builds a feature catalog (`FeatureDescriptor`s) plus optional comparison predicates.
3. The core pipeline (`barsmith_rs`) runs:
   - loads the prepared dataset columnar (and applies optional date filters),
   - prunes to required columns,
   - builds caches/bitsets for fast evaluation,
   - enumerates combinations in a deterministic order (supports resume offsets),
   - evaluates each combination and persists qualifying results to Parquet + DuckDB,
   - optionally uploads batches to S3 (via AWS CLI).

The README contains a detailed flowchart: see `README.md` (“How `comb` works”).

## Durability model

Barsmith writes incremental Parquet parts under `output-dir/results_parquet/` and maintains a DuckDB catalog (`output-dir/cumulative.duckdb`) that provides a stable `results` view across all parts.

Resume is index-based and protected by `run_manifest.json`, which binds the output directory to the CSV fingerprint and resume-sensitive configuration. This avoids silently resuming with a different dataset, catalog, target, direction, date window, sizing/cost model, or pruning mode. Increasing `--max-depth` is allowed because deeper combinations extend the same deterministic stream.

## Core module responsibilities

- `config`: runtime configuration and domain enums.
- `pipeline`: orchestration for dataset loading, catalog pruning, enumeration, evaluation, reporting, and uploads.
- `run_identity`: output-directory compatibility contract and `run_manifest.json` handling.
- `batch_tuning`: pure auto-batch heuristic and tests.
- `subset_pruning`: depth-2 dead-pair cache and background snapshot saver.
- `storage`: Parquet/DuckDB persistence, resume metadata, and top-results queries.
- `bitset`: compact mask storage plus scalar/SIMD scan loops used by the evaluator.
- `stats`: evaluation context and bitset-backed combination evaluation.
- `stats/metrics`: core/full metric accumulation and sample-quality helpers.
- `combinator`: deterministic combination counting, ranking, unranking, and batching.

First-party unsafe Rust is inventoried in `docs/unsafe.md`.

## Hot-path ownership

Barsmith keeps the performance-sensitive path narrow and explicit:

- `pipeline` decides what should be evaluated, but does not own per-row math.
- `bitset` owns mask representation and scanner dispatch. Unsafe SIMD is isolated behind compile-time gates and safety comments.
- `stats` owns per-combination evaluation, eligible/finite gating, and no-stacking traversal.
- `stats/metrics` owns core/full metric accumulation so evaluator control flow stays easier to review.
- `storage` owns writer-side durability. Parquet parts are written to a temporary path and renamed only after a successful write.

Keep expensive work out of the per-combination loop: avoid string formatting, allocation, locking, hashing, filesystem work, and dynamic schema inspection while evaluating a candidate.

## Resume invariants

Resume offsets are safe only when the deterministic enumeration stream is unchanged for the already processed prefix.

`run_manifest.json` binds an output directory to:

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
