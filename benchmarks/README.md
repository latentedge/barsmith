# Barsmith Benchmark Fixtures

Barsmith is performance sensitive, so benchmark data is split by fixture tier.
Only small, reviewable fixtures belong in git.

## Fixture Tiers

| Tier | Location | Purpose | Commit Raw Data |
| --- | --- | --- | --- |
| A | `tests/data/ohlcv_tiny.csv` | Fast smoke tests, CLI examples, and quick local checks. | Yes |
| B | `tests/data/es_30m_sample.csv` | Deeper parity and golden-output checks on a realistic sample. | Yes |
| C | local-only paths in `benchmarks/local-fixtures.toml` | Machine-specific performance baselines and long-run profiling. | No |

## Local Manifest

Copy `benchmarks/fixtures.example.toml` to `benchmarks/local-fixtures.toml` and adjust the local-only paths for your machine. The local manifest is ignored by git.

The default Tier C references intentionally use relative examples from this workspace history. They are documentation, not a requirement that those files exist on every checkout.

## Benchmark Rule

For hot-path changes, capture a baseline before the change and compare after the change on the same machine, toolchain, release profile, fixture, and command. Treat stable microbenchmark regressions as blockers. For noisy end-to-end runs, investigate median regressions above 3% or p95 regressions above 5% and record the decision.

## Rust Benchmark Tool

Run the fast benchmark gate:

```bash
scripts/performance_gate.sh
```

The `smoke` suite is the normal pre-push performance gate. It includes:

- `combinator`: rank/unrank and deterministic index iteration.
- `comb-eval`: the synthetic combination-evaluator hot path.
- `bitset`: gated bitset scanning.
- `stats`: core metric accumulation through the shared evaluator.

Additional targeted suites are available for performance-sensitive areas that are not part of the default smoke baseline:

- `comb-depth5`: starts the synthetic evaluator at depth 5 so max-depth-5 searches are measured directly.
- `target-generation`: measures `2x_atr_tp_atr_stop` ATR target construction on synthetic OHLCV/ATR data. Build with `--features target-generation` when running this suite.
- `select-validate`: runs the strict selection workflow over a tiny discovery result store. It is review-only because it includes CLI startup, storage, and filesystem noise.

Run the broader local suite:

```bash
cargo build --release -p barsmith_bench -p barsmith_cli
target/release/barsmith_bench run \
  --suite all \
  --barsmith-bin target/release/barsmith_cli \
  --samples 7 \
  --out target/barsmith-bench/all-current.json
```

Compare a candidate to a same-machine baseline:

```bash
BARSMITH_PERF_REPORT=target/barsmith-bench/baseline.json scripts/performance_gate.sh
BARSMITH_PERF_BASELINE=target/barsmith-bench/baseline.json \
  BARSMITH_PERF_REPORT=target/barsmith-bench/current.json \
  scripts/performance_gate.sh
```

Keep generated reports under `target/barsmith-bench/**` or another ignored path unless you are intentionally attaching a sanitized artifact to a review.

The report marks stable microbenchmarks as `hard-gate` and CLI end-to-end timings as `review-only`. `compare --fail-on-regression` fails on hard-gate median regressions, p95 regressions corroborated by mean regression, and missing hard-gate benchmarks. p95-only spikes are surfaced for review instead of hard-failing the gate.

Comparison deltas are relative to the baseline: negative means faster, positive means slower. Use the median as the primary signal, p95 for tail behavior, and mean to confirm whether a p95 spike is representative or just noise.

For combination-search refactors, run the `comb-eval` suite directly before a larger CLI profile:

```bash
cargo build --release -p barsmith_bench
target/release/barsmith_bench run \
  --suite comb-eval \
  --samples 21 \
  --out target/barsmith-bench/comb-eval-current.json
```

For max-depth-5 refactors, run the dedicated depth-5 suite:

```bash
target/release/barsmith_bench run \
  --suite comb-depth5 \
  --samples 21 \
  --out target/barsmith-bench/comb-depth5-current.json
```

For target-generation refactors, run:

```bash
cargo run --release -p barsmith_bench --features target-generation -- run \
  --suite target-generation \
  --samples 21 \
  --out target/barsmith-bench/target-generation-current.json
```

Use Tier C CLI runs to validate the full pipeline after the hard gate passes. CLI runs include feature engineering and result ingestion, so they are useful for release confidence but less stable as a regression gate.

## Golden Smoke

Run `scripts/golden_smoke.sh` before and after behavior-sensitive refactors. It exercises dry-run catalog preparation, a tiny real run, manifest creation, Parquet/DuckDB output, resume continuation, and top-result reporting on Tier A data.
