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

For hot-path changes, capture a baseline before the change and compare after the change on the same machine, toolchain, release profile, fixture, and command. Treat stable microbenchmark regressions as blockers. For noisy end-to-end runs, investigate median regressions above 3% or p95 regressions above 5%.

## Golden Smoke

Run `scripts/golden_smoke.sh` before and after behavior-sensitive refactors. It exercises dry-run catalog preparation, a tiny real run, manifest creation, Parquet/DuckDB output, resume continuation, and top-result reporting on Tier A data.
