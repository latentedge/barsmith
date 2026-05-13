# Performance

Barsmith is designed for high-throughput batch exploration, but performance depends heavily on configuration and your machine.

## Build mode

Prefer release builds for real runs:

```bash
cargo run --release -p barsmith_cli -- comb --help
```

## Key knobs

- Catalog size and `--max-depth`: the search space grows combinatorially.
- `--stats-detail core|full`: `core` is much cheaper; `full` computes more metrics.
- `--workers`: scales evaluation across CPU cores (watch memory bandwidth).
- `--batch-size` and `--auto-batch`: impacts scheduling overhead vs per-batch latency.
- `--subset-pruning`: can drastically reduce work for deep searches when many depth-2 pairs are dead.
- `--feature-pairs`: increases catalog size (more predicates).

## Hot-path invariants

Performance-sensitive refactors should preserve these invariants unless the PR includes a measured reason to change them:

- Feature masks are precomputed into compact bitsets before combination evaluation.
- Combination evaluation scans bitsets by word and calls per-hit logic only after eligibility/finite gates are applied.
- Worker threads share immutable catalogs and avoid filesystem or DuckDB work.
- The writer thread owns Parquet/DuckDB mutation.
- Reporting queries run after ingestion; they do not participate in the evaluator hot path.
- `core` stats should remain cheap enough for high-throughput sweeps, while `full` stats can spend more time on shape metrics.

`eval-formulas` is optimized for a ranked formula list rather than a combinatorial sweep. It builds each distinct formula clause mask once per evaluation window, reuses the shared Barsmith evaluator for trade selection and statistics, and keeps plotting/export work outside the evaluator path.

## CPU portability vs speed

This repo’s `.cargo/config.toml` sets `target-cpu=native` for local performance. This is great for on-machine runs, but not ideal for distributing binaries across heterogeneous CPUs.

## Benchmark note

Internal benchmark (not a guarantee): ~120B combination candidates over ~5 days on a MacBook Pro (Apple M4).

## Fixture tiers

Benchmark fixtures are documented in `benchmarks/README.md` and `benchmarks/fixtures.example.toml`.

- Tier A: committed tiny fixture for smoke checks.
- Tier B: committed realistic sample for golden-output and parity checks.
- Tier C: local-only data for release-mode performance gates.

Do not commit Tier C raw data or generated benchmark outputs.

## Local smoke benchmark

Run a small release-mode benchmark smoke from the repo root:

```bash
scripts/golden_smoke.sh
scripts/benchmark_smoke.sh
```

Override the fixture and size without editing the script:

```bash
BARSMITH_BENCH_CSV=tests/data/es_30m_sample.csv \
BARSMITH_BENCH_MAX_COMBOS=1000 \
BARSMITH_BENCH_BATCH_SIZE=1000 \
scripts/benchmark_smoke.sh
```

Record the command, git SHA, fixture tier, machine, Rust toolchain, and `/usr/bin/time` output when comparing refactors.

## Performance budget

- Stable hot-loop microbenchmarks: no regression accepted without an explicit design tradeoff.
- Noisy end-to-end smoke runs: investigate median regressions above 3% or p95 regressions above 5%.
- Run before/after comparisons on the same machine, release profile, fixture, and command.
- Readability-only refactors must prove parity with tests and preserve performance within this budget.

## What to record

For any performance-sensitive PR, record:

- git SHA before and after,
- Rust toolchain,
- machine model and CPU architecture,
- fixture tier and path category (never commit private raw data),
- exact command and environment variables,
- wall-clock output from `/usr/bin/time -p`,
- whether `--stats-detail` was `core` or `full`,
- observed regression or improvement and whether it is inside budget.
