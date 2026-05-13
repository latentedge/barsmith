# Testing

Barsmith has three validation tiers: fast correctness checks, durable run-folder smoke checks, and release-mode performance checks.

## Fast local gate

Run this before opening a behavior-changing PR:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo doc --workspace --no-deps --all-features
cargo audit --deny warnings
```

Any advisory exception must be documented in `.cargo/audit.toml` with rationale, mitigation, and a revisit trigger. If the exception depends on an optional upstream crate being inactive, verify it with `cargo tree -i <crate> -e features`. Dependency advisory handling is documented in `SECURITY.md`.

## Golden smoke

```bash
scripts/golden_smoke.sh
```

This builds the release CLI and exercises:

- dry-run catalog preparation,
- a tiny real run,
- `barsmith_prepared.csv`,
- `run_manifest.json`,
- Parquet part creation,
- DuckDB catalog creation,
- Rust-native top-result querying,
- resume continuation,
- top-result reporting,
- Rust-native ranked formula evaluation,
- holdout selection artifacts,
- strict protocol artifacts,
- overfit and stress diagnostics,
- formula CSV/JSON/FRS/equity-curve exports,
- optional PNG plot rendering.

Override paths with:

```bash
BARSMITH_GOLDEN_CSV=tests/data/ohlcv_tiny.csv \
BARSMITH_GOLDEN_OUT=tmp/golden-smoke \
scripts/golden_smoke.sh
```

## Benchmark smoke

```bash
scripts/benchmark_smoke.sh
```

This is a small release-mode throughput check. Use it as a sanity gate, not as a stable benchmark.
By default it uses normal Cargo build parallelism and all available Cargo build workers.

On memory-constrained machines:

```bash
CARGO_BUILD_JOBS=1 scripts/benchmark_smoke.sh
```

Use environment variables to change the fixture and search size:

```bash
BARSMITH_BENCH_CSV=tests/data/es_30m_sample.csv \
BARSMITH_BENCH_MAX_COMBOS=1000 \
BARSMITH_BENCH_BATCH_SIZE=1000 \
scripts/benchmark_smoke.sh
```

## Fixture tiers

See `benchmarks/README.md`.

- Tier A: tiny committed fixture for smoke checks.
- Tier B: committed realistic sample for deeper parity checks.
- Tier C: local-only private or machine-specific benchmark data.

Do not commit Tier C data or generated outputs.

## When to add tests

Add or update tests when changing:

- CLI flag parsing or config defaults,
- run identity and resume behavior,
- prepared dataset loading or type casting,
- date filtering,
- no-stacking and exit-index logic,
- storage filters and reporting filters,
- combination enumeration, pruning, or bitset scanning,
- target generation or feature catalog construction,
- output schema, DuckDB views, or Parquet write behavior.
