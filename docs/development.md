# Development

## Toolchain

This repo pins a Rust toolchain for consistent formatting/linting in CI. See `rust-toolchain.toml`.

## Common commands

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo doc --workspace --no-deps --all-features
cargo audit --deny warnings
scripts/golden_smoke.sh
scripts/benchmark_smoke.sh
```

`scripts/benchmark_smoke.sh` uses normal Cargo build parallelism by default. On memory-constrained machines, use `CARGO_BUILD_JOBS=1 scripts/benchmark_smoke.sh` as a fallback.

## Development principles

- Keep CLI behavior explicit. Remove unsupported flags instead of accepting no-op compatibility shims.
- Keep hot loops allocation-aware and benchmarked. Prefer readable helper boundaries outside tight loops.
- Keep resume semantics conservative. New settings that affect the search space or result meaning belong in the run identity manifest.
- Keep generated outputs, local benchmark data, Parquet, DuckDB, and raw private CSVs out of git.
- Document breaking changes in `docs/migration.md` and user-facing behavior in the relevant docs page.

## Dependency security (RustSec)

Run locally:

```bash
cargo audit --deny warnings
```

If you must temporarily ignore an advisory, record it in `.cargo/audit.toml` with the dependency path, rationale, mitigation, and revisit trigger, then open a tracking issue to remove the ignore. If the ignore is for an inactive optional upstream dependency, also verify the active feature graph with `cargo tree -i <crate> -e features`.

## Fixtures

Fixtures live under `tests/data/`:

- `ohlcv_tiny.csv`: small default fixture for smoke tests and docs
- `es_30m_sample.csv`: larger golden fixture (kept for deeper tests)

Avoid adding large datasets unless there is a clear test value.

For benchmark fixture tiers and local-only data paths, see `benchmarks/README.md`.

## Documentation changes

User-facing changes should update at least one of:

- `README.md` for top-level behavior and examples.
- `docs/cli.md` for flags.
- `docs/data-contract.md` for input/prepared data requirements.
- `docs/runs.md` and `docs/outputs.md` for resume and run-folder behavior.
- `docs/performance.md` for performance-sensitive behavior.
- `docs/migration.md` for breaking changes.
