# Contributing

Thanks for your interest in contributing to Barsmith.

## Development setup

Prerequisites:
- Rust (stable)

Common commands:
- Format: `cargo fmt --all --check`
- Lint: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Lean CLI check: `cargo check -p barsmith_cli --no-default-features`
- Test: `cargo test --workspace --all-targets --all-features`
- Script syntax: `bash -n scripts/*.sh`
- CLI drift check: `scripts/check_stale_cli_flags.sh`
- Registry schema check: `scripts/check_registry_schema.sh`
- Smoke: `scripts/golden_smoke.sh`
- Performance gate: `scripts/performance_gate.sh`
- CLI benchmark smoke: `scripts/benchmark_smoke.sh`

## Test fixtures

This repo includes committed CSV fixtures under `tests/data/` to keep the test suite self-contained.

- `tests/data/ohlcv_tiny.csv` is the default smoke-test fixture (small, fast, and used by docs/examples).
- `tests/data/es_30m_sample.csv` is a larger “golden” fixture kept for deeper parity-style tests and may be replaced later with a smaller generated fixture if repo size becomes a concern.

## Pull requests

- Keep PRs focused and small when possible.
- Add/adjust tests for behavior changes.
- Prefer clear error messages over panics in runtime paths.
- Avoid committing large datasets, logs, or generated outputs.
- Keep `comb` examples on canonical flag names; do not reintroduce legacy aliases.
- Keep `runs/registry/**/*.json` small and privacy-safe when committing run audit records.
- For performance-sensitive changes, record before/after benchmark evidence using the fixture tiers in `benchmarks/README.md`.
- Update docs when changing user-visible behavior. Start with `docs/testing.md`, `docs/migration.md`, and the relevant CLI/data/run/output page.

## Code style

- `cargo fmt` must pass.
- Avoid `unsafe` unless it’s behind a clearly justified, well-tested, performance-critical boundary.
- Keep hot-path changes allocation-aware and benchmarked. Prefer readable helper boundaries outside tight loops; when a loop must stay dense for speed, document the invariant it relies on.
