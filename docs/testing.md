# Testing

Barsmith has three validation tiers: fast correctness checks, durable run-folder smoke checks, and release-mode performance checks.

## Fast local gate

Run this before opening a behavior-changing PR:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
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

This is a small release-mode CLI throughput check. Use it as a sanity gate, not as the hard performance gate.
By default it builds the release benchmark and CLI binaries once for the `comb-cli` suite, invokes `target/release/barsmith_bench` directly, and writes `target/barsmith-bench/benchmark-smoke.json`. Non-default suites get suite-specific report and scratch paths unless those paths are explicitly overridden. If `BARSMITH_BENCH_SUITE` is set to a non-CLI suite, it skips the CLI binary build.

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

## Performance gate

Use the synthetic hard-gate suite before ending performance-sensitive work:

```bash
scripts/performance_gate.sh
```

This runs the stable `smoke` microbenchmarks: rank/unrank, index iteration,
synthetic comb evaluation, gated bitset scanning, and core statistics. It avoids
CLI startup, feature engineering, and DuckDB/Parquet noise so the result is a
better signal for hot-path regressions.

For hot-path work, capture a same-machine baseline and compare explicitly:

```bash
BARSMITH_PERF_BASELINE=off \
  BARSMITH_PERF_REPORT=target/barsmith-bench/baseline.json \
  scripts/performance_gate.sh
BARSMITH_PERF_REPORT=target/barsmith-bench/current.json scripts/performance_gate.sh
```

The wrapper automatically compares the default smoke suite against
`target/barsmith-bench/baseline.json` when that file exists. Targeted suites use
matching local baselines, such as
`target/barsmith-bench/select-validate-baseline.json`; suite aliases use the
same canonical baseline name.
It rejects report paths that would overwrite the active baseline unless
`BARSMITH_PERF_BASELINE=off` is set.
The default runner uses five untimed warmups per benchmark before measuring
samples, which reduces cold-start noise in the hard gate.
Targeted suites use suite-specific default report and scratch paths, so separate
benchmark runs do not overwrite each other unless a path is explicitly
overridden.

The comparison gate fails on hard-gate median regressions, p95 regressions corroborated by mean regression, and missing hard-gate benchmarks. p95-only spikes and end-to-end CLI benchmark regressions are review-only because they are noisier, but they still need an explicit accept/reject note.

The `smoke` benchmark suite covers the main stable hot paths: combination rank/unrank, index iteration, synthetic combination evaluation, gated bitset scans, and core stats. For combination-search performance work, run `--suite comb-eval` directly. For max-depth-5 work, also run `--suite comb-depth5`. For strict selection workflow changes, run `--suite select-validate`. For ATR/high-low target work, run `--suite target-generation`. Confirm large search changes with a Tier C CLI profile on local data.

For target semantics changes that can affect sizing or overfit-resistant
selection, run both targeted suites through the wrapper so same-machine
baselines are applied when present:

```bash
BARSMITH_PERF_SUITE=target-generation scripts/performance_gate.sh
BARSMITH_PERF_SUITE=select-validate scripts/performance_gate.sh
```

Example targeted benchmark runs:

```bash
cargo run --release -p barsmith_bench -- run \
  --suite comb-depth5 \
  --samples 21 \
  --out target/barsmith-bench/comb-depth5-current.json

cargo run --release -p barsmith_bench --features target-generation -- run \
  --suite target-generation \
  --samples 21 \
  --out target/barsmith-bench/target-generation-current.json
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
- combination-evaluator gating, batch reuse, or stats accumulation,
- target generation or feature catalog construction,
- output schema, DuckDB views, or Parquet write behavior.

For new targets, use `custom_rs/src/targets/TEMPLATE.md` as the checklist and
add tests for direction restrictions, target/RR/exit/risk columns, and any
domain-specific edge cases. ATR/high-low target geometry changes should run the
`target-generation` benchmark suite in addition to the default performance
gate.
