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
- Eligibility and finite-reward gates are precombined once per evaluation context, so each candidate combination reuses the same trade gate.
- Small combinations stay inline; the hot path should not allocate a heap `Vec` for normal depth-1 through depth-8 candidates.
- Worker threads share immutable catalogs and avoid filesystem or DuckDB work.
- The writer thread owns Parquet/DuckDB mutation.
- Reporting queries run after ingestion; they do not participate in the evaluator hot path.
- `core` stats should remain cheap enough for high-throughput sweeps, while `full` stats can spend more time on shape metrics.

`eval-formulas` is optimized for a ranked formula list rather than a combinatorial sweep. It builds each distinct formula clause mask once per evaluation window, reuses the shared Barsmith evaluator for trade selection and statistics, and keeps plotting/export work outside the evaluator path.

## Combination-search fast path

The `comb` evaluator is intentionally narrow:

- `combinator` yields deterministic index combinations and reuses the caller's batch buffer.
- Rank/unrank arithmetic uses closed-form prefix counts; do not replace it with per-prefix loops without benchmark evidence.
- `stats` turns each index combination into borrowed bitset references, orders the tiny mask set with a fixed insertion sort, applies the precomputed trade gate, and accumulates metrics.
- `bitset` owns scalar/SIMD scanning details. On supported Apple Silicon builds, the aarch64 scanner has fixed-depth gated NEON paths for common depth-1 through depth-5 combinations and a generic fallback for deeper sets.
- `pipeline` owns pruning, threading, and storage handoff, but it does not perform per-row metric math.

When profiling combination search, start with the synthetic hard-gate benchmark:

```bash
BARSMITH_PERF_SUITE=comb-eval \
BARSMITH_PERF_REPORT=target/barsmith-bench/comb-eval-current.json \
scripts/performance_gate.sh
```

Use the depth-5 suite for command shapes that run `--max-depth 5`:

```bash
cargo run --release -p barsmith_bench -- run \
  --suite comb-depth5 \
  --samples 21 \
  --out target/barsmith-bench/comb-depth5-current.json
```

Then use a Tier C CLI profile only to confirm end-to-end behavior on your actual data:

```bash
cargo run --release -p barsmith_cli -- comb \
  --csv /path/to/local-tier-c.csv \
  --engine custom \
  --direction long \
  --target 2x_atr_tp_atr_stop \
  --runs-root target/barsmith-profile/candidate \
  --registry-dir target/barsmith-profile/registry \
  --dataset-id local-tier-c \
  --run-id comb_profile \
  --max-depth 3 \
  --min-samples 25 \
  --batch-size 50000 \
  --max-combos 500000 \
  --stats-detail core \
  --report off \
  --profile-eval coarse \
  --profile-eval-sample-rate 256 \
  --no-file-log \
  --quiet \
  --force
```

CLI profiles include feature engineering, pruning, Parquet/DuckDB writes, and OS scheduling noise. Treat them as release-readiness evidence, not as a substitute for the hard-gate microbenchmarks.

## Target-Generation Benchmark

ATR/high-low targets are easy to break while optimizing because gap opens, same-bar stop/target touches, cutoff-capped exits, NaNs, and tick rounding all affect labels and R. Use the target-generation suite before changing `custom_rs/src/engineer/targets.rs`:

```bash
cargo run --release -p barsmith_bench --features target-generation -- run \
  --suite target-generation \
  --samples 21 \
  --out target/barsmith-bench/target-generation-current.json
```

The suite uses synthetic OHLCV/ATR data so it is safe to share as benchmark evidence. It is behind the `target-generation` Cargo feature so the default smoke benchmark binary stays lean and comparable to existing baselines.

## CPU portability vs speed

This repo’s `.cargo/config.toml` sets `target-cpu=native` for local performance. This is great for on-machine runs, but not ideal for distributing binaries across heterogeneous CPUs.

The default release profile is intentionally performance-oriented:

- `opt-level = 3`
- `codegen-units = 1`
- `lto = "thin"`
- local `target-cpu=native`

Treat this profile as the baseline, not as proof that no better profile exists. Changes to `Cargo.toml`, `.cargo/config.toml`, `RUSTFLAGS`, allocator, linker, LTO, panic strategy, or CPU target must include a benchmark report and a comparison against the accepted same-machine baseline. Test FatLTO, `panic = "abort"`, strip settings, portable CPU flags, and PGO separately so build time, binary size, portability, and runtime effects are not mixed together.

The 2026-05-14 release-profile audit kept the existing ThinLTO profile. On the smoke hard-gate suite, `lto = "fat"` and `panic = "abort"` did not beat the accepted same-machine baseline after their build-time cost and p95 regressions were considered. Do not change the default release profile without a new benchmark comparison.

## Benchmark note

Internal benchmark (not a guarantee): ~120B combination candidates over ~5 days on a MacBook Pro (Apple M4).

## Fixture tiers

Benchmark fixtures are documented in `benchmarks/README.md` and `benchmarks/fixtures.example.toml`.

- Tier A: committed tiny fixture for smoke checks.
- Tier B: committed realistic sample for golden-output and parity checks.
- Tier C: local-only data for release-mode performance gates.

Do not commit Tier C raw data or generated benchmark outputs.

## Rust Benchmark Gate

Run a structured benchmark report from the repo root:

```bash
BARSMITH_PERF_REPORT=target/barsmith-bench/current.json scripts/performance_gate.sh
```

Compare two reports:

```bash
BARSMITH_PERF_BASELINE=off \
  BARSMITH_PERF_REPORT=target/barsmith-bench/baseline.json \
  scripts/performance_gate.sh
BARSMITH_PERF_REPORT=target/barsmith-bench/current.json scripts/performance_gate.sh
```

The JSON report records git SHA, dirty state, Rust version, target triple, OS/arch, CPU model, Cargo profile label, fixture hashes, samples, median, p95, min, max, mean, standard deviation, regression policy, and benchmark status.

When reading comparison output, negative deltas are faster than the baseline and positive deltas are slower. Median is the normal-case timing and is the main signal for stable microbenchmarks. p95 is the tail sample and helps catch occasional slow paths. Mean confirms whether a p95 spike reflects the whole run or just one noisy sample.

The `smoke` suite includes deterministic combination enumeration, the combination-evaluator hot path, bitset scanning, and core statistics. Use `--suite all` before risky hot-path refactors. Use `--suite comb-depth5` for max-depth-5 search changes, `--suite select-validate` for strict selection workflow changes, and build `barsmith_bench` with `--features target-generation` for ATR target changes. Use `--suite smoke` for the fast pre-push gate.

The preferred wrapper is:

```bash
scripts/performance_gate.sh
```

Set `BARSMITH_PERF_BASELINE=target/barsmith-bench/baseline.json` to enforce a
same-machine comparison with `--fail-on-regression`, or rely on the wrapper's
default: it automatically uses `target/barsmith-bench/baseline.json` when that
file exists. Set `BARSMITH_PERF_BASELINE=off` only when intentionally creating
or refreshing the accepted local baseline. Without a baseline file, the script
writes the current report only.

## Local smoke benchmark

Run a small release-mode benchmark smoke from the repo root:

```bash
scripts/golden_smoke.sh
scripts/benchmark_smoke.sh
```

`scripts/benchmark_smoke.sh` is a thin wrapper around `barsmith_bench`. By default it runs the `comb-cli` suite and writes `target/barsmith-bench/benchmark-smoke.json`.
For CLI suites it builds the release benchmark and CLI binaries once, then invokes the benchmark binary directly. It is intentionally review-only because it includes CLI startup, feature engineering, storage, and filesystem noise. Use `scripts/performance_gate.sh` for blocking hot-path regression checks.

When running CLI suites through `barsmith_bench` directly, build `barsmith_cli`
first or pass `--barsmith-bin` to an existing release binary. The benchmark
runner does not shell out to Cargo for CLI binaries.

Override the fixture and size without editing the script:

```bash
BARSMITH_BENCH_CSV=tests/data/es_30m_sample.csv \
BARSMITH_BENCH_MAX_COMBOS=1000 \
BARSMITH_BENCH_BATCH_SIZE=1000 \
scripts/benchmark_smoke.sh
```

Record the command, git SHA, fixture tier, machine, Rust toolchain, and `/usr/bin/time` output when comparing refactors.

## Performance budget

- Stable hot-loop microbenchmarks use `hard-gate`: median regressions above 3% block the change unless there is an explicit accepted tradeoff. p95 regressions above 5% block only when the mean also regresses above the median budget; p95-only spikes are marked for review so same-code scheduler noise does not create false hard failures.
- Noisy end-to-end CLI benchmarks use `review-only`: regressions above the same thresholds require investigation and a recorded accept/reject decision, but do not fail the hard microbenchmark gate by themselves.
- Missing hard-gate benchmarks on either side of a comparison fail the gate; update the baseline intentionally when adding, renaming, or removing a hard-gate benchmark.
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
- observed regression or improvement and whether it is inside budget,
- the hard-gate `barsmith_bench compare --fail-on-regression` result.
