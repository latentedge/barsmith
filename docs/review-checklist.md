# Review Checklist

Use this checklist for behavior-sensitive Barsmith changes.

## Correctness

- CLI flags map to `Config` fields intentionally; stale flags are removed instead of kept as no-ops.
- Combination semantics remain AND-only unless a separately designed and benchmarked mode is added.
- Golden smoke passes on Tier A data: `scripts/golden_smoke.sh`.
- Existing workspace tests pass: `cargo test --workspace --all-targets --all-features`.
- Prepared dataset requirements are updated when target, eligibility, reward, or exit-index behavior changes.
- Date filtering, direction filtering, and no-stacking behavior are tested when touched.

## Resume And Storage

- Resume-sensitive config changes are reflected in `run_manifest.json`.
- `--max-depth` changes are intentionally safe only because deterministic enumeration extends the processed prefix.
- Existing state without a manifest fails with a recovery message.
- Parquet parts are not visible as final files until the writer finishes.
- DuckDB metadata and Parquet parts stay consistent across fresh runs, forced runs, and resumed runs.

## Performance

- Hot-path changes avoid per-combination allocation, string work, locks, and dynamic dispatch.
- Before/after timing uses the same machine, toolchain, release profile, fixture, and command.
- `scripts/benchmark_smoke.sh` passes, and larger Tier C benchmarks are recorded for hot-path refactors.
- SIMD/unsafe changes are isolated, documented with safety comments, and covered by scalar parity tests where practical.
- Readability refactors keep tight loops small, measurable, and allocation-aware.

## Unsafe Rust

- `docs/unsafe.md` is updated when any first-party unsafe site is added, removed, or materially changed.
- Unsafe remains isolated to approved modules with a written rationale and local safety comments.
- New unsafe code has a safe fallback or a documented reason one is not practical.
- Performance-motivated unsafe includes benchmark evidence.

## Data Hygiene

- Tier C data and generated output directories stay local-only.
- No secrets, raw data rows, local private paths, or generated Parquet/DuckDB artifacts are committed.

## Documentation

- README still explains what Barsmith is, what is supported, and what is not supported.
- CLI, data contract, run, output, and migration docs match the implementation.
- Breaking changes are listed in `CHANGELOG.md` and `docs/migration.md`.
- Performance-sensitive changes include benchmark evidence or a clear reason why only smoke validation is appropriate.
