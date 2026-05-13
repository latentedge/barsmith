# Stability and Support

Barsmith is currently unstable. Breaking changes can happen before a stable release, and output schemas may evolve.

## Compatibility policy

Until a stable release:

- CLI flags may be removed when they are unsupported or misleading.
- Output schemas may change with migration notes.
- Run folders are protected by `run_manifest.json`, but old folders without a manifest must be recreated or forced.
- The default CLI supports only documented builtin targets.

The project prefers explicit breakage over accepting flags that imply behavior the engine does not implement.

## Supported surface

Supported:

- Rust stable toolchain pinned by `rust-toolchain.toml`.
- `barsmith_cli comb` over OHLCV CSV input.
- Local Parquet and DuckDB outputs.
- Optional S3 upload through the AWS CLI.
- Library use through `barsmith_rs` for callers that provide a prepared dataset.

Not supported:

- Live trading, broker integration, or order routing.
- Financial advice or strategy recommendations.
- OR/mixed combination logic in the default evaluator.
- Private-data benchmark fixtures in git.

## Security and privacy

Do not include secrets, account identifiers, broker data, or private raw datasets in issues, docs, test fixtures, or generated output committed to the repository.

Report vulnerabilities through the process in `SECURITY.md`.

## Breaking-change documentation

Every intentional breaking change should update:

- `CHANGELOG.md`,
- `docs/migration.md`,
- the relevant user doc (`docs/cli.md`, `docs/data-contract.md`, `docs/runs.md`, or `docs/outputs.md`).
