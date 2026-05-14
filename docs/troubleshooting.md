# Troubleshooting

## “Prepared dataset already exists … re-run with --ack-new-df”

The default CLI always writes `barsmith_prepared.csv` into the resolved run folder. If it already exists, pass `--ack-new-df` to overwrite it.

## “Different run identity …”

Barsmith writes `run_manifest.json` and prevents accidentally reusing a run folder with a different dataset, target, direction, catalog, date window, pruning mode, cost/sizing mode, or other resume-sensitive setting. Use `--force` to explicitly clear/overwrite cumulative outputs, or choose a fresh `--run-id`.

## “Missing required '<target>_exit_i' column for --stacking-mode no-stacking”

`--stacking-mode no-stacking` requires an integer `<target>_exit_i` column in the prepared dataset. The builtin engine emits exit indices for its supported targets; custom targets must emit them too.

See `docs/data-contract.md`.

## S3 upload failures

S3 upload uses the AWS CLI (`aws s3 cp`). Ensure:

- `aws` is on PATH
- AWS credentials are configured for the environment (e.g., `aws configure` or env vars)

## GitHub CI linker crash (SIGBUS / ld.lld)

On GitHub-hosted runners, `ld.lld` may occasionally crash when linking very large Rust test binaries (DuckDB + Polars + Arrow).

CI mitigates this by forcing GNU ld (bfd) via `RUSTFLAGS=-C link-arg=-fuse-ld=bfd`.
