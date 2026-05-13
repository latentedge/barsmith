# Quickstart

Barsmith is currently marked **unstable**. Expect breaking changes.

## Install (local)

From the repo root:

```bash
cargo install --path barsmith_cli
```

This installs the `barsmith_cli` binary into Cargo’s bin directory (typically `~/.cargo/bin/`).

## Run a tiny dry-run (no external data)

```bash
barsmith_cli comb \
  --csv tests/data/ohlcv_tiny.csv \
  --direction long \
  --target next_bar_color_and_wicks \
  --runs-root runs/artifacts \
  --dataset-id tiny_sample \
  --run-id quickstart_dry \
  --registry-dir runs/registry \
  --max-depth 3 \
  --min-samples 100 \
  --workers 1 \
  --max-combos 1000 \
  --dry-run
```

## Run a small exploration

```bash
barsmith_cli comb \
  --csv tests/data/ohlcv_tiny.csv \
  --direction long \
  --target next_bar_color_and_wicks \
  --runs-root runs/artifacts \
  --dataset-id tiny_sample \
  --run-id quickstart_real \
  --registry-dir runs/registry \
  --max-depth 3 \
  --min-samples 100 \
  --workers 1 \
  --max-combos 10000 \
  --force
```

## Evaluate ranked formulas

After a `comb` run writes `barsmith_prepared.csv`, evaluate a ranked formula file directly in Rust:

```bash
barsmith_cli eval-formulas \
  --prepared runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real/barsmith_prepared.csv \
  --formulas ./formulas.txt \
  --target next_bar_color_and_wicks \
  --cutoff 2024-12-31 \
  --stacking-mode no-stacking \
  --position-sizing fractional \
  --runs-root runs/artifacts \
  --dataset-id tiny_sample \
  --run-slug forward_test \
  --registry-dir runs/registry \
  --plot \
  --plot-mode combined
```

See `docs/cli.md` for the formula grammar and FRS/plot options.
See `docs/research-protocol.md` for how the default selection protocol uses
pre-window rank and post-window confirmation.

## Query stored results

```bash
barsmith_cli results \
  --output-dir runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real \
  --direction long \
  --target next_bar_color_and_wicks \
  --limit 20
```

To feed stored results into `eval-formulas`, add `--export-formulas formulas.txt`.
For holdout checks, export from a discovery/pre-only `comb` run rather than a
run that searched the full history. The export also writes
`formula_export_manifest.json` for strict protocol runs.

## Data contract

See `docs/data-contract.md`.

## Next steps

- CLI guide: `docs/cli.md`
- Running experiments: `docs/runs.md`
- Outputs and querying: `docs/outputs.md`
