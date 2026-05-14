# Running experiments

This page focuses on long-running `comb` runs: organizing standard run folders, resuming safely, and choosing batch sizes.

## Output directory layout

Barsmith writes each run under `runs/artifacts` by default. Typical contents:

- `barsmith_prepared.csv`
- `run_manifest.json`
- `command.txt`
- `command.json`
- `results_parquet/part-*.parquet`
- `cumulative.duckdb`
- `barsmith.log` (unless `--no-file-log`)
- `checksums.sha256`
- `reports/summary.md`

For long-running experiments, set a stable `--run-id` when you plan to resume:

```bash
barsmith_cli comb \
  --csv ../es_30m.csv \
  --target 2x_atr_tp_atr_stop \
  --direction long \
  --dataset-id es_30m_official_v2 \
  --run-id no_stacking
```

This writes the run under:

```text
runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/<run-id>/
```

`runs/artifacts/` is ignored by Git. The registry JSON under
`runs/registry/comb/<target>/<direction>/<dataset-id>/<run-id>.json` is designed
to be small enough to commit: it records the run ID, dataset ID, target,
direction, Git SHA, command hash, portable run path, top metrics, and artifact
location without embedding raw data, local artifact paths, or formula text. The
top metrics include both best Calmar and best total R so future audits are not
locked to one ranking lens. Non-finite metrics are recorded as explicit strings
such as `Inf`, `-Inf`, or `NaN`.

## Resuming

Barsmith resumes by extending the combination enumeration stream, scoped to a run identity manifest. The identity includes the CSV fingerprint plus settings that change the evaluated search space or result semantics, such as target, direction, feature catalog hash, date window, pruning mode, feature-pair settings, stats detail, costs, sizing, and stacking mode.

Practical rules:

- Reuse the same generated run folder to continue a run.
- Use the same `--run-id` when you need a stable resumable folder.
- Increasing `--max-depth` is allowed; Barsmith continues after the already processed lower-depth prefix.
- If the input CSV or another resume-sensitive setting changes, Barsmith will refuse to reuse the run folder unless you pass `--force`.
- If you want to override the stored resume offset (start from a specific point), pass `--resume-from`.

If `--run-id` is omitted, Barsmith creates `<UTC timestamp>_<git short sha>_<run slug>`.

## Prepared dataset overwrite (`--ack-new-df`)

The default CLI always writes `barsmith_prepared.csv` into the run folder.

- If it already exists, you must pass `--ack-new-df` to overwrite it.
- `--force` clears Parquet/DuckDB outputs but does not implicitly “bless” overwriting `barsmith_prepared.csv`.

## Choosing batch sizes

Batch size controls evaluation granularity:

- too small: overhead dominates (more writer churn, more scheduler overhead)
- too large: memory spikes and long tail latency (slow batches, reduced responsiveness)

Options:

- Start with a moderate `--batch-size` (e.g. 50k–500k) and scale up.
- Use `--auto-batch` for adaptive tuning on long runs.

## Sampling and dry runs

- Use `--dry-run` to validate that the catalog loads and the theoretical combination count looks sane.
- Use `--max-combos` for a short “smoke run” that still produces real outputs.

## Evaluating formulas after a run

Use `eval-formulas` when a run has already produced `barsmith_prepared.csv` and you want to score a curated formula file without rerunning combination search:

```bash
barsmith_cli eval-formulas \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/<run-id>/barsmith_prepared.csv \
  --formulas ./formulas.txt \
  --target 2x_atr_tp_atr_stop \
  --cutoff 2024-12-31 \
  --stacking-mode no-stacking \
  --position-sizing fractional \
  --dataset-id es_30m_official_v2 \
  --run-slug no_stacking_forward \
  --plot \
  --plot-mode combined
```

This writes the forward-test under:

```text
runs/artifacts/forward-test/2x_atr_tp_atr_stop/es_30m_official_v2/2024-12-31/<run-id>/
```

The forward-test folder includes command metadata, a manifest, result CSV/JSON,
selection artifacts, FRS outputs, equity curves, optional plots under `plots/`,
checksums, and `reports/summary.md`. The registry record lives under
`runs/registry/forward-test/<target>/<dataset-id>/<cutoff>/<run-id>.json` and
stores formula hashes rather than raw formula text. Selection artifacts include
`selection_report.json`, `selection_decisions.csv`, `selected_formulas.txt`, and
`reports/selection.md`.

Equity-curve CSVs and plots are derived from evaluated formula rows after
scoring, so they do not affect the combination-search hot path or resume state.

The default `holdout-confirm` selection mode chooses from the pre-window rank
and uses post-window metrics as gates. Use `docs/research-protocol.md` for the
recommended workflow and lockbox guidance.

For holdout-safe evaluation, the source `comb` run that produced exported
formulas should also be limited to the discovery/pre window, usually with
`--date-end <cutoff>`. Exporting candidates from a full-history search leaks the
post or lockbox period into the candidate set.

Strict protocol runs add `protocol_validation.json`, `overfit_report.json`,
`stress_report.json`, and human-readable `reports/overfit.md`,
`reports/stress.md`, or `reports/lockbox.md` when applicable. Registry records
store protocol hashes, formula-export manifest hashes, stage, lockbox attempt
status, overfit status, stress status, PBO, PSR, DSR, and effective-trials
metadata without embedding raw formula text.

## Date filtering

`--date-start` and `--date-end` filter the prepared dataset at load time, so evaluation and reporting see the same time window.

See `docs/data-contract.md` for timestamp requirements.
