# Selection Workflow

This is the recommended path after a `comb` discovery run finishes.

Barsmith treats selection as a research-control problem, not a leaderboard
problem. The safe workflow chooses from the discovery/pre ranking, uses the
post window only as confirmation gates, and reserves lockbox for one frozen
formula.

## Commands

Inspect the protocol first:

```bash
barsmith_cli select explain \
  --protocol research_protocol.json
```

Run strict validation from a discovery/pre-only comb run:

```bash
barsmith_cli select validate \
  --source-output-dir runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v1/pre_2024_12_31_refactor \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v1/full_prepared/barsmith_prepared.csv \
  --target 2x_atr_tp_atr_stop \
  --direction long \
  --cutoff 2024-12-31 \
  --research-protocol research_protocol.json \
  --dataset-id es_30m_official_v1 \
  --run-id validation_2024_12_31 \
  --asset MES \
  --position-sizing contracts
```

The validation command does three things in one strict workflow:

- exports ranked formulas from the discovery comb store,
- writes `candidate_formulas.txt` and `formula_export_manifest.json`,
- runs strict `eval-formulas` with selection, overfit, stress, and standard output artifacts.

The source comb run must include `date_end` metadata, usually by running
discovery with `--date-end`. `select validate` rejects missing metadata or any
source run that ends after the protocol discovery window before it writes
candidate artifacts.

Use `--dry-run` before a long run:

```bash
barsmith_cli select validate \
  --source-output-dir runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v1/pre_2024_12_31_refactor \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v1/full_prepared/barsmith_prepared.csv \
  --target 2x_atr_tp_atr_stop \
  --direction long \
  --cutoff 2024-12-31 \
  --research-protocol research_protocol.json \
  --dataset-id es_30m_official_v1 \
  --run-id validation_2024_12_31 \
  --dry-run
```

Run lockbox once with the frozen selected formula:

```bash
barsmith_cli select lockbox \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v1/full_prepared/barsmith_prepared.csv \
  --formulas runs/artifacts/forward-test/2x_atr_tp_atr_stop/es_30m_official_v1/2024-12-31/validation_2024_12_31/selected_formulas.txt \
  --target 2x_atr_tp_atr_stop \
  --cutoff 2025-06-30 \
  --research-protocol research_protocol.json \
  --formula-export-manifest runs/artifacts/forward-test/2x_atr_tp_atr_stop/es_30m_official_v1/2024-12-31/validation_2024_12_31/formula_export_manifest.json \
  --dataset-id es_30m_official_v1 \
  --run-id lockbox_2025_06_30 \
  --asset MES \
  --position-sizing contracts
```

Lockbox requires exactly one formula. A repeated lockbox formula/protocol is
rejected unless `--ack-rerun-lockbox` is passed, and acknowledged reruns are
recorded as contaminated evidence.

## Presets

`select validate` defaults to `--preset institutional`.

Institutional defaults are intentionally conservative:

- `holdout-confirm` selection only,
- candidate cap from protocol or 1000,
- pre trade floor 4000,
- post trade floor 50 and warning below 100,
- positive pre/post Total R and expectancy,
- max selected drawdown 25R,
- max return degradation 0.25,
- concentration, depth, and density guards enabled,
- overfit and stress diagnostics enabled.

Use `--preset exploratory` when you need a less restrictive development pass.
Use `--preset custom` only when the protocol or review notes explain why the
gate values changed before validation.

## Artifacts

Validation writes the usual forward-test folder plus selection-specific files:

- `candidate_formulas.txt`
- `formula_export_manifest.json`
- `selection_report.json`
- `selection_decisions.csv`
- `selected_formulas.txt`
- `protocol_validation.json`
- `overfit_report.json`
- `stress_report.json`
- `reports/selection.md`
- `reports/overfit.md`
- `reports/stress.md`
- `reports/summary.md`

Standard run manifests and registry records include `workflow_status`:

- `validation-selected-for-lockbox`
- `validation-rejected`
- `lockbox-pass`
- `lockbox-fail`
- `lockbox-contaminated-rerun`

Formula manifests also record source search accounting when available:

- `source_processed_combinations`
- `source_stored_combinations`

Overfit reports record `effective_trials_source`, so DSR reviewers can tell
whether effective trials came from processed source combinations, stored source
rows, an explicit CLI value, or a weaker exported-formula fallback.

## Review Order

Review these files in order:

1. `reports/summary.md`
2. `reports/selection.md`
3. `reports/overfit.md`
4. `reports/stress.md`
5. `selection_decisions.csv`
6. `selected_formulas.txt`

Do not choose the diagnostic top-post formula in holdout mode. It exists to
show what post-window optimization would have picked; it is not the selected
strategy unless you deliberately run a diagnostic workflow and reserve a later
lockbox.
