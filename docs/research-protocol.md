# Research protocol

Barsmith is an exploration tool. Treat every ranked output as research evidence, not as a trading recommendation.

The default protocol is designed to reduce the easiest overfitting failure mode: selecting the strategy that looks best on the same post window used to judge it.

## Windows

`eval-formulas` splits the prepared dataset around `--cutoff`:

- pre window: rows with date `<= cutoff`
- post window: rows with date `> cutoff`

The pre window is the selection surface. The post window is confirmation evidence by default.

## Default selection mode

The default is `--selection-mode holdout-confirm`.

In this mode Barsmith:

1. ranks formulas on the pre window,
2. evaluates only the top `--candidate-top-k` pre-ranked candidates for selection,
3. applies pre/post gates to each candidate,
4. selects the first pre-ranked formula that passes those gates,
5. reports the top post-ranked formula only as a diagnostic.

This means the post window can reject a candidate, but it does not decide which passing formula wins. That is intentional.

## Selection gates

Selection gates are explicit CLI inputs:

- `--candidate-top-k`
- `--pre-min-trades`
- `--post-min-trades`
- `--post-warn-below-trades`
- `--pre-min-total-r`
- `--post-min-total-r`
- `--pre-min-expectancy`
- `--post-min-expectancy`
- `--max-drawdown-r`
- `--min-pre-frs`
- `--max-return-degradation`
- `--max-single-trade-contribution`
- `--embargo-bars`
- `--no-purge-cross-boundary-exits`

Recommended defaults are intentionally conservative: 1,000 pre candidates, at least 100 pre trades, at least 30 post trades, a warning below 50 post trades, positive pre/post Total R, positive pre/post expectancy, FRS enabled, and cross-boundary trade purging enabled.

If `--no-frs` is set, the `--min-pre-frs` gate is ignored.

## Boundary controls

Post-window evaluation can use `--embargo-bars <N>` to skip the first N rows after the cutoff. This is useful when feature construction or execution assumptions create near-boundary leakage risk.

By default, Barsmith purges rows whose `<target>_exit_i` leaves the filtered evaluation window. This keeps a trade opened in one evaluation window from silently relying on an exit outside that window. Disable this only for diagnostics with `--no-purge-cross-boundary-exits`.

## Diagnostic mode

`--selection-mode validation-rank` chooses the best post-ranked formula among pre candidates that pass gates. This is useful for diagnostics and model-development loops, but it uses the post window for selection. Reserve a later lockbox before treating a validation-ranked result as unbiased.

`--selection-mode off` disables selection artifacts and leaves only the raw formula rankings.

## True holdout

A true holdout is a dataset segment that is not used to design features, choose targets, tune thresholds, set selection gates, choose `--candidate-top-k`, or pick among formulas.

In practice:

1. Use pre data to generate and rank formulas.
2. Use post data only as a pass/fail confirmation surface.
3. Freeze the selection policy.
4. Run the selected formula once on a later lockbox segment.
5. Do not iterate on the lockbox result.

Strict workflows use four stages:

1. `discovery`: `comb` searches only the discovery window.
2. `validation`: `eval-formulas --stage validation --strict-protocol` confirms pre-ranked candidates without selecting by post rank.
3. `lockbox`: `eval-formulas --stage lockbox --strict-protocol` evaluates exactly one frozen formula.
4. `live-shadow`: the same one-formula path, labeled as paper/live-forward evidence.

The source combination run matters. If `comb` searched the full history, a later `eval-formulas --cutoff` is not a clean holdout, because the exported candidate set already saw the post or lockbox rows. Strict protocol mode rejects candidate exports whose manifest cannot prove discovery/pre-only provenance.

For the strict `select validate` path, the source comb run must also carry
`date_end` metadata from discovery. Runs without that metadata, or runs whose
`date_end` is after the protocol discovery end, are rejected before validation
artifacts are written.

Lockbox mode is deliberately restrictive. It accepts exactly one formula, disables selection, writes `reports/lockbox.md`, and records lockbox attempt metadata in the registry. Re-running the same lockbox formula/protocol requires `--ack-rerun-lockbox` and is marked as contaminated rerun evidence.

## Strict protocol

Create a protocol file before discovery:

```bash
barsmith_cli protocol init \
  --output research_protocol.json \
  --dataset-id es_30m_official_v2 \
  --target 2x_atr_tp_atr_stop \
  --direction long \
  --engine custom \
  --discovery-start 2020-01-01 \
  --discovery-end 2024-12-31 \
  --validation-start 2025-01-01 \
  --validation-end 2025-06-30 \
  --lockbox-start 2025-07-01 \
  --lockbox-end 2025-12-31 \
  --candidate-top-k 1000
```

Use `barsmith_cli protocol validate --protocol research_protocol.json` in CI or before long runs. Validation rejects unsupported schema versions, `strict=false`, invalid window ordering, and overlapping discovery/validation/lockbox windows. Use `protocol explain` when reviewing a run folder.

Strict `eval-formulas` requires:

- `--strict-protocol`
- `--research-protocol research_protocol.json`
- `--formula-export-manifest formula_export_manifest.json` from `results --export-formulas --research-protocol research_protocol.json`
- a stage via `--stage validation|lockbox|live-shadow`

Strict mode rejects unsupported protocol schema versions, protocol files with `strict=false`, overlapping stage windows, target or direction mismatches, formula manifests without a matching `protocol_sha256`, and formula exports that overlap the validation or lockbox windows.

The recommended CLI entrypoint for strict validation and lockbox evidence is
`barsmith_cli select`. It wraps `results` and `eval-formulas` so the safe path
exports candidates, binds the manifest, runs validation, writes diagnostics, and
records `workflow_status` in one command.

## Overfit diagnostics

Strict protocol mode writes overfit diagnostics by default:

- `overfit_report.json`
- `overfit_decisions.csv`
- `reports/overfit.md`

The report includes the candidate count, effective trials, CSCV/PBO, Probabilistic Sharpe Ratio, Deflated Sharpe Ratio, positive-window ratio, and warnings when the evidence is too thin. These are statistical guardrails, not proof of future profitability.

## Stress diagnostics

Strict protocol mode also writes execution stress diagnostics:

- `stress_report.json`
- `stress_matrix.csv`
- `reports/stress.md`

The default stress grid checks baseline, 1.5x costs, 2x costs, one/two ticks worse entry-and-exit when tick value and dollars-per-R are available, and half the configured max-contract cap when a max-contract cap is set.

## Standard artifacts

When `eval-formulas` writes a standard run folder, selection artifacts are written by default:

- `selection_report.json`: full structured decision report.
- `selection_decisions.csv`: one row per candidate with gate results and rejection reasons.
- `selected_formulas.txt`: ranked-formula file containing only the selected formula, or a no-selection note.
- `protocol_validation.json`: strict protocol decision details when strict mode is enabled.
- `overfit_report.json` and `reports/overfit.md`: multiple-testing and CSCV/PBO diagnostics.
- `stress_report.json` and `reports/stress.md`: cost, slippage, and sizing stress diagnostics.
- `reports/selection.md`: human-readable selection summary.
- `reports/lockbox.md`: one-formula lockbox summary for lockbox/live-shadow runs.
- `reports/summary.md`: run closeout summary with the selected formula hash and diagnostic top-post hash.

Forward-test registry records store formula hashes and decision metadata instead of raw formula text.

## Recommended workflow

Run discovery on the pre window only:

```bash
barsmith_cli comb \
  --csv data/es_30m_official_v2.csv \
  --direction long \
  --target 2x_atr_tp_atr_stop \
  --position-sizing contracts \
  --asset MES \
  --date-end 2024-12-31 \
  --dataset-id es_30m_official_v2 \
  --run-id discovery_pre_2024_12_31
```

Run holdout confirmation through the strict selection workflow:

```bash
barsmith_cli select validate \
  --source-output-dir runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/discovery_pre_2024_12_31 \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/full_prepared/barsmith_prepared.csv \
  --target 2x_atr_tp_atr_stop \
  --direction long \
  --cutoff 2024-12-31 \
  --research-protocol research_protocol.json \
  --stacking-mode no-stacking \
  --preset institutional \
  --candidate-top-k 1000 \
  --dataset-id es_30m_official_v2 \
  --run-id holdout_confirm \
  --plot \
  --plot-mode combined
```

Review `reports/selection.md` first. Treat `diagnostic_top_post_formula_sha256` as a diagnostic, not as the selected strategy in holdout mode.

Run lockbox once with the selected formula:

```bash
barsmith_cli select lockbox \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/full_prepared/barsmith_prepared.csv \
  --formulas runs/artifacts/forward-test/2x_atr_tp_atr_stop/es_30m_official_v2/2024-12-31/holdout_confirm/selected_formulas.txt \
  --target 2x_atr_tp_atr_stop \
  --cutoff 2025-06-30 \
  --research-protocol research_protocol.json \
  --formula-export-manifest runs/artifacts/forward-test/2x_atr_tp_atr_stop/es_30m_official_v2/2024-12-31/holdout_confirm/formula_export_manifest.json \
  --stacking-mode no-stacking \
  --dataset-id es_30m_official_v2 \
  --run-id lockbox_once
```

Low-level `results --export-formulas` and `eval-formulas --strict-protocol`
remain available for diagnostics, but `select` is the documented certification
path.
