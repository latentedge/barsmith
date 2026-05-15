# Outputs and querying

Barsmith writes incremental Parquet batches plus a DuckDB catalog for querying “top results”.

## Files

In each standard run folder:

- `barsmith_prepared.csv`: engineered dataset used for the run
- `run_manifest.json`: resume identity for the run folder
- `command.txt`: shell-quoted command used to launch the run
- `command.json`: structured command metadata, run ID, dataset ID, Git SHA, and artifact URI
- `results_parquet/part-*.parquet`: stored result rows (only combinations that pass storage filters)
- `cumulative.duckdb`: DuckDB database that exposes a `results` view over all Parquet parts
- `barsmith.log`: file log (unless disabled)
- `checksums.sha256`: checksums for small audit metadata; pass `--checksum-artifacts` to include heavy run files
- `reports/summary.md`: human-readable closeout summary for the run

Combination search writes under `runs/artifacts` by default. Pass `--runs-root`
only when you want a different artifact root:

```text
runs/artifacts/
  comb/
    <target>/
      <direction>/
        <dataset-id>/
          <run-id>/
            barsmith_prepared.csv
            run_manifest.json
            command.txt
            command.json
            checksums.sha256
            barsmith.log
            cumulative.duckdb
            results_parquet/
            reports/
              summary.md
```

Barsmith also writes a lightweight Git-trackable registry record under
`runs/registry/comb/<target>/<direction>/<dataset-id>/<run-id>.json` by default.
Registry records include IDs, Git SHA, command hash, portable run path, artifact
URI, sizing metadata, and best-Calmar and best-total-R metrics. They store a
formula hash rather than formula text so private formulas do not need to be
committed.
Non-finite metrics are written explicitly as strings such as `Inf`, `-Inf`, or
`NaN` rather than being silently converted to JSON `null`.

Registry schema version `2` records `position_sizing`,
`stop_distance_column`, `stop_distance_unit`, and `risk_model`. For ATR-stop
contract-sized runs, `risk_model=realized_tick_rounded_target_risk` means the
run used the target-generated risk column after tick rounding. Old explicit
`--stop-distance-column atr` runs are recorded as
`risk_model=raw_stop_distance_column`.

`eval-formulas` and `select validate` use the same standard run-folder contract:

```text
runs/artifacts/
  forward-test/
    <target>/
      <dataset-id>/
        <cutoff>/
          <run-id>/
            command.txt
            command.json
            run_manifest.json
            checksums.sha256
            barsmith.log
            candidate_formulas.txt
            formula_export_manifest.json
            formula_results.csv
            formula_results.json
            selection_report.json
            selection_decisions.csv
            selected_formulas.txt
            protocol_validation.json
            overfit_report.json
            overfit_decisions.csv
            stress_report.json
            stress_matrix.csv
            frs_summary.csv
            frs_windows.csv
            equity_curves.csv
            plots/
              equity_curves.png
            reports/
              summary.md
              selection.md
              overfit.md
              stress.md
              lockbox.md
```

The matching registry record is written under
`runs/registry/forward-test/<target>/<dataset-id>/<cutoff>/<run-id>.json`.
Registry records hash formulas instead of embedding the formula text. Strict
selection records include `workflow_status` so validation and lockbox evidence
can be audited without reading terminal output.
Forward-test manifests and registry records also include the effective
stop-distance column and risk model, so validation results can be compared
against discovery runs without relying on terminal output.

Common forward-test outputs are:

- formula result CSV: pre/post rankings and strategy metrics
- formula result JSON: full structured report for downstream tools
- selection report JSON: selected candidate, policy, warnings, and per-candidate decisions
- selection decisions CSV: one row per pre-ranked candidate with pass/fail reasons
- selected formulas text: a one-formula ranked file for downstream lockbox runs
- protocol validation JSON: strict protocol and provenance decision details
- overfit report JSON/CSV/Markdown: effective trials, PBO/CSCV, PSR, DSR, and warnings
- stress report JSON/CSV/Markdown: cost, slippage, and sizing stress scenarios with pass/fail status
- candidate formulas text: source discovery candidates exported by `select validate`
- formula export manifest: protocol-bound provenance plus source search accounting
- selection markdown: human-readable selection summary under `reports/selection.md`
- FRS summary CSV: one row per formula and FRS scope
- FRS window CSV: annual/window components used by FRS
- equity-curve CSV: selected trades and cumulative equity for top formulas
- PNG plots: optional equity-curve images rendered by Rust

## What gets stored

Barsmith evaluates every enumerated combination, but only persists combinations that meet storage thresholds:

- `--min-samples` (minimum trade/sample count)
- `--max-drawdown` (max drawdown ceiling)

This keeps run folders smaller and reporting faster.

New result batches include `total_return` and `calmar_r` so future audits can
rank by total R without relying on terminal logs or ambiguous Calmar fields.

## Querying with DuckDB

Prefer the Rust CLI for routine top-result queries:

```bash
barsmith_cli results \
  --output-dir runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real \
  --direction long \
  --target next_bar_color_and_wicks \
  --min-samples 1000 \
  --rank-by total-return \
  --limit 20
```

Add `--export-formulas formulas.txt` to write the query as a ranked formula file
for `eval-formulas`. Use a discovery/pre-only source run when the exported file
will feed a holdout confirmation. The export includes comment metadata and a
research note; `eval-formulas` ignores those comments. The command also writes
`formula_export_manifest.json` for strict protocol validation. Manifest schema
version `2` uses `source_output_dir_path_sha256` to make clear that this value
hashes the source run folder path string, not directory contents.

For certification-style research, prefer `barsmith_cli select validate`; it
performs the export and strict evaluation in one auditable run folder.

You can also query `cumulative.duckdb` with DuckDB’s CLI:

```bash
duckdb runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real/cumulative.duckdb "SELECT combination, total_bars, calmar_ratio, max_drawdown FROM results ORDER BY calmar_ratio DESC LIMIT 20"
```

Useful queries:

```bash
# Count stored combinations
duckdb runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real/cumulative.duckdb "SELECT COUNT(*) AS n FROM results"

# Best combos with minimum sample size
duckdb runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real/cumulative.duckdb "SELECT combination, total_bars, calmar_ratio FROM results WHERE total_bars >= 1000 ORDER BY calmar_ratio DESC LIMIT 50"
```

Note: the exact schema is versioned by code and may evolve (this repo is unstable). Prefer inspecting columns via:

```bash
duckdb runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real/cumulative.duckdb "DESCRIBE results"
```

## Resume metadata

`run_manifest.json` binds a run folder to the CSV fingerprint and resume-sensitive configuration. The DuckDB database stores the matching resume offset used to continue enumeration without restarting from zero.

If you delete Parquet parts manually but keep the metadata, Barsmith may warn that resume offsets exist without corresponding stored parts.
