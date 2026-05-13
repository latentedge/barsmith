# Outputs and querying

Barsmith writes incremental Parquet batches plus a DuckDB catalog for querying “top results”.

## Files

In `--output-dir`:

- `barsmith_prepared.csv`: engineered dataset used for the run
- `run_manifest.json`: resume identity for the output directory
- `command.txt`: shell-quoted command used to launch the run
- `command.json`: structured command metadata, run ID, dataset ID, Git SHA, and artifact URI
- `results_parquet/part-*.parquet`: stored result rows (only combinations that pass storage filters)
- `cumulative.duckdb`: DuckDB database that exposes a `results` view over all Parquet parts
- `barsmith.log`: file log (unless disabled)
- `checksums.sha256`: checksums for small audit metadata; pass `--checksum-artifacts` to include heavy run files
- `reports/summary.md`: human-readable closeout summary for the run

For standardized combination-search folders, prefer `--runs-root` instead of a
hand-built `--output-dir`:

```text
<runs-root>/
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

Use `--registry-dir runs/registry` to write a lightweight Git-trackable registry
record at `comb/<target>/<direction>/<dataset-id>/<run-id>.json`. Registry
records include IDs, Git SHA, command hash, portable run path, artifact URI, and
best-Calmar and best-total-R metrics. They store a formula hash rather than
formula text so private formulas do not need to be committed.
Non-finite metrics are written explicitly as strings such as `Inf`, `-Inf`, or
`NaN` rather than being silently converted to JSON `null`.

`eval-formulas` can also use the standard run-folder contract:

```text
<runs-root>/
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
            formula_results.csv
            formula_results.json
            frs_summary.csv
            frs_windows.csv
            equity_curves.csv
            plots/
              equity_curves.png
            reports/
              summary.md
```

The matching registry record is written at
`forward-test/<target>/<dataset-id>/<cutoff>/<run-id>.json`. Registry records
hash formulas instead of embedding the formula text.

Common forward-test outputs are:

- formula result CSV: pre/post rankings and strategy metrics
- formula result JSON: full structured report for downstream tools
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

`run_manifest.json` binds an output directory to the CSV fingerprint and resume-sensitive configuration. The DuckDB database stores the matching resume offset used to continue enumeration without restarting from zero.

If you delete Parquet parts manually but keep the metadata, Barsmith may warn that resume offsets exist without corresponding stored parts.
