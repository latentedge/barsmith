# CLI

Barsmith’s default CLI is `barsmith_cli`. The supported workflows are Rust-native:

- `comb`: generate an engineered dataset and evaluate feature combinations.
- `eval-formulas`: evaluate a ranked formula file against an existing `barsmith_prepared.csv`.
- `results`: query a cumulative Barsmith run folder without opening DuckDB manually.

## Help

```bash
barsmith_cli --help
barsmith_cli comb --help
barsmith_cli eval-formulas --help
barsmith_cli results --help
```

## `comb`

Use `comb` when you want Barsmith to engineer features, enumerate combinations, and write a durable run folder.

### Inputs / outputs

- `--csv <FILE>`: raw OHLCV CSV input
- `--output-dir <DIR>`: explicit run folder for legacy-compatible layouts
- `--runs-root <DIR>`: build the standard run folder as `<runs-root>/comb/<target>/<direction>/<dataset-id>/<run-id>/`
- `--dataset-id <ID>`: dataset label for standard output paths; defaults to the input CSV stem
- `--run-id <ID>` / `--run-slug <TEXT>`: control the standard run folder name
- `--registry-dir <DIR>`: write a lightweight audit registry JSON record under `comb/<target>/<direction>/<dataset-id>/<run-id>.json`
- `--artifact-uri <URI>`: durable storage location recorded in command and registry metadata
- `--checksum-artifacts`: include Parquet, DuckDB, and log files in `checksums.sha256`
- `--target <NAME>`: target identifier (builtin engine supports: `next_bar_up`, `next_bar_down`, `next_bar_color_and_wicks`)
- `--direction long|short|both`: filter which side is evaluated

For new research runs, prefer `--runs-root` plus `--registry-dir`:

```bash
barsmith_cli comb \
  --csv ../es_30m.csv \
  --target 2x_atr_tp_atr_stop \
  --direction long \
  --runs-root runs/artifacts \
  --dataset-id es_30m_official_v2 \
  --run-slug no_stacking \
  --registry-dir runs/registry
```

The generated registry record is safe for Git by design: it stores a portable
run path, formula hash, and metrics, not local artifact paths, raw formula text,
or run artifacts.

### Enumeration

- `--max-depth <N>`: maximum number of predicates per combination
- Combination predicates are evaluated with AND logic. There is no `--logic` flag.
- `--resume-from <OFFSET>`: resume offset in the global enumeration stream
- `--max-combos <N>`: stop after evaluating up to N combinations (useful for sampling / smoke runs)
- `--batch-size <N>`: combinations per batch (evaluation is parallel within a batch)
- `--auto-batch`: adapt batch size based on recent timings
- `--subset-pruning`: prune higher-depth combinations using under-min depth-2 “dead pairs”
- `--require-any-features <comma,list>`: only evaluate combinations that include at least one named feature (enumeration still proceeds)

### Evaluation / storage filters

- `--min-samples <N>`: combos below this sample threshold are evaluated but not persisted
- `--max-drawdown <R>`: combos with drawdown above this are not persisted
- `--stacking-mode stacking|no-stacking`:
  - `stacking`: every mask hit is treated as an independent sample
  - `no-stacking`: enforces one open trade at a time using `<target>_exit_i`

### Reporting

- `--report full|formula|top10|top100|off`
- `--top-k <N>`: size of the final report table (when reporting is enabled)
- `--max-drawdown-report <R>` / `--min-calmar-report <X>`: reporting-only query filters

### Performance

- `--workers <N>`: number of worker threads (omit to use all cores)
- `--stats-detail core|full`: compute cheaper “core” metrics vs full metrics
- `--profile-eval off|coarse|fine`: enable timing instrumentation

### Resume / overwrite knobs

- `--force`: clears existing cumulative outputs under `--output-dir` (DuckDB + Parquet batches) and starts fresh
- `--ack-new-df`: overwrite an existing `output-dir/barsmith_prepared.csv` (the builtin CLI always writes this file)

### S3 upload

- `--s3-output s3://bucket/prefix`
- `--s3-upload-each-batch`

This uses `aws s3 cp` (AWS CLI) and does not embed AWS credential logic inside Barsmith.

### Costs / sizing (optional)

Barsmith can model costs and contract sizing when you provide `--asset` and choose a sizing mode.

Start with:

- `--position-sizing fractional` (default)
- `--asset <CODE>` (e.g. `ES`, `MES`) to load tick/point value defaults

See `barsmith_cli comb --help` for all sizing/cost knobs.

## `eval-formulas`

Use `eval-formulas` when you already have a `barsmith_prepared.csv` and a ranked formula list. This replaces the legacy external script workflow with the same evaluator semantics Barsmith uses internally.

Example:

```bash
barsmith_cli eval-formulas \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/<run-id>/barsmith_prepared.csv \
  --formulas formulas.txt \
  --cutoff 2024-12-31 \
  --asset MES \
  --target 2x_atr_tp_atr_stop \
  --stacking-mode no-stacking \
  --position-sizing contracts \
  --runs-root runs/artifacts \
  --dataset-id es_30m_official_v2 \
  --run-slug contracts_forward \
  --registry-dir runs/registry \
  --plot \
  --plot-mode combined
```

Formula files support one expression per line:

```text
Rank 1: is_kf_positive_surprise && rsi_7>40.0 && close<high
Rank 2: trend_flag && atr>=1.25
```

Supported clause forms:

- Boolean flags: `feature_name`
- Feature vs constant: `feature > 1.25`
- Feature vs feature: `close < high`
- Operators: `>`, `<`, `>=`, `<=`, `=`, `==`, `!=`
- Clause separator: `&&`

Unsupported formula syntax should be removed or translated before running. The evaluator intentionally supports AND-only formulas.

Important flags:

- `--prepared <FILE>`: existing prepared dataset.
- `--formulas <FILE>`: ranked formula file.
- `--output-dir <DIR>`: explicit forward-test run folder.
- `--runs-root <DIR>`: build the standard forward-test folder as `<runs-root>/forward-test/<target>/<dataset-id>/<cutoff>/<run-id>/`.
- `--dataset-id <ID>`: dataset label for standard output paths.
- `--run-id <ID>` / `--run-slug <TEXT>`: control the standard run folder name.
- `--registry-dir <DIR>`: write a lightweight audit registry JSON record under `forward-test/<target>/<dataset-id>/<cutoff>/<run-id>.json`.
- `--artifact-uri <URI>`: durable storage location recorded in command and registry metadata.
- `--checksum-artifacts`: include generated CSV, JSON, and plot files in `checksums.sha256`.
- `--cutoff YYYY-MM-DD`: pre window is `<= cutoff`; post window is `> cutoff`.
- `--target <NAME>`: target column name; RR defaults to `rr_<target>`.
- `--rr-column <NAME>`: override reward/RR column when needed.
- `--stacking-mode stacking|no-stacking`: same semantics as `comb`.
- `--rank-by frs|calmar-equity`: post-window ranking metric.
- `--no-frs`: skip Forward Robustness Score.
- `--frs-scope window|pre|post|all`: calendar windows used for FRS.
- `--position-sizing fractional|contracts`: equity simulation mode. Defaults to `fractional`.
- `--asset <CODE>`: loads tick value, point value, margin, commission, and default slippage for known assets.
- `--plot`: render PNG equity-curve plots from exported curve rows.

When `--output-dir` or `--runs-root` is present, `eval-formulas` defaults these
outputs into the run folder unless you override them explicitly:

- `formula_results.csv`
- `formula_results.json`
- `frs_summary.csv`
- `frs_windows.csv`
- `equity_curves.csv`
- `plots/equity_curves.png` for combined plots, or `plots/` for individual plots
- `reports/summary.md`
- `command.txt`, `command.json`, `run_manifest.json`, and `checksums.sha256`

Contract sizing requires `--asset` and a stop-distance column. ATR-stop targets infer `--stop-distance-column atr`; other targets must provide the column explicitly.

## `results`

Use `results` to query a completed `comb` output directory from Rust:

```bash
barsmith_cli results \
  --output-dir runs/artifacts/comb/next_bar_color_and_wicks/long/tiny_sample/quickstart_real \
  --direction long \
  --target next_bar_color_and_wicks \
  --min-samples 500 \
  --max-drawdown 30 \
  --rank-by total-return \
  --limit 20
```

This command reads `cumulative.duckdb` plus `results_parquet/` and prints the top combinations by `calmar-ratio` or `total-return`.
