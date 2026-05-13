# Barsmith

High-performance feature combination search over engineered OHLCV bars.

WARNING: This repository is currently unstable. Use at your own risk.

Barsmith is a Rust research tool for exploring large feature-combination spaces
over market bar data. It is built for long local runs where deterministic
enumeration, safe resume, and predictable throughput matter more than UI polish.

Barsmith is designed for long-running, resume-friendly exploration runs:
- Load a CSV dataset, engineer features, and search combinations up to a configurable depth.
- Persist results incrementally (Parquet + DuckDB) so you can stop/restart without losing progress.
- Keep memory usage predictable via shared columnar data and reuse of intermediate masks.

This repository is self-contained and does not depend on any external Python code.

## Status and support boundary

Unstable. APIs, CLI flags, and output schemas may change without notice.

Currently supported by the default CLI:
- OHLCV CSV input.
- AND-only feature combinations.
- Builtin targets: `next_bar_up`, `next_bar_down`, and `next_bar_color_and_wicks`.
- Local filesystem outputs, with optional `aws s3 cp` uploads.

Not supported by the default CLI:
- OR/mixed combination logic.
- Broker connectivity, order routing, or live execution.
- Arbitrary custom targets unless they are provided through a prepared dataset or custom engine.

## Key features

- **Combination search (`comb`)**: AND-only feature logic, depth limits, min-sample gating, optional feature-pairs, resume offsets.
- **Resume & durability**: incremental Parquet parts + a DuckDB view for fast `top results` queries.
- **Deterministic runs**: stable CSV fingerprinting and run identity manifests to prevent accidental resume on incompatible inputs or settings.
- **Research-friendly runs**: deterministic dataset fingerprinting + resume metadata to protect long experiments.
- **Optional S3 upload**: upload outputs using `aws s3 cp` (requires AWS CLI on PATH).

## Quickstart

Requires Rust (stable).

Run a small dry-run on the included sample dataset:

```bash
cargo run -p barsmith_cli -- comb \
  --csv tests/data/ohlcv_tiny.csv \
  --direction long \
  --target next_bar_color_and_wicks \
  --output-dir ./tmp/run_01_dry \
  --max-depth 3 \
  --min-samples 100 \
  --workers 1 \
  --max-combos 1000 \
  --dry-run
```

Run a real (small) exploration run:

```bash
cargo run -p barsmith_cli -- comb \
  --csv tests/data/ohlcv_tiny.csv \
  --direction long \
  --target next_bar_color_and_wicks \
  --output-dir ./tmp/run_01 \
  --max-depth 3 \
  --min-samples 100 \
  --workers 1 \
  --max-combos 10000 \
  --force
```

### Install (local)

Install the CLI binary from source:

```bash
cargo install --path barsmith_cli
```

Then run it as:

```bash
barsmith_cli --help
barsmith_cli comb --help
```

## Outputs (what gets written)

`--output-dir` becomes a durable “run folder”. You’ll typically see:

- `barsmith_prepared.csv` (engineered dataset produced for the run)
- `run_manifest.json` (run identity used to validate safe resume)
- `results_parquet/part-*.parquet` (incremental result batches)
- `cumulative.duckdb` (DuckDB catalog and views over all batches)
- `barsmith.log` (unless `--no-file-log` is set)

Barsmith can resume from an existing output directory only when the run manifest matches the current CSV fingerprint and resume-sensitive settings. Increasing `--max-depth` is allowed because the deterministic enumeration stream extends the already processed prefix. Use `--force` or a fresh `--output-dir` for an incompatible run. When reusing an existing `--output-dir`, pass `--ack-new-df` so the CLI can overwrite `barsmith_prepared.csv`.

## How `comb` works

```mermaid
flowchart TD
  A["CLI: barsmith comb"] --> B["Parse args (clap)"]
  B --> C["CombArgs::into_config() -> Config"]
  C --> D["barsmith_builtin::run_builtin_pipeline_with_options"]

  subgraph BuiltinEngine["Builtin engine (barsmith_builtin)"]
    D --> E["Prepare engineered dataset"]
    E --> E1["Read source CSV (OHLCV)"]
    E1 --> E2["Engineer features + label targets"]
    E2 --> E3["Write output-dir/barsmith_prepared.csv"]
    E3 --> F["Build feature catalog"]
    F --> F1["Load prepared CSV (ColumnarData)"]
    F1 --> F2["Build FeatureDescriptors + ComparisonSpecs"]
  end

  F2 --> G["PermutationPipeline::new (computes catalog_hash)"]

  subgraph Pipeline["Core pipeline (barsmith_rs)"]
    G --> H["PermutationPipeline::run"]
    H --> I["Load prepared CSV -> optional date filter -> prune required columns"]
    I --> J{"--dry-run?"}
    J -- yes --> J1["Log catalog summary and exit"]
    J -- no --> K["Init EvaluationContext (target, rewards, eligible, exit indices)"]
    K --> L["Prune constant/duplicate/under-min comparisons"]
    L --> M["Build shared bitset catalog"]
    M --> N["Open CumulativeStore (DuckDB + Parquet) and get resume offset"]
    N --> O["Spawn writer thread"]
    O --> P["Enumerate next batch (IndexCombinationBatcher)"]
    P --> Q["Prune flags (subset cache + scalar-bound structure + optional --require-any-features)"]
    Q --> R["Parallel evaluate combinations (masks + stats)"]
    R --> S["Keep only combos meeting storage filters (--min-samples, --max-drawdown)"]
    S --> T["Send StoreMsg::Ingest to writer"]
    T --> U["Writer: write results_parquet/part-*.parquet + update DuckDB metadata (+ optional S3 upload)"]
    U --> P
    P -->|no more combos or limit hit| V["Send StoreMsg::Flush and wait for writer"]
    V --> W["Final report: DuckDB top_results (if enabled)"]
  end
```

### Prepared dataset requirements (what the pipeline expects)

The core pipeline (`barsmith_rs`) expects a “prepared” dataset that contains:

- `target` as a boolean column (name is whatever you pass via `--target`)
- `rr_<target>` (optional) as a float reward per row (R units)
- `<target>_eligible` (optional) as a boolean gate for trade eligibility
- `<target>_exit_i` (required when `--stacking-mode no-stacking`) as an integer next index to jump to after a trade

The default CLI generates this prepared dataset via `barsmith_builtin` and writes it to `output-dir/barsmith_prepared.csv`.

Note: the built-in CLI always writes `barsmith_prepared.csv`. If that file already exists in `--output-dir`, you must pass `--ack-new-df` to overwrite it.

## Data format

Input must be a CSV with (at minimum) these columns:

- `timestamp` (ISO-8601; UTC recommended)
- `open`, `high`, `low`, `close` (numeric)
- `volume` (numeric)

The default CLI uses `barsmith_builtin` for feature engineering + target labeling, and currently supports:

- `--target next_bar_up` (boolean)
- `--target next_bar_down` (boolean)
- `--target next_bar_color_and_wicks` (compatibility alias for `next_bar_up`)

For richer feature sets/targets, use `barsmith_rs` as a library and provide your own “prepared dataset” (see the requirements above), or treat `custom_rs` as an example engine.

## Validation

Common local gate:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo doc --workspace --no-deps --all-features
scripts/golden_smoke.sh
scripts/benchmark_smoke.sh
```

For the full testing matrix, benchmark fixture tiers, and review expectations,
see `docs/testing.md`, `docs/performance.md`, and `docs/review-checklist.md`.

## Docs

- `docs/README.md` (index)
- `docs/quickstart.md`
- `docs/data-contract.md`
- `docs/cli.md`
- `docs/runs.md`
- `docs/outputs.md`
- `docs/architecture.md`
- `docs/engines.md`
- `docs/performance.md`
- `docs/unsafe.md`
- `docs/testing.md`
- `docs/migration.md`
- `docs/stability.md`
- `docs/review-checklist.md`
- `docs/troubleshooting.md`
- `docs/development.md`

## CLI reference

List commands:

```bash
cargo run -p barsmith_cli -- --help
```

Search combinations:

```bash
cargo run -p barsmith_cli -- comb --help
```

### S3 upload

`--s3-output s3://bucket/prefix` enables upload targets and `--s3-upload-each-batch` uploads after every ingestion batch.

This uses the AWS CLI (`aws s3 cp`) and does not embed AWS credentials logic in Barsmith.

## Performance

Internal benchmark (not a guarantee): on a MacBook Pro (Apple M4), Barsmith explored ~120B combination candidates over ~5 days.

Performance depends heavily on:
- feature catalog size and depth
- `--stats-detail` (`core` vs `full`)
- combination depth and AND-mask selectivity
- storage filters (`--min-samples`, `--max-drawdown`, reporting thresholds)
- CPU and memory bandwidth

## Sample long-run command (macOS)

This is an example command shape for a long local run with the default builtin target. It is macOS-specific (`caffeinate`) and this repo is marked unstable.

```bash
caffeinate -dimsu cargo run --release -p barsmith_cli -- comb --csv ../es_30m.csv --direction short --target next_bar_color_and_wicks --output-dir ../tmp/barsmith/next_bar_color_and_wicks/es_30m_short_no_stacking --max-depth 5 --min-samples 4000 --date-end 2024-12-31 --feature-pairs --auto-batch --batch-size 8000000 --stats-detail core --report formula --top-k 10000 --max-drawdown 25 --max-drawdown-report 25 --min-calmar-report 1.0 --subset-pruning --asset MES --profile-eval off
```

## Project layout

- `barsmith_rs/`: core library (data loading, combination enumeration, evaluation, storage).
- `barsmith_builtin/`: minimal built-in feature engineering + targets (used by the default CLI).
- `barsmith_cli/`: CLI (`comb`).
- `custom_rs/`: richer example feature engineering + targets (not required for the default CLI; treated as an example/experimental engine).
- `tests/`: repository-level fixtures used by smoke tests.
- `benchmarks/`: benchmark fixture manifest docs and local smoke commands.
- `docs/`: user, contributor, architecture, testing, and migration docs.

## Open-source hygiene

This repo is private-first today, but it includes the usual open-source hygiene files:

- `LICENSE` (MIT)
- `SECURITY.md`
- `CONTRIBUTING.md`
- `CODE_OF_CONDUCT.md`
- crate metadata (`license`, `repository`, `rust-version`, etc.) in `Cargo.toml`
- `CHANGELOG.md`
- `rust-toolchain.toml` (pinned toolchain for consistent `rustfmt`/`clippy`)

## License

MIT — see `LICENSE`.

## Disclaimer

Barsmith is a research tool. It does not connect to brokers, place orders, or provide financial advice.
