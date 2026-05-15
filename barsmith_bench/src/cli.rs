use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "barsmith-bench",
    about = "Run and compare Barsmith performance benchmarks"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run a benchmark suite and write structured JSON.
    Run(RunArgs),
    /// Compare two benchmark JSON reports and enforce regression budgets.
    Compare(CompareArgs),
}

#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    /// Suite to run: smoke, all, combinator, comb-eval, comb-depth5,
    /// target-generation, bitset, stats, comb-cli, results-cli,
    /// strict-eval, or select-validate.
    /// target-generation requires --features target-generation.
    #[arg(long = "suite", default_value = "smoke")]
    pub suite: String,

    /// Timed samples per benchmark.
    #[arg(long = "samples", default_value_t = 21)]
    pub samples: usize,

    /// Untimed warmup samples per benchmark.
    #[arg(long = "warmups", default_value_t = 5)]
    pub warmups: usize,

    /// Output JSON path.
    #[arg(long = "out", value_hint = clap::ValueHint::FilePath)]
    pub out: Option<PathBuf>,

    /// Label for the Cargo/profile settings under test.
    #[arg(long = "profile-label", default_value = "release-native")]
    pub profile_label: String,

    /// Fixture CSV used by CLI benchmark suites.
    #[arg(long = "fixture-csv", default_value = "tests/data/ohlcv_tiny.csv")]
    pub fixture_csv: PathBuf,

    /// Working directory for generated benchmark run folders.
    #[arg(long = "work-dir", default_value = "target/barsmith-bench/work")]
    pub work_dir: PathBuf,

    /// Existing barsmith_cli binary for CLI suites. If omitted, uses target/release/barsmith_cli.
    #[arg(long = "barsmith-bin", value_hint = clap::ValueHint::FilePath)]
    pub barsmith_bin: Option<PathBuf>,

    #[arg(long = "max-depth", default_value_t = 2)]
    pub max_depth: usize,

    #[arg(long = "min-samples", default_value_t = 25)]
    pub min_samples: usize,

    #[arg(long = "max-combos", default_value_t = 200)]
    pub max_combos: usize,

    #[arg(long = "batch-size", default_value_t = 200)]
    pub batch_size: usize,

    #[arg(long = "workers", default_value_t = 1)]
    pub workers: usize,

    /// Median regression budget recorded in the benchmark report.
    #[arg(long = "median-budget-pct", default_value_t = 3.0)]
    pub median_budget_pct: f64,

    /// p95 regression budget recorded in the benchmark report.
    #[arg(long = "p95-budget-pct", default_value_t = 5.0)]
    pub p95_budget_pct: f64,
}

#[derive(Args, Debug)]
pub struct CompareArgs {
    #[arg(long = "baseline", value_hint = clap::ValueHint::FilePath)]
    pub baseline: PathBuf,

    #[arg(long = "candidate", value_hint = clap::ValueHint::FilePath)]
    pub candidate: PathBuf,

    #[arg(long = "out", value_hint = clap::ValueHint::FilePath)]
    pub out: Option<PathBuf>,

    #[arg(long = "markdown-out", value_hint = clap::ValueHint::FilePath)]
    pub markdown_out: Option<PathBuf>,

    #[arg(long = "median-budget-pct", default_value_t = 3.0)]
    pub median_budget_pct: f64,

    #[arg(long = "p95-budget-pct", default_value_t = 5.0)]
    pub p95_budget_pct: f64,

    #[arg(long = "fail-on-regression", default_value_t = false)]
    pub fail_on_regression: bool,
}
