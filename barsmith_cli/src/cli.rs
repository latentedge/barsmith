use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand, ValueEnum};

use crate::stats_detail::StatsDetailValue;
use barsmith_rs::asset::find_asset;
use barsmith_rs::config::{
    Config, Direction, EvalProfileMode, PositionSizingMode, ReportMetricsMode, StackingMode,
    StopDistanceUnit,
};
use barsmith_rs::formula_eval::{EquityCurveWindowSelection, FrsScope, RankBy};
use barsmith_rs::protocol::ResearchStage;
use barsmith_rs::selection::{SelectionMode, SelectionPreset};

pub const DEFAULT_CAPITAL_DOLLAR: f64 = 100_000.0;
pub const DEFAULT_RISK_PCT_PER_TRADE: f64 = 1.0;
pub const DEFAULT_RUNS_ROOT: &str = "runs/artifacts";
pub const DEFAULT_REGISTRY_DIR: &str = "runs/registry";

#[derive(Parser, Debug)]
#[command(
    name = "barsmith",
    about = "High-performance feature permutation explorer"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
// Clap builds this once at startup, so variant size is not on a hot path.
#[allow(clippy::large_enum_variant)]
pub enum Commands {
    /// Run feature combination search over an engineered dataset
    #[command(name = "comb")]
    Comb(CombArgs),
    /// Evaluate ranked formulas against an existing barsmith_prepared.csv
    #[command(name = "eval-formulas")]
    EvalFormulas(EvalFormulasArgs),
    /// Query a cumulative Barsmith result store
    #[command(name = "results")]
    Results(ResultsArgs),
    /// Create and inspect strict research protocol manifests
    #[command(subcommand, name = "protocol")]
    Protocol(ProtocolCommand),
    /// Strict choose-the-best workflow for validation and lockbox evidence
    #[command(subcommand, name = "select")]
    Select(SelectCommand),
}

#[derive(Subcommand, Debug)]
pub enum ProtocolCommand {
    /// Create a research_protocol.json file
    #[command(name = "init")]
    Init(Box<ProtocolInitArgs>),
    /// Validate a research protocol file
    #[command(name = "validate")]
    Validate(ProtocolValidateArgs),
    /// Print the important protocol fields
    #[command(name = "explain")]
    Explain(ProtocolValidateArgs),
}

#[derive(Subcommand, Debug)]
// These structs mirror performance-sensitive eval options, but parsing them is
// a startup-only concern.
#[allow(clippy::large_enum_variant)]
pub enum SelectCommand {
    /// Export discovery candidates and run strict holdout validation
    #[command(name = "validate")]
    Validate(Box<SelectValidateArgs>),
    /// Evaluate one frozen selected formula against lockbox/live-shadow data
    #[command(name = "lockbox")]
    Lockbox(Box<SelectLockboxArgs>),
    /// Explain the strict selection workflow for a protocol file
    #[command(name = "explain")]
    Explain(SelectExplainArgs),
}

#[derive(Parser, Debug)]
pub struct SelectExplainArgs {
    /// Path to research_protocol.json.
    #[arg(long = "protocol", value_hint = clap::ValueHint::FilePath)]
    pub protocol: PathBuf,
}

#[derive(Parser, Debug)]
pub struct SelectValidateArgs {
    /// Discovery/pre-only comb run folder containing cumulative.duckdb.
    #[arg(long = "source-output-dir", value_hint = clap::ValueHint::DirPath)]
    pub source_output_dir: PathBuf,

    /// Prepared CSV used for validation. Use a full prepared dataset when validating post rows.
    #[arg(long = "prepared", value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    pub prepared_path: PathBuf,

    #[arg(long, default_value = "2x_atr_tp_atr_stop")]
    pub target: String,

    #[arg(long, default_value = "long")]
    pub direction: DirectionValue,

    /// Cutoff date. Pre window is <= cutoff; post window is > cutoff.
    #[arg(long, default_value = "2024-12-31")]
    pub cutoff: String,

    /// Strict research protocol JSON.
    #[arg(long = "research-protocol", value_hint = clap::ValueHint::FilePath)]
    pub research_protocol: PathBuf,

    /// Policy preset for selection gates.
    #[arg(long = "preset", value_enum, default_value = "institutional")]
    pub preset: SelectionPresetValue,

    /// Print the export/evaluation plan without writing formulas or run artifacts.
    #[arg(long = "dry-run", default_value_t = false)]
    pub dry_run: bool,

    /// Source ranking metric used when exporting discovery candidates.
    #[arg(long = "source-rank-by", value_enum, default_value = "total-return")]
    pub source_rank_by: ResultRankByValue,

    #[arg(long = "min-samples", alias = "min-sample-size", default_value_t = 500)]
    pub min_samples: usize,

    #[arg(long = "min-win-rate", default_value_t = 0.0)]
    pub min_win_rate: f64,

    #[arg(long = "source-max-drawdown", default_value_t = 30.0)]
    pub source_max_drawdown: f64,

    #[arg(long = "source-min-calmar")]
    pub source_min_calmar: Option<f64>,

    #[arg(long = "candidate-top-k")]
    pub candidate_top_k: Option<usize>,

    #[arg(long = "pre-min-trades")]
    pub pre_min_trades: Option<usize>,

    #[arg(long = "post-min-trades")]
    pub post_min_trades: Option<usize>,

    #[arg(long = "post-warn-below-trades")]
    pub post_warn_below_trades: Option<usize>,

    #[arg(long = "pre-min-total-r")]
    pub pre_min_total_r: Option<f64>,

    #[arg(long = "post-min-total-r")]
    pub post_min_total_r: Option<f64>,

    #[arg(long = "pre-min-expectancy")]
    pub pre_min_expectancy: Option<f64>,

    #[arg(long = "post-min-expectancy")]
    pub post_min_expectancy: Option<f64>,

    #[arg(long = "max-drawdown-r")]
    pub selection_max_drawdown_r: Option<f64>,

    #[arg(long = "min-pre-frs")]
    pub min_pre_frs: Option<f64>,

    #[arg(long = "max-return-degradation")]
    pub max_return_degradation: Option<f64>,

    #[arg(long = "max-single-trade-contribution")]
    pub max_single_trade_contribution: Option<f64>,

    #[arg(long = "max-formula-depth")]
    pub max_formula_depth: Option<usize>,

    #[arg(long = "min-density-per-1000-bars")]
    pub min_density_per_1000_bars: Option<f64>,

    #[arg(long = "complexity-penalty", default_value_t = 0.0)]
    pub complexity_penalty: f64,

    #[arg(long = "embargo-bars", default_value_t = 0)]
    pub embargo_bars: usize,

    #[arg(long = "no-purge-cross-boundary-exits", default_value_t = false)]
    pub no_purge_cross_boundary_exits: bool,

    #[arg(long = "rank-by", value_enum, default_value = "frs")]
    pub rank_by: FormulaRankByValue,

    #[arg(long = "no-frs", default_value_t = false)]
    pub no_frs: bool,

    #[arg(
        long = "frs-scope",
        alias = "frs-period",
        value_enum,
        default_value = "all"
    )]
    pub frs_scope: FrsScopeValue,

    #[arg(long = "frs-nmin", default_value_t = 30)]
    pub frs_nmin: usize,

    #[arg(long = "cscv-blocks", default_value_t = 6)]
    pub cscv_blocks: usize,

    #[arg(long = "cscv-max-splits", default_value_t = 64)]
    pub cscv_max_splits: usize,

    #[arg(long = "overfit-candidate-top-k")]
    pub overfit_candidate_top_k: Option<usize>,

    #[arg(long = "max-pbo", default_value_t = 0.25)]
    pub max_pbo: f64,

    #[arg(long = "min-psr", default_value_t = 0.95)]
    pub min_psr: f64,

    #[arg(long = "min-dsr", default_value_t = 0.95)]
    pub min_dsr: f64,

    #[arg(long = "min-positive-window-ratio", default_value_t = 0.5)]
    pub min_positive_window_ratio: f64,

    #[arg(long = "effective-trials")]
    pub effective_trials: Option<usize>,

    #[arg(long = "stress-min-total-r", default_value_t = 0.0)]
    pub stress_min_total_r: f64,

    #[arg(long = "stress-min-expectancy", default_value_t = 0.0)]
    pub stress_min_expectancy: f64,

    #[arg(long = "report-top", default_value_t = 50)]
    pub report_top: usize,

    #[arg(long = "runs-root", value_hint = clap::ValueHint::DirPath, default_value = DEFAULT_RUNS_ROOT)]
    pub runs_root: PathBuf,

    #[arg(long = "dataset-id")]
    pub dataset_id: Option<String>,

    #[arg(long = "run-id")]
    pub run_id: Option<String>,

    #[arg(long = "run-slug")]
    pub run_slug: Option<String>,

    #[arg(long = "registry-dir", value_hint = clap::ValueHint::DirPath, default_value = DEFAULT_REGISTRY_DIR)]
    pub registry_dir: PathBuf,

    #[arg(long = "artifact-uri")]
    pub artifact_uri: Option<String>,

    #[arg(long = "checksum-artifacts", default_value_t = false)]
    pub checksum_artifacts: bool,

    #[arg(long = "no-file-log", default_value_t = false)]
    pub no_file_log: bool,

    #[arg(long = "rr-column")]
    pub rr_column: Option<String>,

    #[arg(long = "stacking-mode", value_enum, default_value = "no-stacking")]
    pub stacking_mode: StackingModeValue,

    #[arg(long = "equity-curves-top-k", default_value_t = 10)]
    pub equity_curves_top_k: usize,

    #[arg(long = "plot", default_value_t = false)]
    pub plot: bool,

    #[arg(long = "plot-mode", value_enum, default_value = "individual")]
    pub plot_mode: PlotModeValue,

    #[arg(long = "max-drawdown")]
    pub max_drawdown: Option<f64>,

    #[arg(long = "min-calmar")]
    pub min_calmar: Option<f64>,

    #[arg(long = "asset")]
    pub asset: Option<String>,

    #[arg(long = "position-sizing", value_enum, default_value = "fractional")]
    pub position_sizing: PositionSizingValue,

    #[arg(long = "stop-distance-column")]
    pub stop_distance_column: Option<String>,

    #[arg(long = "stop-distance-unit", value_enum, default_value = "points")]
    pub stop_distance_unit: StopDistanceUnitValue,

    #[arg(long = "min-contracts", default_value_t = 1)]
    pub min_contracts: usize,

    #[arg(long = "max-contracts")]
    pub max_contracts: Option<usize>,

    #[arg(long = "margin-per-contract-dollar")]
    pub margin_per_contract_dollar: Option<f64>,

    #[arg(long = "commission-per-trade-dollar")]
    pub commission_per_trade_dollar: Option<f64>,

    #[arg(long = "slippage-per-trade-dollar")]
    pub slippage_per_trade_dollar: Option<f64>,

    #[arg(long = "cost-per-trade-dollar")]
    pub cost_per_trade_dollar: Option<f64>,

    #[arg(long = "no-costs", default_value_t = false)]
    pub no_costs: bool,
}

#[derive(Parser, Debug)]
pub struct SelectLockboxArgs {
    #[arg(long = "prepared", value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    pub prepared_path: PathBuf,

    #[arg(long = "formulas", value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    pub formulas_path: PathBuf,

    #[arg(long, default_value = "2x_atr_tp_atr_stop")]
    pub target: String,

    #[arg(long = "cutoff", default_value = "2024-12-31")]
    pub cutoff: String,

    #[arg(long = "stage", value_enum, default_value = "lockbox")]
    pub stage: LockboxStageValue,

    #[arg(long = "research-protocol", value_hint = clap::ValueHint::FilePath)]
    pub research_protocol: PathBuf,

    #[arg(long = "formula-export-manifest", value_hint = clap::ValueHint::FilePath)]
    pub formula_export_manifest: PathBuf,

    #[arg(long = "ack-rerun-lockbox", default_value_t = false)]
    pub ack_rerun_lockbox: bool,

    #[arg(long = "rr-column")]
    pub rr_column: Option<String>,

    #[arg(long = "stacking-mode", value_enum, default_value = "no-stacking")]
    pub stacking_mode: StackingModeValue,

    #[arg(long = "report-top", default_value_t = 50)]
    pub report_top: usize,

    #[arg(long = "runs-root", value_hint = clap::ValueHint::DirPath, default_value = DEFAULT_RUNS_ROOT)]
    pub runs_root: PathBuf,

    #[arg(long = "dataset-id")]
    pub dataset_id: Option<String>,

    #[arg(long = "run-id")]
    pub run_id: Option<String>,

    #[arg(long = "run-slug")]
    pub run_slug: Option<String>,

    #[arg(long = "registry-dir", value_hint = clap::ValueHint::DirPath, default_value = DEFAULT_REGISTRY_DIR)]
    pub registry_dir: PathBuf,

    #[arg(long = "artifact-uri")]
    pub artifact_uri: Option<String>,

    #[arg(long = "checksum-artifacts", default_value_t = false)]
    pub checksum_artifacts: bool,

    #[arg(long = "no-file-log", default_value_t = false)]
    pub no_file_log: bool,

    #[arg(long = "rank-by", value_enum, default_value = "frs")]
    pub rank_by: FormulaRankByValue,

    #[arg(long = "no-frs", default_value_t = false)]
    pub no_frs: bool,

    #[arg(
        long = "frs-scope",
        alias = "frs-period",
        value_enum,
        default_value = "all"
    )]
    pub frs_scope: FrsScopeValue,

    #[arg(long = "frs-nmin", default_value_t = 30)]
    pub frs_nmin: usize,

    #[arg(long = "cscv-blocks", default_value_t = 6)]
    pub cscv_blocks: usize,

    #[arg(long = "cscv-max-splits", default_value_t = 64)]
    pub cscv_max_splits: usize,

    #[arg(long = "overfit-candidate-top-k", default_value_t = 1)]
    pub overfit_candidate_top_k: usize,

    #[arg(long = "max-pbo", default_value_t = 0.25)]
    pub max_pbo: f64,

    #[arg(long = "min-psr", default_value_t = 0.95)]
    pub min_psr: f64,

    #[arg(long = "min-dsr", default_value_t = 0.95)]
    pub min_dsr: f64,

    #[arg(long = "min-positive-window-ratio", default_value_t = 0.5)]
    pub min_positive_window_ratio: f64,

    #[arg(long = "effective-trials")]
    pub effective_trials: Option<usize>,

    #[arg(long = "stress-min-total-r", default_value_t = 0.0)]
    pub stress_min_total_r: f64,

    #[arg(long = "stress-min-expectancy", default_value_t = 0.0)]
    pub stress_min_expectancy: f64,

    #[arg(long = "equity-curves-top-k", default_value_t = 10)]
    pub equity_curves_top_k: usize,

    #[arg(long = "plot", default_value_t = false)]
    pub plot: bool,

    #[arg(long = "plot-mode", value_enum, default_value = "individual")]
    pub plot_mode: PlotModeValue,

    #[arg(long = "max-drawdown")]
    pub max_drawdown: Option<f64>,

    #[arg(long = "min-calmar")]
    pub min_calmar: Option<f64>,

    #[arg(long = "asset")]
    pub asset: Option<String>,

    #[arg(long = "position-sizing", value_enum, default_value = "fractional")]
    pub position_sizing: PositionSizingValue,

    #[arg(long = "stop-distance-column")]
    pub stop_distance_column: Option<String>,

    #[arg(long = "stop-distance-unit", value_enum, default_value = "points")]
    pub stop_distance_unit: StopDistanceUnitValue,

    #[arg(long = "min-contracts", default_value_t = 1)]
    pub min_contracts: usize,

    #[arg(long = "max-contracts")]
    pub max_contracts: Option<usize>,

    #[arg(long = "margin-per-contract-dollar")]
    pub margin_per_contract_dollar: Option<f64>,

    #[arg(long = "commission-per-trade-dollar")]
    pub commission_per_trade_dollar: Option<f64>,

    #[arg(long = "slippage-per-trade-dollar")]
    pub slippage_per_trade_dollar: Option<f64>,

    #[arg(long = "cost-per-trade-dollar")]
    pub cost_per_trade_dollar: Option<f64>,

    #[arg(long = "no-costs", default_value_t = false)]
    pub no_costs: bool,
}

#[derive(Parser, Debug)]
pub struct ProtocolInitArgs {
    /// Output path for research_protocol.json.
    #[arg(long = "output", value_hint = clap::ValueHint::FilePath)]
    pub output: PathBuf,

    /// Dataset identifier used by standard Barsmith run folders.
    #[arg(long = "dataset-id")]
    pub dataset_id: String,

    /// Target identifier.
    #[arg(long = "target")]
    pub target: String,

    /// Optional direction label for the discovery run.
    #[arg(long = "direction")]
    pub direction: Option<String>,

    /// Optional feature-engineering engine label.
    #[arg(long = "engine")]
    pub engine: Option<String>,

    #[arg(long = "discovery-start")]
    pub discovery_start: Option<String>,

    #[arg(long = "discovery-end")]
    pub discovery_end: Option<String>,

    #[arg(long = "validation-start")]
    pub validation_start: Option<String>,

    #[arg(long = "validation-end")]
    pub validation_end: Option<String>,

    #[arg(long = "lockbox-start")]
    pub lockbox_start: Option<String>,

    #[arg(long = "lockbox-end")]
    pub lockbox_end: Option<String>,

    #[arg(long = "candidate-top-k")]
    pub candidate_top_k: Option<usize>,
}

#[derive(Parser, Debug)]
pub struct ProtocolValidateArgs {
    /// Path to research_protocol.json.
    #[arg(long = "protocol", value_hint = clap::ValueHint::FilePath)]
    pub protocol: PathBuf,
}

#[derive(Parser, Debug)]
pub struct EvalFormulasArgs {
    /// Path to barsmith_prepared.csv
    #[arg(long = "prepared", value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    pub prepared_path: PathBuf,

    /// Ranked formula file. Supports lines like `Rank 1: a && b>1.0`.
    #[arg(long = "formulas", value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    pub formulas_path: PathBuf,

    /// Target column name in the prepared CSV
    #[arg(long, default_value = "2x_atr_tp_atr_stop")]
    pub target: String,

    /// Research stage for strict overfit-resistant workflows.
    #[arg(long = "stage", value_enum, default_value = "validation")]
    pub stage: ResearchStageValue,

    /// Research protocol JSON used by --strict-protocol.
    #[arg(long = "research-protocol", value_hint = clap::ValueHint::FilePath)]
    pub research_protocol: Option<PathBuf>,

    /// Enforce protocol, formula provenance, and stage-specific constraints.
    #[arg(long = "strict-protocol", default_value_t = false)]
    pub strict_protocol: bool,

    /// Formula export manifest produced by `results --export-formulas`.
    #[arg(
        long = "formula-export-manifest",
        value_hint = clap::ValueHint::FilePath
    )]
    pub formula_export_manifest: Option<PathBuf>,

    /// Acknowledge that this lockbox formula/protocol was evaluated before.
    #[arg(long = "ack-rerun-lockbox", default_value_t = false)]
    pub ack_rerun_lockbox: bool,

    /// Optional RR column override. Defaults to `rr_<target>`.
    #[arg(long = "rr-column")]
    pub rr_column: Option<String>,

    /// Trade stacking behavior for formula evaluation.
    #[arg(long = "stacking-mode", value_enum, default_value = "no-stacking")]
    pub stacking_mode: StackingModeValue,

    /// Cutoff date. Pre window is <= cutoff; post window is > cutoff.
    #[arg(long, default_value = "2024-12-31")]
    pub cutoff: String,

    /// Starting capital in USD for equity simulation.
    #[arg(long = "capital", default_value_t = DEFAULT_CAPITAL_DOLLAR)]
    pub capital: f64,

    /// Risk percentage per trade, applied to current equity.
    #[arg(long = "risk-pct-per-trade", default_value_t = DEFAULT_RISK_PCT_PER_TRADE)]
    pub risk_pct_per_trade: f64,

    /// Number of ranked formulas to print in each text report window. Use 0 for all.
    #[arg(long = "report-top", default_value_t = 50)]
    pub report_top: usize,

    /// Root directory for standardized forward-test run folders.
    ///
    /// The effective run folder becomes
    /// `<runs-root>/forward-test/<target>/<dataset-id>/<cutoff>/<run-id>/`.
    #[arg(
        long = "runs-root",
        value_hint = clap::ValueHint::DirPath,
        default_value = DEFAULT_RUNS_ROOT
    )]
    pub runs_root: PathBuf,

    /// Dataset identifier used in standardized output paths.
    ///
    /// Defaults to the prepared CSV file stem after path-safe normalization.
    #[arg(long = "dataset-id")]
    pub dataset_id: Option<String>,

    /// Stable run identifier used in standardized output paths and registry records.
    ///
    /// Defaults to `<UTC timestamp>_<git short sha>_<run slug>`.
    #[arg(long = "run-id")]
    pub run_id: Option<String>,

    /// Human-readable suffix used when Barsmith generates --run-id.
    #[arg(long = "run-slug")]
    pub run_slug: Option<String>,

    /// Directory for lightweight Git-trackable run registry records.
    #[arg(
        long = "registry-dir",
        value_hint = clap::ValueHint::DirPath,
        default_value = DEFAULT_REGISTRY_DIR
    )]
    pub registry_dir: PathBuf,

    /// Durable artifact URI for the full run folder, recorded in registry metadata.
    #[arg(long = "artifact-uri")]
    pub artifact_uri: Option<String>,

    /// Include generated CSV, JSON, and plot artifacts in checksums.sha256.
    ///
    /// By default Barsmith hashes only small metadata files so closeout stays cheap.
    #[arg(long = "checksum-artifacts", default_value_t = false)]
    pub checksum_artifacts: bool,

    /// Disable writing barsmith.log into the run folder. When set,
    /// logs are only emitted to stdout/stderr.
    #[arg(long = "no-file-log", default_value_t = false)]
    pub no_file_log: bool,

    /// Optional full result CSV output path.
    #[arg(long = "csv-out", value_hint = clap::ValueHint::FilePath)]
    pub csv_out: Option<PathBuf>,

    /// Optional JSON output path.
    #[arg(long = "json-out", value_hint = clap::ValueHint::FilePath)]
    pub json_out: Option<PathBuf>,

    /// Optional selection report JSON output path.
    #[arg(long = "selection-out", value_hint = clap::ValueHint::FilePath)]
    pub selection_out: Option<PathBuf>,

    /// Optional selection decisions CSV output path.
    #[arg(long = "selection-decisions-out", value_hint = clap::ValueHint::FilePath)]
    pub selection_decisions_out: Option<PathBuf>,

    /// Optional selected formula text output path.
    #[arg(long = "selected-formulas-out", value_hint = clap::ValueHint::FilePath)]
    pub selected_formulas_out: Option<PathBuf>,

    /// Optional strict-protocol validation JSON output path.
    #[arg(long = "protocol-validation-out", value_hint = clap::ValueHint::FilePath)]
    pub protocol_validation_out: Option<PathBuf>,

    /// Primary ranking metric for the post window.
    #[arg(long = "rank-by", value_enum, default_value = "frs")]
    pub rank_by: FormulaRankByValue,

    /// Selection protocol for choosing formulas after pre/post evaluation.
    #[arg(long = "selection-mode", value_enum, default_value = "holdout-confirm")]
    pub selection_mode: SelectionModeValue,

    /// Optional named gate preset recorded in selection reports.
    #[arg(long = "selection-preset", value_enum)]
    pub selection_preset: Option<SelectionPresetValue>,

    /// Number of pre-ranked formulas eligible for selection.
    #[arg(long = "candidate-top-k", default_value_t = 1_000)]
    pub candidate_top_k: usize,

    /// Minimum pre-window trades required for selection.
    #[arg(long = "pre-min-trades", default_value_t = 100)]
    pub pre_min_trades: usize,

    /// Minimum post-window trades required for holdout confirmation.
    #[arg(long = "post-min-trades", default_value_t = 30)]
    pub post_min_trades: usize,

    /// Emit a warning when post trades are below this floor.
    #[arg(long = "post-warn-below-trades", default_value_t = 50)]
    pub post_warn_below_trades: usize,

    #[arg(long = "pre-min-total-r", default_value_t = 0.0)]
    pub pre_min_total_r: f64,

    #[arg(long = "post-min-total-r", default_value_t = 0.0)]
    pub post_min_total_r: f64,

    #[arg(long = "pre-min-expectancy", default_value_t = 0.0)]
    pub pre_min_expectancy: f64,

    #[arg(long = "post-min-expectancy", default_value_t = 0.0)]
    pub post_min_expectancy: f64,

    /// Optional drawdown ceiling used by selection gates.
    #[arg(long = "max-drawdown-r")]
    pub selection_max_drawdown_r: Option<f64>,

    #[arg(long = "min-pre-frs", default_value_t = 0.0)]
    pub min_pre_frs: f64,

    #[arg(long = "max-return-degradation", default_value_t = 0.25)]
    pub max_return_degradation: f64,

    #[arg(long = "max-single-trade-contribution")]
    pub max_single_trade_contribution: Option<f64>,

    /// Reject selected candidates deeper than this formula depth.
    #[arg(long = "max-formula-depth")]
    pub max_formula_depth: Option<usize>,

    /// Reject selected candidates below this trade density.
    #[arg(long = "min-density-per-1000-bars")]
    pub min_density_per_1000_bars: Option<f64>,

    /// Penalize deeper formulas in overfit reports without changing raw metrics.
    #[arg(long = "complexity-penalty", default_value_t = 0.0)]
    pub complexity_penalty: f64,

    /// Bars to skip after the cutoff before post-window evaluation starts.
    #[arg(long = "embargo-bars", default_value_t = 0)]
    pub embargo_bars: usize,

    /// Disable purging rows whose trade exit crosses a window boundary.
    #[arg(long = "no-purge-cross-boundary-exits", default_value_t = false)]
    pub no_purge_cross_boundary_exits: bool,

    /// Disable Forward Robustness Score.
    #[arg(long = "no-frs", default_value_t = false)]
    pub no_frs: bool,

    /// FRS calendar-window scope.
    #[arg(
        long = "frs-scope",
        alias = "frs-period",
        value_enum,
        default_value = "all"
    )]
    pub frs_scope: FrsScopeValue,

    /// Trade-count floor for FRS trade score.
    #[arg(long = "frs-nmin", default_value_t = 30)]
    pub frs_nmin: usize,

    #[arg(long = "frs-alpha", default_value_t = 2.0)]
    pub frs_alpha: f64,

    #[arg(long = "frs-beta", default_value_t = 2.0)]
    pub frs_beta: f64,

    #[arg(long = "frs-gamma", default_value_t = 1.0)]
    pub frs_gamma: f64,

    #[arg(long = "frs-delta", default_value_t = 1.0)]
    pub frs_delta: f64,

    /// Optional FRS summary CSV output path.
    #[arg(long = "frs-out", value_hint = clap::ValueHint::FilePath)]
    pub frs_out: Option<PathBuf>,

    /// Optional FRS per-window CSV output path.
    #[arg(long = "frs-windows-out", value_hint = clap::ValueHint::FilePath)]
    pub frs_windows_out: Option<PathBuf>,

    /// Compute multiple-testing and CSCV/PBO diagnostics.
    #[arg(long = "overfit-report", default_value_t = false)]
    pub overfit_report: bool,

    /// Optional overfit report JSON output path.
    #[arg(long = "overfit-out", value_hint = clap::ValueHint::FilePath)]
    pub overfit_out: Option<PathBuf>,

    /// Optional overfit decisions CSV output path.
    #[arg(long = "overfit-decisions-out", value_hint = clap::ValueHint::FilePath)]
    pub overfit_decisions_out: Option<PathBuf>,

    /// Number of chronological CSCV blocks for PBO diagnostics.
    #[arg(long = "cscv-blocks", default_value_t = 6)]
    pub cscv_blocks: usize,

    /// Maximum CSCV splits to evaluate.
    #[arg(long = "cscv-max-splits", default_value_t = 64)]
    pub cscv_max_splits: usize,

    /// Candidate cap used by overfit diagnostics.
    #[arg(long = "overfit-candidate-top-k", default_value_t = 100)]
    pub overfit_candidate_top_k: usize,

    /// Fail overfit diagnostics when PBO exceeds this value.
    #[arg(long = "max-pbo", default_value_t = 0.25)]
    pub max_pbo: f64,

    /// Fail overfit diagnostics when PSR is below this value.
    #[arg(long = "min-psr", default_value_t = 0.95)]
    pub min_psr: f64,

    /// Fail overfit diagnostics when DSR is below this value.
    #[arg(long = "min-dsr", default_value_t = 0.95)]
    pub min_dsr: f64,

    /// Fail overfit diagnostics below this positive block ratio.
    #[arg(long = "min-positive-window-ratio", default_value_t = 0.5)]
    pub min_positive_window_ratio: f64,

    /// Override the effective trial count used by Deflated Sharpe.
    #[arg(long = "effective-trials")]
    pub effective_trials: Option<usize>,

    /// Compute execution stress diagnostics.
    #[arg(long = "stress-report", default_value_t = false)]
    pub stress_report: bool,

    /// Optional stress report JSON output path.
    #[arg(long = "stress-out", value_hint = clap::ValueHint::FilePath)]
    pub stress_out: Option<PathBuf>,

    /// Optional stress matrix CSV output path.
    #[arg(long = "stress-matrix-out", value_hint = clap::ValueHint::FilePath)]
    pub stress_matrix_out: Option<PathBuf>,

    /// Fail stress diagnostics when worst stressed Total R is below this value.
    #[arg(long = "stress-min-total-r", default_value_t = 0.0)]
    pub stress_min_total_r: f64,

    /// Fail stress diagnostics when worst stressed expectancy is below this value.
    #[arg(long = "stress-min-expectancy", default_value_t = 0.0)]
    pub stress_min_expectancy: f64,

    /// Export equity curves for the top K strategies. Use 0 to disable.
    #[arg(long = "equity-curves-top-k", default_value_t = 10)]
    pub equity_curves_top_k: usize,

    /// Which ranking list selects strategies for equity curve export.
    #[arg(long = "equity-curves-rank-by", value_enum, default_value = "post")]
    pub equity_curves_rank_by: EquityCurveRankByValue,

    /// Which window(s) to export equity curves for.
    #[arg(long = "equity-curves-window", value_enum, default_value = "both")]
    pub equity_curves_window: EquityCurveWindowValue,

    /// Optional equity curve CSV output path.
    #[arg(long = "equity-curves-out", value_hint = clap::ValueHint::FilePath)]
    pub equity_curves_out: Option<PathBuf>,

    /// Render plot images from equity curve rows.
    #[arg(long = "plot", default_value_t = false)]
    pub plot: bool,

    /// Plot mode.
    #[arg(long = "plot-mode", value_enum, default_value = "individual")]
    pub plot_mode: PlotModeValue,

    /// Output PNG path for combined plot.
    #[arg(long = "plot-out", value_hint = clap::ValueHint::FilePath)]
    pub plot_out: Option<PathBuf>,

    /// Output directory for individual plots.
    #[arg(long = "plot-dir", value_hint = clap::ValueHint::DirPath)]
    pub plot_dir: Option<PathBuf>,

    /// X-axis for plots.
    #[arg(long = "plot-x", value_enum, default_value = "timestamp")]
    pub plot_x: PlotXValue,

    /// Metric to plot.
    #[arg(long = "plot-metric", value_enum, default_value = "dollar")]
    pub plot_metric: PlotMetricValue,

    /// Optional R-space max drawdown filter applied to reported window results.
    #[arg(long = "max-drawdown")]
    pub max_drawdown: Option<f64>,

    /// Optional minimum equity Calmar filter applied to reported window results.
    #[arg(long = "min-calmar")]
    pub min_calmar: Option<f64>,

    /// Asset code for cost model and contracts sizing.
    #[arg(long = "asset")]
    pub asset: Option<String>,

    /// Position sizing mode for equity simulation.
    #[arg(long = "position-sizing", value_enum, default_value = "fractional")]
    pub position_sizing: PositionSizingValue,

    /// Per-trade stop distance column for contract sizing.
    #[arg(long = "stop-distance-column")]
    pub stop_distance_column: Option<String>,

    /// Unit for --stop-distance-column.
    #[arg(long = "stop-distance-unit", value_enum, default_value = "points")]
    pub stop_distance_unit: StopDistanceUnitValue,

    #[arg(long = "min-contracts", default_value_t = 1)]
    pub min_contracts: usize,

    #[arg(long = "max-contracts")]
    pub max_contracts: Option<usize>,

    #[arg(long = "margin-per-contract-dollar")]
    pub margin_per_contract_dollar: Option<f64>,

    #[arg(long = "commission-per-trade-dollar")]
    pub commission_per_trade_dollar: Option<f64>,

    #[arg(long = "slippage-per-trade-dollar")]
    pub slippage_per_trade_dollar: Option<f64>,

    #[arg(long = "cost-per-trade-dollar")]
    pub cost_per_trade_dollar: Option<f64>,

    /// Disable cost model entirely and keep raw R semantics.
    #[arg(long = "no-costs", default_value_t = false)]
    pub no_costs: bool,
}

#[derive(Parser, Debug)]
pub struct ResultsArgs {
    /// Barsmith run folder containing cumulative.duckdb and results_parquet/
    #[arg(long = "output-dir", value_hint = clap::ValueHint::DirPath)]
    pub output_dir: PathBuf,

    #[arg(long, default_value = "long")]
    pub direction: DirectionValue,

    #[arg(long, default_value = "next_bar_color_and_wicks")]
    pub target: String,

    #[arg(long = "min-samples", alias = "min-sample-size", default_value_t = 500)]
    pub min_samples: usize,

    #[arg(long = "min-win-rate", default_value_t = 0.0)]
    pub min_win_rate: f64,

    #[arg(long = "max-drawdown", default_value_t = 10_000.0)]
    pub max_drawdown: f64,

    #[arg(long = "min-calmar")]
    pub min_calmar: Option<f64>,

    #[arg(long = "rank-by", value_enum, default_value = "calmar-ratio")]
    pub rank_by: ResultRankByValue,

    #[arg(long, default_value_t = 20)]
    pub limit: usize,

    /// Write the query result as a ranked formula file for eval-formulas.
    #[arg(long = "export-formulas", value_hint = clap::ValueHint::FilePath)]
    pub export_formulas: Option<PathBuf>,

    /// Write formula-export provenance metadata to this path.
    #[arg(
        long = "export-formula-manifest",
        value_hint = clap::ValueHint::FilePath,
        requires = "export_formulas"
    )]
    pub export_formula_manifest: Option<PathBuf>,

    /// Optional research protocol to bind into the formula-export manifest.
    #[arg(
        long = "research-protocol",
        value_hint = clap::ValueHint::FilePath,
        requires = "export_formulas"
    )]
    pub research_protocol: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct CombArgs {
    /// Path to the input CSV file with OHLCV data
    #[arg(long = "csv", value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    pub csv_path: PathBuf,

    /// Direction to analyze (long/short/both)
    #[arg(long, default_value = "long")]
    pub direction: DirectionValue,

    /// Target generator identifier
    #[arg(long, default_value = "next_bar_color_and_wicks")]
    pub target: String,

    /// Feature-engineering engine. `auto` uses the builtin engine for simple
    /// next-bar targets and the custom engine for richer prepared targets.
    #[arg(long = "engine", value_enum, default_value = "auto")]
    pub engine: EngineValue,

    /// Root directory for standardized run folders.
    ///
    /// The effective run folder becomes
    /// `<runs-root>/comb/<target>/<direction>/<dataset-id>/<run-id>/`.
    #[arg(
        long = "runs-root",
        value_hint = clap::ValueHint::DirPath,
        default_value = DEFAULT_RUNS_ROOT
    )]
    pub runs_root: PathBuf,

    /// Dataset identifier used in standardized output paths.
    ///
    /// Defaults to the input CSV file stem after path-safe normalization.
    #[arg(long = "dataset-id")]
    pub dataset_id: Option<String>,

    /// Stable run identifier used in standardized output paths and registry records.
    ///
    /// Defaults to `<UTC timestamp>_<git short sha>_<run slug>`.
    #[arg(long = "run-id")]
    pub run_id: Option<String>,

    /// Human-readable suffix used when Barsmith generates --run-id.
    #[arg(long = "run-slug")]
    pub run_slug: Option<String>,

    /// Directory for lightweight Git-trackable run registry records.
    #[arg(
        long = "registry-dir",
        value_hint = clap::ValueHint::DirPath,
        default_value = DEFAULT_REGISTRY_DIR
    )]
    pub registry_dir: PathBuf,

    /// Durable artifact URI for the full run folder, recorded in registry metadata.
    #[arg(long = "artifact-uri")]
    pub artifact_uri: Option<String>,

    /// Include heavy artifacts such as Parquet parts, DuckDB, and logs in checksums.sha256.
    ///
    /// By default Barsmith hashes only small metadata files so closeout stays cheap.
    #[arg(long = "checksum-artifacts", default_value_t = false)]
    pub checksum_artifacts: bool,

    /// Optional S3 base URI to upload output artefacts to (e.g. s3://bucket/prefix).
    ///
    /// When combined with --s3-upload-each-batch, Barsmith will upload the
    /// newly produced Parquet part (and resume metadata) after every batch.
    #[arg(long = "s3-output", value_name = "S3_URI")]
    pub s3_output: Option<String>,

    /// Upload results to S3 after every batch is ingested.
    #[arg(long = "s3-upload-each-batch", default_value_t = false)]
    pub s3_upload_each_batch: bool,

    /// Maximum feature depth per combination
    #[arg(long = "max-depth", default_value_t = 3)]
    pub max_depth: usize,

    /// Minimum samples required for storing a combination in cumulative
    /// results. Combinations below this threshold are evaluated but not
    /// persisted.
    #[arg(long = "min-samples", alias = "min-sample-size", default_value_t = 100)]
    pub min_samples: usize,

    /// Minimum samples required for a combination to be considered
    /// "eligible" in reporting (top tables, overview). When omitted, this
    /// defaults to the value of --min-samples.
    #[arg(long = "min-samples-report")]
    pub min_samples_report: Option<usize>,

    /// Inclusive start date filter (YYYY-MM-DD)
    #[arg(long = "date-start")]
    pub date_start: Option<String>,

    /// Inclusive end date filter (YYYY-MM-DD)
    #[arg(long = "date-end")]
    pub date_end: Option<String>,

    /// Batch size for combination streaming
    #[arg(long = "batch-size", default_value_t = 20_000)]
    pub batch_size: usize,

    /// Enable adaptive batch sizing based on recent filter/eval timings
    #[arg(long = "auto-batch", default_value_t = false)]
    pub auto_batch: bool,

    /// Number of worker threads (omit to use all logical cores)
    #[arg(long = "workers", alias = "n-jobs")]
    pub workers: Option<usize>,

    /// Optional resume offset for combination enumeration
    #[arg(long = "resume-from", alias = "resume-offset", default_value_t = 0)]
    pub resume_from: u64,

    /// Optional cap on combinations to evaluate this run
    #[arg(long = "max-combos", alias = "limit")]
    pub max_combos: Option<usize>,

    /// Enable dry-run mode (emit plan only)
    #[arg(long = "dry-run", default_value_t = false)]
    pub dry_run: bool,

    /// Reduce log noise (suppresses catalog/resume summaries)
    #[arg(long = "quiet", default_value_t = false)]
    pub quiet: bool,

    /// Control final metrics report emission (presets also update --report-top)
    #[arg(
        long = "report",
        alias = "report-metrics",
        value_enum,
        default_value = "full"
    )]
    pub report_metrics: ReportMetricsValue,

    /// Number of combinations to include in the final summary table
    #[arg(long = "top-k", alias = "report-top", default_value_t = 5)]
    pub top_k: usize,

    /// Force recompute even if existing results already cover the requested max-depth or CSV fingerprint
    #[arg(long = "force", alias = "force-recompute", default_value_t = false)]
    pub force: bool,

    /// Acknowledge that a newly engineered dataset differs from an existing
    /// barsmith_prepared.csv in the run folder; overwrite and continue.
    #[arg(long = "ack-new-df", default_value_t = false)]
    pub ack_new_df: bool,

    /// Enable numeric feature-to-feature comparisons (pairwise conditions)
    #[arg(
        long = "feature-pairs",
        alias = "enable-feature-pairs",
        default_value_t = false
    )]
    pub feature_pairs: bool,

    /// Maximum number of feature-to-feature comparison predicates to generate
    #[arg(long = "feature-pairs-limit", alias = "feature-pairs-max")]
    pub feature_pairs_limit: Option<usize>,

    /// Maximum allowed drawdown (in R units) for a combination to be stored
    /// in results_parquet and considered in top-results queries.
    #[arg(long = "max-drawdown", default_value_t = 30.0)]
    pub max_drawdown: f64,

    /// Optional drawdown ceiling applied only to reporting queries. When
    /// omitted, reporting uses --max-drawdown as its filter.
    #[arg(long = "max-drawdown-report")]
    pub max_drawdown_report: Option<f64>,

    /// Optional minimum Calmar ratio applied only to reporting queries.
    /// When omitted, reporting does not enforce a Calmar floor.
    #[arg(long = "min-calmar-report")]
    pub min_calmar_report: Option<f64>,

    /// Disable writing barsmith.log into the run folder. When set,
    /// logs are only emitted to stdout/stderr.
    #[arg(long = "no-file-log", default_value_t = false)]
    pub no_file_log: bool,

    /// Enable subset-based pruning of higher-depth combinations using
    /// zero-sample depth-2 pairs as dead prefixes.
    #[arg(
        long = "subset-pruning",
        alias = "enable-subset-pruning",
        default_value_t = false
    )]
    pub subset_pruning: bool,

    /// Control how much per-combination statistics detail is computed.
    ///
    /// `core` keeps the hot path cheap; `full` fills the wider report metrics.
    #[arg(long = "stats-detail", value_enum, default_value = "core")]
    pub stats_detail: StatsDetailValue,

    /// Emit eval_ms timing breakdowns for profiling hot spots.
    #[arg(long = "profile-eval", value_enum, default_value = "off")]
    pub profile_eval: EvalProfileValue,

    /// Optional sampling rate for eval profiling. When > 1, only ~1/N
    /// combinations are instrumented (deterministically) to reduce overhead.
    #[arg(long = "profile-eval-sample-rate", default_value_t = 1)]
    pub profile_eval_sample_rate: usize,

    /// Starting capital in USD for equity simulation and dollar metrics.
    /// When omitted, a default (e.g., 100_000) is applied.
    #[arg(long = "capital")]
    pub capital: Option<f64>,

    /// Risk percentage per trade, applied to current equity.
    /// When omitted, a default (e.g., 1.0) is applied.
    #[arg(long = "risk-pct-per-trade")]
    pub risk_pct_per_trade: Option<f64>,

    /// Asset code for cost modeling (e.g., ES, MES).
    #[arg(long = "asset")]
    pub asset: Option<String>,

    /// Position sizing mode for equity simulation.
    ///
    /// - fractional: risk `--risk-pct-per-trade` of current equity each trade
    /// - contracts: compute integer contracts from risk budget and stop distance
    #[arg(long = "position-sizing", value_enum, default_value = "fractional")]
    pub position_sizing: PositionSizingValue,

    /// Column name containing per-trade stop distance for contract sizing (in points or ticks).
    ///
    /// If omitted in `contracts` mode, Barsmith will try to infer a default for ATR-stop targets.
    #[arg(long = "stop-distance-column")]
    pub stop_distance_column: Option<String>,

    /// Unit for --stop-distance-column.
    #[arg(long = "stop-distance-unit", value_enum, default_value = "points")]
    pub stop_distance_unit: StopDistanceUnitValue,

    /// Minimum contracts to trade in contract sizing mode (default: 1).
    #[arg(long = "min-contracts", default_value_t = 1)]
    pub min_contracts: usize,

    /// Optional maximum contracts cap in contract sizing mode.
    #[arg(long = "max-contracts")]
    pub max_contracts: Option<usize>,

    /// Initial/overnight margin per contract in USD for contracts sizing (optional).
    ///
    /// When set, Barsmith caps contracts as floor(current_equity / margin_per_contract_dollar).
    #[arg(long = "margin-per-contract-dollar")]
    pub margin_per_contract_dollar: Option<f64>,

    /// Round-trip commission per trade in dollars (overrides asset default).
    #[arg(long = "commission-per-trade-dollar")]
    pub commission_per_trade_dollar: Option<f64>,

    /// Round-trip slippage per trade in dollars (overrides asset default).
    #[arg(long = "slippage-per-trade-dollar")]
    pub slippage_per_trade_dollar: Option<f64>,

    /// Round-trip total cost per trade in dollars (overrides commission+slippage).
    #[arg(long = "cost-per-trade-dollar")]
    pub cost_per_trade_dollar: Option<f64>,

    /// Disable cost model entirely and keep raw R semantics.
    #[arg(long = "no-costs", default_value_t = false)]
    pub no_costs: bool,

    /// Optional gating: only evaluate combinations that include at least one
    /// of the provided feature names (comma-delimited).
    ///
    /// Example: --require-any-features is_tribar_hl_green,is_tribar_hl_red
    #[arg(long = "require-any-features", value_delimiter = ',', num_args = 0..)]
    pub require_any_features: Vec<String>,

    /// Trade stacking behavior for `comb` evaluation.
    ///
    /// - stacking: treat every mask-hit bar as an independent trade sample.
    /// - no-stacking: enforce one open trade at a time using target exit indices.
    #[arg(long = "stacking-mode", value_enum, default_value = "no-stacking")]
    pub stacking_mode: StackingModeValue,
    // Zero-sample pruning, cross-run seeding, coverage checks, and
    // storage-backed membership reuse have been removed in favor of a
    // simpler, evaluation-only engine. The corresponding flags are no
    // longer exposed at the CLI level.
}

impl Cli {
    pub fn parse() -> Self {
        <Cli as Parser>::parse()
    }
}

impl CombArgs {
    pub fn into_config(self, output_dir: PathBuf) -> Result<Config> {
        let include_date_start = parse_optional_date(self.date_start.as_deref())?;
        let include_date_end = parse_optional_date(self.date_end.as_deref())?;
        let direction = self.direction.to_direction();
        // Preset report modes pick their own size only when the user left
        // `--top-k` at the default.
        let report_top = match self.report_metrics {
            ReportMetricsValue::Off => 0,
            ReportMetricsValue::Full => self.top_k.max(1),
            ReportMetricsValue::Formula => self.top_k.max(1),
            ReportMetricsValue::Top10 => {
                let effective = if self.top_k == 5 { 10 } else { self.top_k };
                effective.max(1)
            }
            ReportMetricsValue::Top100 => {
                let effective = if self.top_k == 5 { 100 } else { self.top_k };
                effective.max(1)
            }
        };
        // `--resume-from 0` is an explicit request to ignore stored offsets.
        let explicit_resume_offset = detect_explicit_resume_flag(std::env::args());
        let min_sample_size = self.min_samples;
        let min_sample_size_report = self.min_samples_report.unwrap_or(min_sample_size);

        // CLI defaults are resolved once before the immutable runtime config.
        let capital = self.capital.unwrap_or(DEFAULT_CAPITAL_DOLLAR);
        let risk_pct = self
            .risk_pct_per_trade
            .unwrap_or(DEFAULT_RISK_PCT_PER_TRADE);
        let risk_per_trade_dollar = if risk_pct > 0.0 {
            Some(capital * risk_pct / 100.0)
        } else {
            None
        };

        // Cost inputs are round-trip USD values. Fractional sizing also gets an
        // R-unit conversion once the risk budget is known.
        let mut asset_code: Option<String> = None;
        let mut effective_cost_dollar: Option<f64> = None;
        let mut tick_size: Option<f64> = None;
        let mut point_value: Option<f64> = None;
        let mut tick_value: Option<f64> = None;
        let mut margin_per_contract_dollar: Option<f64> = None;

        if !self.no_costs {
            if let Some(code) = &self.asset {
                let asset = find_asset(code)
                    .ok_or_else(|| anyhow::anyhow!("Unknown asset code '{}'", code))?;

                let base_commission = 2.0 * asset.ibkr_commission_per_side;
                let base_slippage = asset.default_slippage_ticks * asset.tick_value;
                tick_size = Some(asset.tick_size);
                point_value = Some(asset.point_value);
                tick_value = Some(asset.tick_value);
                margin_per_contract_dollar = Some(asset.margin_per_contract_dollar);

                let commission = self.commission_per_trade_dollar.unwrap_or(base_commission);
                let slippage = self.slippage_per_trade_dollar.unwrap_or(base_slippage);
                let cost = self.cost_per_trade_dollar.unwrap_or(commission + slippage);

                asset_code = Some(asset.code.to_string());
                effective_cost_dollar = Some(cost);
            } else if let Some(cost) = self.cost_per_trade_dollar {
                effective_cost_dollar = Some(cost);
            } else if self.commission_per_trade_dollar.is_some()
                || self.slippage_per_trade_dollar.is_some()
            {
                let commission = self.commission_per_trade_dollar.unwrap_or(0.0);
                let slippage = self.slippage_per_trade_dollar.unwrap_or(0.0);
                effective_cost_dollar = Some(commission + slippage);
            }
        } else if let Some(code) = &self.asset {
            // Targets still need the asset tick grid even when cost modeling is off.
            let asset =
                find_asset(code).ok_or_else(|| anyhow::anyhow!("Unknown asset code '{}'", code))?;
            asset_code = Some(asset.code.to_string());
            tick_size = Some(asset.tick_size);
            point_value = Some(asset.point_value);
            tick_value = Some(asset.tick_value);
            margin_per_contract_dollar = Some(asset.margin_per_contract_dollar);
        }

        let position_sizing = self.position_sizing.to_mode();
        if matches!(position_sizing, PositionSizingMode::Contracts) && self.asset.is_none() {
            return Err(anyhow::anyhow!(
                "--position-sizing contracts requires --asset so point/tick values are known"
            ));
        }

        let stop_distance_column = if matches!(position_sizing, PositionSizingMode::Contracts) {
            self.stop_distance_column
                .or_else(|| infer_stop_distance_column(&self.target))
        } else {
            None
        };
        if matches!(position_sizing, PositionSizingMode::Contracts)
            && stop_distance_column.is_none()
        {
            return Err(anyhow::anyhow!(
                "--position-sizing contracts requires --stop-distance-column (or a target that infers it)"
            ));
        }

        let (cost_per_trade_dollar, cost_per_trade_r, dollars_per_r) = match position_sizing {
            PositionSizingMode::Fractional => {
                match (effective_cost_dollar, risk_per_trade_dollar) {
                    (Some(cost), Some(risk_dollar)) if risk_dollar > 0.0 => {
                        let cost_r = cost / risk_dollar;
                        (Some(cost), Some(cost_r), Some(risk_dollar))
                    }
                    (Some(cost), _) => (Some(cost), None, risk_per_trade_dollar),
                    (None, _) => (None, None, risk_per_trade_dollar),
                }
            }
            PositionSizingMode::Contracts => (effective_cost_dollar, None, risk_per_trade_dollar),
        };

        if !matches!(self.profile_eval, EvalProfileValue::Off) && self.profile_eval_sample_rate == 0
        {
            return Err(anyhow::anyhow!("--profile-eval-sample-rate must be >= 1"));
        }

        if self.s3_upload_each_batch && self.s3_output.is_none() {
            return Err(anyhow::anyhow!(
                "--s3-upload-each-batch requires --s3-output"
            ));
        }

        let target = if self.target == "atr_stop" {
            "2x_atr_tp_atr_stop".to_string()
        } else {
            self.target
        };

        Ok(Config {
            input_csv: self.csv_path.clone(),
            source_csv: Some(self.csv_path),
            direction,
            target,
            output_dir,
            max_depth: self.max_depth,
            min_sample_size,
            min_sample_size_report,
            include_date_start,
            include_date_end,
            batch_size: self.batch_size.max(1),
            n_workers: normalize_workers(self.workers),
            auto_batch: self.auto_batch,
            resume_offset: self.resume_from,
            explicit_resume_offset,
            max_combos: self.max_combos,
            dry_run: self.dry_run,
            quiet: self.quiet,
            report_metrics: self.report_metrics.to_mode(),
            report_top,
            force_recompute: self.force,
            max_drawdown: self.max_drawdown,
            max_drawdown_report: self.max_drawdown_report,
            min_calmar_report: self.min_calmar_report,
            // Scalar predicates always use strict min pruning; the field remains
            // in Config so run metadata stays explicit.
            strict_min_pruning: true,
            enable_feature_pairs: self.feature_pairs,
            feature_pairs_limit: self.feature_pairs_limit,
            enable_subset_pruning: self.subset_pruning,
            catalog_hash: None,
            stats_detail: self.stats_detail.to_mode(),
            eval_profile: self.profile_eval.to_mode(),
            eval_profile_sample_rate: self.profile_eval_sample_rate.max(1),
            s3_output: self.s3_output,
            s3_upload_each_batch: self.s3_upload_each_batch,
            capital_dollar: Some(capital),
            risk_pct_per_trade: Some(risk_pct),
            equity_time_years: None,
            asset: asset_code,
            risk_per_trade_dollar,
            cost_per_trade_dollar,
            cost_per_trade_r,
            dollars_per_r,
            tick_size,
            stacking_mode: self.stacking_mode.to_mode(),
            position_sizing,
            stop_distance_column,
            stop_distance_unit: self.stop_distance_unit.to_mode(),
            min_contracts: self.min_contracts.max(1),
            max_contracts: self.max_contracts,
            point_value,
            tick_value,
            margin_per_contract_dollar: self
                .margin_per_contract_dollar
                .or(margin_per_contract_dollar),
            require_any_features: {
                let mut names: Vec<String> = self
                    .require_any_features
                    .into_iter()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                names.sort();
                names.dedup();
                names
            },
        })
    }
}

fn infer_stop_distance_column(target: &str) -> Option<String> {
    match target {
        "2x_atr_tp_atr_stop" | "3x_atr_tp_atr_stop" | "atr_tp_atr_stop" | "atr_stop" => {
            Some("atr".to_string())
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum EvalProfileValue {
    Off,
    Coarse,
    Fine,
}

impl EvalProfileValue {
    fn to_mode(self) -> EvalProfileMode {
        match self {
            EvalProfileValue::Off => EvalProfileMode::Off,
            EvalProfileValue::Coarse => EvalProfileMode::Coarse,
            EvalProfileValue::Fine => EvalProfileMode::Fine,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StackingModeValue {
    Stacking,
    #[value(name = "no-stacking")]
    NoStacking,
}

impl StackingModeValue {
    pub fn to_mode(self) -> StackingMode {
        match self {
            StackingModeValue::Stacking => StackingMode::Stacking,
            StackingModeValue::NoStacking => StackingMode::NoStacking,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PositionSizingValue {
    Fractional,
    Contracts,
}

impl PositionSizingValue {
    pub fn to_mode(self) -> PositionSizingMode {
        match self {
            PositionSizingValue::Fractional => PositionSizingMode::Fractional,
            PositionSizingValue::Contracts => PositionSizingMode::Contracts,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StopDistanceUnitValue {
    Points,
    Ticks,
}

impl StopDistanceUnitValue {
    pub fn to_mode(self) -> StopDistanceUnit {
        match self {
            StopDistanceUnitValue::Points => StopDistanceUnit::Points,
            StopDistanceUnitValue::Ticks => StopDistanceUnit::Ticks,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FormulaRankByValue {
    #[value(name = "calmar-equity")]
    CalmarEquity,
    Frs,
}

impl FormulaRankByValue {
    pub fn to_rank_by(self) -> RankBy {
        match self {
            Self::CalmarEquity => RankBy::CalmarEquity,
            Self::Frs => RankBy::Frs,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum SelectionModeValue {
    Off,
    #[value(name = "holdout-confirm")]
    HoldoutConfirm,
    #[value(name = "validation-rank")]
    ValidationRank,
}

impl SelectionModeValue {
    pub fn to_mode(self) -> SelectionMode {
        match self {
            Self::Off => SelectionMode::Off,
            Self::HoldoutConfirm => SelectionMode::HoldoutConfirm,
            Self::ValidationRank => SelectionMode::ValidationRank,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum SelectionPresetValue {
    Exploratory,
    Institutional,
    Custom,
}

impl SelectionPresetValue {
    pub fn to_preset(self) -> SelectionPreset {
        match self {
            Self::Exploratory => SelectionPreset::Exploratory,
            Self::Institutional => SelectionPreset::Institutional,
            Self::Custom => SelectionPreset::Custom,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ResearchStageValue {
    Discovery,
    Validation,
    Lockbox,
    #[value(name = "live-shadow")]
    LiveShadow,
}

impl ResearchStageValue {
    pub fn to_stage(self) -> ResearchStage {
        match self {
            Self::Discovery => ResearchStage::Discovery,
            Self::Validation => ResearchStage::Validation,
            Self::Lockbox => ResearchStage::Lockbox,
            Self::LiveShadow => ResearchStage::LiveShadow,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum LockboxStageValue {
    Lockbox,
    #[value(name = "live-shadow")]
    LiveShadow,
}

impl LockboxStageValue {
    pub fn to_stage_value(self) -> ResearchStageValue {
        match self {
            Self::Lockbox => ResearchStageValue::Lockbox,
            Self::LiveShadow => ResearchStageValue::LiveShadow,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ResultRankByValue {
    #[value(name = "calmar-ratio")]
    CalmarRatio,
    #[value(name = "total-return")]
    TotalReturn,
}

impl ResultRankByValue {
    pub fn to_rank_by(self) -> barsmith_rs::storage::ResultRankBy {
        match self {
            Self::CalmarRatio => barsmith_rs::storage::ResultRankBy::CalmarRatio,
            Self::TotalReturn => barsmith_rs::storage::ResultRankBy::TotalReturn,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FrsScopeValue {
    Window,
    Pre,
    Post,
    All,
}

impl FrsScopeValue {
    pub fn to_scope(self) -> FrsScope {
        match self {
            Self::Window => FrsScope::Window,
            Self::Pre => FrsScope::Pre,
            Self::Post => FrsScope::Post,
            Self::All => FrsScope::All,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum EquityCurveRankByValue {
    Pre,
    Post,
}

impl EquityCurveRankByValue {
    pub fn to_rank_by(self, post_rank_by: RankBy) -> RankBy {
        match self {
            Self::Pre => RankBy::CalmarEquity,
            Self::Post => post_rank_by,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum EquityCurveWindowValue {
    Pre,
    Post,
    Both,
}

impl EquityCurveWindowValue {
    pub fn to_selection(self) -> EquityCurveWindowSelection {
        match self {
            Self::Pre => EquityCurveWindowSelection::Pre,
            Self::Post => EquityCurveWindowSelection::Post,
            Self::Both => EquityCurveWindowSelection::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PlotModeValue {
    Individual,
    Combined,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PlotXValue {
    Timestamp,
    #[value(name = "trade-index")]
    TradeIndex,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PlotMetricValue {
    Dollar,
    R,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum EngineValue {
    Auto,
    Builtin,
    Custom,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum DirectionValue {
    Long,
    Short,
    Both,
}

impl DirectionValue {
    pub fn to_direction(self) -> Direction {
        match self {
            DirectionValue::Long => Direction::Long,
            DirectionValue::Short => Direction::Short,
            DirectionValue::Both => Direction::Both,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum ReportMetricsValue {
    Full,
    /// Only print ranked combination formulas (no metrics), honoring --top-k.
    Formula,
    Top10,
    Top100,
    Off,
}

impl ReportMetricsValue {
    fn to_mode(self) -> ReportMetricsMode {
        match self {
            ReportMetricsValue::Full => ReportMetricsMode::Full,
            ReportMetricsValue::Formula => ReportMetricsMode::FormulasOnly,
            ReportMetricsValue::Top10 => ReportMetricsMode::Full,
            ReportMetricsValue::Top100 => ReportMetricsMode::Full,
            ReportMetricsValue::Off => ReportMetricsMode::Off,
        }
    }
}

fn normalize_workers(value: Option<usize>) -> usize {
    value.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    })
}

fn detect_explicit_resume_flag<I, S>(args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    for arg in args {
        let raw = arg.as_ref();
        if raw == "--resume-from"
            || raw.starts_with("--resume-from=")
            || raw == "--resume-offset"
            || raw.starts_with("--resume-offset=")
        {
            return true;
        }
    }
    false
}

fn parse_optional_date(value: Option<&str>) -> Result<Option<NaiveDate>> {
    match value {
        Some(raw) => {
            let parsed = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                .with_context(|| format!("Invalid date format for {raw}. Expected YYYY-MM-DD"))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn base_args() -> CombArgs {
        CombArgs {
            csv_path: PathBuf::from("dummy.csv"),
            direction: DirectionValue::Long,
            target: "next_bar_color_and_wicks".to_string(),
            engine: EngineValue::Auto,
            runs_root: PathBuf::from(DEFAULT_RUNS_ROOT),
            dataset_id: None,
            run_id: None,
            run_slug: None,
            registry_dir: PathBuf::from(DEFAULT_REGISTRY_DIR),
            artifact_uri: None,
            checksum_artifacts: false,
            s3_output: None,
            s3_upload_each_batch: false,
            max_depth: 3,
            min_samples: 100,
            min_samples_report: None,
            date_start: None,
            date_end: None,
            batch_size: 20_000,
            auto_batch: false,
            workers: Some(4),
            resume_from: 0,
            max_combos: None,
            dry_run: false,
            quiet: false,
            report_metrics: ReportMetricsValue::Full,
            top_k: 5,
            force: false,
            ack_new_df: false,
            feature_pairs: false,
            feature_pairs_limit: None,
            max_drawdown: 50.0,
            max_drawdown_report: None,
            min_calmar_report: None,
            no_file_log: false,
            subset_pruning: false,
            stats_detail: StatsDetailValue::Full,
            profile_eval: EvalProfileValue::Off,
            profile_eval_sample_rate: 1,
            capital: None,
            risk_pct_per_trade: None,
            asset: None,
            position_sizing: PositionSizingValue::Fractional,
            stop_distance_column: None,
            stop_distance_unit: StopDistanceUnitValue::Points,
            min_contracts: 1,
            max_contracts: None,
            margin_per_contract_dollar: None,
            commission_per_trade_dollar: None,
            slippage_per_trade_dollar: None,
            cost_per_trade_dollar: None,
            no_costs: false,
            require_any_features: Vec::new(),
            stacking_mode: StackingModeValue::NoStacking,
        }
    }

    #[test]
    fn removed_logic_flags_are_rejected() {
        for flag in ["--logic", "--logic-mode", "--early-exit-when-reused"] {
            let err = Cli::try_parse_from(["barsmith", "comb", "--csv", "dummy.csv", flag, "or"])
                .expect_err("removed logic flag should not parse");

            assert!(
                matches!(err.kind(), clap::error::ErrorKind::UnknownArgument),
                "expected {flag} to be rejected as an unknown argument, got {err:?}"
            );
        }
    }

    #[test]
    fn run_producing_output_dir_flags_are_rejected() {
        for command in ["comb", "eval-formulas"] {
            let args = if command == "comb" {
                vec![
                    "barsmith",
                    command,
                    "--csv",
                    "dummy.csv",
                    "--output-dir",
                    "out",
                ]
            } else {
                vec![
                    "barsmith",
                    command,
                    "--prepared",
                    "prepared.csv",
                    "--formulas",
                    "formulas.txt",
                    "--output-dir",
                    "out",
                ]
            };
            let err =
                Cli::try_parse_from(args).expect_err("run-producing --output-dir should not parse");

            assert!(
                matches!(err.kind(), clap::error::ErrorKind::UnknownArgument),
                "expected {command} --output-dir to be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn detect_explicit_resume_flag_matches_expected_patterns() {
        assert!(!detect_explicit_resume_flag([
            "barsmith_cli",
            "--csv",
            "foo.csv"
        ]));
        assert!(detect_explicit_resume_flag([
            "barsmith_cli",
            "--resume-from",
            "1000"
        ]));
        assert!(detect_explicit_resume_flag([
            "barsmith_cli",
            "--resume-from=0"
        ]));
        assert!(detect_explicit_resume_flag([
            "barsmith_cli",
            "--resume-offset=42"
        ]));
    }

    #[test]
    fn report_full_uses_top_k_and_clamps_to_one() {
        let mut args = base_args();
        args.report_metrics = ReportMetricsValue::Full;
        args.top_k = 0;
        let config = args.into_config(PathBuf::from("out")).expect("config");
        assert!(
            matches!(config.report_metrics, ReportMetricsMode::Full),
            "report_metrics should be Full for full report mode"
        );
        assert_eq!(
            config.report_top, 1,
            "Full report should clamp top_k to at least 1"
        );
    }

    #[test]
    fn report_formula_uses_top_k_and_clamps_to_one() {
        let mut args = base_args();
        args.report_metrics = ReportMetricsValue::Formula;
        args.top_k = 0;
        let config = args.into_config(PathBuf::from("out")).expect("config");
        assert!(
            matches!(config.report_metrics, ReportMetricsMode::FormulasOnly),
            "report_metrics should be FormulasOnly for formula report mode"
        );
        assert_eq!(
            config.report_top, 1,
            "Formula report should clamp top_k to at least 1"
        );
    }

    #[test]
    fn report_top10_uses_preset_when_top_k_is_default() {
        let mut args = base_args();
        args.report_metrics = ReportMetricsValue::Top10;
        args.top_k = 5; // default
        let config = args.into_config(PathBuf::from("out")).expect("config");
        assert!(
            matches!(config.report_metrics, ReportMetricsMode::Full),
            "report_metrics should be Full for top10 preset"
        );
        assert_eq!(
            config.report_top, 10,
            "Top10 preset should default to 10 when top_k is left at its default"
        );
    }

    #[test]
    fn report_top10_respects_explicit_top_k_override() {
        let mut args = base_args();
        args.report_metrics = ReportMetricsValue::Top10;
        args.top_k = 3;
        let config = args.into_config(PathBuf::from("out")).expect("config");
        assert!(
            matches!(config.report_metrics, ReportMetricsMode::Full),
            "report_metrics should be Full for top10 preset"
        );
        assert_eq!(
            config.report_top, 3,
            "Top10 preset should respect an explicit top_k override"
        );
    }

    #[test]
    fn report_top100_uses_preset_when_top_k_is_default() {
        let mut args = base_args();
        args.report_metrics = ReportMetricsValue::Top100;
        args.top_k = 5; // default
        let config = args.into_config(PathBuf::from("out")).expect("config");
        assert!(
            matches!(config.report_metrics, ReportMetricsMode::Full),
            "report_metrics should be Full for top100 preset"
        );
        assert_eq!(
            config.report_top, 100,
            "Top100 preset should default to 100 when top_k is left at its default"
        );
    }

    #[test]
    fn report_top100_respects_explicit_top_k_override() {
        let mut args = base_args();
        args.report_metrics = ReportMetricsValue::Top100;
        args.top_k = 12;
        let config = args.into_config(PathBuf::from("out")).expect("config");
        assert!(
            matches!(config.report_metrics, ReportMetricsMode::Full),
            "report_metrics should be Full for top100 preset"
        );
        assert_eq!(
            config.report_top, 12,
            "Top100 preset should respect an explicit top_k override"
        );
    }

    #[test]
    fn report_off_disables_reporting() {
        let mut args = base_args();
        args.report_metrics = ReportMetricsValue::Off;
        let config = args.into_config(PathBuf::from("out")).expect("config");
        assert!(
            matches!(config.report_metrics, ReportMetricsMode::Off),
            "report_metrics should be Off when reporting is disabled"
        );
        assert_eq!(
            config.report_top, 0,
            "report_top should be zero when reporting is disabled"
        );
    }

    #[test]
    fn parse_optional_date_accepts_valid_yyyy_mm_dd() {
        let parsed = parse_optional_date(Some("2024-11-30"))
            .expect("parse should succeed")
            .expect("date should be present");
        let expected = NaiveDate::from_ymd_opt(2024, 11, 30).expect("valid date");
        assert_eq!(parsed, expected);
    }

    #[test]
    fn normalize_workers_prefers_explicit_value() {
        assert_eq!(normalize_workers(Some(2)), 2);
        assert!(
            normalize_workers(None) >= 1,
            "normalize_workers without explicit value should return at least 1"
        );
    }
}
