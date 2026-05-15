use std::path::PathBuf;

use barsmith_rs::protocol::ResearchStage;
use serde::Serialize;

use super::helpers::{serialize_metric, serialize_optional_metric};

pub(super) const REGISTRY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunKind {
    Comb,
    ForwardTest,
}

impl RunKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Comb => "comb",
            Self::ForwardTest => "forward-test",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StandardOutputPlan {
    pub run_kind: RunKind,
    pub output_dir: PathBuf,
    pub run_path: PathBuf,
    pub run_id: String,
    pub dataset_id: String,
    pub target: String,
    pub direction: String,
    pub cutoff: Option<String>,
    pub created_at: String,
    pub git_sha: Option<String>,
    pub git_short_sha: Option<String>,
    pub registry_dir: Option<PathBuf>,
    pub artifact_uri: Option<String>,
    pub checksum_artifacts: bool,
    pub(super) command_argv: Vec<String>,
    pub(super) command_line: String,
}

#[derive(Debug, Serialize)]
pub(super) struct CommandRecord<'a> {
    pub schema_version: u32,
    pub run_kind: RunKind,
    pub run_id: &'a str,
    pub dataset_id: &'a str,
    pub target: &'a str,
    pub direction: &'a str,
    pub cutoff: Option<&'a str>,
    pub created_at: &'a str,
    pub git_sha: Option<&'a str>,
    pub git_short_sha: Option<&'a str>,
    pub output_dir: String,
    pub artifact_uri: Option<&'a str>,
    pub argv: &'a [String],
    pub command_line: &'a str,
}

#[derive(Debug, Serialize)]
pub(super) struct RegistryRecord<'a> {
    pub schema_version: u32,
    pub run_kind: RunKind,
    pub run_id: &'a str,
    pub dataset_id: &'a str,
    pub target: &'a str,
    pub direction: &'a str,
    pub cutoff: Option<&'a str>,
    pub started_at: &'a str,
    pub completed_at: &'a str,
    pub git_sha: Option<&'a str>,
    pub git_short_sha: Option<&'a str>,
    pub artifact_uri: Option<&'a str>,
    pub run_path: String,
    pub command_sha256: String,
    pub top_calmar: Option<TopResultRecord>,
    pub top_total_r: Option<TopResultRecord>,
    pub checksum_file: String,
}

#[derive(Debug, Serialize)]
pub(super) struct ForwardManifest<'a> {
    pub schema_version: u32,
    pub run_kind: RunKind,
    pub run_id: &'a str,
    pub dataset_id: &'a str,
    pub target: &'a str,
    pub cutoff: &'a str,
    pub created_at: &'a str,
    pub completed_at: &'a str,
    pub git_sha: Option<&'a str>,
    pub prepared_sha256: String,
    pub formulas_sha256: String,
    pub rr_column: Option<&'a str>,
    pub stacking_mode: String,
    pub position_sizing: String,
    pub rank_by: String,
    pub frs_enabled: bool,
    pub frs_scope: String,
    pub selection_mode: String,
    pub candidate_top_k: usize,
    pub purge_cross_boundary_exits: bool,
    pub embargo_bars: usize,
    pub plot_enabled: bool,
    pub plot_mode: String,
    pub stage: ResearchStage,
    pub workflow_status: String,
    pub strict_protocol: bool,
    pub protocol_sha256: Option<String>,
    pub formula_export_manifest_sha256: Option<String>,
    pub overfit_status: Option<String>,
    pub stress_status: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct ForwardRegistryRecord<'a> {
    pub schema_version: u32,
    pub run_kind: RunKind,
    pub run_id: &'a str,
    pub dataset_id: &'a str,
    pub target: &'a str,
    pub cutoff: &'a str,
    pub started_at: &'a str,
    pub completed_at: &'a str,
    pub git_sha: Option<&'a str>,
    pub git_short_sha: Option<&'a str>,
    pub artifact_uri: Option<&'a str>,
    pub run_path: String,
    pub command_sha256: String,
    pub prepared_sha256: String,
    pub formulas_sha256: String,
    pub top_pre_calmar: Option<ForwardTopResultRecord>,
    pub top_post_ranked: Option<ForwardTopResultRecord>,
    pub top_post_total_r: Option<ForwardTopResultRecord>,
    pub selected_formula_sha256: Option<String>,
    pub selected_pre_rank: Option<usize>,
    pub selected_post_rank: Option<usize>,
    pub selection_status: Option<String>,
    pub diagnostic_top_post_formula_sha256: Option<String>,
    pub stage: ResearchStage,
    pub workflow_status: String,
    pub strict_protocol: bool,
    pub protocol_sha256: Option<String>,
    pub formula_export_manifest_sha256: Option<String>,
    pub lockbox_attempt_number: Option<usize>,
    pub lockbox_status: Option<String>,
    pub overfit_status: Option<String>,
    pub stress_status: Option<String>,
    pub pbo: Option<f64>,
    pub dsr: Option<f64>,
    pub psr: Option<f64>,
    pub effective_trials: Option<usize>,
    pub checksum_file: String,
    pub artifact_files: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct ForwardTopResultRecord {
    pub metric_basis: &'static str,
    pub formula_sha256: String,
    pub source_rank: usize,
    pub display_rank: usize,
    pub previous_rank: Option<usize>,
    pub trades: usize,
    pub mask_hits: usize,
    #[serde(serialize_with = "serialize_metric")]
    pub win_rate: f64,
    #[serde(serialize_with = "serialize_metric")]
    pub total_return_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    pub max_drawdown_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    pub calmar_equity: f64,
    #[serde(serialize_with = "serialize_optional_metric")]
    pub frs: Option<f64>,
}

#[derive(Debug, Serialize)]
pub(super) struct TopResultRecord {
    pub metric_basis: &'static str,
    pub formula_sha256: String,
    pub depth: u32,
    pub total_bars: u64,
    pub profitable_bars: u64,
    #[serde(serialize_with = "serialize_metric")]
    pub win_rate: f64,
    #[serde(serialize_with = "serialize_metric")]
    pub total_return_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    pub max_drawdown_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    pub calmar_ratio: f64,
    pub resume_offset: u64,
}
