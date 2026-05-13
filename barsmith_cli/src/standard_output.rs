use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use barsmith_rs::config::Config;
use barsmith_rs::formula_eval::{FormulaEvaluationReport, FormulaResult};
use barsmith_rs::protocol::ResearchStage;
use barsmith_rs::storage::{ResultQuery, ResultRankBy, ResultRow, query_result_store};
use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::cli::{CombArgs, DirectionValue, EvalFormulasArgs, PlotModeValue};

const REGISTRY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunKind {
    Comb,
    ForwardTest,
}

impl RunKind {
    fn as_str(self) -> &'static str {
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
    command_argv: Vec<String>,
    command_line: String,
}

#[derive(Debug, Serialize)]
struct CommandRecord<'a> {
    schema_version: u32,
    run_kind: RunKind,
    run_id: &'a str,
    dataset_id: &'a str,
    target: &'a str,
    direction: &'a str,
    cutoff: Option<&'a str>,
    created_at: &'a str,
    git_sha: Option<&'a str>,
    git_short_sha: Option<&'a str>,
    output_dir: String,
    artifact_uri: Option<&'a str>,
    argv: &'a [String],
    command_line: &'a str,
}

#[derive(Debug, Serialize)]
struct RegistryRecord<'a> {
    schema_version: u32,
    run_kind: RunKind,
    run_id: &'a str,
    dataset_id: &'a str,
    target: &'a str,
    direction: &'a str,
    cutoff: Option<&'a str>,
    started_at: &'a str,
    completed_at: &'a str,
    git_sha: Option<&'a str>,
    git_short_sha: Option<&'a str>,
    artifact_uri: Option<&'a str>,
    run_path: String,
    command_sha256: String,
    top_calmar: Option<TopResultRecord>,
    top_total_r: Option<TopResultRecord>,
    checksum_file: String,
}

#[derive(Debug, Serialize)]
struct ForwardManifest<'a> {
    schema_version: u32,
    run_kind: RunKind,
    run_id: &'a str,
    dataset_id: &'a str,
    target: &'a str,
    cutoff: &'a str,
    created_at: &'a str,
    completed_at: &'a str,
    git_sha: Option<&'a str>,
    prepared_sha256: String,
    formulas_sha256: String,
    rr_column: Option<&'a str>,
    stacking_mode: String,
    position_sizing: String,
    rank_by: String,
    frs_enabled: bool,
    frs_scope: String,
    selection_mode: String,
    candidate_top_k: usize,
    purge_cross_boundary_exits: bool,
    embargo_bars: usize,
    plot_enabled: bool,
    plot_mode: String,
    stage: ResearchStage,
    strict_protocol: bool,
    protocol_sha256: Option<String>,
    formula_export_manifest_sha256: Option<String>,
    overfit_status: Option<String>,
    stress_status: Option<String>,
}

#[derive(Debug, Serialize)]
struct ForwardRegistryRecord<'a> {
    schema_version: u32,
    run_kind: RunKind,
    run_id: &'a str,
    dataset_id: &'a str,
    target: &'a str,
    cutoff: &'a str,
    started_at: &'a str,
    completed_at: &'a str,
    git_sha: Option<&'a str>,
    git_short_sha: Option<&'a str>,
    artifact_uri: Option<&'a str>,
    run_path: String,
    command_sha256: String,
    prepared_sha256: String,
    formulas_sha256: String,
    top_pre_calmar: Option<ForwardTopResultRecord>,
    top_post_ranked: Option<ForwardTopResultRecord>,
    top_post_total_r: Option<ForwardTopResultRecord>,
    selected_formula_sha256: Option<String>,
    selected_pre_rank: Option<usize>,
    selected_post_rank: Option<usize>,
    selection_status: Option<String>,
    diagnostic_top_post_formula_sha256: Option<String>,
    stage: ResearchStage,
    strict_protocol: bool,
    protocol_sha256: Option<String>,
    formula_export_manifest_sha256: Option<String>,
    lockbox_attempt_number: Option<usize>,
    lockbox_status: Option<String>,
    overfit_status: Option<String>,
    stress_status: Option<String>,
    pbo: Option<f64>,
    dsr: Option<f64>,
    psr: Option<f64>,
    effective_trials: Option<usize>,
    checksum_file: String,
    artifact_files: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ForwardTopResultRecord {
    metric_basis: &'static str,
    formula_sha256: String,
    source_rank: usize,
    display_rank: usize,
    previous_rank: Option<usize>,
    trades: usize,
    mask_hits: usize,
    #[serde(serialize_with = "serialize_metric")]
    win_rate: f64,
    #[serde(serialize_with = "serialize_metric")]
    total_return_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    max_drawdown_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    calmar_equity: f64,
    #[serde(serialize_with = "serialize_optional_metric")]
    frs: Option<f64>,
}

#[derive(Debug, Serialize)]
struct TopResultRecord {
    metric_basis: &'static str,
    formula_sha256: String,
    depth: u32,
    total_bars: u64,
    profitable_bars: u64,
    #[serde(serialize_with = "serialize_metric")]
    win_rate: f64,
    #[serde(serialize_with = "serialize_metric")]
    total_return_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    max_drawdown_r: f64,
    #[serde(serialize_with = "serialize_metric")]
    calmar_ratio: f64,
    resume_offset: u64,
}

pub fn resolve_comb_output(args: &CombArgs, argv: &[OsString]) -> Result<StandardOutputPlan> {
    let target = sanitize_segment(&normalize_target(&args.target));
    let direction = format_direction(args.direction);
    let dataset_id = args
        .dataset_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| dataset_id_from_csv(&args.csv_path));

    let git_sha = git_rev_parse(["rev-parse", "HEAD"]);
    let git_short_sha = git_rev_parse(["rev-parse", "--short=12", "HEAD"]);
    let created_at = now_compact();
    let slug = args
        .run_slug
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| "run".to_string());
    let run_id = args
        .run_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| {
            let sha = git_short_sha.as_deref().unwrap_or("nogit");
            sanitize_segment(&format!("{created_at}_{sha}_{slug}"))
        });

    let run_path = PathBuf::from(RunKind::Comb.as_str())
        .join(&target)
        .join(direction)
        .join(&dataset_id)
        .join(&run_id);
    let output_dir = match (&args.runs_root, &args.output_dir) {
        (Some(root), None) => root.join(&run_path),
        (None, Some(path)) => path.clone(),
        (Some(_), Some(_)) => {
            return Err(anyhow!(
                "--runs-root and --output-dir are mutually exclusive"
            ));
        }
        (None, None) => {
            return Err(anyhow!(
                "either --output-dir or --runs-root must be provided"
            ));
        }
    };

    let command_argv: Vec<String> = argv
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let command_line = shell_join(&command_argv);

    Ok(StandardOutputPlan {
        run_kind: RunKind::Comb,
        output_dir,
        run_path,
        run_id,
        dataset_id,
        target,
        direction: direction.to_string(),
        cutoff: None,
        created_at: now_iso(),
        git_sha,
        git_short_sha,
        registry_dir: args.registry_dir.clone(),
        artifact_uri: args.artifact_uri.clone(),
        checksum_artifacts: args.checksum_artifacts,
        command_argv,
        command_line,
    })
}

pub fn resolve_forward_output(
    args: &EvalFormulasArgs,
    argv: &[OsString],
) -> Result<Option<StandardOutputPlan>> {
    if args.runs_root.is_none() && args.output_dir.is_none() {
        return Ok(None);
    }

    let target = sanitize_segment(&normalize_target(&args.target));
    let cutoff = sanitize_segment(&args.cutoff);
    let dataset_id = args
        .dataset_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| dataset_id_from_prepared(&args.prepared_path));

    let git_sha = git_rev_parse(["rev-parse", "HEAD"]);
    let git_short_sha = git_rev_parse(["rev-parse", "--short=12", "HEAD"]);
    let created_at = now_compact();
    let slug = args
        .run_slug
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| "forward_test".to_string());
    let run_id = args
        .run_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| {
            let sha = git_short_sha.as_deref().unwrap_or("nogit");
            sanitize_segment(&format!("{created_at}_{sha}_{slug}"))
        });

    let run_path = PathBuf::from(RunKind::ForwardTest.as_str())
        .join(&target)
        .join(&dataset_id)
        .join(&cutoff)
        .join(&run_id);
    let output_dir = match (&args.runs_root, &args.output_dir) {
        (Some(root), None) => root.join(&run_path),
        (None, Some(path)) => path.clone(),
        (Some(_), Some(_)) => {
            return Err(anyhow!(
                "--runs-root and --output-dir are mutually exclusive"
            ));
        }
        (None, None) => unreachable!("handled above"),
    };

    let command_argv: Vec<String> = argv
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let command_line = shell_join(&command_argv);

    Ok(Some(StandardOutputPlan {
        run_kind: RunKind::ForwardTest,
        output_dir,
        run_path,
        run_id,
        dataset_id,
        target,
        direction: "n/a".to_string(),
        cutoff: Some(cutoff),
        created_at: now_iso(),
        git_sha,
        git_short_sha,
        registry_dir: args.registry_dir.clone(),
        artifact_uri: args.artifact_uri.clone(),
        checksum_artifacts: args.checksum_artifacts,
        command_argv,
        command_line,
    }))
}

pub fn apply_forward_output_defaults(args: &mut EvalFormulasArgs, plan: &StandardOutputPlan) {
    args.csv_out
        .get_or_insert_with(|| plan.output_dir.join("formula_results.csv"));
    args.json_out
        .get_or_insert_with(|| plan.output_dir.join("formula_results.json"));
    if args.selection_mode.to_mode().is_enabled() && !args.stage.to_stage().is_lockbox_like() {
        args.selection_out
            .get_or_insert_with(|| plan.output_dir.join("selection_report.json"));
        args.selection_decisions_out
            .get_or_insert_with(|| plan.output_dir.join("selection_decisions.csv"));
        args.selected_formulas_out
            .get_or_insert_with(|| plan.output_dir.join("selected_formulas.txt"));
    }
    if args.strict_protocol {
        args.protocol_validation_out
            .get_or_insert_with(|| plan.output_dir.join("protocol_validation.json"));
    }

    if !args.no_frs {
        args.frs_out
            .get_or_insert_with(|| plan.output_dir.join("frs_summary.csv"));
        args.frs_windows_out
            .get_or_insert_with(|| plan.output_dir.join("frs_windows.csv"));
    }
    if args.strict_protocol || args.overfit_report {
        args.overfit_out
            .get_or_insert_with(|| plan.output_dir.join("overfit_report.json"));
        args.overfit_decisions_out
            .get_or_insert_with(|| plan.output_dir.join("overfit_decisions.csv"));
    }
    if args.strict_protocol || args.stress_report {
        args.stress_out
            .get_or_insert_with(|| plan.output_dir.join("stress_report.json"));
        args.stress_matrix_out
            .get_or_insert_with(|| plan.output_dir.join("stress_matrix.csv"));
    }

    if args.equity_curves_top_k > 0 {
        args.equity_curves_out
            .get_or_insert_with(|| plan.output_dir.join("equity_curves.csv"));
    }

    if args.plot {
        match args.plot_mode {
            PlotModeValue::Combined => {
                args.plot_out
                    .get_or_insert_with(|| plan.output_dir.join("plots").join("equity_curves.png"));
            }
            PlotModeValue::Individual => {
                args.plot_dir
                    .get_or_insert_with(|| plan.output_dir.join("plots"));
            }
        }
    }
}

pub fn write_start_files(plan: &StandardOutputPlan) -> Result<()> {
    fs::create_dir_all(&plan.output_dir)
        .with_context(|| format!("failed to create {}", plan.output_dir.display()))?;
    fs::create_dir_all(plan.output_dir.join("reports")).with_context(|| {
        format!(
            "failed to create reports dir in {}",
            plan.output_dir.display()
        )
    })?;

    fs::write(plan.output_dir.join("command.txt"), &plan.command_line)
        .with_context(|| "failed to write command.txt")?;

    let command_record = CommandRecord {
        schema_version: REGISTRY_SCHEMA_VERSION,
        run_kind: plan.run_kind,
        run_id: &plan.run_id,
        dataset_id: &plan.dataset_id,
        target: &plan.target,
        direction: &plan.direction,
        cutoff: plan.cutoff.as_deref(),
        created_at: &plan.created_at,
        git_sha: plan.git_sha.as_deref(),
        git_short_sha: plan.git_short_sha.as_deref(),
        output_dir: plan.output_dir.display().to_string(),
        artifact_uri: plan.artifact_uri.as_deref(),
        argv: &plan.command_argv,
        command_line: &plan.command_line,
    };
    write_json_atomic(&plan.output_dir.join("command.json"), &command_record)
}

pub fn write_closeout_files(plan: &StandardOutputPlan, config: &Config) -> Result<()> {
    let completed_at = now_iso();
    let top_calmar = query_top_result(config, ResultRankBy::CalmarRatio)
        .ok()
        .flatten();
    let top_total_r = query_top_result(config, ResultRankBy::TotalReturn)
        .ok()
        .flatten();
    write_summary(
        plan,
        config,
        top_calmar.as_ref(),
        top_total_r.as_ref(),
        &completed_at,
    )?;
    let checksums = write_checksums(plan)?;

    if let Some(registry_dir) = &plan.registry_dir {
        fs::create_dir_all(registry_dir)
            .with_context(|| format!("failed to create {}", registry_dir.display()))?;
        let record = RegistryRecord {
            schema_version: REGISTRY_SCHEMA_VERSION,
            run_kind: plan.run_kind,
            run_id: &plan.run_id,
            dataset_id: &plan.dataset_id,
            target: &plan.target,
            direction: &plan.direction,
            cutoff: plan.cutoff.as_deref(),
            started_at: &plan.created_at,
            completed_at: &completed_at,
            git_sha: plan.git_sha.as_deref(),
            git_short_sha: plan.git_short_sha.as_deref(),
            artifact_uri: plan.artifact_uri.as_deref(),
            run_path: path_for_json(&plan.run_path),
            command_sha256: sha256_bytes(plan.command_line.as_bytes()),
            top_calmar: top_calmar
                .as_ref()
                .map(|row| top_result_record("stored results ordered by calmar_ratio", row)),
            top_total_r: top_total_r
                .as_ref()
                .map(|row| top_result_record("stored results ordered by total_return", row)),
            checksum_file: checksums
                .strip_prefix(&plan.output_dir)
                .unwrap_or(&checksums)
                .display()
                .to_string(),
        };
        let path = registry_dir.join(&plan.run_path).with_extension("json");
        write_json_atomic(&path, &record)?;
    }

    Ok(())
}

pub fn write_forward_closeout_files(
    plan: &StandardOutputPlan,
    args: &EvalFormulasArgs,
    report: &FormulaEvaluationReport,
    written_files: &[PathBuf],
) -> Result<()> {
    let completed_at = now_iso();
    let prepared_sha256 = sha256_file(&args.prepared_path)?;
    let formulas_sha256 = sha256_file(&args.formulas_path)?;
    let mut closeout_files = written_files.to_vec();
    if let Some(path) = write_forward_selection_markdown(plan, report)? {
        closeout_files.push(path);
    }
    if let Some(path) = write_overfit_markdown(plan, report)? {
        closeout_files.push(path);
    }
    if let Some(path) = write_stress_markdown(plan, report)? {
        closeout_files.push(path);
    }
    if let Some(path) = write_lockbox_markdown(plan, report)? {
        closeout_files.push(path);
    }

    write_forward_manifest(
        plan,
        args,
        report,
        &completed_at,
        &prepared_sha256,
        &formulas_sha256,
    )?;
    write_forward_summary(plan, args, report, &closeout_files, &completed_at)?;
    let checksums = write_forward_checksums(plan, &closeout_files)?;
    let lockbox_attempt_number = lockbox_attempt_number(plan, args, report)?;

    if let Some(registry_dir) = &plan.registry_dir {
        fs::create_dir_all(registry_dir)
            .with_context(|| format!("failed to create {}", registry_dir.display()))?;

        let artifact_files = closeout_files
            .iter()
            .filter_map(|path| relative_to(&plan.output_dir, path))
            .collect::<Vec<_>>();
        let record =
            ForwardRegistryRecord {
                schema_version: REGISTRY_SCHEMA_VERSION,
                run_kind: RunKind::ForwardTest,
                run_id: &plan.run_id,
                dataset_id: &plan.dataset_id,
                target: &plan.target,
                cutoff: plan.cutoff.as_deref().unwrap_or("n/a"),
                started_at: &plan.created_at,
                completed_at: &completed_at,
                git_sha: plan.git_sha.as_deref(),
                git_short_sha: plan.git_short_sha.as_deref(),
                artifact_uri: plan.artifact_uri.as_deref(),
                run_path: path_for_json(&plan.run_path),
                command_sha256: sha256_bytes(plan.command_line.as_bytes()),
                prepared_sha256,
                formulas_sha256,
                top_pre_calmar: report
                    .pre
                    .results
                    .first()
                    .map(|row| forward_top_result_record("pre sorted by calmar_equity", row)),
                top_post_ranked: report.post.results.first().map(|row| {
                    forward_top_result_record("post sorted by selected rank metric", row)
                }),
                top_post_total_r: best_total_return(&report.post.results)
                    .map(|row| forward_top_result_record("post sorted by total_return_r", row)),
                selected_formula_sha256: report
                    .selection
                    .as_ref()
                    .and_then(|selection| selection.selected.as_ref())
                    .map(|selected| sha256_text(&selected.formula)),
                selected_pre_rank: report
                    .selection
                    .as_ref()
                    .and_then(|selection| selection.selected.as_ref())
                    .map(|selected| selected.pre_rank),
                selected_post_rank: report
                    .selection
                    .as_ref()
                    .and_then(|selection| selection.selected.as_ref())
                    .and_then(|selected| selected.post_rank),
                selection_status: report
                    .selection
                    .as_ref()
                    .and_then(|selection| selection.selected.as_ref())
                    .map(|selected| format!("{:?}", selected.status)),
                diagnostic_top_post_formula_sha256: report
                    .selection
                    .as_ref()
                    .and_then(|selection| selection.diagnostic_top_post.as_ref())
                    .map(|diagnostic| sha256_text(&diagnostic.formula)),
                stage: report.stage,
                strict_protocol: report
                    .strict_protocol
                    .as_ref()
                    .map(|validation| validation.strict)
                    .unwrap_or(false),
                protocol_sha256: report
                    .strict_protocol
                    .as_ref()
                    .and_then(|validation| validation.protocol_sha256.clone()),
                formula_export_manifest_sha256: report
                    .strict_protocol
                    .as_ref()
                    .and_then(|validation| validation.formula_export_manifest_sha256.clone()),
                lockbox_attempt_number,
                lockbox_status: lockbox_status(report, lockbox_attempt_number),
                overfit_status: report
                    .overfit
                    .as_ref()
                    .map(|row| format!("{:?}", row.status)),
                stress_status: report
                    .stress
                    .as_ref()
                    .map(|row| format!("{:?}", row.status)),
                pbo: report.overfit.as_ref().and_then(|row| row.pbo),
                dsr: report.overfit.as_ref().and_then(|row| row.dsr),
                psr: report.overfit.as_ref().and_then(|row| row.psr),
                effective_trials: report.overfit.as_ref().map(|row| row.effective_trials),
                checksum_file: checksums
                    .strip_prefix(&plan.output_dir)
                    .unwrap_or(&checksums)
                    .display()
                    .to_string(),
                artifact_files,
            };
        let path = registry_dir.join(&plan.run_path).with_extension("json");
        write_json_atomic(&path, &record)?;
    }

    Ok(())
}

fn write_forward_manifest(
    plan: &StandardOutputPlan,
    args: &EvalFormulasArgs,
    report: &FormulaEvaluationReport,
    completed_at: &str,
    prepared_sha256: &str,
    formulas_sha256: &str,
) -> Result<()> {
    let manifest = ForwardManifest {
        schema_version: REGISTRY_SCHEMA_VERSION,
        run_kind: RunKind::ForwardTest,
        run_id: &plan.run_id,
        dataset_id: &plan.dataset_id,
        target: &plan.target,
        cutoff: plan.cutoff.as_deref().unwrap_or("n/a"),
        created_at: &plan.created_at,
        completed_at,
        git_sha: plan.git_sha.as_deref(),
        prepared_sha256: prepared_sha256.to_string(),
        formulas_sha256: formulas_sha256.to_string(),
        rr_column: args.rr_column.as_deref(),
        stacking_mode: format!("{:?}", args.stacking_mode),
        position_sizing: format!("{:?}", args.position_sizing),
        rank_by: format!("{:?}", args.rank_by),
        frs_enabled: !args.no_frs,
        frs_scope: format!("{:?}", args.frs_scope),
        selection_mode: format!("{:?}", args.selection_mode),
        candidate_top_k: args.candidate_top_k,
        purge_cross_boundary_exits: !args.no_purge_cross_boundary_exits,
        embargo_bars: args.embargo_bars,
        plot_enabled: args.plot,
        plot_mode: format!("{:?}", args.plot_mode),
        stage: report.stage,
        strict_protocol: report
            .strict_protocol
            .as_ref()
            .map(|validation| validation.strict)
            .unwrap_or(false),
        protocol_sha256: report
            .strict_protocol
            .as_ref()
            .and_then(|validation| validation.protocol_sha256.clone()),
        formula_export_manifest_sha256: report
            .strict_protocol
            .as_ref()
            .and_then(|validation| validation.formula_export_manifest_sha256.clone()),
        overfit_status: report
            .overfit
            .as_ref()
            .map(|row| format!("{:?}", row.status)),
        stress_status: report
            .stress
            .as_ref()
            .map(|row| format!("{:?}", row.status)),
    };
    write_json_atomic(&plan.output_dir.join("run_manifest.json"), &manifest)
}

fn write_forward_selection_markdown(
    plan: &StandardOutputPlan,
    report: &FormulaEvaluationReport,
) -> Result<Option<PathBuf>> {
    let Some(selection) = &report.selection else {
        return Ok(None);
    };

    let mut body = String::new();
    body.push_str("# Barsmith Selection Report\n\n");
    body.push_str(&format!("- Mode: `{:?}`\n", selection.mode));
    body.push_str(&format!(
        "- Candidate cap: `{}`\n",
        selection.policy.candidate_top_k
    ));
    body.push_str(&format!(
        "- Pre trade floor: `{}`\n",
        selection.policy.pre_min_trades
    ));
    body.push_str(&format!(
        "- Post trade floor: `{}`\n",
        selection.policy.post_min_trades
    ));
    body.push_str(&format!(
        "- Post trade warning floor: `{}`\n",
        selection.policy.post_warn_below_trades
    ));
    body.push_str(&format!(
        "- Purge cross-boundary exits: `{}`\n",
        selection.policy.purge_cross_boundary_exits
    ));
    body.push_str(&format!(
        "- Embargo bars: `{}`\n\n",
        selection.policy.embargo_bars
    ));

    body.push_str("## Decision\n\n");
    if let Some(selected) = &selection.selected {
        body.push_str(&format!(
            "- Selected formula SHA-256: `{}`\n",
            sha256_text(&selected.formula)
        ));
        body.push_str(&format!("- Source rank: `{}`\n", selected.source_rank));
        body.push_str(&format!("- Pre rank: `{}`\n", selected.pre_rank));
        body.push_str(&format!(
            "- Post rank: `{}`\n",
            selected
                .post_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        ));
        body.push_str(&format!("- Pre trades: `{}`\n", selected.pre_trades));
        body.push_str(&format!(
            "- Post trades: `{}`\n",
            selected
                .post_trades
                .map(|trades| trades.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        ));
        body.push_str(&format!("- Pre Total R: `{:.4}`\n", selected.pre_total_r));
        body.push_str(&format!(
            "- Post Total R: `{}`\n",
            selected
                .post_total_r
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "n/a".to_string())
        ));
        body.push('\n');
        body.push_str("```text\n");
        body.push_str(&selected.formula);
        body.push_str("\n```\n");
    } else {
        body.push_str("No formula passed the configured selection gates.\n");
    }

    if let Some(diagnostic) = &selection.diagnostic_top_post {
        body.push_str("\n## Diagnostic Top Post\n\n");
        body.push_str(&format!(
            "- Formula SHA-256: `{}`\n",
            sha256_text(&diagnostic.formula)
        ));
        body.push_str(&format!("- Post rank: `{}`\n", diagnostic.post_rank));
        body.push_str(&format!(
            "- Post Total R: `{:.4}`\n",
            diagnostic.post_total_r
        ));
        body.push_str(
            "- Note: this is a diagnostic row only unless `selection-mode` is `validation-rank`.\n",
        );
    }

    if !selection.warnings.is_empty() {
        body.push_str("\n## Warnings\n\n");
        for warning in &selection.warnings {
            body.push_str(&format!("- {warning}\n"));
        }
    }

    body.push_str("\n## Candidate Decisions\n\n");
    body.push_str("| Pre Rank | Post Rank | Status | Reasons | Formula SHA-256 |\n");
    body.push_str("| ---: | ---: | --- | --- | --- |\n");
    for decision in &selection.decisions {
        let post_rank = decision
            .post_rank
            .map(|rank| rank.to_string())
            .unwrap_or_else(|| "n/a".to_string());
        let reasons = if decision.reasons.is_empty() {
            "pass".to_string()
        } else {
            decision
                .reasons
                .iter()
                .map(|reason| reason.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        body.push_str(&format!(
            "| {} | {} | `{:?}` | {} | `{}` |\n",
            decision.pre_rank,
            post_rank,
            decision.status,
            reasons,
            sha256_text(&decision.formula)
        ));
    }

    let path = plan.output_dir.join("reports").join("selection.md");
    fs::write(&path, body).with_context(|| "failed to write reports/selection.md")?;
    Ok(Some(path))
}

fn write_overfit_markdown(
    plan: &StandardOutputPlan,
    report: &FormulaEvaluationReport,
) -> Result<Option<PathBuf>> {
    let Some(overfit) = &report.overfit else {
        return Ok(None);
    };
    let mut body = String::new();
    body.push_str("# Barsmith Overfit Diagnostics\n\n");
    body.push_str(&format!("- Status: `{:?}`\n", overfit.status));
    body.push_str(&format!(
        "- Candidate count: `{}`\n",
        overfit.candidate_count
    ));
    body.push_str(&format!(
        "- Effective trials: `{}`\n",
        overfit.effective_trials
    ));
    body.push_str(&format!(
        "- CSCV blocks: `{}` of requested `{}`\n",
        overfit.cscv_blocks_applied, overfit.cscv_blocks_requested
    ));
    body.push_str(&format!("- CSCV splits: `{}`\n", overfit.cscv_splits));
    body.push_str(&format!("- PBO: `{}`\n", optional_metric(overfit.pbo)));
    body.push_str(&format!("- PSR: `{}`\n", optional_metric(overfit.psr)));
    body.push_str(&format!("- DSR: `{}`\n", optional_metric(overfit.dsr)));
    body.push_str(&format!(
        "- Positive block ratio: `{}`\n",
        optional_metric(overfit.selected_positive_window_ratio)
    ));
    if let Some(hash) = overfit.selected_formula_sha256.as_deref() {
        body.push_str(&format!("- Selected formula SHA-256: `{hash}`\n"));
    }
    if !overfit.warnings.is_empty() {
        body.push_str("\n## Warnings\n\n");
        for warning in &overfit.warnings {
            body.push_str(&format!("- {warning}\n"));
        }
    }
    body.push_str("\n## CSCV Decisions\n\n");
    body.push_str("| Split | Test Rank | Percentile | Logit | Overfit | Formula SHA-256 |\n");
    body.push_str("| ---: | ---: | ---: | ---: | --- | --- |\n");
    for decision in &overfit.decisions {
        body.push_str(&format!(
            "| {} | {} | {:.4} | {:.4} | `{}` | `{}` |\n",
            decision.split_index,
            decision.test_rank,
            decision.test_percentile,
            decision.logit,
            decision.overfit,
            sha256_text(&decision.selected_formula)
        ));
    }

    let path = plan.output_dir.join("reports").join("overfit.md");
    fs::write(&path, body).with_context(|| "failed to write reports/overfit.md")?;
    Ok(Some(path))
}

fn write_stress_markdown(
    plan: &StandardOutputPlan,
    report: &FormulaEvaluationReport,
) -> Result<Option<PathBuf>> {
    let Some(stress) = &report.stress else {
        return Ok(None);
    };
    let mut body = String::new();
    body.push_str("# Barsmith Stress Diagnostics\n\n");
    body.push_str(&format!("- Status: `{:?}`\n", stress.status));
    if let Some(hash) = stress.selected_formula_sha256.as_deref() {
        body.push_str(&format!("- Selected formula SHA-256: `{hash}`\n"));
    }
    body.push_str("\n## Scenarios\n\n");
    body.push_str("| Scenario | Cost Multiplier | Extra R | Extra $ | Max Contracts | Post Total R | Post Expectancy | Pass |\n");
    body.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
    for scenario in &stress.scenarios {
        let max_contracts = scenario
            .max_contracts_override
            .map(|value| value.to_string())
            .unwrap_or_else(|| "base".to_string());
        body.push_str(&format!(
            "| `{}` | {:.2} | {:.4} | {:.2} | {} | {:.4} | {:.4} | `{}` |\n",
            scenario.scenario,
            scenario.cost_multiplier,
            scenario.extra_cost_per_trade_r,
            scenario.extra_cost_per_trade_dollar,
            max_contracts,
            scenario.post_total_r,
            scenario.post_expectancy,
            scenario.pass
        ));
    }
    if !stress.warnings.is_empty() {
        body.push_str("\n## Warnings\n\n");
        for warning in &stress.warnings {
            body.push_str(&format!("- {warning}\n"));
        }
    }

    let path = plan.output_dir.join("reports").join("stress.md");
    fs::write(&path, body).with_context(|| "failed to write reports/stress.md")?;
    Ok(Some(path))
}

fn write_lockbox_markdown(
    plan: &StandardOutputPlan,
    report: &FormulaEvaluationReport,
) -> Result<Option<PathBuf>> {
    if !report.stage.is_lockbox_like() {
        return Ok(None);
    }
    let mut body = String::new();
    body.push_str("# Barsmith Lockbox Report\n\n");
    body.push_str(&format!("- Stage: `{}`\n", report.stage.as_str()));
    body.push_str("- Lockbox evaluates one frozen formula and does not select among candidates.\n");
    if let Some(row) = report.post.results.first() {
        body.push_str(&format!(
            "- Formula SHA-256: `{}`\n",
            sha256_text(&row.formula)
        ));
        body.push_str(&format!("- Post trades: `{}`\n", row.trades));
        body.push_str(&format!(
            "- Post Total R: `{:.4}`\n",
            row.stats.total_return
        ));
        body.push_str(&format!(
            "- Post expectancy: `{:.4}`\n",
            row.stats.expectancy
        ));
        body.push_str(&format!(
            "- Post max drawdown R: `{:.4}`\n",
            row.stats.max_drawdown
        ));
    } else {
        body.push_str("No lockbox result survived the configured filters.\n");
    }

    let path = plan.output_dir.join("reports").join("lockbox.md");
    fs::write(&path, body).with_context(|| "failed to write reports/lockbox.md")?;
    Ok(Some(path))
}

fn write_forward_summary(
    plan: &StandardOutputPlan,
    args: &EvalFormulasArgs,
    report: &FormulaEvaluationReport,
    written_files: &[PathBuf],
    completed_at: &str,
) -> Result<()> {
    let mut summary = String::new();
    summary.push_str("# Barsmith Forward-Test Summary\n\n");
    summary.push_str(&format!("- Run ID: `{}`\n", plan.run_id));
    summary.push_str(&format!("- Target: `{}`\n", plan.target));
    summary.push_str(&format!("- Dataset ID: `{}`\n", plan.dataset_id));
    summary.push_str(&format!(
        "- Cutoff: `{}`\n",
        plan.cutoff.as_deref().unwrap_or("n/a")
    ));
    summary.push_str(&format!("- Started: `{}`\n", plan.created_at));
    summary.push_str(&format!("- Completed: `{completed_at}`\n"));
    if let Some(sha) = plan.git_sha.as_deref() {
        summary.push_str(&format!("- Git SHA: `{sha}`\n"));
    }
    if let Some(uri) = plan.artifact_uri.as_deref() {
        summary.push_str(&format!("- Artifact URI: `{uri}`\n"));
    }
    summary.push_str(&format!("- Rank metric: `{:?}`\n", args.rank_by));
    summary.push_str(&format!("- Stage: `{}`\n", report.stage.as_str()));
    summary.push_str(&format!(
        "- Strict protocol: `{}`\n",
        report
            .strict_protocol
            .as_ref()
            .map(|validation| validation.strict)
            .unwrap_or(false)
    ));
    summary.push_str(&format!("- FRS enabled: `{}`\n", !args.no_frs));
    summary.push_str(&format!(
        "- Position sizing: `{:?}`\n",
        args.position_sizing
    ));
    summary.push_str(&format!(
        "- Pre rows: `{}` | Post rows: `{}`\n",
        report.pre.rows, report.post.rows
    ));

    push_forward_result(
        &mut summary,
        "Top Pre Result",
        "pre sorted by calmar_equity",
        report.pre.results.first(),
    );
    push_forward_result(
        &mut summary,
        "Top Post Result",
        "post sorted by selected rank metric",
        report.post.results.first(),
    );
    push_forward_result(
        &mut summary,
        "Best Post Total R",
        "post sorted by total_return_r",
        best_total_return(&report.post.results),
    );
    push_selection_summary(&mut summary, report);
    push_overfit_summary(&mut summary, report);
    push_stress_summary(&mut summary, report);

    let relative_files = written_files
        .iter()
        .filter_map(|path| relative_to(&plan.output_dir, path))
        .collect::<Vec<_>>();
    if !relative_files.is_empty() {
        summary.push_str("\n## Output Files\n\n");
        for path in relative_files {
            summary.push_str(&format!("- `{path}`\n"));
        }
    }

    fs::write(plan.output_dir.join("reports").join("summary.md"), summary)
        .with_context(|| "failed to write reports/summary.md")
}

fn push_selection_summary(summary: &mut String, report: &FormulaEvaluationReport) {
    summary.push_str("\n## Selection Decision\n\n");
    let Some(selection) = &report.selection else {
        summary.push_str("Selection mode was disabled.\n");
        return;
    };
    summary.push_str(&format!("- Mode: `{:?}`\n", selection.mode));
    summary.push_str(&format!(
        "- Candidate cap: `{}`\n",
        selection.policy.candidate_top_k
    ));
    summary.push_str(&format!(
        "- Purge cross-boundary exits: `{}`\n",
        selection.policy.purge_cross_boundary_exits
    ));
    summary.push_str(&format!(
        "- Embargo bars: `{}`\n",
        selection.policy.embargo_bars
    ));
    if let Some(selected) = &selection.selected {
        summary.push_str(&format!(
            "- Selected formula SHA-256: `{}`\n",
            sha256_text(&selected.formula)
        ));
        summary.push_str(&format!("- Pre rank: `{}`\n", selected.pre_rank));
        summary.push_str(&format!(
            "- Post rank: `{}`\n",
            selected
                .post_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        ));
        summary.push_str(&format!("- Pre Total R: `{:.4}`\n", selected.pre_total_r));
        summary.push_str(&format!(
            "- Post Total R: `{}`\n",
            selected
                .post_total_r
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "n/a".to_string())
        ));
    } else {
        summary.push_str("No formula passed the configured selection gates.\n");
    }
    if let Some(diagnostic) = &selection.diagnostic_top_post {
        summary.push_str(&format!(
            "- Diagnostic top-post formula SHA-256: `{}`\n",
            sha256_text(&diagnostic.formula)
        ));
        summary.push_str(
            "- Diagnostic note: this row is not the selected strategy in holdout mode.\n",
        );
    }
    for warning in &selection.warnings {
        summary.push_str(&format!("- Warning: `{warning}`\n"));
    }
}

fn push_overfit_summary(summary: &mut String, report: &FormulaEvaluationReport) {
    let Some(overfit) = &report.overfit else {
        return;
    };
    summary.push_str("\n## Overfit Diagnostics\n\n");
    summary.push_str(&format!("- Status: `{:?}`\n", overfit.status));
    summary.push_str(&format!("- Candidates: `{}`\n", overfit.candidate_count));
    summary.push_str(&format!(
        "- Effective trials: `{}`\n",
        overfit.effective_trials
    ));
    summary.push_str(&format!("- CSCV splits: `{}`\n", overfit.cscv_splits));
    summary.push_str(&format!("- PBO: `{}`\n", optional_metric(overfit.pbo)));
    summary.push_str(&format!("- PSR: `{}`\n", optional_metric(overfit.psr)));
    summary.push_str(&format!("- DSR: `{}`\n", optional_metric(overfit.dsr)));
    summary.push_str(&format!(
        "- Positive block ratio: `{}`\n",
        optional_metric(overfit.selected_positive_window_ratio)
    ));
    for warning in &overfit.warnings {
        summary.push_str(&format!("- Warning: `{warning}`\n"));
    }
}

fn push_stress_summary(summary: &mut String, report: &FormulaEvaluationReport) {
    let Some(stress) = &report.stress else {
        return;
    };
    summary.push_str("\n## Stress Diagnostics\n\n");
    summary.push_str(&format!("- Status: `{:?}`\n", stress.status));
    summary.push_str(&format!("- Scenarios: `{}`\n", stress.scenarios.len()));
    for scenario in &stress.scenarios {
        let max_contracts = scenario
            .max_contracts_override
            .map(|value| value.to_string())
            .unwrap_or_else(|| "base".to_string());
        summary.push_str(&format!(
            "- `{}`: max contracts `{}`, post Total R `{:.4}`, post expectancy `{:.4}`, pass `{}`\n",
            scenario.scenario,
            max_contracts,
            scenario.post_total_r,
            scenario.post_expectancy,
            scenario.pass
        ));
    }
    for warning in &stress.warnings {
        summary.push_str(&format!("- Warning: `{warning}`\n"));
    }
}

fn push_forward_result(
    summary: &mut String,
    heading: &str,
    metric_basis: &str,
    result: Option<&FormulaResult>,
) {
    summary.push_str(&format!("\n## {heading}\n\n"));
    let Some(row) = result else {
        summary.push_str("No formula matched the configured filters.\n");
        return;
    };
    summary.push_str(&format!("- Metric basis: `{metric_basis}`\n"));
    summary.push_str(&format!(
        "- Formula SHA-256: `{}`\n",
        sha256_text(&row.formula)
    ));
    summary.push_str(&format!("- Source rank: `{}`\n", row.source_rank));
    summary.push_str(&format!("- Display rank: `{}`\n", row.display_rank));
    summary.push_str(&format!("- Trades: `{}`\n", row.trades));
    summary.push_str(&format!("- Mask hits: `{}`\n", row.mask_hits));
    summary.push_str(&format!("- Win rate: `{:.2}%`\n", row.stats.win_rate));
    summary.push_str(&format!("- Total R: `{:.4}`\n", row.stats.total_return));
    summary.push_str(&format!(
        "- Max drawdown R: `{:.4}`\n",
        row.stats.max_drawdown
    ));
    summary.push_str(&format!(
        "- Calmar equity: `{:.4}`\n",
        row.stats.calmar_equity
    ));
    if let Some(frs) = row.frs {
        summary.push_str(&format!("- FRS: `{:.4}`\n", frs.frs));
    }
}

fn write_forward_checksums(
    plan: &StandardOutputPlan,
    written_files: &[PathBuf],
) -> Result<PathBuf> {
    let mut paths = vec![
        plan.output_dir.join("command.txt"),
        plan.output_dir.join("command.json"),
        plan.output_dir.join("run_manifest.json"),
        plan.output_dir.join("reports").join("summary.md"),
    ];
    if plan.checksum_artifacts {
        paths.extend(
            written_files
                .iter()
                .filter(|path| path.starts_with(&plan.output_dir))
                .cloned(),
        );
        paths.push(plan.output_dir.join("barsmith.log"));
    }

    write_checksum_file(plan, paths)
}

fn query_top_result(config: &Config, rank_by: ResultRankBy) -> Result<Option<ResultRow>> {
    let query = ResultQuery {
        output_dir: config.output_dir.clone(),
        direction: format_config_direction(config.direction).to_string(),
        target: config.target.clone(),
        min_sample_size: config.min_sample_size_report.max(1),
        min_win_rate: 0.0,
        max_drawdown: config.max_drawdown_report.unwrap_or(config.max_drawdown),
        min_calmar: config.min_calmar_report,
        rank_by,
        limit: 1,
    };
    Ok(query_result_store(&query)?.into_iter().next())
}

fn write_summary(
    plan: &StandardOutputPlan,
    config: &Config,
    top_calmar: Option<&ResultRow>,
    top_total_r: Option<&ResultRow>,
    completed_at: &str,
) -> Result<()> {
    let mut summary = String::new();
    summary.push_str("# Barsmith Run Summary\n\n");
    summary.push_str(&format!("- Run ID: `{}`\n", plan.run_id));
    summary.push_str(&format!("- Target: `{}`\n", plan.target));
    summary.push_str(&format!("- Direction: `{}`\n", plan.direction));
    summary.push_str(&format!("- Dataset ID: `{}`\n", plan.dataset_id));
    summary.push_str(&format!("- Started: `{}`\n", plan.created_at));
    summary.push_str(&format!("- Completed: `{completed_at}`\n"));
    if let Some(sha) = plan.git_sha.as_deref() {
        summary.push_str(&format!("- Git SHA: `{sha}`\n"));
    }
    if let Some(uri) = plan.artifact_uri.as_deref() {
        summary.push_str(&format!("- Artifact URI: `{uri}`\n"));
    }
    summary.push_str(&format!(
        "- Output directory: `{}`\n",
        plan.output_dir.display()
    ));
    summary.push_str(&format!(
        "- Report filters: min_samples={}, max_drawdown={}, min_calmar={}\n",
        config.min_sample_size_report.max(1),
        config.max_drawdown_report.unwrap_or(config.max_drawdown),
        config
            .min_calmar_report
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    ));
    summary.push('\n');

    push_comb_result(
        &mut summary,
        "Top Stored Result By Calmar",
        "stored results ordered by calmar_ratio",
        top_calmar,
    );
    push_comb_result(
        &mut summary,
        "Top Stored Result By Total R",
        "stored results ordered by total_return",
        top_total_r,
    );

    fs::write(plan.output_dir.join("reports").join("summary.md"), summary)
        .with_context(|| "failed to write reports/summary.md")
}

fn push_comb_result(
    summary: &mut String,
    heading: &str,
    metric_basis: &str,
    result: Option<&ResultRow>,
) {
    summary.push_str(&format!("## {heading}\n\n"));
    let Some(row) = result else {
        summary.push_str("No stored result rows matched the report filters.\n\n");
        return;
    };
    summary.push_str(&format!("- Metric basis: `{metric_basis}`\n"));
    summary.push_str(&format!(
        "- Formula SHA-256: `{}`\n",
        sha256_text(&row.combination)
    ));
    summary.push_str(&format!("- Depth: `{}`\n", row.depth));
    summary.push_str(&format!("- Total bars/trades: `{}`\n", row.total_bars));
    summary.push_str(&format!("- Profitable bars: `{}`\n", row.profitable_bars));
    summary.push_str(&format!("- Win rate: `{:.2}%`\n", row.win_rate));
    summary.push_str(&format!("- Total R: `{:.4}`\n", row.total_return));
    summary.push_str(&format!("- Max drawdown R: `{:.4}`\n", row.max_drawdown));
    summary.push_str(&format!("- Calmar ratio: `{:.4}`\n", row.calmar_ratio));
    summary.push_str(&format!("- Resume offset: `{}`\n\n", row.resume_offset));
}

fn write_checksums(plan: &StandardOutputPlan) -> Result<PathBuf> {
    let mut paths = vec![
        plan.output_dir.join("command.txt"),
        plan.output_dir.join("command.json"),
        plan.output_dir.join("run_manifest.json"),
        plan.output_dir.join("reports").join("summary.md"),
    ];
    if plan.checksum_artifacts {
        paths.push(plan.output_dir.join("cumulative.duckdb"));
        paths.push(plan.output_dir.join("barsmith.log"));
        let results_dir = plan.output_dir.join("results_parquet");
        if results_dir.exists() {
            let mut parquet_parts = Vec::new();
            for entry in fs::read_dir(&results_dir)? {
                let path = entry?.path();
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("part-") && name.ends_with(".parquet"))
                {
                    parquet_parts.push(path);
                }
            }
            parquet_parts.sort();
            paths.extend(parquet_parts);
        }
    }

    write_checksum_file(plan, paths)
}

fn write_checksum_file(plan: &StandardOutputPlan, paths: Vec<PathBuf>) -> Result<PathBuf> {
    let mut lines = String::new();
    for path in paths {
        if path.is_file() {
            let digest = sha256_file(&path)?;
            let rel = path
                .strip_prefix(&plan.output_dir)
                .unwrap_or(&path)
                .display()
                .to_string();
            lines.push_str(&format!("{digest}  {rel}\n"));
        }
    }

    let checksum_path = plan.output_dir.join("checksums.sha256");
    fs::write(&checksum_path, lines).with_context(|| "failed to write checksums.sha256")?;
    Ok(checksum_path)
}

fn top_result_record(metric_basis: &'static str, row: &ResultRow) -> TopResultRecord {
    TopResultRecord {
        metric_basis,
        formula_sha256: sha256_text(&row.combination),
        depth: row.depth,
        total_bars: row.total_bars,
        profitable_bars: row.profitable_bars,
        win_rate: row.win_rate,
        total_return_r: row.total_return,
        max_drawdown_r: row.max_drawdown,
        calmar_ratio: row.calmar_ratio,
        resume_offset: row.resume_offset,
    }
}

fn forward_top_result_record(
    metric_basis: &'static str,
    row: &FormulaResult,
) -> ForwardTopResultRecord {
    ForwardTopResultRecord {
        metric_basis,
        formula_sha256: sha256_text(&row.formula),
        source_rank: row.source_rank,
        display_rank: row.display_rank,
        previous_rank: row.previous_rank,
        trades: row.trades,
        mask_hits: row.mask_hits,
        win_rate: row.stats.win_rate,
        total_return_r: row.stats.total_return,
        max_drawdown_r: row.stats.max_drawdown,
        calmar_equity: row.stats.calmar_equity,
        frs: row.frs.map(|frs| frs.frs),
    }
}

fn best_total_return(results: &[FormulaResult]) -> Option<&FormulaResult> {
    results
        .iter()
        .max_by(|left, right| left.stats.total_return.total_cmp(&right.stats.total_return))
}

fn lockbox_attempt_number(
    plan: &StandardOutputPlan,
    args: &EvalFormulasArgs,
    report: &FormulaEvaluationReport,
) -> Result<Option<usize>> {
    if !report.stage.is_lockbox_like() {
        return Ok(None);
    }
    let Some(registry_dir) = &plan.registry_dir else {
        return Ok(None);
    };
    let Some(protocol_sha256) = report
        .strict_protocol
        .as_ref()
        .and_then(|validation| validation.protocol_sha256.as_deref())
    else {
        return Ok(Some(1));
    };
    let Some(formula_sha256) = report
        .post
        .results
        .first()
        .map(|row| sha256_text(&row.formula))
    else {
        return Ok(Some(1));
    };
    let previous = count_matching_lockbox_attempts(registry_dir, protocol_sha256, &formula_sha256)?;
    if previous > 0 && !args.ack_rerun_lockbox {
        return Err(anyhow!(
            "lockbox formula/protocol was already evaluated {previous} time(s); pass --ack-rerun-lockbox to record a contaminated rerun"
        ));
    }
    Ok(Some(previous + 1))
}

fn count_matching_lockbox_attempts(
    registry_dir: &Path,
    protocol_sha256: &str,
    formula_sha256: &str,
) -> Result<usize> {
    if !registry_dir.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![registry_dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in
            fs::read_dir(&path).with_context(|| format!("failed to read {}", path.display()))?
        {
            let entry_path = entry?.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
                continue;
            }
            if entry_path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(text) = fs::read_to_string(&entry_path) else {
                continue;
            };
            let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            let is_lockbox = json
                .get("stage")
                .and_then(|value| value.as_str())
                .is_some_and(|stage| stage == "lockbox" || stage == "live-shadow");
            let same_protocol = json.get("protocol_sha256").and_then(|value| value.as_str())
                == Some(protocol_sha256);
            let same_formula = json
                .get("selected_formula_sha256")
                .and_then(|value| value.as_str())
                == Some(formula_sha256);
            if is_lockbox && same_protocol && same_formula {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn lockbox_status(
    report: &FormulaEvaluationReport,
    attempt_number: Option<usize>,
) -> Option<String> {
    if !report.stage.is_lockbox_like() {
        return None;
    }
    Some(match attempt_number {
        Some(1) => "clean_first_attempt".to_string(),
        Some(_) => "acknowledged_rerun_contaminated".to_string(),
        None => "not_tracked_no_registry".to_string(),
    })
}

fn optional_metric(value: Option<f64>) -> String {
    value
        .map(format_metric)
        .unwrap_or_else(|| "n/a".to_string())
}

fn relative_to(base: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(base).ok().map(path_for_json)
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(&tmp_path, bytes)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| format!("failed to replace {}", path.display()))
}

fn dataset_id_from_csv(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(sanitize_segment)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "dataset".to_string())
}

fn dataset_id_from_prepared(path: &Path) -> String {
    let stem = path.file_stem().and_then(|stem| stem.to_str());
    if matches!(stem, Some("barsmith_prepared")) {
        if let Some(parent) = path.parent().and_then(|parent| parent.file_name()) {
            if let Some(name) = parent.to_str() {
                return sanitize_segment(name);
            }
        }
    }
    stem.map(sanitize_segment)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "dataset".to_string())
}

fn normalize_target(target: &str) -> String {
    if target == "atr_stop" {
        "2x_atr_tp_atr_stop".to_string()
    } else {
        target.to_string()
    }
}

fn sanitize_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_was_sep = false;
    for ch in raw.trim().chars() {
        let normalized = if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        if normalized == '_' {
            if !last_was_sep {
                out.push(normalized);
            }
            last_was_sep = true;
        } else {
            out.push(normalized);
            last_was_sep = false;
        }
    }
    let trimmed = out.trim_matches(['_', '.', '-']).to_string();
    if trimmed.is_empty() {
        "run".to_string()
    } else {
        trimmed
    }
}

fn shell_join(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| {
            if arg
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || "-_./:=,+".contains(ch))
            {
                arg.clone()
            } else {
                format!("'{}'", arg.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn git_rev_parse<const N: usize>(args: [&str; N]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

fn now_iso() -> String {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn now_compact() -> String {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

fn format_direction(direction: DirectionValue) -> &'static str {
    match direction {
        DirectionValue::Long => "long",
        DirectionValue::Short => "short",
        DirectionValue::Both => "both",
    }
}

fn format_config_direction(direction: barsmith_rs::config::Direction) -> &'static str {
    match direction {
        barsmith_rs::config::Direction::Long => "long",
        barsmith_rs::config::Direction::Short => "short",
        barsmith_rs::config::Direction::Both => "both",
    }
}

fn sha256_text(value: &str) -> String {
    sha256_bytes(value.as_bytes())
}

fn sha256_bytes(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn path_for_json(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn serialize_metric<S>(value: &f64, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if value.is_finite() {
        serializer.serialize_f64(*value)
    } else {
        serializer.serialize_str(&format_metric(*value))
    }
}

fn serialize_optional_metric<S>(
    value: &Option<f64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) if value.is_finite() => serializer.serialize_some(value),
        Some(value) => serializer.serialize_some(&format_metric(*value)),
        None => serializer.serialize_none(),
    }
}

fn format_metric(value: f64) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "Inf".to_string()
    } else if value.is_infinite() && value.is_sign_negative() {
        "-Inf".to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{
        EvalProfileValue, PositionSizingValue, ReportMetricsValue, StackingModeValue,
        StopDistanceUnitValue,
    };
    use crate::stats_detail::StatsDetailValue;

    fn args() -> CombArgs {
        CombArgs {
            csv_path: PathBuf::from("data/ES 30m official.csv"),
            direction: DirectionValue::Long,
            target: "2x_atr_tp_atr_stop".to_string(),
            engine: crate::cli::EngineValue::Auto,
            output_dir: None,
            runs_root: Some(PathBuf::from("runs/artifacts")),
            dataset_id: Some("ES 30m Official V2".to_string()),
            run_id: Some("Manual Run 01".to_string()),
            run_slug: None,
            registry_dir: Some(PathBuf::from("runs/registry")),
            artifact_uri: Some("s3://bucket/barsmith/run".to_string()),
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
            workers: Some(1),
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
            max_drawdown: 30.0,
            max_drawdown_report: None,
            min_calmar_report: None,
            no_file_log: false,
            subset_pruning: false,
            stats_detail: StatsDetailValue::Core,
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
    fn runs_root_builds_canonical_run_path() {
        let plan = resolve_comb_output(
            &args(),
            &[OsString::from("barsmith"), OsString::from("comb")],
        )
        .expect("plan");

        assert_eq!(plan.dataset_id, "es_30m_official_v2");
        assert_eq!(plan.run_id, "manual_run_01");
        assert_eq!(
            plan.output_dir,
            PathBuf::from(
                "runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/manual_run_01"
            )
        );
        assert_eq!(
            plan.run_path,
            PathBuf::from("comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/manual_run_01")
        );
    }

    #[test]
    fn explicit_output_dir_keeps_legacy_layout() {
        let mut args = args();
        args.output_dir = Some(PathBuf::from("tmp/out"));
        args.runs_root = None;
        let plan =
            resolve_comb_output(&args, &[OsString::from("barsmith"), OsString::from("comb")])
                .expect("plan");

        assert_eq!(plan.output_dir, PathBuf::from("tmp/out"));
    }
}
