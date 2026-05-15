use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use barsmith_rs::config::Config;
use barsmith_rs::formula_eval::{FormulaEvaluationReport, FormulaResult};
use barsmith_rs::storage::{ResultQuery, ResultRankBy, ResultRow, query_result_store};

use crate::cli::EvalFormulasArgs;

use super::checksums::{
    sha256_bytes, sha256_file, sha256_text, write_checksums, write_forward_checksums,
};
use super::helpers::{
    format_config_direction, now_iso, path_for_json, relative_to, write_json_atomic,
};
use super::records::{
    CommandRecord, ForwardManifest, ForwardRegistryRecord, ForwardTopResultRecord,
    REGISTRY_SCHEMA_VERSION, RegistryRecord, RunKind, StandardOutputPlan, TopResultRecord,
};
use super::reports::{
    best_total_return, write_forward_selection_markdown, write_forward_summary,
    write_lockbox_markdown, write_overfit_markdown, write_stress_markdown,
};

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
    let lockbox_attempt_number = lockbox_attempt_number(plan, args, report)?;
    let workflow_status = workflow_status(report, lockbox_attempt_number);
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
        &workflow_status,
    )?;
    write_forward_summary(
        plan,
        args,
        report,
        &closeout_files,
        &completed_at,
        &workflow_status,
    )?;
    let checksums = write_forward_checksums(plan, &closeout_files)?;

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
                workflow_status: workflow_status.clone(),
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
    workflow_status: &str,
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
        workflow_status: workflow_status.to_string(),
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

fn workflow_status(
    report: &FormulaEvaluationReport,
    lockbox_attempt_number: Option<usize>,
) -> String {
    if report.stage.is_lockbox_like() {
        if lockbox_attempt_number.is_some_and(|attempt| attempt > 1) {
            return "lockbox-contaminated-rerun".to_string();
        }
        return if !report.post.results.is_empty() {
            "lockbox-pass".to_string()
        } else {
            "lockbox-fail".to_string()
        };
    }

    if report
        .selection
        .as_ref()
        .and_then(|selection| selection.selected.as_ref())
        .is_some()
    {
        "validation-selected-for-lockbox".to_string()
    } else {
        "validation-rejected".to_string()
    }
}
