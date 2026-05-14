use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use barsmith_rs::formula_eval::{FormulaEvaluationReport, FormulaResult};
use barsmith_rs::protocol::sha256_text;

use crate::cli::EvalFormulasArgs;

use super::StandardOutputPlan;
use super::helpers::{optional_metric, relative_to};

pub(super) fn write_forward_selection_markdown(
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

pub(super) fn write_overfit_markdown(
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

pub(super) fn write_stress_markdown(
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

pub(super) fn write_lockbox_markdown(
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

pub(super) fn write_forward_summary(
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

pub(super) fn best_total_return(results: &[FormulaResult]) -> Option<&FormulaResult> {
    results
        .iter()
        .max_by(|left, right| left.stats.total_return.total_cmp(&right.stats.total_return))
}
