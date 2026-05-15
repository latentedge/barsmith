use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use barsmith_rs::asset::find_asset;
use barsmith_rs::config::PositionSizingMode;
use barsmith_rs::formula::parse_ranked_formulas;
use barsmith_rs::formula_eval::{
    FormulaEvalRequest, FormulaEvaluationReport, FormulaResult, FormulaWindowReport,
    equity_curve_rows, run_formula_evaluation,
};
use barsmith_rs::frs::{ForwardRobustnessComponents, FrsOptions};
use barsmith_rs::overfit::OverfitOptions;
use barsmith_rs::protocol::{
    FormulaExportManifest, ResearchProtocol, ResearchStage, load_json,
    validate_strict_research_inputs,
};
use barsmith_rs::selection::{
    RejectionReason, SelectionDecision, SelectionPolicy, SelectionReport,
};
use barsmith_rs::stress::StressOptions;
use chrono::NaiveDate;
use serde::Serialize;

use crate::cli::EvalFormulasArgs;
#[cfg(feature = "plotting")]
use crate::plot;
use crate::target_semantics::{
    inferred_stop_distance_column, normalize_target, reject_ambiguous_direction_label,
};

pub struct EvalRunResult {
    pub report: FormulaEvaluationReport,
    pub written_files: Vec<PathBuf>,
}

pub fn run(args: &EvalFormulasArgs) -> Result<EvalRunResult> {
    let request = build_request(args)?;
    let report = run_formula_evaluation(&request)?;
    let mut written_files = Vec::new();

    print_report(&report, args.report_top);

    if let Some(path) = &args.csv_out {
        write_formula_results_csv(path, &report)?;
        written_files.push(path.clone());
        println!("Formula results written: {}", path.display());
    }
    if let Some(path) = &args.json_out {
        write_json(path, &report)?;
        written_files.push(path.clone());
        println!("Formula JSON written: {}", path.display());
    }
    if let (Some(path), Some(selection)) = (&args.selection_out, &report.selection) {
        write_json(path, selection)?;
        written_files.push(path.clone());
        println!("Selection report written: {}", path.display());
    }
    if let (Some(path), Some(selection)) = (&args.selection_decisions_out, &report.selection) {
        write_selection_decisions_csv(path, selection)?;
        written_files.push(path.clone());
        println!("Selection decisions written: {}", path.display());
    }
    if let (Some(path), Some(selection)) = (&args.selected_formulas_out, &report.selection) {
        write_selected_formulas(path, selection)?;
        written_files.push(path.clone());
        println!("Selected formulas written: {}", path.display());
    }
    if let (Some(path), Some(validation)) = (&args.protocol_validation_out, &report.strict_protocol)
    {
        write_json(path, validation)?;
        written_files.push(path.clone());
        println!("Protocol validation written: {}", path.display());
    }
    if let Some(path) = &args.frs_out {
        write_frs_summary_csv(path, &report)?;
        written_files.push(path.clone());
        println!("FRS summary written: {}", path.display());
    }
    if let Some(path) = &args.frs_windows_out {
        write_csv(path, &report.frs_window_rows)?;
        written_files.push(path.clone());
        println!("FRS windows written: {}", path.display());
    }
    if let (Some(path), Some(overfit)) = (&args.overfit_out, &report.overfit) {
        write_json(path, overfit)?;
        written_files.push(path.clone());
        println!("Overfit report written: {}", path.display());
    }
    if let (Some(path), Some(overfit)) = (&args.overfit_decisions_out, &report.overfit) {
        write_overfit_decisions_csv(path, overfit)?;
        written_files.push(path.clone());
        println!("Overfit decisions written: {}", path.display());
    }
    if let (Some(path), Some(stress)) = (&args.stress_out, &report.stress) {
        write_json(path, stress)?;
        written_files.push(path.clone());
        println!("Stress report written: {}", path.display());
    }
    if let (Some(path), Some(stress)) = (&args.stress_matrix_out, &report.stress) {
        write_csv(path, &stress.scenarios)?;
        written_files.push(path.clone());
        println!("Stress matrix written: {}", path.display());
    }

    let needs_curves = args.equity_curves_out.is_some() || args.plot;
    if needs_curves {
        let rank_source = args
            .equity_curves_rank_by
            .to_rank_by(args.rank_by.to_rank_by());
        let curves = equity_curve_rows(
            &report,
            &request,
            rank_source,
            args.equity_curves_window.to_selection(),
            args.equity_curves_top_k,
        )?;
        if curves.is_empty() {
            println!("No equity curve rows to export.");
        } else {
            if let Some(path) = &args.equity_curves_out {
                write_csv(path, &curves)?;
                written_files.push(path.clone());
                println!("Equity curves written: {}", path.display());
            }
            if args.plot {
                #[cfg(feature = "plotting")]
                written_files.extend(plot::render_plots(&curves, args)?);
                #[cfg(not(feature = "plotting"))]
                return Err(anyhow!(
                    "plot rendering is not available in this build; rebuild barsmith_cli with the default `plotting` feature"
                ));
            }
        }
    }

    Ok(EvalRunResult {
        report,
        written_files,
    })
}

fn build_request(args: &EvalFormulasArgs) -> Result<FormulaEvalRequest> {
    let formulas_text = fs::read_to_string(&args.formulas_path)
        .with_context(|| format!("failed to read {}", args.formulas_path.display()))?;
    let formulas = parse_ranked_formulas(&formulas_text)?;
    let cutoff = NaiveDate::parse_from_str(&args.cutoff, "%Y-%m-%d")
        .with_context(|| format!("invalid --cutoff '{}'", args.cutoff))?;
    let stage = args.stage.to_stage();
    validate_stage_formula_rules(stage, args, formulas.len())?;
    let protocol: Option<ResearchProtocol> = args
        .research_protocol
        .as_deref()
        .map(load_json)
        .transpose()?;
    let formula_manifest: Option<FormulaExportManifest> = args
        .formula_export_manifest
        .as_deref()
        .map(load_json)
        .transpose()?;
    let target = normalize_target(&args.target);
    reject_ambiguous_direction_label(
        &target,
        protocol
            .as_ref()
            .and_then(|protocol| protocol.direction.as_deref()),
    )?;
    reject_ambiguous_direction_label(
        &target,
        formula_manifest
            .as_ref()
            .map(|manifest| manifest.direction.as_str()),
    )?;
    let strict_protocol = Some(validate_strict_research_inputs(
        stage,
        args.strict_protocol,
        protocol.as_ref(),
        formula_manifest.as_ref(),
        &target,
        cutoff,
    )?);
    let effective_trials =
        resolve_effective_trials(args, formula_manifest.as_ref(), formulas.len());

    let position_sizing = args.position_sizing.to_mode();
    let stop_distance_column = if position_sizing == PositionSizingMode::Contracts {
        args.stop_distance_column
            .clone()
            .or_else(|| inferred_stop_distance_column(&args.target))
    } else {
        None
    };

    if position_sizing == PositionSizingMode::Contracts && args.asset.is_none() {
        return Err(anyhow!(
            "--position-sizing contracts requires --asset so point/tick values are known"
        ));
    }
    if position_sizing == PositionSizingMode::Contracts && stop_distance_column.is_none() {
        return Err(anyhow!(
            "--position-sizing contracts requires --stop-distance-column (or a target that infers it)"
        ));
    }

    let market = resolve_market_inputs(args)?;
    let risk_per_trade_dollar = if args.capital > 0.0 && args.risk_pct_per_trade > 0.0 {
        Some(args.capital * args.risk_pct_per_trade / 100.0)
    } else {
        None
    };
    let cost_per_trade_r = match (
        position_sizing,
        market.cost_per_trade_dollar,
        risk_per_trade_dollar,
    ) {
        (PositionSizingMode::Fractional, Some(cost), Some(risk)) if risk > 0.0 => Some(cost / risk),
        _ => None,
    };

    Ok(FormulaEvalRequest {
        prepared_path: args.prepared_path.clone(),
        formulas,
        target,
        rr_column: args.rr_column.clone(),
        cutoff,
        stacking_mode: args.stacking_mode.to_mode(),
        capital_dollar: args.capital,
        risk_pct_per_trade: args.risk_pct_per_trade,
        asset: market.asset_code,
        cost_per_trade_dollar: market.cost_per_trade_dollar,
        cost_per_trade_r,
        dollars_per_r: risk_per_trade_dollar,
        position_sizing,
        stop_distance_column,
        stop_distance_unit: args.stop_distance_unit.to_mode(),
        min_contracts: args.min_contracts.max(1),
        max_contracts: args.max_contracts,
        point_value: market.point_value,
        tick_value: market.tick_value,
        margin_per_contract_dollar: args
            .margin_per_contract_dollar
            .or(market.margin_per_contract_dollar),
        max_drawdown: args.max_drawdown,
        min_calmar: args.min_calmar,
        rank_by: args.rank_by.to_rank_by(),
        frs_enabled: !args.no_frs,
        frs_scope: args.frs_scope.to_scope(),
        frs_options: FrsOptions {
            n_min: args.frs_nmin,
            alpha: args.frs_alpha,
            beta: args.frs_beta,
            gamma: args.frs_gamma,
            delta: args.frs_delta,
        },
        selection_mode: selection_mode_for_stage(stage, args.selection_mode.to_mode()),
        selection_preset: args.selection_preset.map(|preset| preset.to_preset()),
        selection_policy: SelectionPolicy {
            candidate_top_k: args.candidate_top_k,
            pre_min_trades: args.pre_min_trades,
            post_min_trades: args.post_min_trades,
            post_warn_below_trades: args.post_warn_below_trades,
            pre_min_total_r: args.pre_min_total_r,
            post_min_total_r: args.post_min_total_r,
            pre_min_expectancy: args.pre_min_expectancy,
            post_min_expectancy: args.post_min_expectancy,
            max_drawdown_r: args.selection_max_drawdown_r,
            min_pre_frs: (!args.no_frs).then_some(args.min_pre_frs),
            max_return_degradation: Some(args.max_return_degradation),
            max_single_trade_contribution: args.max_single_trade_contribution,
            max_formula_depth: args.max_formula_depth,
            min_density_per_1000_bars: args.min_density_per_1000_bars,
            complexity_penalty: args.complexity_penalty,
            embargo_bars: args.embargo_bars,
            purge_cross_boundary_exits: !args.no_purge_cross_boundary_exits,
        },
        stage,
        strict_protocol,
        overfit_options: (args.strict_protocol || args.overfit_report).then_some(OverfitOptions {
            candidate_top_k: args.overfit_candidate_top_k,
            cscv_blocks: args.cscv_blocks,
            cscv_max_splits: args.cscv_max_splits,
            max_pbo: args.max_pbo,
            min_psr: args.min_psr,
            min_dsr: args.min_dsr,
            min_positive_window_ratio: args.min_positive_window_ratio,
            effective_trials: Some(effective_trials.value),
            effective_trials_source: Some(effective_trials.source),
            effective_trials_warning: effective_trials.warning,
            complexity_penalty: args.complexity_penalty,
        }),
        stress_options: (args.strict_protocol || args.stress_report).then_some(StressOptions {
            min_total_r: args.stress_min_total_r,
            min_expectancy: args.stress_min_expectancy,
        }),
    })
}

struct EffectiveTrialsResolution {
    value: usize,
    source: String,
    warning: Option<String>,
}

fn resolve_effective_trials(
    args: &EvalFormulasArgs,
    manifest: Option<&FormulaExportManifest>,
    formula_count: usize,
) -> EffectiveTrialsResolution {
    if let Some(value) = args.effective_trials {
        return EffectiveTrialsResolution {
            value: value.max(1),
            source: "explicit_cli".to_string(),
            warning: None,
        };
    }

    if let Some(value) = manifest
        .and_then(|manifest| manifest.source_processed_combinations)
        .filter(|value| *value > 0)
    {
        return EffectiveTrialsResolution {
            value: value.min(usize::MAX as u64) as usize,
            source: "source_processed_combinations".to_string(),
            warning: None,
        };
    }

    if let Some(value) = manifest
        .and_then(|manifest| manifest.source_stored_combinations)
        .filter(|value| *value > 0)
    {
        return EffectiveTrialsResolution {
            value: value.min(usize::MAX as u64) as usize,
            source: "source_stored_combinations".to_string(),
            warning: Some(
                "effective trials fell back to stored source rows; discarded combinations may make DSR too optimistic"
                    .to_string(),
            ),
        };
    }

    if let Some(manifest) = manifest {
        return EffectiveTrialsResolution {
            value: manifest.exported_rows.max(1),
            source: "formula_exported_rows".to_string(),
            warning: Some(
                "effective trials fell back to exported formulas; this understates the original search space when the comb run evaluated more candidates"
                    .to_string(),
            ),
        };
    }

    EffectiveTrialsResolution {
        value: formula_count.max(1),
        source: "evaluated_formula_count".to_string(),
        warning: Some(
            "effective trials used the evaluated formula count because no formula export manifest was provided"
                .to_string(),
        ),
    }
}

fn validate_stage_formula_rules(
    stage: ResearchStage,
    args: &EvalFormulasArgs,
    formula_count: usize,
) -> Result<()> {
    if stage.is_lockbox_like() && formula_count != 1 {
        return Err(anyhow!(
            "{} stage requires exactly one frozen formula; got {}",
            stage.as_str(),
            formula_count
        ));
    }
    if stage.is_lockbox_like()
        && args.selection_mode.to_mode() == barsmith_rs::selection::SelectionMode::ValidationRank
    {
        return Err(anyhow!(
            "{} stage cannot use validation-rank because it chooses by post-window performance",
            stage.as_str()
        ));
    }
    Ok(())
}

fn selection_mode_for_stage(
    stage: ResearchStage,
    requested: barsmith_rs::selection::SelectionMode,
) -> barsmith_rs::selection::SelectionMode {
    if stage.is_lockbox_like() {
        barsmith_rs::selection::SelectionMode::Off
    } else {
        requested
    }
}

#[derive(Debug)]
struct MarketInputs {
    asset_code: Option<String>,
    cost_per_trade_dollar: Option<f64>,
    point_value: Option<f64>,
    tick_value: Option<f64>,
    margin_per_contract_dollar: Option<f64>,
}

fn resolve_market_inputs(args: &EvalFormulasArgs) -> Result<MarketInputs> {
    let mut asset_code = None;
    let mut point_value = None;
    let mut tick_value = None;
    let mut margin_per_contract_dollar = None;
    let mut cost_per_trade_dollar = None;

    if let Some(code) = &args.asset {
        let asset = find_asset(code).ok_or_else(|| anyhow!("Unknown asset code '{}'", code))?;
        asset_code = Some(asset.code.to_string());
        point_value = Some(asset.point_value);
        tick_value = Some(asset.tick_value);
        margin_per_contract_dollar = Some(asset.margin_per_contract_dollar);

        if !args.no_costs {
            let commission = args
                .commission_per_trade_dollar
                .unwrap_or(2.0 * asset.ibkr_commission_per_side);
            let slippage = args
                .slippage_per_trade_dollar
                .unwrap_or(asset.default_slippage_ticks * asset.tick_value);
            cost_per_trade_dollar =
                Some(args.cost_per_trade_dollar.unwrap_or(commission + slippage));
        }
    } else if !args.no_costs {
        if let Some(cost) = args.cost_per_trade_dollar {
            cost_per_trade_dollar = Some(cost);
        } else if args.commission_per_trade_dollar.is_some()
            || args.slippage_per_trade_dollar.is_some()
        {
            cost_per_trade_dollar = Some(
                args.commission_per_trade_dollar.unwrap_or(0.0)
                    + args.slippage_per_trade_dollar.unwrap_or(0.0),
            );
        }
    }

    Ok(MarketInputs {
        asset_code,
        cost_per_trade_dollar,
        point_value,
        tick_value,
        margin_per_contract_dollar,
    })
}

fn print_report(report: &FormulaEvaluationReport, report_top: usize) {
    println!("{}", "=".repeat(80));
    println!("Barsmith formula evaluation");
    println!("{}", "=".repeat(80));
    println!("Prepared CSV: {}", report.prepared_path.display());
    println!("Target: {}", report.target);
    println!("RR column: {}", report.rr_column);
    println!("Cutoff: {}", report.cutoff);
    println!("Stage: {}", report.stage.as_str());
    if let Some(strict) = &report.strict_protocol {
        println!("Strict protocol: {}", strict.strict);
        for warning in &strict.warnings {
            println!("  Protocol warning: {warning}");
        }
    }
    print_window_report(&report.pre, report_top);
    print_window_report(&report.post, report_top);
    print_selection_report(report.selection.as_ref());
    print_overfit_report(report.overfit.as_ref());
    print_stress_report(report.stress.as_ref());
}

fn print_window_report(window: &FormulaWindowReport, report_top: usize) {
    println!();
    println!("=== Window: {} (rows={}) ===", window.label, window.rows);
    if let Some(bh) = &window.buy_and_hold {
        println!(
            "Buy & Hold: {:?} -> {:?} | Total {}% | CAGR {}% | Max DD {}% | Calmar {}",
            bh.start,
            bh.end,
            f2(bh.total_return_pct),
            f2(bh.cagr_pct),
            f2(bh.max_drawdown_pct),
            f2(bh.calmar)
        );
    }
    let limit = if report_top == 0 {
        window.results.len()
    } else {
        report_top.min(window.results.len())
    };
    for result in window.results.iter().take(limit) {
        print_formula_result(result);
    }
    if limit < window.results.len() {
        println!(
            "... {} more formulas not printed",
            window.results.len() - limit
        );
    }
}

fn print_formula_result(result: &FormulaResult) {
    let prev = result
        .previous_rank
        .map(|rank| format!(" (prev {rank})"))
        .unwrap_or_default();
    println!();
    println!("Rank {}{}: {}", result.display_rank, prev, result.formula);
    println!(
        "  Mask hits: {} | Trades: {} | Win rate: {}% | Label hit: {}%",
        result.mask_hits,
        result.trades,
        f2(result.stats.win_rate),
        f2(result.stats.label_hit_rate)
    );
    println!(
        "  Total R: {} | Expectancy: {}R | Max DD: {}R | Calmar equity: {}",
        f2(result.stats.total_return),
        f4(result.stats.expectancy),
        f2(result.stats.max_drawdown),
        f2(result.stats.calmar_equity)
    );
    println!(
        "  Final capital: ${} | CAGR: {}% | Equity DD: {}% | Sharpe: {} | Sortino: {}",
        f2(result.stats.final_capital),
        f2(result.stats.cagr_pct),
        f2(result.stats.max_drawdown_pct_equity),
        f2(result.stats.sharpe_equity),
        f2(result.stats.sortino_equity)
    );
    if let Some(frs) = result.frs {
        println!(
            "  FRS: {} | windows={} | P={} | trade_score={}",
            f4(frs.frs),
            frs.k,
            f2(frs.p),
            f2(frs.trade_score)
        );
    }
}

fn print_selection_report(selection: Option<&SelectionReport>) {
    let Some(selection) = selection else {
        return;
    };
    println!();
    println!("=== Selection ({:?}) ===", selection.mode);
    if let Some(selected) = &selection.selected {
        println!(
            "Selected pre-rank {} formula: {}",
            selected.pre_rank, selected.formula
        );
        println!(
            "  Post rank: {} | Pre Total R: {} | Post Total R: {} | Status: {:?}",
            selected
                .post_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            f2(selected.pre_total_r),
            selected
                .post_total_r
                .map(f2)
                .unwrap_or_else(|| "n/a".to_string()),
            selected.status
        );
    } else {
        println!("No formula passed the configured selection gates.");
    }
    if let Some(top_post) = &selection.diagnostic_top_post {
        println!(
            "Diagnostic top post formula (not selected by holdout mode): rank {} | Total R {} | {}",
            top_post.post_rank,
            f2(top_post.post_total_r),
            top_post.formula
        );
    }
    for warning in &selection.warnings {
        println!("  Warning: {warning}");
    }
}

fn print_overfit_report(overfit: Option<&barsmith_rs::overfit::OverfitReport>) {
    let Some(overfit) = overfit else {
        return;
    };
    println!();
    println!("=== Overfit Diagnostics ({:?}) ===", overfit.status);
    println!(
        "Candidates: {} | Effective trials: {} | CSCV splits: {}",
        overfit.candidate_count, overfit.effective_trials, overfit.cscv_splits
    );
    println!(
        "PBO: {} | PSR: {} | DSR: {} | Positive windows: {}",
        opt_f4(overfit.pbo),
        opt_f4(overfit.psr),
        opt_f4(overfit.dsr),
        opt_f4(overfit.selected_positive_window_ratio)
    );
    for warning in &overfit.warnings {
        println!("  Warning: {warning}");
    }
}

fn print_stress_report(stress: Option<&barsmith_rs::stress::StressReport>) {
    let Some(stress) = stress else {
        return;
    };
    println!();
    println!("=== Stress Diagnostics ({:?}) ===", stress.status);
    for scenario in &stress.scenarios {
        let max_contracts = scenario
            .max_contracts_override
            .map(|value| value.to_string())
            .unwrap_or_else(|| "base".to_string());
        println!(
            "{} | Max contracts {} | Post Total R {} | Post expectancy {} | pass={}",
            scenario.scenario,
            max_contracts,
            f2(scenario.post_total_r),
            f4(scenario.post_expectancy),
            scenario.pass
        );
    }
    for warning in &stress.warnings {
        println!("  Warning: {warning}");
    }
}

#[derive(Serialize)]
struct FormulaCsvRow<'a> {
    window: &'a str,
    rank: usize,
    previous_rank: Option<usize>,
    source_rank: usize,
    formula: &'a str,
    mask_hits: usize,
    trades: usize,
    win_rate: f64,
    label_hit_rate: f64,
    expectancy: f64,
    total_return: f64,
    max_drawdown: f64,
    profit_factor: f64,
    calmar_ratio: f64,
    final_capital: f64,
    total_return_pct: f64,
    cagr_pct: f64,
    max_drawdown_pct_equity: f64,
    calmar_equity: f64,
    sharpe_equity: f64,
    sortino_equity: f64,
    frs: Option<f64>,
}

fn write_formula_results_csv(path: &Path, report: &FormulaEvaluationReport) -> Result<()> {
    ensure_parent(path)?;
    let mut writer = csv::Writer::from_path(path)?;
    for (window, results) in [
        (report.pre.label.as_str(), report.pre.results.as_slice()),
        (report.post.label.as_str(), report.post.results.as_slice()),
    ] {
        for result in results {
            writer.serialize(FormulaCsvRow {
                window,
                rank: result.display_rank,
                previous_rank: result.previous_rank,
                source_rank: result.source_rank,
                formula: &result.formula,
                mask_hits: result.mask_hits,
                trades: result.trades,
                win_rate: result.stats.win_rate,
                label_hit_rate: result.stats.label_hit_rate,
                expectancy: result.stats.expectancy,
                total_return: result.stats.total_return,
                max_drawdown: result.stats.max_drawdown,
                profit_factor: result.stats.profit_factor,
                calmar_ratio: result.stats.calmar_ratio,
                final_capital: result.stats.final_capital,
                total_return_pct: result.stats.total_return_pct,
                cagr_pct: result.stats.cagr_pct,
                max_drawdown_pct_equity: result.stats.max_drawdown_pct_equity,
                calmar_equity: result.stats.calmar_equity,
                sharpe_equity: result.stats.sharpe_equity,
                sortino_equity: result.stats.sortino_equity,
                frs: result.frs.map(|frs| frs.frs),
            })?;
        }
    }
    writer.flush()?;
    Ok(())
}

#[derive(Serialize)]
struct FrsCsvRow<'a> {
    scope: &'a str,
    source_rank: usize,
    formula: &'a str,
    frs: f64,
    k: usize,
    p: f64,
    c: f64,
    tail_penalty: f64,
    stability: f64,
    n_med: f64,
    trade_score: f64,
}

fn write_frs_summary_csv(path: &Path, report: &FormulaEvaluationReport) -> Result<()> {
    ensure_parent(path)?;
    let mut writer = csv::Writer::from_path(path)?;
    for row in &report.frs_rows {
        let ForwardRobustnessComponents {
            frs,
            k,
            p,
            c,
            tail_penalty,
            stability,
            n_med,
            trade_score,
            ..
        } = row.components;
        writer.serialize(FrsCsvRow {
            scope: &row.scope,
            source_rank: row.source_rank,
            formula: &row.formula,
            frs,
            k,
            p,
            c,
            tail_penalty,
            stability,
            n_med,
            trade_score,
        })?;
    }
    writer.flush()?;
    Ok(())
}

#[derive(Serialize)]
struct SelectionDecisionCsvRow<'a> {
    formula: &'a str,
    source_rank: usize,
    pre_rank: usize,
    post_rank: Option<usize>,
    status: String,
    reasons: String,
    warnings: String,
    pre_trades: usize,
    post_trades: Option<usize>,
    pre_total_r: f64,
    post_total_r: Option<f64>,
    pre_expectancy: f64,
    post_expectancy: Option<f64>,
    pre_max_drawdown_r: f64,
    post_max_drawdown_r: Option<f64>,
    pre_calmar_equity: f64,
    post_calmar_equity: Option<f64>,
    pre_frs: Option<f64>,
    post_frs: Option<f64>,
    post_to_pre_total_r_ratio: Option<f64>,
    pre_largest_win_share: Option<f64>,
    post_largest_win_share: Option<f64>,
    formula_depth: usize,
    pre_density_per_1000_bars: f64,
    post_density_per_1000_bars: Option<f64>,
    complexity_penalty: f64,
}

fn write_selection_decisions_csv(path: &Path, selection: &SelectionReport) -> Result<()> {
    ensure_parent(path)?;
    let mut writer = csv::Writer::from_path(path)?;
    for decision in &selection.decisions {
        writer.serialize(selection_decision_csv_row(decision))?;
    }
    writer.flush()?;
    Ok(())
}

fn selection_decision_csv_row(decision: &SelectionDecision) -> SelectionDecisionCsvRow<'_> {
    let reasons = decision
        .reasons
        .iter()
        .map(RejectionReason::as_str)
        .collect::<Vec<_>>()
        .join("|");
    let warnings = decision.warnings.join("|");
    SelectionDecisionCsvRow {
        formula: &decision.formula,
        source_rank: decision.source_rank,
        pre_rank: decision.pre_rank,
        post_rank: decision.post_rank,
        status: format!("{:?}", decision.status),
        reasons,
        warnings,
        pre_trades: decision.pre_trades,
        post_trades: decision.post_trades,
        pre_total_r: decision.pre_total_r,
        post_total_r: decision.post_total_r,
        pre_expectancy: decision.pre_expectancy,
        post_expectancy: decision.post_expectancy,
        pre_max_drawdown_r: decision.pre_max_drawdown_r,
        post_max_drawdown_r: decision.post_max_drawdown_r,
        pre_calmar_equity: decision.pre_calmar_equity,
        post_calmar_equity: decision.post_calmar_equity,
        pre_frs: decision.pre_frs,
        post_frs: decision.post_frs,
        post_to_pre_total_r_ratio: decision.post_to_pre_total_r_ratio,
        pre_largest_win_share: decision.pre_largest_win_share,
        post_largest_win_share: decision.post_largest_win_share,
        formula_depth: decision.formula_depth,
        pre_density_per_1000_bars: decision.pre_density_per_1000_bars,
        post_density_per_1000_bars: decision.post_density_per_1000_bars,
        complexity_penalty: decision.complexity_penalty,
    }
}

fn write_selected_formulas(path: &Path, selection: &SelectionReport) -> Result<()> {
    ensure_parent(path)?;
    let text = match &selection.selected {
        Some(selected) => format!("Rank {}: {}\n", selected.source_rank, selected.formula),
        None => "# No formula passed the configured selection gates.\n".to_string(),
    };
    fs::write(path, text).with_context(|| format!("failed to write {}", path.display()))
}

#[derive(Serialize)]
struct OverfitDecisionCsvRow<'a> {
    split_index: usize,
    train_blocks: String,
    test_blocks: String,
    selected_formula: &'a str,
    train_metric: f64,
    test_metric: f64,
    test_rank: usize,
    candidate_count: usize,
    test_percentile: f64,
    logit: f64,
    overfit: bool,
}

fn write_overfit_decisions_csv(
    path: &Path,
    overfit: &barsmith_rs::overfit::OverfitReport,
) -> Result<()> {
    ensure_parent(path)?;
    let mut writer = csv::Writer::from_path(path)?;
    for row in &overfit.decisions {
        writer.serialize(OverfitDecisionCsvRow {
            split_index: row.split_index,
            train_blocks: row.train_blocks.join("|"),
            test_blocks: row.test_blocks.join("|"),
            selected_formula: &row.selected_formula,
            train_metric: row.train_metric,
            test_metric: row.test_metric,
            test_rank: row.test_rank,
            candidate_count: row.candidate_count,
            test_percentile: row.test_percentile,
            logit: row.logit,
            overfit: row.overfit,
        })?;
    }
    writer.flush()?;
    Ok(())
}

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    ensure_parent(path)?;
    let mut writer = csv::Writer::from_path(path)?;
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    ensure_parent(path)?;
    let file = fs::File::create(path)?;
    serde_json::to_writer_pretty(file, value)?;
    Ok(())
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

fn f2(value: f64) -> String {
    format_float(value, 2)
}

fn f4(value: f64) -> String {
    format_float(value, 4)
}

fn opt_f4(value: Option<f64>) -> String {
    value.map(f4).unwrap_or_else(|| "n/a".to_string())
}

fn format_float(value: f64, decimals: usize) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "Inf".to_string()
    } else if value.is_infinite() && value.is_sign_negative() {
        "-Inf".to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else {
        format!("{value:.decimals$}")
    }
}
