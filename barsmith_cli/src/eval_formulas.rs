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
use chrono::NaiveDate;
use serde::Serialize;

use crate::cli::EvalFormulasArgs;
use crate::plot;

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
                written_files.extend(plot::render_plots(&curves, args)?);
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

    let position_sizing = args.position_sizing.to_mode();
    let stop_distance_column = if position_sizing == PositionSizingMode::Contracts {
        args.stop_distance_column
            .clone()
            .or_else(|| infer_stop_distance_column(&args.target))
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
        target: normalize_target(&args.target),
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
    })
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

fn normalize_target(target: &str) -> String {
    if target == "atr_stop" {
        "2x_atr_tp_atr_stop".to_string()
    } else {
        target.to_string()
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

fn print_report(report: &FormulaEvaluationReport, report_top: usize) {
    println!("{}", "=".repeat(80));
    println!("Barsmith formula evaluation");
    println!("{}", "=".repeat(80));
    println!("Prepared CSV: {}", report.prepared_path.display());
    println!("Target: {}", report.target);
    println!("RR column: {}", report.rr_column);
    println!("Cutoff: {}", report.cutoff);
    print_window_report(&report.pre, report_top);
    print_window_report(&report.post, report_top);
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
