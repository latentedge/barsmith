use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use polars::prelude::*;
use serde::Serialize;

use crate::bitset::{
    BitsetCatalog, BitsetMask, scan_bitsets_scalar_dyn_gated, sort_bitsets_by_support,
};
use crate::combinator::IndexCombination;
use crate::config::{
    Config, Direction, EvalProfileMode, PositionSizingMode, ReportMetricsMode, StackingMode,
    StatsDetail, StopDistanceUnit,
};
use crate::data::ColumnarData;
use crate::formula::{FormulaClause, FormulaOperator, RankedFormula};
use crate::frs::{ForwardRobustnessComponents, FrsOptions, compute_frs};
use crate::mask::MaskCache;
use crate::overfit::{
    CscvDecision, OverfitOptions, OverfitReport, ResearchGateStatus, deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
};
use crate::protocol::{ResearchStage, StrictProtocolValidation, sha256_text};
use crate::selection::{SelectionMode, SelectionPolicy, SelectionReport, build_selection_report};
use crate::stats::{EvaluationContext, StatSummary, evaluate_combination_indices};
use crate::stress::{StressOptions, StressReport, StressScenarioResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankBy {
    CalmarEquity,
    Frs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrsScope {
    Window,
    Pre,
    Post,
    All,
}

#[derive(Debug, Clone)]
pub struct FormulaEvalRequest {
    pub prepared_path: PathBuf,
    pub formulas: Vec<RankedFormula>,
    pub target: String,
    pub rr_column: Option<String>,
    pub cutoff: NaiveDate,
    pub stacking_mode: StackingMode,
    pub capital_dollar: f64,
    pub risk_pct_per_trade: f64,
    pub asset: Option<String>,
    pub cost_per_trade_dollar: Option<f64>,
    pub cost_per_trade_r: Option<f64>,
    pub dollars_per_r: Option<f64>,
    pub position_sizing: PositionSizingMode,
    pub stop_distance_column: Option<String>,
    pub stop_distance_unit: StopDistanceUnit,
    pub min_contracts: usize,
    pub max_contracts: Option<usize>,
    pub point_value: Option<f64>,
    pub tick_value: Option<f64>,
    pub margin_per_contract_dollar: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub min_calmar: Option<f64>,
    pub rank_by: RankBy,
    pub frs_enabled: bool,
    pub frs_scope: FrsScope,
    pub frs_options: FrsOptions,
    pub selection_mode: SelectionMode,
    pub selection_policy: SelectionPolicy,
    pub stage: ResearchStage,
    pub strict_protocol: Option<StrictProtocolValidation>,
    pub overfit_options: Option<OverfitOptions>,
    pub stress_options: Option<StressOptions>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FormulaEvaluationReport {
    pub prepared_path: PathBuf,
    pub target: String,
    pub rr_column: String,
    pub cutoff: NaiveDate,
    pub pre: FormulaWindowReport,
    pub post: FormulaWindowReport,
    pub selection: Option<SelectionReport>,
    pub stage: ResearchStage,
    pub strict_protocol: Option<StrictProtocolValidation>,
    pub overfit: Option<OverfitReport>,
    pub stress: Option<StressReport>,
    pub frs_rows: Vec<FrsSummaryRow>,
    pub frs_window_rows: Vec<FrsWindowRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FormulaWindowReport {
    pub label: String,
    pub rows: usize,
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
    pub buy_and_hold: Option<BuyAndHoldSummary>,
    pub guard: WindowGuardReport,
    pub results: Vec<FormulaResult>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WindowGuardReport {
    pub rows_after_date_filter: usize,
    pub embargo_bars_requested: usize,
    pub embargo_bars_applied: usize,
    pub rows_after_embargo: usize,
    pub cross_boundary_rows_purged: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct FormulaResult {
    pub source_rank: usize,
    pub display_rank: usize,
    pub previous_rank: Option<usize>,
    pub formula: String,
    pub mask_hits: usize,
    pub trades: usize,
    pub density_per_1000_bars: f64,
    pub recall_pct: f64,
    pub stats: StatSummary,
    pub frs: Option<ForwardRobustnessComponents>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrsSummaryRow {
    pub scope: String,
    pub formula: String,
    pub source_rank: usize,
    pub components: ForwardRobustnessComponents,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrsWindowRow {
    pub scope: String,
    pub formula: String,
    pub source_rank: usize,
    pub window_label: String,
    pub year: i32,
    pub rows: usize,
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
    pub total_return_r: f64,
    pub max_drawdown_r: f64,
    pub trades: usize,
    pub calmar_r: f64,
    pub expectancy_r: f64,
    pub total_return_pct: f64,
    pub cagr_pct: f64,
    pub max_drawdown_pct_equity: f64,
    pub calmar_equity: f64,
    pub final_capital: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EquityCurveRow {
    pub rank_by: String,
    pub window: String,
    pub rank: usize,
    pub formula: String,
    pub timestamp: String,
    pub trade_index: usize,
    pub rr: f64,
    pub equity_r: f64,
    pub equity_dollar: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BuyAndHoldSummary {
    pub rows: usize,
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
    pub start_close: f64,
    pub end_close: f64,
    pub total_return_pct: f64,
    pub cagr_pct: f64,
    pub max_drawdown_pct: f64,
    pub calmar: f64,
    pub final_capital: Option<f64>,
}

#[derive(Clone)]
struct FormulaBitsetPlan {
    bitsets: BitsetCatalog,
    combinations: Vec<IndexCombination>,
}

#[derive(Debug, Clone)]
struct DateWindow {
    label: String,
    year: Option<i32>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    apply_embargo: bool,
}

#[derive(Debug)]
struct FrsAccumulator {
    returns_r: Vec<f64>,
    max_drawdowns_r: Vec<f64>,
    trades: Vec<usize>,
    source_rank: usize,
}

impl FrsAccumulator {
    fn new(source_rank: usize) -> Self {
        Self {
            returns_r: Vec::new(),
            max_drawdowns_r: Vec::new(),
            trades: Vec::new(),
            source_rank,
        }
    }
}

pub fn run_formula_evaluation(request: &FormulaEvalRequest) -> Result<FormulaEvaluationReport> {
    if request.formulas.is_empty() {
        return Err(anyhow!("at least one formula is required"));
    }
    if !request.prepared_path.exists() {
        return Err(anyhow!(
            "prepared CSV not found: {}",
            request.prepared_path.display()
        ));
    }

    let full_data = ColumnarData::load(&request.prepared_path)?;
    validate_required_columns(&full_data, request)?;

    let rr_column = request
        .rr_column
        .clone()
        .unwrap_or_else(|| format!("rr_{}", request.target));

    let pre_window = DateWindow {
        label: format!("<= {}", request.cutoff),
        year: None,
        start: None,
        end: Some(request.cutoff),
        apply_embargo: false,
    };
    let post_window = DateWindow {
        label: format!(">  {}", request.cutoff),
        year: None,
        start: Some(next_day(request.cutoff)?),
        end: None,
        apply_embargo: true,
    };

    let mut frs_pre = HashMap::new();
    let mut frs_post = HashMap::new();
    let mut frs_rows = Vec::new();
    let mut frs_window_rows = Vec::new();

    if request.frs_enabled {
        match request.frs_scope {
            FrsScope::Window => {
                frs_pre = compute_frs_for_scope(
                    "pre",
                    &full_data,
                    request,
                    &pre_window,
                    &mut frs_rows,
                    &mut frs_window_rows,
                )?;
                frs_post = compute_frs_for_scope(
                    "post",
                    &full_data,
                    request,
                    &post_window,
                    &mut frs_rows,
                    &mut frs_window_rows,
                )?;
            }
            FrsScope::Pre => {
                frs_pre = compute_frs_for_scope(
                    "pre",
                    &full_data,
                    request,
                    &pre_window,
                    &mut frs_rows,
                    &mut frs_window_rows,
                )?;
            }
            FrsScope::Post => {
                frs_post = compute_frs_for_scope(
                    "post",
                    &full_data,
                    request,
                    &post_window,
                    &mut frs_rows,
                    &mut frs_window_rows,
                )?;
            }
            FrsScope::All => {
                let all = DateWindow {
                    label: "all".to_string(),
                    year: None,
                    start: None,
                    end: None,
                    apply_embargo: false,
                };
                let frs_all = compute_frs_for_scope(
                    "all",
                    &full_data,
                    request,
                    &all,
                    &mut frs_rows,
                    &mut frs_window_rows,
                )?;
                frs_pre = frs_all.clone();
                frs_post = frs_all;
            }
        }
    }

    let mut pre = evaluate_window(&full_data, request, &pre_window)?;
    attach_frs(&mut pre.results, &frs_pre);
    sort_results(&mut pre.results, RankBy::CalmarEquity, None);

    let previous_ranks = pre
        .results
        .iter()
        .map(|result| (result.formula.clone(), result.display_rank))
        .collect::<HashMap<_, _>>();

    let mut post = evaluate_window(&full_data, request, &post_window)?;
    attach_frs(&mut post.results, &frs_post);
    sort_results(&mut post.results, request.rank_by, Some(&previous_ranks));
    let selection = build_selection_report(
        request.selection_mode,
        request.selection_policy,
        &pre,
        &post,
    );
    let overfit = match &request.overfit_options {
        Some(options) => Some(compute_overfit_report(
            &full_data,
            request,
            &pre,
            selection.as_ref(),
            options,
        )?),
        None => None,
    };
    let stress = match &request.stress_options {
        Some(options) => Some(compute_stress_report(
            &full_data,
            request,
            &pre,
            selection.as_ref(),
            options,
        )?),
        None => None,
    };

    Ok(FormulaEvaluationReport {
        prepared_path: request.prepared_path.clone(),
        target: request.target.clone(),
        rr_column,
        cutoff: request.cutoff,
        pre,
        post,
        selection,
        stage: request.stage,
        strict_protocol: request.strict_protocol.clone(),
        overfit,
        stress,
        frs_rows,
        frs_window_rows,
    })
}

fn compute_overfit_report(
    full_data: &ColumnarData,
    request: &FormulaEvalRequest,
    pre: &FormulaWindowReport,
    selection: Option<&SelectionReport>,
    options: &OverfitOptions,
) -> Result<OverfitReport> {
    let mut warnings = Vec::new();
    let candidates = candidate_formulas(request, pre, options.candidate_top_k);
    let candidate_count = candidates.len();
    let effective_trials = options
        .effective_trials
        .unwrap_or_else(|| request.formulas.len().max(candidate_count).max(1));
    let selected_formula = selection
        .and_then(|selection| selection.selected.as_ref())
        .map(|selected| selected.formula.clone())
        .or_else(|| pre.results.first().map(|result| result.formula.clone()));

    if candidate_count < 2 {
        warnings.push("PBO requires at least two candidate formulas".to_string());
    }

    let block_windows = chronological_blocks(full_data, options.cscv_blocks)?;
    if block_windows.len() < 4 {
        warnings.push("PBO/CSCV requires at least four chronological blocks".to_string());
    }
    if block_windows.len() % 2 != 0 {
        warnings.push(
            "CSCV uses an even number of applied blocks; the final block was dropped".to_string(),
        );
    }
    let applied_blocks = if block_windows.len() % 2 == 0 {
        block_windows
    } else {
        block_windows
            .into_iter()
            .take(options.cscv_blocks.saturating_sub(1))
            .collect()
    };

    let mut block_metrics = Vec::new();
    if candidate_count >= 2 && applied_blocks.len() >= 4 {
        let mut block_request = request.clone();
        block_request.formulas = candidates.clone();
        block_request.overfit_options = None;
        block_request.stress_options = None;
        for window in &applied_blocks {
            let mut report = evaluate_window(full_data, &block_request, window)?;
            sort_results(&mut report.results, RankBy::CalmarEquity, None);
            let metrics = report
                .results
                .into_iter()
                .map(|result| {
                    let metric = result.stats.total_return
                        - options.complexity_penalty * result.stats.depth as f64;
                    (result.formula, metric)
                })
                .collect::<HashMap<_, _>>();
            block_metrics.push(metrics);
        }
    }

    let splits = cscv_splits(applied_blocks.len(), options.cscv_max_splits);
    let mut decisions = Vec::new();
    for (split_idx, train_indices) in splits.iter().enumerate() {
        let train_set = train_indices.iter().copied().collect::<BTreeSet<_>>();
        let test_indices = (0..applied_blocks.len())
            .filter(|idx| !train_set.contains(idx))
            .collect::<Vec<_>>();
        let mut train_scores = HashMap::new();
        let mut test_scores = HashMap::new();
        for formula in &candidates {
            let mut train = 0.0;
            let mut test = 0.0;
            for idx in train_indices {
                train += block_metrics
                    .get(*idx)
                    .and_then(|metrics| metrics.get(&formula.expression))
                    .copied()
                    .unwrap_or(f64::NEG_INFINITY);
            }
            for idx in &test_indices {
                test += block_metrics
                    .get(*idx)
                    .and_then(|metrics| metrics.get(&formula.expression))
                    .copied()
                    .unwrap_or(f64::NEG_INFINITY);
            }
            train_scores.insert(formula.expression.clone(), train);
            test_scores.insert(formula.expression.clone(), test);
        }

        let Some((selected, train_metric)) = best_formula_by_score(&candidates, &train_scores)
        else {
            continue;
        };
        let test_metric = test_scores
            .get(&selected)
            .copied()
            .unwrap_or(f64::NEG_INFINITY);
        let mut ranked_test = test_scores.iter().collect::<Vec<_>>();
        ranked_test
            .sort_by(|left, right| right.1.total_cmp(left.1).then_with(|| left.0.cmp(right.0)));
        let test_rank = ranked_test
            .iter()
            .position(|(formula, _)| *formula == &selected)
            .map(|idx| idx + 1)
            .unwrap_or(candidate_count);
        let percentile = ((candidate_count - test_rank + 1) as f64 / candidate_count as f64)
            .clamp(1e-9, 1.0 - 1e-9);
        let logit = (percentile / (1.0 - percentile)).ln();
        decisions.push(CscvDecision {
            split_index: split_idx + 1,
            train_blocks: train_indices
                .iter()
                .map(|idx| applied_blocks[*idx].label.clone())
                .collect(),
            test_blocks: test_indices
                .iter()
                .map(|idx| applied_blocks[*idx].label.clone())
                .collect(),
            selected_formula: selected,
            train_metric,
            test_metric,
            test_rank,
            candidate_count,
            test_percentile: percentile,
            logit,
            overfit: logit <= 0.0,
        });
    }

    let pbo = if decisions.is_empty() {
        None
    } else {
        Some(decisions.iter().filter(|row| row.overfit).count() as f64 / decisions.len() as f64)
    };
    let selected_samples = selected_formula
        .as_ref()
        .map(|formula| {
            block_metrics
                .iter()
                .filter_map(|metrics| metrics.get(formula).copied())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let psr = probabilistic_sharpe_ratio(&selected_samples, 0.0);
    let dsr = deflated_sharpe_ratio(&selected_samples, effective_trials);
    let selected_positive_window_ratio = if selected_samples.is_empty() {
        None
    } else {
        Some(
            selected_samples
                .iter()
                .filter(|value| **value > 0.0)
                .count() as f64
                / selected_samples.len() as f64,
        )
    };

    let mut status = ResearchGateStatus::Pass;
    if pbo.is_none() && psr.is_none() && dsr.is_none() {
        status = ResearchGateStatus::Unavailable;
    }
    if pbo.is_some_and(|value| value > options.max_pbo)
        || psr.is_some_and(|value| value < options.min_psr)
        || dsr.is_some_and(|value| value < options.min_dsr)
        || selected_positive_window_ratio
            .is_some_and(|value| value < options.min_positive_window_ratio)
    {
        status = ResearchGateStatus::Fail;
    }
    if !warnings.is_empty() && status == ResearchGateStatus::Pass {
        status = ResearchGateStatus::Warning;
    }

    Ok(OverfitReport {
        schema_version: 1,
        status,
        candidate_count,
        effective_trials,
        cscv_blocks_requested: options.cscv_blocks,
        cscv_blocks_applied: applied_blocks.len(),
        cscv_splits: decisions.len(),
        pbo,
        psr,
        dsr,
        selected_formula_sha256: selected_formula.as_deref().map(sha256_text),
        selected_formula,
        selected_positive_window_ratio,
        warnings,
        decisions,
    })
}

fn compute_stress_report(
    full_data: &ColumnarData,
    request: &FormulaEvalRequest,
    pre: &FormulaWindowReport,
    selection: Option<&SelectionReport>,
    options: &StressOptions,
) -> Result<StressReport> {
    let mut warnings = Vec::new();
    let selected_formula = selection
        .and_then(|selection| selection.selected.as_ref())
        .map(|selected| selected.formula.clone())
        .or_else(|| pre.results.first().map(|result| result.formula.clone()));
    let Some(selected_formula) = selected_formula else {
        return Ok(StressReport {
            schema_version: 1,
            status: ResearchGateStatus::Unavailable,
            selected_formula: None,
            selected_formula_sha256: None,
            scenarios: Vec::new(),
            warnings: vec![
                "no selected or top pre formula was available for stress testing".to_string(),
            ],
        });
    };
    let selected_ranked = request
        .formulas
        .iter()
        .find(|formula| formula.expression == selected_formula)
        .cloned()
        .ok_or_else(|| anyhow!("selected formula was not found in the request formula set"))?;

    let mut scenarios = Vec::new();
    for scenario in stress_scenarios(request) {
        let mut stress_request = request.clone();
        stress_request.formulas = vec![selected_ranked.clone()];
        stress_request.overfit_options = None;
        stress_request.stress_options = None;
        if let Some(base) = request.cost_per_trade_r {
            stress_request.cost_per_trade_r =
                Some(base * scenario.cost_multiplier + scenario.extra_cost_per_trade_r);
        }
        if let Some(base) = request.cost_per_trade_dollar {
            stress_request.cost_per_trade_dollar =
                Some(base * scenario.cost_multiplier + scenario.extra_cost_per_trade_dollar);
        }
        if let Some(max_contracts) = scenario.max_contracts_override {
            stress_request.max_contracts = Some(max_contracts);
        }
        let pre_window = DateWindow {
            label: "pre".to_string(),
            year: None,
            start: None,
            end: Some(request.cutoff),
            apply_embargo: false,
        };
        let post_window = DateWindow {
            label: "post".to_string(),
            year: None,
            start: Some(next_day(request.cutoff)?),
            end: None,
            apply_embargo: true,
        };
        let pre_result = evaluate_window(full_data, &stress_request, &pre_window)?
            .results
            .into_iter()
            .next();
        let post_result = evaluate_window(full_data, &stress_request, &post_window)?
            .results
            .into_iter()
            .next();
        let (Some(pre_result), Some(post_result)) = (pre_result, post_result) else {
            warnings.push(format!(
                "stress scenario '{}' produced no result after filters",
                scenario.name
            ));
            continue;
        };
        let pass = post_result.stats.total_return >= options.min_total_r
            && post_result.stats.expectancy >= options.min_expectancy;
        scenarios.push(StressScenarioResult {
            scenario: scenario.name,
            cost_multiplier: scenario.cost_multiplier,
            extra_cost_per_trade_r: scenario.extra_cost_per_trade_r,
            extra_cost_per_trade_dollar: scenario.extra_cost_per_trade_dollar,
            max_contracts_override: scenario.max_contracts_override,
            pre_trades: pre_result.trades,
            post_trades: post_result.trades,
            pre_total_r: pre_result.stats.total_return,
            post_total_r: post_result.stats.total_return,
            pre_expectancy: pre_result.stats.expectancy,
            post_expectancy: post_result.stats.expectancy,
            post_max_drawdown_r: post_result.stats.max_drawdown,
            pass,
        });
    }

    let status = if scenarios.is_empty() {
        ResearchGateStatus::Unavailable
    } else if scenarios.iter().all(|scenario| scenario.pass) {
        if warnings.is_empty() {
            ResearchGateStatus::Pass
        } else {
            ResearchGateStatus::Warning
        }
    } else {
        ResearchGateStatus::Fail
    };

    Ok(StressReport {
        schema_version: 1,
        status,
        selected_formula_sha256: Some(sha256_text(&selected_formula)),
        selected_formula: Some(selected_formula),
        scenarios,
        warnings,
    })
}

#[derive(Debug)]
struct StressScenario {
    name: String,
    cost_multiplier: f64,
    extra_cost_per_trade_r: f64,
    extra_cost_per_trade_dollar: f64,
    max_contracts_override: Option<usize>,
}

fn stress_scenarios(request: &FormulaEvalRequest) -> Vec<StressScenario> {
    let mut scenarios = vec![
        StressScenario {
            name: "baseline".to_string(),
            cost_multiplier: 1.0,
            extra_cost_per_trade_r: 0.0,
            extra_cost_per_trade_dollar: 0.0,
            max_contracts_override: None,
        },
        StressScenario {
            name: "cost_1_5x".to_string(),
            cost_multiplier: 1.5,
            extra_cost_per_trade_r: 0.0,
            extra_cost_per_trade_dollar: 0.0,
            max_contracts_override: None,
        },
        StressScenario {
            name: "cost_2x".to_string(),
            cost_multiplier: 2.0,
            extra_cost_per_trade_r: 0.0,
            extra_cost_per_trade_dollar: 0.0,
            max_contracts_override: None,
        },
    ];
    if let (Some(tick_value), Some(dollars_per_r)) = (request.tick_value, request.dollars_per_r) {
        if tick_value > 0.0 && dollars_per_r > 0.0 {
            for ticks in [1.0, 2.0] {
                scenarios.push(StressScenario {
                    name: format!("{}_ticks_worse_entry_exit", ticks as usize),
                    cost_multiplier: 1.0,
                    extra_cost_per_trade_r: (2.0 * ticks * tick_value) / dollars_per_r,
                    extra_cost_per_trade_dollar: 2.0 * ticks * tick_value,
                    max_contracts_override: None,
                });
            }
        }
    }
    if let Some(max_contracts) = request.max_contracts {
        let reduced = (max_contracts / 2).max(request.min_contracts).max(1);
        if reduced < max_contracts {
            scenarios.push(StressScenario {
                name: "half_max_contracts".to_string(),
                cost_multiplier: 1.0,
                extra_cost_per_trade_r: 0.0,
                extra_cost_per_trade_dollar: 0.0,
                max_contracts_override: Some(reduced),
            });
        }
    }
    scenarios
}

fn candidate_formulas(
    request: &FormulaEvalRequest,
    pre: &FormulaWindowReport,
    limit: usize,
) -> Vec<RankedFormula> {
    let by_expression = request
        .formulas
        .iter()
        .map(|formula| (formula.expression.as_str(), formula))
        .collect::<HashMap<_, _>>();
    pre.results
        .iter()
        .take(limit.max(1))
        .filter_map(|result| by_expression.get(result.formula.as_str()).copied().cloned())
        .collect()
}

fn best_formula_by_score(
    candidates: &[RankedFormula],
    scores: &HashMap<String, f64>,
) -> Option<(String, f64)> {
    candidates
        .iter()
        .filter_map(|formula| {
            scores
                .get(&formula.expression)
                .copied()
                .map(|score| (formula.expression.clone(), score))
        })
        .max_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| right.0.cmp(&left.0))
        })
}

fn chronological_blocks(data: &ColumnarData, requested_blocks: usize) -> Result<Vec<DateWindow>> {
    let (Some(start), Some(end)) = date_bounds(data)? else {
        return Ok(Vec::new());
    };
    let requested = requested_blocks.max(2);
    let days = (end - start).num_days().max(1) + 1;
    let applied = requested.min(days as usize).max(2);
    let mut out = Vec::new();
    for idx in 0..applied {
        let start_offset = ((idx as i64) * days) / applied as i64;
        let end_offset = (((idx as i64 + 1) * days) / applied as i64) - 1;
        let block_start = start + chrono::Duration::days(start_offset);
        let block_end = start + chrono::Duration::days(end_offset.max(start_offset));
        out.push(DateWindow {
            label: format!("block-{:02}", idx + 1),
            year: None,
            start: Some(block_start),
            end: Some(block_end.min(end)),
            apply_embargo: false,
        });
    }
    Ok(out)
}

fn cscv_splits(blocks: usize, max_splits: usize) -> Vec<Vec<usize>> {
    if blocks < 4 || blocks % 2 != 0 || max_splits == 0 {
        return Vec::new();
    }
    let half = blocks / 2;
    let mut out = Vec::new();
    let mut current = Vec::with_capacity(half);
    cscv_splits_rec(blocks, half, 0, &mut current, &mut out, max_splits);
    out
}

fn cscv_splits_rec(
    blocks: usize,
    half: usize,
    start: usize,
    current: &mut Vec<usize>,
    out: &mut Vec<Vec<usize>>,
    max_splits: usize,
) {
    if out.len() >= max_splits {
        return;
    }
    if current.len() == half {
        out.push(current.clone());
        return;
    }
    for idx in start..blocks {
        current.push(idx);
        cscv_splits_rec(blocks, half, idx + 1, current, out, max_splits);
        current.pop();
        if out.len() >= max_splits {
            return;
        }
    }
}

pub fn equity_curve_rows(
    report: &FormulaEvaluationReport,
    request: &FormulaEvalRequest,
    rank_source: RankBy,
    windows: EquityCurveWindowSelection,
    top_k: usize,
) -> Result<Vec<EquityCurveRow>> {
    if top_k == 0 {
        return Ok(Vec::new());
    }

    let full_data = ColumnarData::load(&request.prepared_path)?;
    let source_results = match rank_source {
        RankBy::CalmarEquity => &report.pre.results,
        RankBy::Frs => &report.post.results,
    };

    let mut rows = Vec::new();
    if matches!(
        windows,
        EquityCurveWindowSelection::Pre | EquityCurveWindowSelection::Both
    ) {
        let window = DateWindow {
            label: "pre".to_string(),
            year: None,
            start: None,
            end: Some(request.cutoff),
            apply_embargo: false,
        };
        rows.extend(equity_curve_rows_for_window(
            &full_data,
            request,
            source_results,
            &window,
            top_k,
            rank_source,
        )?);
    }
    if matches!(
        windows,
        EquityCurveWindowSelection::Post | EquityCurveWindowSelection::Both
    ) {
        let window = DateWindow {
            label: "post".to_string(),
            year: None,
            start: Some(next_day(request.cutoff)?),
            end: None,
            apply_embargo: true,
        };
        rows.extend(equity_curve_rows_for_window(
            &full_data,
            request,
            source_results,
            &window,
            top_k,
            rank_source,
        )?);
    }
    Ok(rows)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EquityCurveWindowSelection {
    Pre,
    Post,
    Both,
}

fn evaluate_window(
    full_data: &ColumnarData,
    request: &FormulaEvalRequest,
    window: &DateWindow,
) -> Result<FormulaWindowReport> {
    let (data, guard) = filter_for_window(full_data, request, window)?;
    let rows = data.approx_rows();
    let (start, end) = date_bounds(&data)?;
    if rows == 0 {
        return Ok(FormulaWindowReport {
            label: window.label.clone(),
            rows,
            start,
            end,
            buy_and_hold: None,
            guard,
            results: Vec::new(),
        });
    }

    let years = equity_time_years(&data)?;
    let config = runtime_config(request, Some(years));
    let ctx = EvaluationContext::new_with_reward_column(
        Arc::new(data.clone()),
        Arc::new(MaskCache::new()),
        &config,
        Arc::new(HashMap::new()),
        request.rr_column.as_deref(),
    )?;
    let plan = build_formula_bitsets(&data, &request.formulas)?;

    let mut results = Vec::with_capacity(request.formulas.len());
    for (formula, indices) in request.formulas.iter().zip(plan.combinations.iter()) {
        let stats = evaluate_combination_indices(indices, &ctx, &plan.bitsets, 0)?;
        if request
            .max_drawdown
            .is_some_and(|limit| stats.max_drawdown > limit)
        {
            continue;
        }
        if request
            .min_calmar
            .is_some_and(|limit| stats.calmar_equity < limit)
        {
            continue;
        }
        let trades = stats.total_bars;
        let mask_hits = stats.mask_hits;
        let density = if rows > 0 {
            trades as f64 / (rows as f64 / 1000.0)
        } else {
            0.0
        };
        let recall_pct = if rows > 0 {
            mask_hits as f64 / rows as f64 * 100.0
        } else {
            0.0
        };
        results.push(FormulaResult {
            source_rank: formula.rank,
            display_rank: 0,
            previous_rank: None,
            formula: formula.expression.clone(),
            mask_hits,
            trades,
            density_per_1000_bars: density,
            recall_pct,
            stats,
            frs: None,
        });
    }

    Ok(FormulaWindowReport {
        label: window.label.clone(),
        rows,
        start,
        end,
        buy_and_hold: buy_and_hold(&data, request.capital_dollar).ok().flatten(),
        guard,
        results,
    })
}

fn compute_frs_for_scope(
    scope: &str,
    full_data: &ColumnarData,
    request: &FormulaEvalRequest,
    scope_window: &DateWindow,
    summary_rows: &mut Vec<FrsSummaryRow>,
    window_rows: &mut Vec<FrsWindowRow>,
) -> Result<HashMap<String, ForwardRobustnessComponents>> {
    let annual_windows = annual_windows(full_data, scope_window)?;
    let mut accum: HashMap<String, FrsAccumulator> = HashMap::new();

    for window in annual_windows {
        let report = evaluate_window(full_data, request, &window)?;
        for result in report.results {
            let entry = accum
                .entry(result.formula.clone())
                .or_insert_with(|| FrsAccumulator::new(result.source_rank));
            entry.returns_r.push(result.stats.total_return);
            entry.max_drawdowns_r.push(result.stats.max_drawdown);
            entry.trades.push(result.trades);

            window_rows.push(FrsWindowRow {
                scope: scope.to_string(),
                formula: result.formula,
                source_rank: result.source_rank,
                window_label: window.label.clone(),
                year: window.year.unwrap_or_default(),
                rows: report.rows,
                start: report.start,
                end: report.end,
                total_return_r: result.stats.total_return,
                max_drawdown_r: result.stats.max_drawdown,
                trades: result.trades,
                calmar_r: result.stats.total_return / (result.stats.max_drawdown + 1e-9),
                expectancy_r: result.stats.expectancy,
                total_return_pct: result.stats.total_return_pct,
                cagr_pct: result.stats.cagr_pct,
                max_drawdown_pct_equity: result.stats.max_drawdown_pct_equity,
                calmar_equity: result.stats.calmar_equity,
                final_capital: result.stats.final_capital,
            });
        }
    }

    let mut out = HashMap::new();
    for (formula, values) in accum {
        let components = compute_frs(
            &values.returns_r,
            &values.max_drawdowns_r,
            &values.trades,
            request.frs_options,
        );
        summary_rows.push(FrsSummaryRow {
            scope: scope.to_string(),
            formula: formula.clone(),
            source_rank: values.source_rank,
            components,
        });
        out.insert(formula, components);
    }
    Ok(out)
}

fn attach_frs(
    results: &mut [FormulaResult],
    frs_by_formula: &HashMap<String, ForwardRobustnessComponents>,
) {
    for result in results {
        result.frs = frs_by_formula.get(&result.formula).copied();
    }
}

fn sort_results(
    results: &mut [FormulaResult],
    rank_by: RankBy,
    previous_ranks: Option<&HashMap<String, usize>>,
) {
    results.sort_by(|a, b| {
        let a_metric = primary_metric(a, rank_by);
        let b_metric = primary_metric(b, rank_by);
        b_metric
            .total_cmp(&a_metric)
            .then_with(|| match previous_ranks {
                Some(prev) => prev
                    .get(&a.formula)
                    .copied()
                    .unwrap_or(usize::MAX)
                    .cmp(&prev.get(&b.formula).copied().unwrap_or(usize::MAX)),
                None => a.source_rank.cmp(&b.source_rank),
            })
    });

    for (idx, result) in results.iter_mut().enumerate() {
        result.display_rank = idx + 1;
        result.previous_rank = previous_ranks.and_then(|prev| prev.get(&result.formula).copied());
    }
}

fn primary_metric(result: &FormulaResult, rank_by: RankBy) -> f64 {
    match rank_by {
        RankBy::CalmarEquity => result.stats.calmar_equity,
        RankBy::Frs => result.frs.map(|frs| frs.frs).unwrap_or(f64::NEG_INFINITY),
    }
}

fn build_formula_bitsets(
    data: &ColumnarData,
    formulas: &[RankedFormula],
) -> Result<FormulaBitsetPlan> {
    let mut clause_to_index: HashMap<FormulaClause, usize> = HashMap::new();
    let mut bitsets = Vec::new();
    let mut name_to_index = HashMap::new();
    let mut combinations = Vec::with_capacity(formulas.len());

    for formula in formulas {
        let mut indices = Vec::with_capacity(formula.clauses.len());
        for clause in &formula.clauses {
            let idx = match clause_to_index.get(clause) {
                Some(idx) => *idx,
                None => {
                    let idx = bitsets.len();
                    let mask = mask_for_clause(data, clause)?;
                    bitsets.push(BitsetMask::from_bools(&mask));
                    name_to_index.insert(clause.raw.clone(), idx);
                    clause_to_index.insert(clause.clone(), idx);
                    idx
                }
            };
            indices.push(idx);
        }
        combinations.push(indices.into_iter().collect());
    }

    Ok(FormulaBitsetPlan {
        bitsets: BitsetCatalog::new(bitsets, name_to_index),
        combinations,
    })
}

fn mask_for_clause(data: &ColumnarData, clause: &FormulaClause) -> Result<Vec<bool>> {
    if clause.is_flag() {
        return flag_mask(data, &clause.left)
            .with_context(|| format!("failed to build flag mask for '{}'", clause.raw));
    }

    let op = clause
        .operator
        .ok_or_else(|| anyhow!("missing operator for clause '{}'", clause.raw))?;
    let right = clause
        .right
        .as_deref()
        .ok_or_else(|| anyhow!("missing right-hand side for clause '{}'", clause.raw))?;
    let left_values = float_values(data, &clause.left)?;

    if data.has_column(right) {
        let right_values = float_values(data, right)?;
        Ok(left_values
            .iter()
            .zip(right_values.iter())
            .map(|(left, right)| compare_optional(*left, *right, op))
            .collect())
    } else {
        let right_value = right
            .parse::<f64>()
            .with_context(|| format!("unsupported RHS '{right}' in clause '{}'", clause.raw))?;
        Ok(left_values
            .iter()
            .map(|left| compare_optional(*left, Some(right_value), op))
            .collect())
    }
}

fn flag_mask(data: &ColumnarData, column: &str) -> Result<Vec<bool>> {
    let frame = data.data_frame();
    let series = frame
        .column(column)
        .with_context(|| format!("missing feature column '{column}'"))?;
    if matches!(series.dtype(), DataType::Boolean) {
        return Ok(series
            .bool()?
            .into_iter()
            .map(|value| value.unwrap_or(false))
            .collect());
    }
    Ok(data
        .float_column(column)?
        .into_iter()
        .map(|value| value.is_some_and(|raw| raw > 0.5))
        .collect())
}

fn float_values(data: &ColumnarData, column: &str) -> Result<Vec<Option<f64>>> {
    Ok(data
        .float_column(column)
        .with_context(|| format!("missing numeric column '{column}'"))?
        .into_iter()
        .map(|value| value.filter(|raw| raw.is_finite()))
        .collect())
}

fn compare_optional(left: Option<f64>, right: Option<f64>, op: FormulaOperator) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return false;
    };
    match op {
        FormulaOperator::GreaterThan => left > right,
        FormulaOperator::LessThan => left < right,
        FormulaOperator::GreaterEqual => left >= right,
        FormulaOperator::LessEqual => left <= right,
        FormulaOperator::Equal => left == right,
        FormulaOperator::NotEqual => left != right,
    }
}

fn validate_required_columns(data: &ColumnarData, request: &FormulaEvalRequest) -> Result<()> {
    let rr_column = request
        .rr_column
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| format!("rr_{}", request.target));

    for column in ["timestamp", request.target.as_str(), rr_column.as_str()] {
        if !data.has_column(column) {
            return Err(anyhow!(
                "prepared CSV is missing required column '{column}'"
            ));
        }
    }

    if request.stacking_mode == StackingMode::NoStacking {
        let exit_column = format!("{}_exit_i", request.target);
        if !data.has_column(&exit_column) {
            return Err(anyhow!(
                "--stacking-mode no-stacking requires missing column '{exit_column}'"
            ));
        }
    }

    if request.position_sizing == PositionSizingMode::Contracts {
        let stop_column = request.stop_distance_column.as_deref().ok_or_else(|| {
            anyhow!("--position-sizing contracts requires --stop-distance-column")
        })?;
        if !data.has_column(stop_column) {
            return Err(anyhow!(
                "prepared CSV is missing stop-distance column '{stop_column}'"
            ));
        }
    }

    Ok(())
}

fn filter_for_window(
    data: &ColumnarData,
    request: &FormulaEvalRequest,
    window: &DateWindow,
) -> Result<(ColumnarData, WindowGuardReport)> {
    let mut filtered = data.filter_by_date_range(window.start, window.end)?;
    let mut guard = WindowGuardReport {
        rows_after_date_filter: filtered.approx_rows(),
        embargo_bars_requested: if window.apply_embargo {
            request.selection_policy.embargo_bars
        } else {
            0
        },
        ..Default::default()
    };

    if guard.embargo_bars_requested > 0 && filtered.approx_rows() > 0 {
        let applied = guard.embargo_bars_requested.min(filtered.approx_rows());
        filtered = filtered.slice_rows(applied, filtered.approx_rows().saturating_sub(applied))?;
        guard.embargo_bars_applied = applied;
    }
    guard.rows_after_embargo = filtered.approx_rows();

    if request.selection_policy.purge_cross_boundary_exits {
        let (purged, guarded) = purge_cross_boundary_exits(filtered, request)?;
        guard.cross_boundary_rows_purged = purged;
        filtered = guarded;
    }

    Ok((filtered, guard))
}

fn purge_cross_boundary_exits(
    data: ColumnarData,
    request: &FormulaEvalRequest,
) -> Result<(usize, ColumnarData)> {
    let exit_column = format!("{}_exit_i", request.target);
    if !data.has_column(&exit_column) || data.approx_rows() == 0 {
        return Ok((0, data));
    }

    let rr_column = request
        .rr_column
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| format!("rr_{}", request.target));
    if !data.has_column(&rr_column) || !data.has_column(&request.target) {
        return Ok((0, data));
    }

    let rows = data.approx_rows();
    let exits = data.i64_column(&exit_column)?;
    let target = data.boolean_column(&request.target)?;
    let rewards = data.float_column(&rr_column)?;
    let eligible_column = format!("{}_eligible", request.target);
    let eligible = if data.has_column(&eligible_column) {
        Some(data.boolean_column(&eligible_column)?)
    } else {
        None
    };

    let mut target_values = Vec::with_capacity(rows);
    let mut reward_values = Vec::with_capacity(rows);
    let mut eligible_values = eligible.as_ref().map(|_| Vec::with_capacity(rows));
    let mut purged = 0usize;

    for idx in 0..rows {
        let exit_i = exits.get(idx);
        let crosses_boundary = !matches!(exit_i, Some(v) if v >= 0 && (v as usize) < rows);
        let target_value = target.get(idx).unwrap_or(false);
        let reward_value = rewards.get(idx).unwrap_or(f64::NAN);
        let eligible_value = eligible
            .as_ref()
            .and_then(|values| values.get(idx))
            .unwrap_or(true);

        if crosses_boundary {
            if target_value || reward_value.is_finite() || eligible_value {
                purged += 1;
            }
            target_values.push(false);
            reward_values.push(f64::NAN);
            if let Some(values) = eligible_values.as_mut() {
                values.push(false);
            }
        } else {
            target_values.push(target_value);
            reward_values.push(reward_value);
            if let Some(values) = eligible_values.as_mut() {
                values.push(eligible_value);
            }
        }
    }

    if purged == 0 {
        return Ok((0, data));
    }

    let mut frame = data.data_frame().as_ref().clone();
    replace_series(
        &mut frame,
        &request.target,
        Series::new(request.target.as_str().into(), target_values),
    )?;
    replace_series(
        &mut frame,
        &rr_column,
        Series::new(rr_column.as_str().into(), reward_values),
    )?;
    if let Some(values) = eligible_values {
        replace_series(
            &mut frame,
            &eligible_column,
            Series::new(eligible_column.as_str().into(), values),
        )?;
    }

    Ok((purged, ColumnarData::from_frame(frame)))
}

fn replace_series(frame: &mut DataFrame, name: &str, series: Series) -> Result<()> {
    if frame.column(name).is_ok() {
        *frame = frame
            .drop(name)
            .with_context(|| format!("failed to drop column '{name}' before replacement"))?;
    }
    frame
        .with_column(series.into())
        .with_context(|| format!("failed to replace guarded column '{name}'"))?;
    Ok(())
}

fn annual_windows(data: &ColumnarData, scope: &DateWindow) -> Result<Vec<DateWindow>> {
    let dates = timestamp_dates(data)?;
    let mut years = BTreeSet::new();
    for date in dates.into_iter().flatten() {
        if scope.start.is_some_and(|start| date < start) {
            continue;
        }
        if scope.end.is_some_and(|end| date > end) {
            continue;
        }
        years.insert(date.year());
    }

    let mut out = Vec::new();
    for year in years {
        let mut start = NaiveDate::from_ymd_opt(year, 1, 1)
            .ok_or_else(|| anyhow!("invalid calendar year {year}"))?;
        let mut end = NaiveDate::from_ymd_opt(year, 12, 31)
            .ok_or_else(|| anyhow!("invalid calendar year {year}"))?;
        if let Some(scope_start) = scope.start {
            start = start.max(scope_start);
        }
        if let Some(scope_end) = scope.end {
            end = end.min(scope_end);
        }
        if start > end {
            continue;
        }
        out.push(DateWindow {
            label: format!("{year}-{}", year + 1),
            year: Some(year),
            start: Some(start),
            end: Some(end),
            apply_embargo: false,
        });
    }
    Ok(out)
}

fn date_bounds(data: &ColumnarData) -> Result<(Option<NaiveDate>, Option<NaiveDate>)> {
    let mut dates = timestamp_dates(data)?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if dates.is_empty() {
        return Ok((None, None));
    }
    dates.sort_unstable();
    Ok((dates.first().copied(), dates.last().copied()))
}

fn timestamp_strings(data: &ColumnarData) -> Result<Vec<String>> {
    let frame = data.data_frame();
    let series = frame
        .column("timestamp")
        .context("missing timestamp column for equity curve export")?;

    Ok(series
        .as_materialized_series()
        .iter()
        .map(|value| value.to_string())
        .collect())
}

fn timestamp_dates(data: &ColumnarData) -> Result<Vec<Option<NaiveDate>>> {
    let frame = data.data_frame();
    let series = frame
        .column("timestamp")
        .context("missing timestamp column for date handling")?;

    match series.dtype() {
        DataType::Datetime(unit, _) => {
            let ca = series.datetime()?;
            let mut out = Vec::with_capacity(ca.len());
            for value in ca.physical().iter() {
                out.push(match value {
                    Some(raw) => datetime_physical_to_date(raw, *unit),
                    None => None,
                });
            }
            Ok(out)
        }
        _ => {
            let mut out = Vec::with_capacity(series.len());
            for value in series.as_materialized_series().iter() {
                use polars::prelude::AnyValue;
                let parsed = match value {
                    AnyValue::String(raw) => parse_date_string(raw),
                    AnyValue::StringOwned(raw) => parse_date_string(raw.as_str()),
                    AnyValue::Null => None,
                    other => parse_date_string(&other.to_string()),
                };
                out.push(parsed);
            }
            Ok(out)
        }
    }
}

fn datetime_physical_to_date(raw: i64, unit: TimeUnit) -> Option<NaiveDate> {
    let (secs, nsecs) = match unit {
        TimeUnit::Nanoseconds => (raw / 1_000_000_000, (raw % 1_000_000_000) as u32),
        TimeUnit::Microseconds => (raw / 1_000_000, (raw % 1_000_000) as u32 * 1_000),
        TimeUnit::Milliseconds => (raw / 1_000, (raw % 1_000) as u32 * 1_000_000),
    };
    DateTime::<Utc>::from_timestamp(secs, nsecs).map(|dt| dt.date_naive())
}

fn parse_date_string(raw: &str) -> Option<NaiveDate> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.date_naive())
        .ok()
        .or_else(|| NaiveDate::parse_from_str(raw.get(..10)?, "%Y-%m-%d").ok())
}

fn equity_time_years(data: &ColumnarData) -> Result<f64> {
    let (Some(start), Some(end)) = date_bounds(data)? else {
        return Ok(1.0);
    };
    let days = (end - start).num_days().max(1) as f64;
    Ok((days / 365.25).max(1e-9))
}

fn next_day(date: NaiveDate) -> Result<NaiveDate> {
    date.succ_opt()
        .ok_or_else(|| anyhow!("cutoff date {date} cannot be advanced by one day"))
}

fn runtime_config(request: &FormulaEvalRequest, equity_time_years: Option<f64>) -> Config {
    Config {
        input_csv: request.prepared_path.clone(),
        source_csv: Some(request.prepared_path.clone()),
        direction: Direction::Long,
        target: request.target.clone(),
        output_dir: request
            .prepared_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
        max_depth: 1,
        min_sample_size: 0,
        min_sample_size_report: 0,
        include_date_start: None,
        include_date_end: None,
        batch_size: 1,
        n_workers: 1,
        auto_batch: false,
        resume_offset: 0,
        explicit_resume_offset: false,
        max_combos: None,
        dry_run: false,
        quiet: true,
        report_metrics: ReportMetricsMode::Off,
        report_top: 0,
        force_recompute: false,
        max_drawdown: f64::INFINITY,
        max_drawdown_report: None,
        min_calmar_report: None,
        strict_min_pruning: true,
        enable_subset_pruning: false,
        enable_feature_pairs: false,
        feature_pairs_limit: None,
        catalog_hash: None,
        stats_detail: StatsDetail::Full,
        eval_profile: EvalProfileMode::Off,
        eval_profile_sample_rate: 1,
        s3_output: None,
        s3_upload_each_batch: false,
        capital_dollar: Some(request.capital_dollar),
        risk_pct_per_trade: Some(request.risk_pct_per_trade),
        equity_time_years,
        asset: request.asset.clone(),
        risk_per_trade_dollar: request.dollars_per_r,
        cost_per_trade_dollar: request.cost_per_trade_dollar,
        cost_per_trade_r: request.cost_per_trade_r,
        dollars_per_r: request.dollars_per_r,
        tick_size: None,
        stacking_mode: request.stacking_mode,
        position_sizing: request.position_sizing,
        stop_distance_column: request.stop_distance_column.clone(),
        stop_distance_unit: request.stop_distance_unit,
        min_contracts: request.min_contracts.max(1),
        max_contracts: request.max_contracts,
        point_value: request.point_value,
        tick_value: request.tick_value,
        margin_per_contract_dollar: request.margin_per_contract_dollar,
        require_any_features: Vec::new(),
    }
}

fn buy_and_hold(data: &ColumnarData, capital_dollar: f64) -> Result<Option<BuyAndHoldSummary>> {
    if !data.has_column("close") {
        return Ok(None);
    }
    let closes = data
        .float_column("close")?
        .into_iter()
        .map(|value| value.filter(|raw| raw.is_finite() && *raw > 0.0))
        .collect::<Vec<_>>();
    let dates = timestamp_dates(data)?;

    let mut samples = Vec::new();
    for (close, date) in closes.into_iter().zip(dates.into_iter()) {
        if let (Some(close), Some(date)) = (close, date) {
            samples.push((date, close));
        }
    }
    if samples.len() < 2 {
        return Ok(None);
    }

    let Some((start, end)) = samples.first().copied().zip(samples.last().copied()) else {
        return Ok(None);
    };
    let start_close = start.1;
    let mut peak = 1.0_f64;
    let mut max_drawdown_pct = 0.0_f64;
    for (_, close) in &samples {
        let equity = *close / start_close;
        if equity > peak {
            peak = equity;
        }
        let dd = (peak - equity) / peak.max(f64::EPSILON) * 100.0;
        if dd > max_drawdown_pct {
            max_drawdown_pct = dd;
        }
    }
    let growth = end.1 / start_close;
    let total_return_pct = (growth - 1.0) * 100.0;
    let years = ((end.0 - start.0).num_days().max(1) as f64 / 365.25).max(1e-9);
    let cagr_pct = if growth.is_finite() && growth > 0.0 {
        (growth.powf(1.0 / years) - 1.0) * 100.0
    } else {
        total_return_pct
    };
    let calmar = if max_drawdown_pct > 0.0 {
        cagr_pct / max_drawdown_pct
    } else if cagr_pct > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };

    Ok(Some(BuyAndHoldSummary {
        rows: data.approx_rows(),
        start: Some(start.0),
        end: Some(end.0),
        start_close,
        end_close: end.1,
        total_return_pct,
        cagr_pct,
        max_drawdown_pct,
        calmar,
        final_capital: (capital_dollar > 0.0).then_some(capital_dollar * growth),
    }))
}

fn equity_curve_rows_for_window(
    full_data: &ColumnarData,
    request: &FormulaEvalRequest,
    source_results: &[FormulaResult],
    window: &DateWindow,
    top_k: usize,
    rank_source: RankBy,
) -> Result<Vec<EquityCurveRow>> {
    let (data, _guard) = filter_for_window(full_data, request, window)?;
    if data.approx_rows() == 0 {
        return Ok(Vec::new());
    }

    let config = runtime_config(request, Some(equity_time_years(&data)?));
    let ctx = EvaluationContext::new_with_reward_column(
        Arc::new(data.clone()),
        Arc::new(MaskCache::new()),
        &config,
        Arc::new(HashMap::new()),
        request.rr_column.as_deref(),
    )?;
    let formulas = source_results
        .iter()
        .take(top_k)
        .filter_map(|result| {
            request
                .formulas
                .iter()
                .find(|formula| formula.expression == result.formula)
        })
        .cloned()
        .collect::<Vec<_>>();
    let plan = build_formula_bitsets(&data, &formulas)?;
    let timestamps = timestamp_strings(&data)?;

    let mut rows = Vec::new();
    for (display_idx, (formula, indices)) in
        formulas.iter().zip(plan.combinations.iter()).enumerate()
    {
        let trade_indices = selected_trade_indices(indices, &ctx, &plan.bitsets)?;
        if trade_indices.is_empty() {
            continue;
        }
        let mut equity_r = 0.0_f64;
        let mut capital = request.capital_dollar;

        for (trade_idx, row_idx) in trade_indices.into_iter().enumerate() {
            let rr = ctx
                .rewards()
                .and_then(|values| values.get(row_idx).copied())
                .unwrap_or(0.0);
            equity_r += rr;

            let equity_dollar = if request.capital_dollar > 0.0 && request.risk_pct_per_trade > 0.0
            {
                let pnl = match request.position_sizing {
                    PositionSizingMode::Fractional => {
                        rr * capital * (request.risk_pct_per_trade / 100.0)
                    }
                    PositionSizingMode::Contracts => {
                        let rpc = ctx
                            .risk_per_contract_dollar()
                            .and_then(|values| values.get(row_idx).copied())
                            .unwrap_or(f64::NAN);
                        if rpc.is_finite() && rpc > 0.0 {
                            let budget = capital * (request.risk_pct_per_trade / 100.0);
                            let mut contracts = (budget / rpc).floor().max(0.0) as usize;
                            contracts = contracts.max(request.min_contracts.max(1));
                            if let Some(max_contracts) = request.max_contracts {
                                contracts = contracts.min(max_contracts);
                            }
                            if let Some(margin) = request.margin_per_contract_dollar {
                                if margin.is_finite() && margin > 0.0 {
                                    contracts = contracts.min((capital / margin).floor() as usize);
                                }
                            }
                            rr * rpc * contracts as f64
                        } else {
                            0.0
                        }
                    }
                };
                capital += pnl;
                Some(capital)
            } else {
                None
            };

            rows.push(EquityCurveRow {
                rank_by: rank_source.label().to_string(),
                window: window.label.clone(),
                rank: display_idx + 1,
                formula: formula.expression.clone(),
                timestamp: timestamps.get(row_idx).cloned().unwrap_or_default(),
                trade_index: trade_idx + 1,
                rr,
                equity_r,
                equity_dollar,
            });
        }
    }

    Ok(rows)
}

fn selected_trade_indices(
    indices: &IndexCombination,
    ctx: &EvaluationContext,
    bitsets: &BitsetCatalog,
) -> Result<Vec<usize>> {
    let target_len = ctx.target().len();
    let mut combo_bitsets = Vec::with_capacity(indices.len());
    for idx in indices {
        combo_bitsets.push(
            bitsets
                .get_by_index(*idx)
                .ok_or_else(|| anyhow!("missing formula bitset index {idx}"))?,
        );
    }
    sort_bitsets_by_support(combo_bitsets.as_mut_slice());

    let rewards = ctx.rewards();
    let eligible = ctx.eligible();
    let exit_indices = ctx.exit_indices();
    let no_stacking = ctx.stacking_mode() == StackingMode::NoStacking;
    let mut selected = Vec::new();
    let mut next_free_idx = 0usize;

    let mut on_hit = |idx: usize| {
        if no_stacking && idx < next_free_idx {
            return;
        }
        if eligible.is_some_and(|mask| idx < mask.len() && !mask[idx]) {
            return;
        }
        let Some(rr) = rewards.and_then(|values| values.get(idx).copied()) else {
            return;
        };
        if !rr.is_finite() {
            return;
        }

        selected.push(idx);
        if no_stacking {
            let exit_i = exit_indices.and_then(|values| values.get(idx).copied());
            let candidate = match exit_i {
                Some(value) if value != usize::MAX && value > idx => value,
                _ => idx.saturating_add(1),
            };
            if candidate > next_free_idx {
                next_free_idx = candidate;
            }
        }
    };

    scan_bitsets_scalar_dyn_gated(&combo_bitsets, target_len, None, None, &mut on_hit);
    Ok(selected)
}

impl RankBy {
    pub fn label(self) -> &'static str {
        match self {
            Self::CalmarEquity => "calmar_equity",
            Self::Frs => "frs",
        }
    }
}
