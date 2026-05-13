use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use polars::prelude::*;
use serde::Serialize;

use crate::bitset::{BitsetCatalog, BitsetMask, scan_bitsets_scalar_dyn_gated};
use crate::combinator::IndexCombination;
use crate::config::{
    Config, Direction, EvalProfileMode, PositionSizingMode, ReportMetricsMode, StackingMode,
    StatsDetail, StopDistanceUnit,
};
use crate::data::ColumnarData;
use crate::formula::{FormulaClause, FormulaOperator, RankedFormula};
use crate::frs::{ForwardRobustnessComponents, FrsOptions, compute_frs};
use crate::mask::MaskCache;
use crate::stats::{EvaluationContext, StatSummary, evaluate_combination_indices};

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
}

#[derive(Debug, Clone, Serialize)]
pub struct FormulaEvaluationReport {
    pub prepared_path: PathBuf,
    pub target: String,
    pub rr_column: String,
    pub cutoff: NaiveDate,
    pub pre: FormulaWindowReport,
    pub post: FormulaWindowReport,
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
    pub results: Vec<FormulaResult>,
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
    };
    let post_window = DateWindow {
        label: format!(">  {}", request.cutoff),
        year: None,
        start: Some(next_day(request.cutoff)?),
        end: None,
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

    Ok(FormulaEvaluationReport {
        prepared_path: request.prepared_path.clone(),
        target: request.target.clone(),
        rr_column,
        cutoff: request.cutoff,
        pre,
        post,
        frs_rows,
        frs_window_rows,
    })
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
    let data = filter_for_window(full_data, window)?;
    let rows = data.approx_rows();
    let (start, end) = date_bounds(&data)?;
    if rows == 0 {
        return Ok(FormulaWindowReport {
            label: window.label.clone(),
            rows,
            start,
            end,
            buy_and_hold: None,
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
        combinations.push(indices);
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

fn filter_for_window(data: &ColumnarData, window: &DateWindow) -> Result<ColumnarData> {
    data.filter_by_date_range(window.start, window.end)
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

    let start = samples.first().copied().unwrap();
    let end = samples.last().copied().unwrap();
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
    let data = filter_for_window(full_data, window)?;
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
    combo_bitsets.sort_by_key(|mask| mask.support);

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
