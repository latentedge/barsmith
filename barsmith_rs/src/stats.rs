use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use smallvec::SmallVec;

pub use crate::bitset::BitsetCatalog;
use crate::bitset::{BitsetMask, scan_bitsets_scalar_dyn_gated, scan_bitsets_simd_dyn_gated};
use crate::combinator::Combination;
use crate::config::{
    Config, Direction, EvalProfileMode, PositionSizingMode, StackingMode, StatsDetail,
    StopDistanceUnit,
};
use crate::data::ColumnarData;
use crate::feature::{ComparisonOperator, ComparisonSpec, FeatureDescriptor};
use crate::mask::{MaskBuffer, MaskCache};

mod metrics;

use metrics::{CoreStatsAccumulator, classify_sample, compute_statistics};
#[cfg(test)]
use metrics::{compute_full_statistics, percentile_triplet, round_to};

thread_local! {
    static RETURNS_BUFFER: RefCell<Vec<f64>> = const { RefCell::new(Vec::new()) };
    static RISK_PER_CONTRACT_BUFFER: RefCell<Vec<f64>> = const { RefCell::new(Vec::new()) };
    static SORTED_RETURNS_BUFFER: RefCell<Vec<f64>> = const { RefCell::new(Vec::new()) };
}

#[cfg(feature = "bench-api")]
pub fn benchmark_core_statistics_fixture(rows: usize) -> Vec<f64> {
    (0..rows)
        .map(|idx| match idx % 11 {
            0 => 2.0,
            1 | 2 => 1.0,
            3 => 0.5,
            4 | 5 => -0.75,
            6 => -1.25,
            _ => 0.25,
        })
        .collect()
}

#[cfg(feature = "bench-api")]
pub fn benchmark_core_statistics_checksum(rows: usize, repeats: usize) -> f64 {
    let returns = benchmark_core_statistics_fixture(rows);
    benchmark_core_statistics_checksum_for_returns(&returns, repeats)
}

#[cfg(feature = "bench-api")]
pub fn benchmark_core_statistics_checksum_for_returns(returns: &[f64], repeats: usize) -> f64 {
    let mut checksum = 0.0;
    for _ in 0..repeats {
        let summary = compute_statistics(
            3,
            returns.len(),
            returns.len() / 2,
            Some(returns),
            None,
            returns.len(),
            StatsDetail::Core,
            PositionSizingMode::Fractional,
            None,
            Some(0.0),
            Some(100_000.0),
            Some(1.0),
            Some(1.0),
            1,
            None,
            None,
        );
        checksum += summary.total_return + summary.max_drawdown + summary.final_capital;
    }
    checksum
}

#[derive(Clone)]
pub struct EvaluationContext {
    data: Arc<ColumnarData>,
    mask_cache: Arc<MaskCache>,
    comparisons: Arc<HashMap<String, ComparisonSpec>>,
    target: Arc<Vec<bool>>,
    rewards: Option<Arc<Vec<f64>>>,
    risk_per_contract_dollar: Option<Arc<Vec<f64>>>,
    eligible: Option<Arc<Vec<bool>>>,
    trade_gate_bitset: Option<Arc<BitsetMask>>,
    stacking_mode: StackingMode,
    exit_indices: Option<Arc<Vec<usize>>>,
    row_count: usize,
    stats_detail: StatsDetail,
    position_sizing: PositionSizingMode,
    min_contracts: usize,
    max_contracts: Option<usize>,
    cost_per_trade_dollar: Option<f64>,
    margin_per_contract_dollar: Option<f64>,
    cost_per_trade_r: Option<f64>,
    dollars_per_r: Option<f64>,
    capital_dollar: Option<f64>,
    risk_pct_per_trade: Option<f64>,
    equity_time_years: Option<f64>,
}

impl EvaluationContext {
    pub fn new(
        data: Arc<ColumnarData>,
        mask_cache: Arc<MaskCache>,
        config: &Config,
        comparisons: Arc<HashMap<String, ComparisonSpec>>,
    ) -> Result<Self> {
        Self::new_inner(data, mask_cache, config, comparisons, None)
    }

    pub fn new_with_reward_column(
        data: Arc<ColumnarData>,
        mask_cache: Arc<MaskCache>,
        config: &Config,
        comparisons: Arc<HashMap<String, ComparisonSpec>>,
        reward_column: Option<&str>,
    ) -> Result<Self> {
        Self::new_inner(data, mask_cache, config, comparisons, reward_column)
    }

    fn new_inner(
        data: Arc<ColumnarData>,
        mask_cache: Arc<MaskCache>,
        config: &Config,
        comparisons: Arc<HashMap<String, ComparisonSpec>>,
        reward_column_override: Option<&str>,
    ) -> Result<Self> {
        let position_sizing = config.position_sizing;
        let min_contracts = config.min_contracts.max(1);
        let max_contracts = config.max_contracts;

        let target = Arc::new(load_boolean_vector(&data, &config.target)?);
        let eligible = {
            let column = format!("{}_eligible", config.target);
            if data.has_column(&column) {
                Some(Arc::new(load_boolean_vector(&data, &column)?))
            } else {
                None
            }
        };
        let eligible_bitset = eligible
            .as_deref()
            .map(|values| BitsetMask::from_bools(values.as_slice()))
            .map(Arc::new);

        let stop_distance_unit = config.stop_distance_unit;
        let risk_per_contract_dollar = if matches!(position_sizing, PositionSizingMode::Contracts) {
            let stop_col = config.stop_distance_column.as_deref().ok_or_else(|| {
                anyhow!(
                    "Missing stop_distance_column in config for position_sizing=contracts. Provide --stop-distance-column (or use a target that infers it)."
                )
            })?;
            let stop_distance = load_float_vector(&data, stop_col)?;
            let multiplier = match stop_distance_unit {
                StopDistanceUnit::Points => config.point_value.ok_or_else(|| {
                    anyhow!("Missing point_value in config for contracts sizing. Provide --asset or set config.point_value.")
                })?,
                StopDistanceUnit::Ticks => config.tick_value.ok_or_else(|| {
                    anyhow!("Missing tick_value in config for contracts sizing. Provide --asset or set config.tick_value.")
                })?,
            };
            if !multiplier.is_finite() || multiplier <= 0.0 {
                return Err(anyhow!("Invalid stop-distance multiplier: {multiplier}"));
            }
            let mut rpc: Vec<f64> = Vec::with_capacity(stop_distance.len());
            for v in stop_distance {
                if v.is_finite() && v > 0.0 {
                    let dollars = v * multiplier;
                    if dollars.is_finite() && dollars > 0.0 {
                        rpc.push(dollars);
                    } else {
                        rpc.push(f64::NAN);
                    }
                } else {
                    rpc.push(f64::NAN);
                }
            }
            Some(Arc::new(rpc))
        } else {
            None
        };

        let reward_column =
            detect_reward_column_with_override(&data, config, reward_column_override)?;
        let (rewards, reward_finite_bitset) = match reward_column {
            Some(column) => {
                let mut values = load_float_vector(&data, &column)?;
                let mut any_non_finite = false;
                match position_sizing {
                    PositionSizingMode::Fractional => {
                        if let Some(cost_r) = config.cost_per_trade_r {
                            if cost_r != 0.0 {
                                for v in &mut values {
                                    if v.is_finite() {
                                        *v -= cost_r;
                                    } else {
                                        any_non_finite = true;
                                    }
                                }
                            }
                        }
                    }
                    PositionSizingMode::Contracts => {
                        let rpc = risk_per_contract_dollar.as_deref().ok_or_else(|| {
                            anyhow!("Missing risk_per_contract_dollar series for contracts sizing")
                        })?;
                        let cost_dollar = config.cost_per_trade_dollar.unwrap_or(0.0);
                        for (idx, v) in values.iter_mut().enumerate() {
                            let rpc_i = rpc.get(idx).copied().unwrap_or(f64::NAN);
                            if !v.is_finite() {
                                any_non_finite = true;
                                continue;
                            }
                            if !rpc_i.is_finite() || rpc_i <= 0.0 {
                                *v = f64::NAN;
                                any_non_finite = true;
                                continue;
                            }
                            if cost_dollar != 0.0 {
                                *v -= cost_dollar / rpc_i;
                            }
                        }
                    }
                }
                if !any_non_finite {
                    any_non_finite = values.iter().any(|v| !v.is_finite());
                }
                let finite = if any_non_finite {
                    Some(Arc::new(BitsetMask::from_finite_f64(&values)))
                } else {
                    None
                };
                (Some(Arc::new(values)), finite)
            }
            None => (None, None),
        };
        let trade_gate_bitset = BitsetMask::from_eval_gates(
            target.len(),
            eligible_bitset.as_deref(),
            reward_finite_bitset.as_deref(),
        )
        .map(Arc::new);

        let stacking_mode = config.stacking_mode;
        let exit_indices = if stacking_mode == StackingMode::NoStacking {
            let column = format!("{}_exit_i", config.target);
            if !data.has_column(&column) {
                return Err(anyhow!(
                    "Missing required '{}' column for --stacking-mode no-stacking. Re-generate the prepared dataset (barsmith_prepared.csv) with a feature-engineering step that emits exit indices.",
                    column
                ));
            }
            let ca = data.i64_column(&column)?;
            let mut values: Vec<usize> = Vec::with_capacity(ca.len());
            for opt in ca.into_iter() {
                let idx = match opt {
                    Some(v) if v >= 0 => v as usize,
                    _ => usize::MAX,
                };
                values.push(idx);
            }
            Some(Arc::new(values))
        } else {
            None
        };
        Ok(Self {
            data: Arc::clone(&data),
            mask_cache,
            comparisons,
            target,
            rewards,
            risk_per_contract_dollar,
            eligible,
            trade_gate_bitset,
            stacking_mode,
            exit_indices,
            row_count: data.approx_rows(),
            stats_detail: config.stats_detail,
            position_sizing,
            min_contracts,
            max_contracts,
            cost_per_trade_dollar: config.cost_per_trade_dollar,
            margin_per_contract_dollar: config.margin_per_contract_dollar,
            cost_per_trade_r: config.cost_per_trade_r,
            dollars_per_r: config.dollars_per_r,
            capital_dollar: config.capital_dollar,
            risk_pct_per_trade: config.risk_pct_per_trade,
            equity_time_years: config.equity_time_years,
        })
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn target(&self) -> &[bool] {
        self.target.as_ref()
    }

    pub fn rewards(&self) -> Option<&[f64]> {
        self.rewards.as_deref().map(|values| values.as_slice())
    }

    pub fn eligible(&self) -> Option<&[bool]> {
        self.eligible.as_deref().map(|values| values.as_slice())
    }

    pub fn stacking_mode(&self) -> StackingMode {
        self.stacking_mode
    }

    pub fn exit_indices(&self) -> Option<&[usize]> {
        self.exit_indices.as_deref().map(|values| values.as_slice())
    }

    fn trade_gate_bitset(&self) -> Option<&BitsetMask> {
        self.trade_gate_bitset.as_deref()
    }

    pub fn position_sizing(&self) -> PositionSizingMode {
        self.position_sizing
    }

    pub fn min_contracts(&self) -> usize {
        self.min_contracts
    }

    pub fn max_contracts(&self) -> Option<usize> {
        self.max_contracts
    }

    pub fn margin_per_contract_dollar(&self) -> Option<f64> {
        self.margin_per_contract_dollar
    }

    pub fn risk_per_contract_dollar(&self) -> Option<&[f64]> {
        self.risk_per_contract_dollar
            .as_deref()
            .map(|values| values.as_slice())
    }

    #[allow(dead_code)]
    pub fn cost_per_trade_dollar(&self) -> Option<f64> {
        self.cost_per_trade_dollar
    }

    pub fn cost_per_trade_r(&self) -> Option<f64> {
        self.cost_per_trade_r
    }

    pub fn dollars_per_r(&self) -> Option<f64> {
        self.dollars_per_r
    }

    pub fn capital_dollar(&self) -> Option<f64> {
        self.capital_dollar
    }

    pub fn risk_pct_per_trade(&self) -> Option<f64> {
        self.risk_pct_per_trade
    }

    pub fn equity_time_years(&self) -> Option<f64> {
        self.equity_time_years
    }

    /// Return true if the named feature corresponds to a feature-to-feature
    /// comparison (i.e., its ComparisonSpec has a right-hand-side feature).
    /// Feature-to-constant thresholds return false.
    pub fn is_feature_pair(&self, feature: &str) -> bool {
        match self.comparisons.get(feature) {
            Some(spec) => spec.rhs_feature.is_some(),
            None => false,
        }
    }

    pub fn feature_mask(&self, feature: &str) -> Result<MaskBuffer> {
        if let Some(mask) = self.mask_cache.get(feature) {
            return Ok(mask);
        }
        if let Some(spec) = self.comparisons.get(feature) {
            return self.build_comparison_mask(feature, spec);
        }
        let column = self
            .data
            .boolean_column(feature)
            .with_context(|| format!("Feature column '{feature}' missing from dataset"))?;
        let mask = column
            .into_iter()
            .map(|value| value.unwrap_or(false))
            .collect();
        Ok(self.mask_cache.get_or_insert(feature, mask))
    }

    fn build_comparison_mask(&self, feature: &str, spec: &ComparisonSpec) -> Result<MaskBuffer> {
        // Feature-to-feature comparison
        if let Some(rhs) = &spec.rhs_feature {
            let left = self
                .data
                .float_column(&spec.base_feature)
                .with_context(|| {
                    format!(
                        "Numeric column '{}' missing for comparison",
                        spec.base_feature
                    )
                })?;
            let right = self
                .data
                .float_column(rhs)
                .with_context(|| format!("Numeric column '{}' missing for comparison", rhs))?;
            let mask = left
                .into_iter()
                .zip(&right)
                .map(|(l, r)| match (l, r) {
                    (Some(a), Some(b)) if a.is_finite() && b.is_finite() => {
                        apply_pair_operator(a, b, spec.operator)
                    }
                    _ => false,
                })
                .collect();
            return Ok(self.mask_cache.get_or_insert(feature, mask));
        }

        // Feature-to-threshold comparison
        let threshold = spec.threshold.unwrap_or(0.0);
        let column = self
            .data
            .float_column(&spec.base_feature)
            .with_context(|| {
                format!(
                    "Numeric column '{}' missing for comparison",
                    spec.base_feature
                )
            })?;
        let mask = column
            .into_iter()
            .map(|value| match value {
                Some(raw) if raw.is_finite() => apply_operator(raw, threshold, spec.operator),
                _ => false,
            })
            .collect();
        Ok(self.mask_cache.get_or_insert(feature, mask))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StatSummary {
    pub depth: usize,
    /// Number of bars where the combo mask is true (used for recall/density reporting).
    pub mask_hits: usize,
    pub total_bars: usize,
    pub profitable_bars: usize,
    pub unprofitable_bars: usize,
    /// Net-R win rate (% of trades with RR > 0).
    pub win_rate: f64,
    /// Target/label hit-rate (% of trades where the engineered target is true).
    pub label_hit_rate: f64,
    /// Number of trades where the engineered target is true.
    pub label_hits: usize,
    /// Number of trades where the engineered target is false.
    pub label_misses: usize,
    pub expectancy: f64,
    pub profit_factor: f64,
    /// Average reward on winning trades only (R per winning bar).
    pub avg_winning_rr: f64,
    pub calmar_ratio: f64,
    pub max_drawdown: f64,
    pub win_loss_ratio: f64,
    pub ulcer_index: f64,
    pub pain_ratio: f64,
    pub max_consecutive_wins: usize,
    pub max_consecutive_losses: usize,
    /// Average length of winning streaks (trades with RR > 0).
    pub avg_win_streak: f64,
    /// Average length of losing streaks (trades with RR < 0).
    pub avg_loss_streak: f64,
    /// Median R per trade for this combination.
    pub median_rr: f64,
    /// Average losing R per trade (negative value).
    pub avg_losing_rr: f64,
    /// 5th percentile R (downside tail).
    pub p05_rr: f64,
    /// 95th percentile R (upside tail).
    pub p95_rr: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub sample_quality: &'static str,
    pub total_return: f64,
    pub cost_per_trade_r: f64,
    pub dollars_per_r: f64,
    pub total_return_dollar: f64,
    pub max_drawdown_dollar: f64,
    pub expectancy_dollar: f64,
    pub final_capital: f64,
    pub total_return_pct: f64,
    pub cagr_pct: f64,
    pub max_drawdown_pct_equity: f64,
    pub calmar_equity: f64,
    pub sharpe_equity: f64,
    pub sortino_equity: f64,
}

impl StatSummary {
    #[allow(dead_code)]
    fn empty(depth: usize, sample_size: usize) -> Self {
        Self {
            depth,
            mask_hits: sample_size,
            total_bars: sample_size,
            profitable_bars: 0,
            unprofitable_bars: sample_size,
            win_rate: 0.0,
            label_hit_rate: 0.0,
            label_hits: 0,
            label_misses: sample_size,
            expectancy: 0.0,
            profit_factor: 0.0,
            avg_winning_rr: 0.0,
            calmar_ratio: 0.0,
            max_drawdown: 0.0,
            win_loss_ratio: 0.0,
            ulcer_index: 0.0,
            pain_ratio: 0.0,
            max_consecutive_wins: 0,
            max_consecutive_losses: 0,
            avg_win_streak: 0.0,
            avg_loss_streak: 0.0,
            median_rr: 0.0,
            avg_losing_rr: 0.0,
            p05_rr: 0.0,
            p95_rr: 0.0,
            largest_win: 0.0,
            largest_loss: 0.0,
            sample_quality: classify_sample(sample_size),
            total_return: 0.0,
            cost_per_trade_r: 0.0,
            dollars_per_r: 0.0,
            total_return_dollar: 0.0,
            max_drawdown_dollar: 0.0,
            expectancy_dollar: 0.0,
            final_capital: 0.0,
            total_return_pct: 0.0,
            cagr_pct: 0.0,
            max_drawdown_pct_equity: 0.0,
            calmar_equity: 0.0,
            sharpe_equity: 0.0,
            sortino_equity: 0.0,
        }
    }

    fn under_min(depth: usize, total_bars: usize) -> Self {
        Self {
            depth,
            mask_hits: total_bars,
            total_bars,
            profitable_bars: 0,
            unprofitable_bars: total_bars,
            win_rate: 0.0,
            label_hit_rate: 0.0,
            label_hits: 0,
            label_misses: total_bars,
            expectancy: 0.0,
            profit_factor: 0.0,
            avg_winning_rr: 0.0,
            calmar_ratio: 0.0,
            max_drawdown: 0.0,
            win_loss_ratio: 0.0,
            ulcer_index: 0.0,
            pain_ratio: 0.0,
            max_consecutive_wins: 0,
            max_consecutive_losses: 0,
            avg_win_streak: 0.0,
            avg_loss_streak: 0.0,
            median_rr: 0.0,
            avg_losing_rr: 0.0,
            p05_rr: 0.0,
            p95_rr: 0.0,
            largest_win: 0.0,
            largest_loss: 0.0,
            sample_quality: classify_sample(total_bars),
            total_return: 0.0,
            cost_per_trade_r: 0.0,
            dollars_per_r: 0.0,
            total_return_dollar: 0.0,
            max_drawdown_dollar: 0.0,
            expectancy_dollar: 0.0,
            final_capital: 0.0,
            total_return_pct: 0.0,
            cagr_pct: 0.0,
            max_drawdown_pct_equity: 0.0,
            calmar_equity: 0.0,
            sharpe_equity: 0.0,
            sortino_equity: 0.0,
        }
    }
}

pub fn evaluate_combination(
    combination: &Combination,
    ctx: &EvaluationContext,
    bitsets: &BitsetCatalog,
    min_sample_size: usize,
) -> Result<StatSummary> {
    let depth = combination.len();

    let mut combo_bitsets: Vec<&BitsetMask> = Vec::with_capacity(combination.len());
    for descriptor in combination {
        let name = descriptor.name.as_str();
        let mask = bitsets
            .get(name)
            .ok_or_else(|| anyhow!("Missing bitset for feature '{name}'"))?;
        combo_bitsets.push(mask);
    }

    // Reorder bitsets by ascending support (sparsest first) so that
    // intersections clear bits sooner and later ANDs become cheaper.
    combo_bitsets.sort_by_key(|m| m.support);

    Ok(evaluate_for_bitsets(
        depth,
        ctx,
        &combo_bitsets,
        min_sample_size,
    ))
}

pub fn evaluate_combination_indices(
    indices: &[usize],
    ctx: &EvaluationContext,
    bitsets: &BitsetCatalog,
    min_sample_size: usize,
) -> Result<StatSummary> {
    let depth = indices.len();
    let mut combo_bitsets: SmallVec<[&BitsetMask; 8]> = SmallVec::with_capacity(indices.len());
    for &idx in indices {
        let mask = bitsets
            .get_by_index(idx)
            .ok_or_else(|| anyhow!("Missing bitset for feature index {idx}"))?;
        combo_bitsets.push(mask);
    }

    // Reorder bitsets by ascending support to reduce intersection cost.
    combo_bitsets.sort_by_key(|m| m.support);

    Ok(evaluate_for_bitsets(
        depth,
        ctx,
        &combo_bitsets,
        min_sample_size,
    ))
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EvalProfileTotals {
    pub combos_profiled: u64,
    pub build_ns: u64,
    pub scan_ns: u64,
    pub on_hit_ns: u64,
    pub finalize_ns: u64,
    pub mask_hits: u64,
    pub trades: u64,
}

impl EvalProfileTotals {
    pub fn add_assign(&mut self, other: Self) {
        self.combos_profiled += other.combos_profiled;
        self.build_ns += other.build_ns;
        self.scan_ns += other.scan_ns;
        self.on_hit_ns += other.on_hit_ns;
        self.finalize_ns += other.finalize_ns;
        self.mask_hits += other.mask_hits;
        self.trades += other.trades;
    }

    pub fn ms(self) -> (u64, u64, u64, u64) {
        (
            self.build_ns / 1_000_000,
            self.scan_ns / 1_000_000,
            self.on_hit_ns / 1_000_000,
            self.finalize_ns / 1_000_000,
        )
    }
}

fn should_profile_indices(indices: &[usize], sample_rate: usize) -> bool {
    if sample_rate <= 1 {
        return true;
    }
    // Deterministic sampling keeps profiling cheap in the hot loop.
    let first = indices.first().copied().unwrap_or(0) as u64;
    let depth = indices.len() as u64;
    let hash = first
        .wrapping_mul(0x9E37_79B1_85EB_CA87)
        .wrapping_add(depth);
    (hash % sample_rate as u64) == 0
}

pub fn evaluate_combination_indices_profiled(
    indices: &[usize],
    ctx: &EvaluationContext,
    bitsets: &BitsetCatalog,
    min_sample_size: usize,
    mode: EvalProfileMode,
    sample_rate: usize,
) -> Result<(StatSummary, EvalProfileTotals)> {
    let do_profile =
        mode != EvalProfileMode::Off && should_profile_indices(indices, sample_rate.max(1));
    if !do_profile {
        return Ok((
            evaluate_combination_indices(indices, ctx, bitsets, min_sample_size)?,
            EvalProfileTotals::default(),
        ));
    }

    let depth = indices.len();
    let build_start = Instant::now();
    let mut combo_bitsets: SmallVec<[&BitsetMask; 8]> = SmallVec::with_capacity(indices.len());
    for &idx in indices {
        let mask = bitsets
            .get_by_index(idx)
            .ok_or_else(|| anyhow!("Missing bitset for feature index {idx}"))?;
        combo_bitsets.push(mask);
    }
    combo_bitsets.sort_by_key(|m| m.support);
    let build_ns = build_start.elapsed().as_nanos() as u64;

    let (stat, mut profile) =
        evaluate_for_bitsets_profiled(depth, ctx, &combo_bitsets, min_sample_size, mode);
    profile.combos_profiled = 1;
    profile.build_ns = build_ns;
    Ok((stat, profile))
}

#[allow(clippy::collapsible_else_if)]
fn evaluate_for_bitsets_profiled(
    depth: usize,
    ctx: &EvaluationContext,
    combo_bitsets: &[&BitsetMask],
    min_sample_size: usize,
    mode: EvalProfileMode,
) -> (StatSummary, EvalProfileTotals) {
    let mut profile = EvalProfileTotals::default();
    let target = ctx.target();
    let rewards = ctx.rewards();
    let no_stacking = ctx.stacking_mode() == StackingMode::NoStacking;
    let exit_indices = if no_stacking {
        ctx.exit_indices()
            .expect("exit indices must be present when stacking_mode is NoStacking")
    } else {
        &[]
    };
    let scan_gate = ctx.trade_gate_bitset();
    let position_sizing = ctx.position_sizing();
    let risk_per_contract = ctx.risk_per_contract_dollar();
    let min_contracts = ctx.min_contracts();
    let max_contracts = ctx.max_contracts();
    let max_len = combo_bitsets
        .first()
        .map(|bitset| bitset.len.min(target.len()))
        .unwrap_or(0);

    if let Some(smallest) = combo_bitsets.first() {
        if smallest.support < min_sample_size {
            return (StatSummary::under_min(depth, smallest.support), profile);
        }
    }

    if combo_bitsets.is_empty() || max_len == 0 {
        let finalize_start = Instant::now();
        let stat = compute_statistics(
            depth,
            0,
            0,
            None,
            None,
            ctx.row_count(),
            ctx.stats_detail,
            position_sizing,
            ctx.dollars_per_r(),
            ctx.cost_per_trade_r(),
            ctx.capital_dollar(),
            ctx.risk_pct_per_trade(),
            ctx.equity_time_years(),
            min_contracts,
            max_contracts,
            ctx.margin_per_contract_dollar(),
        );
        profile.finalize_ns += finalize_start.elapsed().as_nanos() as u64;
        return (stat, profile);
    }

    #[cfg(feature = "simd-eval")]
    let use_simd = combo_bitsets.len() >= 2;
    #[cfg(not(feature = "simd-eval"))]
    let use_simd = false;

    if let Some(reward_series) = rewards {
        if matches!(ctx.stats_detail, StatsDetail::Core) {
            if no_stacking {
                let mut total = 0usize;
                let mut label_hits = 0usize;
                let mut acc = CoreStatsAccumulator::new(
                    position_sizing,
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                );
                let mut next_free_idx = 0usize;

                let mut on_hit_inner = |idx: usize| {
                    if idx < next_free_idx {
                        return;
                    }
                    if idx >= reward_series.len() {
                        return;
                    }
                    let rr_net = reward_series[idx];

                    total += 1;
                    acc.total_bars += 1;
                    let rpc = if matches!(position_sizing, PositionSizingMode::Contracts) {
                        risk_per_contract.and_then(|values| values.get(idx).copied())
                    } else {
                        None
                    };
                    acc.push(rr_net, rpc);
                    if target[idx] {
                        label_hits += 1;
                    }

                    let exit_i = exit_indices[idx];
                    let candidate = if exit_i == usize::MAX || exit_i < idx {
                        idx.saturating_add(1)
                    } else {
                        exit_i
                    };
                    if candidate > next_free_idx {
                        next_free_idx = candidate;
                    }
                };

                let scan_start = Instant::now();
                let scan_total = if use_simd {
                    if matches!(mode, EvalProfileMode::Fine) {
                        let mut on_hit = |idx: usize| {
                            let start = Instant::now();
                            on_hit_inner(idx);
                            profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                        };
                        scan_bitsets_simd_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit,
                        )
                    } else {
                        scan_bitsets_simd_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit_inner,
                        )
                    }
                } else if matches!(mode, EvalProfileMode::Fine) {
                    let mut on_hit = |idx: usize| {
                        let start = Instant::now();
                        on_hit_inner(idx);
                        profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                    };
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                } else {
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit_inner,
                    )
                };
                profile.scan_ns += scan_start.elapsed().as_nanos() as u64;
                profile.mask_hits += scan_total as u64;
                profile.trades += total as u64;

                let finalize_start = Instant::now();
                let stat = if total < min_sample_size {
                    StatSummary::under_min(depth, total)
                } else {
                    acc.finalize(depth, label_hits, ctx.equity_time_years())
                };
                profile.finalize_ns += finalize_start.elapsed().as_nanos() as u64;
                (stat, profile)
            } else {
                let mut total = 0usize;
                let mut label_hits = 0usize;
                let mut acc = CoreStatsAccumulator::new(
                    position_sizing,
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                );

                let mut on_hit_inner = |idx: usize| {
                    if idx >= reward_series.len() {
                        return;
                    }
                    let rr_net = reward_series[idx];

                    total += 1;
                    acc.total_bars += 1;
                    let rpc = if matches!(position_sizing, PositionSizingMode::Contracts) {
                        risk_per_contract.and_then(|values| values.get(idx).copied())
                    } else {
                        None
                    };
                    acc.push(rr_net, rpc);
                    if target[idx] {
                        label_hits += 1;
                    }
                };

                let scan_start = Instant::now();
                let scan_total = if use_simd {
                    if matches!(mode, EvalProfileMode::Fine) {
                        let mut on_hit = |idx: usize| {
                            let start = Instant::now();
                            on_hit_inner(idx);
                            profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                        };
                        scan_bitsets_simd_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit,
                        )
                    } else {
                        scan_bitsets_simd_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit_inner,
                        )
                    }
                } else if matches!(mode, EvalProfileMode::Fine) {
                    let mut on_hit = |idx: usize| {
                        let start = Instant::now();
                        on_hit_inner(idx);
                        profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                    };
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                } else {
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit_inner,
                    )
                };
                profile.scan_ns += scan_start.elapsed().as_nanos() as u64;
                profile.mask_hits += scan_total as u64;
                profile.trades += total as u64;

                let finalize_start = Instant::now();
                let stat = if total < min_sample_size {
                    StatSummary::under_min(depth, total)
                } else {
                    acc.finalize(depth, label_hits, ctx.equity_time_years())
                };
                profile.finalize_ns += finalize_start.elapsed().as_nanos() as u64;
                (stat, profile)
            }
        } else {
            RETURNS_BUFFER.with(|cell| {
                let mut returns = cell.borrow_mut();
                returns.clear();

                RISK_PER_CONTRACT_BUFFER.with(|risk_cell| {
                    let mut risks = risk_cell.borrow_mut();
                    risks.clear();
                    let want_risk = matches!(position_sizing, PositionSizingMode::Contracts);

                    if no_stacking {
                        let mut total = 0usize;
                        let mut label_hits = 0usize;
                        let mut next_free_idx = 0usize;

                        let mut on_hit_inner = |idx: usize| {
                            if idx < next_free_idx {
                                return;
                            }
                            if idx >= reward_series.len() {
                                return;
                            }
                            let rr_net = reward_series[idx];
                            total += 1;
                            if target[idx] {
                                label_hits += 1;
                            }
                            returns.push(rr_net);
                            if want_risk {
                                let rpc = risk_per_contract
                                    .and_then(|values| values.get(idx).copied())
                                    .unwrap_or(f64::NAN);
                                risks.push(rpc);
                            }

                            let exit_i = exit_indices[idx];
                            let candidate = if exit_i == usize::MAX || exit_i < idx {
                                idx.saturating_add(1)
                            } else {
                                exit_i
                            };
                            if candidate > next_free_idx {
                                next_free_idx = candidate;
                            }
                        };

                        let scan_start = Instant::now();
                        let scan_total = if use_simd {
                            if matches!(mode, EvalProfileMode::Fine) {
                                let mut on_hit = |idx: usize| {
                                    let start = Instant::now();
                                    on_hit_inner(idx);
                                    profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                                };
                                scan_bitsets_simd_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit,
                                )
                            } else {
                                scan_bitsets_simd_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit_inner,
                                )
                            }
                        } else if matches!(mode, EvalProfileMode::Fine) {
                            let mut on_hit = |idx: usize| {
                                let start = Instant::now();
                                on_hit_inner(idx);
                                profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                            };
                            scan_bitsets_scalar_dyn_gated(
                                combo_bitsets,
                                max_len,
                                scan_gate,
                                None,
                                &mut on_hit,
                            )
                        } else {
                            scan_bitsets_scalar_dyn_gated(
                                combo_bitsets,
                                max_len,
                                scan_gate,
                                None,
                                &mut on_hit_inner,
                            )
                        };
                        profile.scan_ns += scan_start.elapsed().as_nanos() as u64;
                        profile.mask_hits += scan_total as u64;
                        profile.trades += total as u64;

                        let finalize_start = Instant::now();
                        let stat = if total < min_sample_size {
                            StatSummary::under_min(depth, total)
                        } else {
                            compute_statistics(
                                depth,
                                total,
                                label_hits,
                                Some(&returns[..]),
                                if want_risk { Some(&risks[..]) } else { None },
                                ctx.row_count(),
                                ctx.stats_detail,
                                position_sizing,
                                ctx.dollars_per_r(),
                                ctx.cost_per_trade_r(),
                                ctx.capital_dollar(),
                                ctx.risk_pct_per_trade(),
                                ctx.equity_time_years(),
                                min_contracts,
                                max_contracts,
                                ctx.margin_per_contract_dollar(),
                            )
                        };
                        profile.finalize_ns += finalize_start.elapsed().as_nanos() as u64;
                        (stat, profile)
                    } else {
                        let mut total = 0usize;
                        let mut label_hits = 0usize;

                        let mut on_hit_inner = |idx: usize| {
                            if idx >= reward_series.len() {
                                return;
                            }
                            let rr_net = reward_series[idx];
                            total += 1;
                            if target[idx] {
                                label_hits += 1;
                            }
                            returns.push(rr_net);
                            if want_risk {
                                let rpc = risk_per_contract
                                    .and_then(|values| values.get(idx).copied())
                                    .unwrap_or(f64::NAN);
                                risks.push(rpc);
                            }
                        };

                        let scan_start = Instant::now();
                        let scan_total = if use_simd {
                            if matches!(mode, EvalProfileMode::Fine) {
                                let mut on_hit = |idx: usize| {
                                    let start = Instant::now();
                                    on_hit_inner(idx);
                                    profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                                };
                                scan_bitsets_simd_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit,
                                )
                            } else {
                                scan_bitsets_simd_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit_inner,
                                )
                            }
                        } else if matches!(mode, EvalProfileMode::Fine) {
                            let mut on_hit = |idx: usize| {
                                let start = Instant::now();
                                on_hit_inner(idx);
                                profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                            };
                            scan_bitsets_scalar_dyn_gated(
                                combo_bitsets,
                                max_len,
                                scan_gate,
                                None,
                                &mut on_hit,
                            )
                        } else {
                            scan_bitsets_scalar_dyn_gated(
                                combo_bitsets,
                                max_len,
                                scan_gate,
                                None,
                                &mut on_hit_inner,
                            )
                        };
                        profile.scan_ns += scan_start.elapsed().as_nanos() as u64;
                        profile.mask_hits += scan_total as u64;
                        profile.trades += total as u64;

                        let finalize_start = Instant::now();
                        let stat = if total < min_sample_size {
                            StatSummary::under_min(depth, total)
                        } else {
                            compute_statistics(
                                depth,
                                total,
                                label_hits,
                                Some(&returns[..]),
                                if want_risk { Some(&risks[..]) } else { None },
                                ctx.row_count(),
                                ctx.stats_detail,
                                position_sizing,
                                ctx.dollars_per_r(),
                                ctx.cost_per_trade_r(),
                                ctx.capital_dollar(),
                                ctx.risk_pct_per_trade(),
                                ctx.equity_time_years(),
                                min_contracts,
                                max_contracts,
                                ctx.margin_per_contract_dollar(),
                            )
                        };
                        profile.finalize_ns += finalize_start.elapsed().as_nanos() as u64;
                        (stat, profile)
                    }
                })
            })
        }
    } else {
        if no_stacking {
            let mut total = 0usize;
            let mut wins = 0usize;
            let mut next_free_idx = 0usize;

            let mut on_hit_inner = |idx: usize| {
                if idx < next_free_idx {
                    return;
                }
                total += 1;
                if target[idx] {
                    wins += 1;
                }

                let exit_i = exit_indices[idx];
                let candidate = if exit_i == usize::MAX || exit_i < idx {
                    idx.saturating_add(1)
                } else {
                    exit_i
                };
                if candidate > next_free_idx {
                    next_free_idx = candidate;
                }
            };

            let scan_start = Instant::now();
            let scan_total = if use_simd {
                if matches!(mode, EvalProfileMode::Fine) {
                    let mut on_hit = |idx: usize| {
                        let start = Instant::now();
                        on_hit_inner(idx);
                        profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                    };
                    scan_bitsets_simd_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                } else {
                    scan_bitsets_simd_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit_inner,
                    )
                }
            } else if matches!(mode, EvalProfileMode::Fine) {
                let mut on_hit = |idx: usize| {
                    let start = Instant::now();
                    on_hit_inner(idx);
                    profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                };
                scan_bitsets_scalar_dyn_gated(combo_bitsets, max_len, scan_gate, None, &mut on_hit)
            } else {
                scan_bitsets_scalar_dyn_gated(
                    combo_bitsets,
                    max_len,
                    scan_gate,
                    None,
                    &mut on_hit_inner,
                )
            };
            profile.scan_ns += scan_start.elapsed().as_nanos() as u64;
            profile.mask_hits += scan_total as u64;
            profile.trades += total as u64;

            let finalize_start = Instant::now();
            let stat = if total < min_sample_size {
                StatSummary::under_min(depth, total)
            } else {
                compute_statistics(
                    depth,
                    total,
                    wins,
                    None,
                    None,
                    ctx.row_count(),
                    ctx.stats_detail,
                    position_sizing,
                    ctx.dollars_per_r(),
                    ctx.cost_per_trade_r(),
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    ctx.equity_time_years(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                )
            };
            profile.finalize_ns += finalize_start.elapsed().as_nanos() as u64;
            (stat, profile)
        } else {
            let mut total = 0usize;
            let mut wins = 0usize;

            let mut on_hit_inner = |idx: usize| {
                total += 1;
                if target[idx] {
                    wins += 1;
                }
            };

            let scan_start = Instant::now();
            let scan_total = if use_simd {
                if matches!(mode, EvalProfileMode::Fine) {
                    let mut on_hit = |idx: usize| {
                        let start = Instant::now();
                        on_hit_inner(idx);
                        profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                    };
                    scan_bitsets_simd_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                } else {
                    scan_bitsets_simd_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit_inner,
                    )
                }
            } else if matches!(mode, EvalProfileMode::Fine) {
                let mut on_hit = |idx: usize| {
                    let start = Instant::now();
                    on_hit_inner(idx);
                    profile.on_hit_ns += start.elapsed().as_nanos() as u64;
                };
                scan_bitsets_scalar_dyn_gated(combo_bitsets, max_len, scan_gate, None, &mut on_hit)
            } else {
                scan_bitsets_scalar_dyn_gated(
                    combo_bitsets,
                    max_len,
                    scan_gate,
                    None,
                    &mut on_hit_inner,
                )
            };
            profile.scan_ns += scan_start.elapsed().as_nanos() as u64;
            profile.mask_hits += scan_total as u64;
            profile.trades += total as u64;

            let finalize_start = Instant::now();
            let stat = if total < min_sample_size {
                StatSummary::under_min(depth, total)
            } else {
                compute_statistics(
                    depth,
                    total,
                    wins,
                    None,
                    None,
                    ctx.row_count(),
                    ctx.stats_detail,
                    position_sizing,
                    ctx.dollars_per_r(),
                    ctx.cost_per_trade_r(),
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    ctx.equity_time_years(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                )
            };
            profile.finalize_ns += finalize_start.elapsed().as_nanos() as u64;
            (stat, profile)
        }
    }
}

#[allow(clippy::collapsible_else_if)]
fn evaluate_for_bitsets(
    depth: usize,
    ctx: &EvaluationContext,
    combo_bitsets: &[&BitsetMask],
    min_sample_size: usize,
) -> StatSummary {
    let target = ctx.target();
    let rewards = ctx.rewards();
    let no_stacking = ctx.stacking_mode() == StackingMode::NoStacking;
    let exit_indices = if no_stacking {
        ctx.exit_indices()
            .expect("exit indices must be present when stacking_mode is NoStacking")
    } else {
        &[]
    };
    let scan_gate = ctx.trade_gate_bitset();
    let position_sizing = ctx.position_sizing();
    let risk_per_contract = ctx.risk_per_contract_dollar();
    let min_contracts = ctx.min_contracts();
    let max_contracts = ctx.max_contracts();
    // All bitsets are built from masks aligned to the target length, but
    // we still clamp to the smaller of the two for safety.
    let max_len = combo_bitsets
        .first()
        .map(|bitset| bitset.len.min(target.len()))
        .unwrap_or(0);

    // If the sparsest feature in this combination has support below the
    // minimum sample size, the intersection can never reach the threshold.
    // Reject these combinations up front without scanning any bits.
    if let Some(smallest) = combo_bitsets.first() {
        if smallest.support < min_sample_size {
            return StatSummary::under_min(depth, smallest.support);
        }
    }

    if combo_bitsets.is_empty() || max_len == 0 {
        return compute_statistics(
            depth,
            0,
            0,
            None,
            None,
            ctx.row_count(),
            ctx.stats_detail,
            position_sizing,
            ctx.dollars_per_r(),
            ctx.cost_per_trade_r(),
            ctx.capital_dollar(),
            ctx.risk_pct_per_trade(),
            ctx.equity_time_years(),
            min_contracts,
            max_contracts,
            ctx.margin_per_contract_dollar(),
        );
    }
    #[cfg(feature = "simd-eval")]
    let use_simd = combo_bitsets.len() >= 2;
    #[cfg(not(feature = "simd-eval"))]
    let use_simd = false;

    if let Some(reward_series) = rewards {
        // Core stats stream directly during bitset scans; full detail keeps
        // the RR sequence for percentiles, streaks, and richer metrics.
        if matches!(ctx.stats_detail, StatsDetail::Core) {
            if no_stacking {
                let mut total = 0usize;
                let mut label_hits = 0usize;
                let mut acc = CoreStatsAccumulator::new(
                    position_sizing,
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                );
                let mut next_free_idx = 0usize;

                let mut on_hit = |idx: usize| {
                    if idx < next_free_idx {
                        return;
                    }
                    if idx >= reward_series.len() {
                        return;
                    }
                    let rr_net = reward_series[idx];

                    total += 1;
                    acc.total_bars += 1;
                    let rpc = if matches!(position_sizing, PositionSizingMode::Contracts) {
                        risk_per_contract.and_then(|values| values.get(idx).copied())
                    } else {
                        None
                    };
                    acc.push(rr_net, rpc);
                    if target[idx] {
                        label_hits += 1;
                    }

                    let exit_i = exit_indices[idx];
                    let candidate = if exit_i == usize::MAX || exit_i < idx {
                        idx.saturating_add(1)
                    } else {
                        exit_i
                    };
                    if candidate > next_free_idx {
                        next_free_idx = candidate;
                    }
                };

                let scan_total = if use_simd {
                    #[cfg(feature = "simd-eval")]
                    {
                        scan_bitsets_simd_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit,
                        )
                    }
                    #[cfg(not(feature = "simd-eval"))]
                    {
                        scan_bitsets_scalar_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit,
                        )
                    }
                } else {
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                };

                if total < min_sample_size {
                    let mut stat = StatSummary::under_min(depth, total);
                    stat.mask_hits = scan_total;
                    stat
                } else {
                    let mut stat = acc.finalize(depth, label_hits, ctx.equity_time_years());
                    stat.mask_hits = scan_total;
                    stat
                }
            } else {
                let mut total = 0usize;
                let mut label_hits = 0usize;
                let mut acc = CoreStatsAccumulator::new(
                    position_sizing,
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                );

                let mut on_hit = |idx: usize| {
                    if idx >= reward_series.len() {
                        return;
                    }
                    let rr_net = reward_series[idx];

                    total += 1;
                    acc.total_bars += 1;
                    let rpc = if matches!(position_sizing, PositionSizingMode::Contracts) {
                        risk_per_contract.and_then(|values| values.get(idx).copied())
                    } else {
                        None
                    };
                    acc.push(rr_net, rpc);
                    if target[idx] {
                        label_hits += 1;
                    }
                };

                let scan_total = if use_simd {
                    #[cfg(feature = "simd-eval")]
                    {
                        scan_bitsets_simd_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit,
                        )
                    }
                    #[cfg(not(feature = "simd-eval"))]
                    {
                        scan_bitsets_scalar_dyn_gated(
                            combo_bitsets,
                            max_len,
                            scan_gate,
                            None,
                            &mut on_hit,
                        )
                    }
                } else {
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                };

                if total < min_sample_size {
                    let mut stat = StatSummary::under_min(depth, total);
                    stat.mask_hits = scan_total;
                    stat
                } else {
                    let mut stat = acc.finalize(depth, label_hits, ctx.equity_time_years());
                    stat.mask_hits = scan_total;
                    stat
                }
            }
        } else {
            RETURNS_BUFFER.with(|cell| {
                let mut returns = cell.borrow_mut();
                returns.clear();

                RISK_PER_CONTRACT_BUFFER.with(|risk_cell| {
                    let mut risks = risk_cell.borrow_mut();
                    risks.clear();
                    let want_risk = matches!(position_sizing, PositionSizingMode::Contracts);

                    if no_stacking {
                        let mut total = 0usize;
                        let mut label_hits = 0usize;
                        let mut next_free_idx = 0usize;

                        let mut on_hit = |idx: usize| {
                            if idx < next_free_idx {
                                return;
                            }
                            if idx >= reward_series.len() {
                                return;
                            }
                            let rr_net = reward_series[idx];
                            total += 1;
                            if target[idx] {
                                label_hits += 1;
                            }
                            returns.push(rr_net);
                            if want_risk {
                                let rpc = risk_per_contract
                                    .and_then(|values| values.get(idx).copied())
                                    .unwrap_or(f64::NAN);
                                risks.push(rpc);
                            }

                            let exit_i = exit_indices[idx];
                            let candidate = if exit_i == usize::MAX || exit_i < idx {
                                idx.saturating_add(1)
                            } else {
                                exit_i
                            };
                            if candidate > next_free_idx {
                                next_free_idx = candidate;
                            }
                        };

                        let scan_total = if use_simd {
                            #[cfg(feature = "simd-eval")]
                            {
                                scan_bitsets_simd_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit,
                                )
                            }
                            #[cfg(not(feature = "simd-eval"))]
                            {
                                scan_bitsets_scalar_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit,
                                )
                            }
                        } else {
                            scan_bitsets_scalar_dyn_gated(
                                combo_bitsets,
                                max_len,
                                scan_gate,
                                None,
                                &mut on_hit,
                            )
                        };

                        if total < min_sample_size {
                            let mut stat = StatSummary::under_min(depth, total);
                            stat.mask_hits = scan_total;
                            stat
                        } else {
                            let mut stat = compute_statistics(
                                depth,
                                total,
                                label_hits,
                                Some(&returns[..]),
                                if want_risk { Some(&risks[..]) } else { None },
                                ctx.row_count(),
                                ctx.stats_detail,
                                position_sizing,
                                ctx.dollars_per_r(),
                                ctx.cost_per_trade_r(),
                                ctx.capital_dollar(),
                                ctx.risk_pct_per_trade(),
                                ctx.equity_time_years(),
                                min_contracts,
                                max_contracts,
                                ctx.margin_per_contract_dollar(),
                            );
                            stat.mask_hits = scan_total;
                            stat
                        }
                    } else {
                        let mut total = 0usize;
                        let mut label_hits = 0usize;

                        let mut on_hit = |idx: usize| {
                            if idx >= reward_series.len() {
                                return;
                            }
                            let rr_net = reward_series[idx];
                            total += 1;
                            if target[idx] {
                                label_hits += 1;
                            }
                            returns.push(rr_net);
                            if want_risk {
                                let rpc = risk_per_contract
                                    .and_then(|values| values.get(idx).copied())
                                    .unwrap_or(f64::NAN);
                                risks.push(rpc);
                            }
                        };

                        let scan_total = if use_simd {
                            #[cfg(feature = "simd-eval")]
                            {
                                scan_bitsets_simd_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit,
                                )
                            }
                            #[cfg(not(feature = "simd-eval"))]
                            {
                                scan_bitsets_scalar_dyn_gated(
                                    combo_bitsets,
                                    max_len,
                                    scan_gate,
                                    None,
                                    &mut on_hit,
                                )
                            }
                        } else {
                            scan_bitsets_scalar_dyn_gated(
                                combo_bitsets,
                                max_len,
                                scan_gate,
                                None,
                                &mut on_hit,
                            )
                        };

                        if total < min_sample_size {
                            let mut stat = StatSummary::under_min(depth, total);
                            stat.mask_hits = scan_total;
                            stat
                        } else {
                            let mut stat = compute_statistics(
                                depth,
                                total,
                                label_hits,
                                Some(&returns[..]),
                                if want_risk { Some(&risks[..]) } else { None },
                                ctx.row_count(),
                                ctx.stats_detail,
                                position_sizing,
                                ctx.dollars_per_r(),
                                ctx.cost_per_trade_r(),
                                ctx.capital_dollar(),
                                ctx.risk_pct_per_trade(),
                                ctx.equity_time_years(),
                                min_contracts,
                                max_contracts,
                                ctx.margin_per_contract_dollar(),
                            );
                            stat.mask_hits = scan_total;
                            stat
                        }
                    }
                })
            })
        }
    } else {
        if no_stacking {
            let mut total = 0usize;
            let mut wins = 0usize;
            let mut next_free_idx = 0usize;

            let mut on_hit = |idx: usize| {
                if idx < next_free_idx {
                    return;
                }
                total += 1;
                if target[idx] {
                    wins += 1;
                }

                let exit_i = exit_indices[idx];
                let candidate = if exit_i == usize::MAX || exit_i < idx {
                    idx.saturating_add(1)
                } else {
                    exit_i
                };
                if candidate > next_free_idx {
                    next_free_idx = candidate;
                }
            };

            let scan_total = if use_simd {
                #[cfg(feature = "simd-eval")]
                {
                    scan_bitsets_simd_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                }
                #[cfg(not(feature = "simd-eval"))]
                {
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                }
            } else {
                scan_bitsets_scalar_dyn_gated(combo_bitsets, max_len, scan_gate, None, &mut on_hit)
            };

            if total < min_sample_size {
                let mut stat = StatSummary::under_min(depth, total);
                stat.mask_hits = scan_total;
                stat
            } else {
                let mut stat = compute_statistics(
                    depth,
                    total,
                    wins,
                    None,
                    None,
                    ctx.row_count(),
                    ctx.stats_detail,
                    position_sizing,
                    ctx.dollars_per_r(),
                    ctx.cost_per_trade_r(),
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    ctx.equity_time_years(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                );
                stat.mask_hits = scan_total;
                stat
            }
        } else {
            let mut total = 0usize;
            let mut wins = 0usize;

            let mut on_hit = |idx: usize| {
                total += 1;
                if target[idx] {
                    wins += 1;
                }
            };

            let scan_total = if use_simd {
                #[cfg(feature = "simd-eval")]
                {
                    scan_bitsets_simd_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                }
                #[cfg(not(feature = "simd-eval"))]
                {
                    scan_bitsets_scalar_dyn_gated(
                        combo_bitsets,
                        max_len,
                        scan_gate,
                        None,
                        &mut on_hit,
                    )
                }
            } else {
                scan_bitsets_scalar_dyn_gated(combo_bitsets, max_len, scan_gate, None, &mut on_hit)
            };

            if total < min_sample_size {
                let mut stat = StatSummary::under_min(depth, total);
                stat.mask_hits = scan_total;
                stat
            } else {
                let mut stat = compute_statistics(
                    depth,
                    total,
                    wins,
                    None,
                    None,
                    ctx.row_count(),
                    ctx.stats_detail,
                    position_sizing,
                    ctx.dollars_per_r(),
                    ctx.cost_per_trade_r(),
                    ctx.capital_dollar(),
                    ctx.risk_pct_per_trade(),
                    ctx.equity_time_years(),
                    min_contracts,
                    max_contracts,
                    ctx.margin_per_contract_dollar(),
                );
                stat.mask_hits = scan_total;
                stat
            }
        }
    }
}

fn load_boolean_vector(data: &ColumnarData, column: &str) -> Result<Vec<bool>> {
    Ok(data
        .boolean_column(column)?
        .into_iter()
        .map(|value| value.unwrap_or(false))
        .collect())
}

fn load_float_vector(data: &ColumnarData, column: &str) -> Result<Vec<f64>> {
    Ok(data
        .float_column(column)?
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect())
}

pub fn build_bitset_catalog(
    ctx: &EvaluationContext,
    features: &[FeatureDescriptor],
) -> Result<BitsetCatalog> {
    let mut bitsets = Vec::with_capacity(features.len());
    let mut name_to_index = HashMap::with_capacity(features.len());

    for (idx, descriptor) in features.iter().enumerate() {
        let name = descriptor.name.as_str();
        let mask = ctx.feature_mask(name)?;
        let bitset = BitsetMask::from_bools(mask.as_ref());
        bitsets.push(bitset);
        // Descriptor names are unique in normal catalogs; retain the first
        // index if a caller provides duplicates.
        name_to_index.entry(name.to_string()).or_insert(idx);
    }

    Ok(BitsetCatalog::new(bitsets, name_to_index))
}

fn apply_operator(value: f64, threshold: f64, operator: ComparisonOperator) -> bool {
    match operator {
        ComparisonOperator::GreaterThan => value > threshold,
        ComparisonOperator::LessThan => value < threshold,
        ComparisonOperator::GreaterEqual => value >= threshold,
        ComparisonOperator::LessEqual => value <= threshold,
    }
}

fn apply_pair_operator(left: f64, right: f64, operator: ComparisonOperator) -> bool {
    match operator {
        ComparisonOperator::GreaterThan => left > right,
        ComparisonOperator::LessThan => left < right,
        ComparisonOperator::GreaterEqual => left >= right,
        ComparisonOperator::LessEqual => left <= right,
    }
}

pub(crate) fn detect_reward_column(data: &ColumnarData, config: &Config) -> Result<Option<String>> {
    detect_reward_column_with_override(data, config, None)
}

pub(crate) fn detect_reward_column_with_override(
    data: &ColumnarData,
    config: &Config,
    override_column: Option<&str>,
) -> Result<Option<String>> {
    if let Some(column) = override_column
        .map(str::trim)
        .filter(|column| !column.is_empty())
    {
        if data.has_column(column) {
            return Ok(Some(column.to_string()));
        }
        return Err(anyhow!(
            "Configured RR column '{column}' is missing from dataset"
        ));
    }

    const FALLBACK: [&str; 4] = ["rr", "reward", "r_multiple", "returns"];
    let mut candidates: Vec<String> = Vec::new();
    candidates.push(format!("rr_{}", config.target));
    match config.direction {
        Direction::Long => candidates.push("rr_long".into()),
        Direction::Short => candidates.push("rr_short".into()),
        Direction::Both => {
            candidates.push("rr_long".into());
            candidates.push("rr_short".into());
        }
    }
    candidates.extend(FALLBACK.iter().map(|candidate| (*candidate).to_string()));
    for candidate in candidates {
        let normalized = candidate.trim();
        if normalized.is_empty() {
            continue;
        }
        if data.has_column(normalized) {
            return Ok(Some(normalized.to_string()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
