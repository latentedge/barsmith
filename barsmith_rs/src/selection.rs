use std::collections::HashMap;

use serde::Serialize;

use crate::formula_eval::{FormulaResult, FormulaWindowReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelectionMode {
    Off,
    HoldoutConfirm,
    ValidationRank,
}

impl SelectionMode {
    pub fn is_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelectionPreset {
    Exploratory,
    Institutional,
    Custom,
}

impl SelectionPreset {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exploratory => "exploratory",
            Self::Institutional => "institutional",
            Self::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SelectionPolicy {
    pub candidate_top_k: usize,
    pub pre_min_trades: usize,
    pub post_min_trades: usize,
    pub post_warn_below_trades: usize,
    pub pre_min_total_r: f64,
    pub post_min_total_r: f64,
    pub pre_min_expectancy: f64,
    pub post_min_expectancy: f64,
    pub max_drawdown_r: Option<f64>,
    pub min_pre_frs: Option<f64>,
    pub max_return_degradation: Option<f64>,
    pub max_single_trade_contribution: Option<f64>,
    pub max_formula_depth: Option<usize>,
    pub min_density_per_1000_bars: Option<f64>,
    pub complexity_penalty: f64,
    pub embargo_bars: usize,
    pub purge_cross_boundary_exits: bool,
}

impl Default for SelectionPolicy {
    fn default() -> Self {
        Self {
            candidate_top_k: 1_000,
            pre_min_trades: 100,
            post_min_trades: 30,
            post_warn_below_trades: 50,
            pre_min_total_r: 0.0,
            post_min_total_r: 0.0,
            pre_min_expectancy: 0.0,
            post_min_expectancy: 0.0,
            max_drawdown_r: None,
            min_pre_frs: Some(0.0),
            max_return_degradation: Some(0.25),
            max_single_trade_contribution: None,
            max_formula_depth: None,
            min_density_per_1000_bars: None,
            complexity_penalty: 0.0,
            embargo_bars: 0,
            purge_cross_boundary_exits: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionStatus {
    Selected,
    Passed,
    Rejected,
    MissingPostResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    MissingPostResult,
    PreTradesBelowFloor,
    PostTradesBelowFloor,
    PreTotalRBelowFloor,
    PostTotalRBelowFloor,
    PreExpectancyBelowFloor,
    PostExpectancyBelowFloor,
    PreDrawdownAboveLimit,
    PostDrawdownAboveLimit,
    PreFrsBelowFloor,
    ReturnDegradationAboveLimit,
    SingleTradeContributionAboveLimit,
    FormulaDepthAboveLimit,
    DensityBelowFloor,
}

impl RejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingPostResult => "missing_post_result",
            Self::PreTradesBelowFloor => "pre_trades_below_floor",
            Self::PostTradesBelowFloor => "post_trades_below_floor",
            Self::PreTotalRBelowFloor => "pre_total_r_below_floor",
            Self::PostTotalRBelowFloor => "post_total_r_below_floor",
            Self::PreExpectancyBelowFloor => "pre_expectancy_below_floor",
            Self::PostExpectancyBelowFloor => "post_expectancy_below_floor",
            Self::PreDrawdownAboveLimit => "pre_drawdown_above_limit",
            Self::PostDrawdownAboveLimit => "post_drawdown_above_limit",
            Self::PreFrsBelowFloor => "pre_frs_below_floor",
            Self::ReturnDegradationAboveLimit => "return_degradation_above_limit",
            Self::SingleTradeContributionAboveLimit => "single_trade_contribution_above_limit",
            Self::FormulaDepthAboveLimit => "formula_depth_above_limit",
            Self::DensityBelowFloor => "density_below_floor",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectionDecision {
    pub formula: String,
    pub source_rank: usize,
    pub pre_rank: usize,
    pub post_rank: Option<usize>,
    pub status: SelectionStatus,
    pub reasons: Vec<RejectionReason>,
    pub warnings: Vec<String>,
    pub pre_trades: usize,
    pub post_trades: Option<usize>,
    pub pre_total_r: f64,
    pub post_total_r: Option<f64>,
    pub pre_expectancy: f64,
    pub post_expectancy: Option<f64>,
    pub pre_max_drawdown_r: f64,
    pub post_max_drawdown_r: Option<f64>,
    pub pre_calmar_equity: f64,
    pub post_calmar_equity: Option<f64>,
    pub pre_frs: Option<f64>,
    pub post_frs: Option<f64>,
    pub post_to_pre_total_r_ratio: Option<f64>,
    pub pre_largest_win_share: Option<f64>,
    pub post_largest_win_share: Option<f64>,
    pub formula_depth: usize,
    pub pre_density_per_1000_bars: f64,
    pub post_density_per_1000_bars: Option<f64>,
    pub complexity_penalty: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectionDiagnostic {
    pub formula: String,
    pub source_rank: usize,
    pub post_rank: usize,
    pub post_total_r: f64,
    pub post_calmar_equity: f64,
    pub post_frs: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectionReport {
    pub mode: SelectionMode,
    pub preset: Option<SelectionPreset>,
    pub policy: SelectionPolicy,
    pub selected: Option<SelectionDecision>,
    pub decisions: Vec<SelectionDecision>,
    pub diagnostic_top_post: Option<SelectionDiagnostic>,
    pub warnings: Vec<String>,
}

pub fn build_selection_report(
    mode: SelectionMode,
    policy: SelectionPolicy,
    pre: &FormulaWindowReport,
    post: &FormulaWindowReport,
) -> Option<SelectionReport> {
    build_selection_report_with_preset(mode, None, policy, pre, post)
}

pub fn build_selection_report_with_preset(
    mode: SelectionMode,
    preset: Option<SelectionPreset>,
    policy: SelectionPolicy,
    pre: &FormulaWindowReport,
    post: &FormulaWindowReport,
) -> Option<SelectionReport> {
    if !mode.is_enabled() {
        return None;
    }

    let post_by_formula = post
        .results
        .iter()
        .map(|result| (result.formula.as_str(), result))
        .collect::<HashMap<_, _>>();

    let mut decisions = Vec::new();
    for pre_result in pre.results.iter().take(policy.candidate_top_k) {
        let post_result = post_by_formula.get(pre_result.formula.as_str()).copied();
        decisions.push(decision_for_candidate(pre_result, post_result, &policy));
    }

    let selected_idx = match mode {
        SelectionMode::Off => None,
        SelectionMode::HoldoutConfirm => decisions
            .iter()
            .position(|decision| decision.reasons.is_empty()),
        SelectionMode::ValidationRank => decisions
            .iter()
            .enumerate()
            .filter(|(_, decision)| decision.reasons.is_empty())
            .min_by_key(|(_, decision)| decision.post_rank.unwrap_or(usize::MAX))
            .map(|(idx, _)| idx),
    };

    if let Some(idx) = selected_idx {
        for (decision_idx, decision) in decisions.iter_mut().enumerate() {
            if decision_idx == idx {
                decision.status = SelectionStatus::Selected;
            } else if decision.reasons.is_empty() {
                decision.status = SelectionStatus::Passed;
            }
        }
    }

    let selected = selected_idx.and_then(|idx| decisions.get(idx).cloned());
    let diagnostic_top_post = post.results.first().map(|result| SelectionDiagnostic {
        formula: result.formula.clone(),
        source_rank: result.source_rank,
        post_rank: result.display_rank,
        post_total_r: result.stats.total_return,
        post_calmar_equity: result.stats.calmar_equity,
        post_frs: result.frs.map(|frs| frs.frs),
    });

    let mut warnings = Vec::new();
    if matches!(mode, SelectionMode::ValidationRank) {
        warnings.push(
            "validation-rank uses post-window performance to choose among pre candidates; reserve a later lockbox before treating the result as unbiased"
                .to_string(),
        );
    }
    if selected.is_none() {
        warnings.push("no candidate passed the configured selection gates".to_string());
    }
    if policy.candidate_top_k == 0 {
        warnings.push(
            "candidate_top_k is zero, so no formulas were eligible for selection".to_string(),
        );
    }

    Some(SelectionReport {
        mode,
        preset,
        policy,
        selected,
        decisions,
        diagnostic_top_post,
        warnings,
    })
}

fn decision_for_candidate(
    pre: &FormulaResult,
    post: Option<&FormulaResult>,
    policy: &SelectionPolicy,
) -> SelectionDecision {
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();

    if pre.trades < policy.pre_min_trades {
        reasons.push(RejectionReason::PreTradesBelowFloor);
    }
    if pre.stats.total_return <= policy.pre_min_total_r {
        reasons.push(RejectionReason::PreTotalRBelowFloor);
    }
    if pre.stats.expectancy <= policy.pre_min_expectancy {
        reasons.push(RejectionReason::PreExpectancyBelowFloor);
    }
    if policy
        .max_drawdown_r
        .is_some_and(|limit| pre.stats.max_drawdown > limit)
    {
        reasons.push(RejectionReason::PreDrawdownAboveLimit);
    }
    if policy
        .min_pre_frs
        .is_some_and(|floor| pre.frs.map(|frs| frs.frs).unwrap_or(0.0) <= floor)
    {
        reasons.push(RejectionReason::PreFrsBelowFloor);
    }
    let pre_largest_win_share = largest_win_share(pre);
    if policy
        .max_single_trade_contribution
        .is_some_and(|limit| pre_largest_win_share.unwrap_or(0.0) > limit)
    {
        reasons.push(RejectionReason::SingleTradeContributionAboveLimit);
    }
    if policy
        .max_formula_depth
        .is_some_and(|limit| pre.stats.depth > limit)
    {
        reasons.push(RejectionReason::FormulaDepthAboveLimit);
    }
    if policy
        .min_density_per_1000_bars
        .is_some_and(|floor| pre.density_per_1000_bars < floor)
    {
        reasons.push(RejectionReason::DensityBelowFloor);
    }

    let mut status = SelectionStatus::Rejected;
    let (
        post_rank,
        post_trades,
        post_total_r,
        post_expectancy,
        post_max_drawdown_r,
        post_calmar_equity,
        post_frs,
        post_to_pre_total_r_ratio,
        post_largest_win_share,
        post_density_per_1000_bars,
    ) = match post {
        Some(post) => {
            if post.trades < policy.post_min_trades {
                reasons.push(RejectionReason::PostTradesBelowFloor);
            } else if post.trades < policy.post_warn_below_trades {
                warnings.push(format!(
                    "post trades ({}) are below the recommended warning floor ({})",
                    post.trades, policy.post_warn_below_trades
                ));
            }
            if post.stats.total_return <= policy.post_min_total_r {
                reasons.push(RejectionReason::PostTotalRBelowFloor);
            }
            if post.stats.expectancy <= policy.post_min_expectancy {
                reasons.push(RejectionReason::PostExpectancyBelowFloor);
            }
            if policy
                .max_drawdown_r
                .is_some_and(|limit| post.stats.max_drawdown > limit)
            {
                reasons.push(RejectionReason::PostDrawdownAboveLimit);
            }

            let ratio = return_ratio(pre.stats.total_return, post.stats.total_return);
            if policy.max_return_degradation.is_some_and(|floor| {
                ratio
                    .map(|value| value < floor)
                    .unwrap_or(pre.stats.total_return > 0.0)
            }) {
                reasons.push(RejectionReason::ReturnDegradationAboveLimit);
            }

            let post_share = largest_win_share(post);
            if policy
                .max_single_trade_contribution
                .is_some_and(|limit| post_share.unwrap_or(0.0) > limit)
            {
                reasons.push(RejectionReason::SingleTradeContributionAboveLimit);
            }
            if policy
                .min_density_per_1000_bars
                .is_some_and(|floor| post.density_per_1000_bars < floor)
            {
                reasons.push(RejectionReason::DensityBelowFloor);
            }

            (
                Some(post.display_rank),
                Some(post.trades),
                Some(post.stats.total_return),
                Some(post.stats.expectancy),
                Some(post.stats.max_drawdown),
                Some(post.stats.calmar_equity),
                post.frs.map(|frs| frs.frs),
                ratio,
                post_share,
                Some(post.density_per_1000_bars),
            )
        }
        None => {
            reasons.push(RejectionReason::MissingPostResult);
            status = SelectionStatus::MissingPostResult;
            (None, None, None, None, None, None, None, None, None, None)
        }
    };

    if reasons.is_empty() {
        status = SelectionStatus::Passed;
    }

    SelectionDecision {
        formula: pre.formula.clone(),
        source_rank: pre.source_rank,
        pre_rank: pre.display_rank,
        post_rank,
        status,
        reasons,
        warnings,
        pre_trades: pre.trades,
        post_trades,
        pre_total_r: pre.stats.total_return,
        post_total_r,
        pre_expectancy: pre.stats.expectancy,
        post_expectancy,
        pre_max_drawdown_r: pre.stats.max_drawdown,
        post_max_drawdown_r,
        pre_calmar_equity: pre.stats.calmar_equity,
        post_calmar_equity,
        pre_frs: pre.frs.map(|frs| frs.frs),
        post_frs,
        post_to_pre_total_r_ratio,
        pre_largest_win_share,
        post_largest_win_share,
        formula_depth: pre.stats.depth,
        pre_density_per_1000_bars: pre.density_per_1000_bars,
        post_density_per_1000_bars,
        complexity_penalty: policy.complexity_penalty * pre.stats.depth as f64,
    }
}

fn return_ratio(pre_total_r: f64, post_total_r: f64) -> Option<f64> {
    if pre_total_r > 0.0 && pre_total_r.is_finite() && post_total_r.is_finite() {
        Some(post_total_r / pre_total_r)
    } else {
        None
    }
}

fn largest_win_share(result: &FormulaResult) -> Option<f64> {
    let total = result.stats.total_return;
    let largest = result.stats.largest_win;
    if total > 0.0 && largest > 0.0 && total.is_finite() && largest.is_finite() {
        Some(largest / total)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formula_eval::FormulaWindowReport;
    use crate::frs::ForwardRobustnessComponents;
    use crate::stats::StatSummary;

    fn stats(total_return: f64, trades: usize, expectancy: f64, dd: f64) -> StatSummary {
        StatSummary {
            depth: 1,
            mask_hits: trades,
            total_bars: trades,
            profitable_bars: trades,
            unprofitable_bars: 0,
            win_rate: 100.0,
            label_hit_rate: 100.0,
            label_hits: trades,
            label_misses: 0,
            expectancy,
            profit_factor: 1.0,
            avg_winning_rr: expectancy.max(0.0),
            calmar_ratio: total_return / dd.max(1e-9),
            max_drawdown: dd,
            win_loss_ratio: 1.0,
            ulcer_index: 0.0,
            pain_ratio: 0.0,
            max_consecutive_wins: trades,
            max_consecutive_losses: 0,
            avg_win_streak: trades as f64,
            avg_loss_streak: 0.0,
            median_rr: expectancy,
            avg_losing_rr: 0.0,
            p05_rr: expectancy,
            p95_rr: expectancy,
            largest_win: expectancy.max(0.0),
            largest_loss: 0.0,
            sample_quality: "test",
            total_return,
            cost_per_trade_r: 0.0,
            dollars_per_r: 1.0,
            total_return_dollar: total_return,
            max_drawdown_dollar: dd,
            expectancy_dollar: expectancy,
            final_capital: 100_000.0 + total_return,
            total_return_pct: total_return,
            cagr_pct: total_return,
            max_drawdown_pct_equity: dd,
            calmar_equity: total_return / dd.max(1e-9),
            sharpe_equity: 0.0,
            sortino_equity: 0.0,
        }
    }

    fn frs(value: f64) -> ForwardRobustnessComponents {
        ForwardRobustnessComponents {
            frs: value,
            k: 1,
            p: 1.0,
            c: value,
            c_plus: value,
            dd_min: 1.0,
            dd_median: 1.0,
            dd_mean: 1.0,
            dd_max: 1.0,
            t: 1.0,
            tail_penalty: 1.0,
            r_min: value,
            r_median: value,
            r_max: value,
            mu_r: value,
            sigma_r: 0.0,
            stability: 1.0,
            n_med: 50.0,
            n_min: 30,
            trade_score: 1.0,
        }
    }

    fn result(formula: &str, display_rank: usize, total_r: f64, trades: usize) -> FormulaResult {
        FormulaResult {
            source_rank: display_rank,
            display_rank,
            previous_rank: None,
            formula: formula.to_string(),
            mask_hits: trades,
            trades,
            density_per_1000_bars: trades as f64,
            recall_pct: 10.0,
            stats: stats(total_r, trades, total_r / trades.max(1) as f64, 2.0),
            frs: Some(frs(total_r.max(0.0))),
        }
    }

    fn window(results: Vec<FormulaResult>) -> FormulaWindowReport {
        FormulaWindowReport {
            label: "test".to_string(),
            rows: 1_000,
            start: None,
            end: None,
            buy_and_hold: None,
            guard: Default::default(),
            results,
        }
    }

    #[test]
    fn holdout_confirm_selects_highest_pre_rank_that_passes_post_gates() {
        let policy = SelectionPolicy {
            pre_min_trades: 10,
            post_min_trades: 10,
            min_pre_frs: Some(0.0),
            ..SelectionPolicy::default()
        };
        let pre = window(vec![result("a", 1, 10.0, 20), result("b", 2, 8.0, 20)]);
        let post = window(vec![result("b", 1, 20.0, 20), result("a", 2, -1.0, 20)]);

        let report =
            build_selection_report(SelectionMode::HoldoutConfirm, policy, &pre, &post).unwrap();
        let selected = report.selected.expect("selected formula");
        assert_eq!(selected.formula, "b");
        assert_eq!(selected.pre_rank, 2);
        assert_eq!(selected.status, SelectionStatus::Selected);
    }

    #[test]
    fn validation_rank_can_choose_best_post_among_pre_candidates() {
        let policy = SelectionPolicy {
            pre_min_trades: 10,
            post_min_trades: 10,
            min_pre_frs: Some(0.0),
            ..SelectionPolicy::default()
        };
        let pre = window(vec![result("a", 1, 10.0, 20), result("b", 2, 9.0, 20)]);
        let post = window(vec![result("b", 1, 20.0, 20), result("a", 2, 10.0, 20)]);

        let report =
            build_selection_report(SelectionMode::ValidationRank, policy, &pre, &post).unwrap();
        assert_eq!(report.selected.expect("selected").formula, "b");
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("lockbox"))
        );
    }

    #[test]
    fn rejected_candidates_explain_the_failed_gate() {
        let policy = SelectionPolicy {
            pre_min_trades: 10,
            post_min_trades: 30,
            min_pre_frs: Some(0.0),
            ..SelectionPolicy::default()
        };
        let pre = window(vec![result("a", 1, 10.0, 20)]);
        let post = window(vec![result("a", 1, 5.0, 5)]);

        let report =
            build_selection_report(SelectionMode::HoldoutConfirm, policy, &pre, &post).unwrap();
        let decision = report.decisions.first().unwrap();
        assert_eq!(decision.status, SelectionStatus::Rejected);
        assert!(
            decision
                .reasons
                .contains(&RejectionReason::PostTradesBelowFloor)
        );
    }
}
