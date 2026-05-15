use std::fs;
use std::path::PathBuf;

use barsmith_rs::config::{PositionSizingMode, StackingMode, StopDistanceUnit};
use barsmith_rs::formula::parse_ranked_formulas;
use barsmith_rs::formula_eval::{
    EquityCurveWindowSelection, FormulaEvalRequest, FrsScope, RankBy, equity_curve_rows,
    run_formula_evaluation,
};
use barsmith_rs::frs::FrsOptions;
use barsmith_rs::protocol::ResearchStage;
use barsmith_rs::selection::{SelectionMode, SelectionPolicy};
use chrono::NaiveDate;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn request(stacking_mode: StackingMode) -> FormulaEvalRequest {
    let formulas = parse_ranked_formulas(
        &fs::read_to_string(fixture_path("formula_eval_formulas.txt")).unwrap(),
    )
    .unwrap();

    FormulaEvalRequest {
        prepared_path: fixture_path("formula_eval_prepared.csv"),
        formulas,
        target: "2x_atr_tp_atr_stop".to_string(),
        rr_column: None,
        cutoff: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
        stacking_mode,
        capital_dollar: 100_000.0,
        risk_pct_per_trade: 1.0,
        asset: None,
        cost_per_trade_dollar: None,
        cost_per_trade_r: None,
        dollars_per_r: Some(1_000.0),
        position_sizing: PositionSizingMode::Fractional,
        stop_distance_column: None,
        stop_distance_unit: StopDistanceUnit::Points,
        min_contracts: 1,
        max_contracts: None,
        point_value: None,
        tick_value: None,
        margin_per_contract_dollar: None,
        max_drawdown: None,
        min_calmar: None,
        rank_by: RankBy::Frs,
        frs_enabled: true,
        frs_scope: FrsScope::All,
        frs_options: FrsOptions::default(),
        selection_mode: SelectionMode::HoldoutConfirm,
        selection_policy: SelectionPolicy {
            candidate_top_k: 100,
            pre_min_trades: 1,
            post_min_trades: 1,
            post_warn_below_trades: 1,
            min_pre_frs: Some(0.0),
            max_return_degradation: None,
            purge_cross_boundary_exits: false,
            ..SelectionPolicy::default()
        },
        stage: ResearchStage::Validation,
        selection_preset: None,
        strict_protocol: None,
        overfit_options: None,
        stress_options: None,
    }
}

#[test]
fn formula_eval_splits_pre_post_and_attaches_frs() {
    let report = run_formula_evaluation(&request(StackingMode::NoStacking)).unwrap();

    assert_eq!(report.pre.rows, 3);
    assert_eq!(report.post.rows, 3);
    assert_eq!(report.pre.results.len(), 3);
    assert_eq!(report.post.results.len(), 3);
    assert!(report.post.results.iter().all(|row| row.frs.is_some()));
    assert!(!report.frs_rows.is_empty());
    assert!(!report.frs_window_rows.is_empty());
    assert!(report.selection.is_some());
}

#[test]
fn no_stacking_selects_fewer_trades_than_stacking() {
    let no_stacking = run_formula_evaluation(&request(StackingMode::NoStacking)).unwrap();
    let stacking = run_formula_evaluation(&request(StackingMode::Stacking)).unwrap();

    let no_stack_rank_1 = no_stacking
        .pre
        .results
        .iter()
        .find(|row| row.formula == "flag_a && x>y")
        .unwrap();
    let stack_rank_1 = stacking
        .pre
        .results
        .iter()
        .find(|row| row.formula == "flag_a && x>y")
        .unwrap();

    assert_eq!(no_stack_rank_1.mask_hits, 2);
    assert_eq!(stack_rank_1.mask_hits, 2);
    assert_eq!(no_stack_rank_1.trades, 1);
    assert_eq!(stack_rank_1.trades, 2);
}

#[test]
fn equity_curve_export_uses_ranked_window_selection() {
    let req = request(StackingMode::NoStacking);
    let report = run_formula_evaluation(&req).unwrap();
    let rows = equity_curve_rows(
        &report,
        &req,
        RankBy::Frs,
        EquityCurveWindowSelection::Both,
        2,
    )
    .unwrap();

    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| row.equity_dollar.is_some()));
    assert!(rows.iter().any(|row| row.window == "pre"));
    assert!(rows.iter().any(|row| row.window == "post"));
}

#[test]
fn window_guards_report_embargo_and_cross_boundary_purge() {
    let mut req = request(StackingMode::NoStacking);
    req.selection_mode = SelectionMode::Off;
    req.selection_policy.embargo_bars = 1;
    req.selection_policy.purge_cross_boundary_exits = true;

    let report = run_formula_evaluation(&req).unwrap();

    assert_eq!(report.post.guard.rows_after_date_filter, 3);
    assert_eq!(report.post.guard.embargo_bars_requested, 1);
    assert_eq!(report.post.guard.embargo_bars_applied, 1);
    assert_eq!(report.post.rows, 2);
    assert!(report.pre.guard.cross_boundary_rows_purged > 0);
    assert!(report.post.guard.cross_boundary_rows_purged > 0);
    assert!(report.selection.is_none());
}
