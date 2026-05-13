use super::*;
use crate::config::{EvalProfileMode, ReportMetricsMode, StackingMode};
use std::path::PathBuf;
use tempfile::tempdir;

fn dummy_config(target: &str, direction: Direction) -> Config {
    Config {
        input_csv: PathBuf::from("dummy.csv"),
        source_csv: None,
        direction,
        target: target.to_string(),
        output_dir: PathBuf::from("dummy_out"),
        max_depth: 1,
        min_sample_size: 1,
        min_sample_size_report: 1,
        include_date_start: None,
        include_date_end: None,
        batch_size: 10,
        n_workers: 1,
        auto_batch: false,
        resume_offset: 0,
        explicit_resume_offset: false,
        max_combos: None,
        dry_run: false,
        quiet: true,
        report_metrics: ReportMetricsMode::Off,
        report_top: 5,
        force_recompute: false,
        max_drawdown: 50.0,
        max_drawdown_report: None,
        min_calmar_report: None,
        strict_min_pruning: true,
        enable_feature_pairs: false,
        feature_pairs_limit: None,
        enable_subset_pruning: false,
        catalog_hash: None,
        stats_detail: crate::config::StatsDetail::Full,
        eval_profile: EvalProfileMode::Off,
        eval_profile_sample_rate: 1,
        s3_output: None,
        s3_upload_each_batch: false,
        capital_dollar: None,
        risk_pct_per_trade: None,
        equity_time_years: None,
        asset: None,
        risk_per_trade_dollar: None,
        cost_per_trade_dollar: None,
        cost_per_trade_r: None,
        dollars_per_r: None,
        tick_size: None,
        stacking_mode: StackingMode::Stacking,
        position_sizing: crate::config::PositionSizingMode::Fractional,
        stop_distance_column: None,
        stop_distance_unit: crate::config::StopDistanceUnit::Points,
        min_contracts: 1,
        max_contracts: None,
        point_value: None,
        tick_value: None,
        margin_per_contract_dollar: None,
        require_any_features: Vec::new(),
    }
}

#[test]
fn classify_sample_buckets_match_thresholds() {
    assert_eq!(classify_sample(0), "poor");
    assert_eq!(classify_sample(29), "poor");
    assert_eq!(classify_sample(30), "fair");
    assert_eq!(classify_sample(49), "fair");
    assert_eq!(classify_sample(50), "good");
    assert_eq!(classify_sample(99), "good");
    assert_eq!(classify_sample(100), "excellent");
    assert_eq!(classify_sample(1_000), "excellent");
}

#[test]
fn round_to_uses_bankers_rounding_for_ties() {
    let a = round_to(0.125, 2);
    let b = round_to(0.135, 2);
    assert!((a - 0.12).abs() < 1e-9, "0.125 should round to 0.12");
    assert!((b - 0.14).abs() < 1e-9, "0.135 should round to 0.14");
}

#[test]
fn apply_operator_and_pair_operator_match_comparisons() {
    use crate::feature::ComparisonOperator;

    assert!(apply_operator(2.0, 1.0, ComparisonOperator::GreaterThan));
    assert!(!apply_operator(0.5, 1.0, ComparisonOperator::GreaterThan));
    assert!(apply_operator(1.0, 1.0, ComparisonOperator::GreaterEqual));
    assert!(apply_pair_operator(1.0, 2.0, ComparisonOperator::LessThan));
    assert!(!apply_pair_operator(3.0, 2.0, ComparisonOperator::LessThan));
}

#[test]
fn detect_reward_column_prefers_target_specific_rr() -> Result<()> {
    let dir = tempdir()?;
    let csv_path = dir.path().join("rr.csv");
    // Target-specific RR should win over the generic long-direction column.
    std::fs::write(&csv_path, "rr_next_bar_color_and_wicks,rr_long\n0.1,0.2\n")?;

    let data = ColumnarData::load(&csv_path)?;
    let config = dummy_config("next_bar_color_and_wicks", Direction::Long);
    let detected = detect_reward_column(&data, &config)?;
    assert_eq!(
        detected.as_deref(),
        Some("rr_next_bar_color_and_wicks"),
        "target-specific rr_<target> should be preferred when present"
    );
    Ok(())
}

#[test]
fn detect_reward_column_falls_back_to_directional_then_generic() -> Result<()> {
    let dir = tempdir()?;
    let csv_path = dir.path().join("rr_fallback.csv");
    // No rr_<target> column; should prefer rr_long for Long direction.
    std::fs::write(&csv_path, "rr_long,rr\n0.1,0.2\n")?;

    let data = ColumnarData::load(&csv_path)?;
    let config = dummy_config("some_other_target", Direction::Long);
    let detected = detect_reward_column(&data, &config)?;
    assert_eq!(
        detected.as_deref(),
        Some("rr_long"),
        "rr_long should be chosen when rr_<target> is absent"
    );
    Ok(())
}

#[test]
fn percentile_triplet_matches_sort_reference() {
    let values = vec![-2.0, -1.0, 0.5, 1.0, 3.0, 5.0, 8.0, 9.0, 10.0, 12.0];

    let (median_sel, p05_sel, p95_sel) = percentile_triplet(&values);

    let mut sorted = values.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let len = sorted.len();
    let mid = len / 2;
    let median_ref = if len % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    };
    let idx_p05 = ((len - 1) as f64 * 0.05).round() as usize;
    let idx_p95 = ((len - 1) as f64 * 0.95).round() as usize;
    let p05_ref = sorted[idx_p05.min(len - 1)];
    let p95_ref = sorted[idx_p95.min(len - 1)];

    assert!((median_sel - median_ref).abs() < 1e-9);
    assert!((p05_sel - p05_ref).abs() < 1e-9);
    assert!((p95_sel - p95_ref).abs() < 1e-9);
}

#[test]
fn net_r_win_rate_and_label_hit_rate_diverge_when_labels_and_profit_disagree() {
    // Three trades in R-space: one loss, one win, one flat.
    let rr = vec![-0.5, 1.0, 0.0];
    let total_bars = rr.len();
    // Engineered target is true on the last two trades (win + flat).
    let label_hits = 2usize;

    let stats = compute_full_statistics(
        1,          // depth
        total_bars, // total_bars
        &rr,        // filtered_rr (already net R)
        None,       // risk_per_contract_dollar
        label_hits, // label hits
        None,       // dollars_per_r
        None,       // cost_per_trade_r
        None,       // capital_dollar
        None,       // risk_pct_per_trade
        None,       // equity_time_years
        crate::config::PositionSizingMode::Fractional,
        1,
        None,
        None,
    );

    assert_eq!(stats.total_bars, 3);
    // Net-profitable trades: only the +1.0R.
    assert_eq!(stats.profitable_bars, 1);
    assert_eq!(stats.unprofitable_bars, 1);
    // Win rate is based on net R, not labels: 1/3.
    let expected_win_rate = 100.0 / 3.0;
    assert!((stats.win_rate - expected_win_rate).abs() < 1e-9);

    // Label metrics reflect target behaviour (2/3 hits).
    assert_eq!(stats.label_hits, 2);
    assert_eq!(stats.label_misses, 1);
    let expected_label_hit_rate = 200.0 / 3.0;
    assert!((stats.label_hit_rate - expected_label_hit_rate).abs() < 1e-9);
}

#[test]
fn contracts_equity_simulation_respects_margin_cap() {
    // One winning trade with 1R=rpc dollars and contracts computed from risk budget,
    // then capped by margin_per_contract_dollar.
    let rr = vec![1.0];
    let rpc = vec![100.0];
    let stats = compute_full_statistics(
        1,
        1,
        &rr,
        Some(&rpc),
        0,
        None,
        None,
        Some(10_000.0),
        Some(10.0),
        Some(1.0),
        crate::config::PositionSizingMode::Contracts,
        1,
        None,
        Some(2_500.0),
    );

    // risk budget = 10k*10% = 1k => floor(1k/100)=10 by risk
    // margin cap = floor(10k/2500)=4 => pnl = 1R * $100 * 4 = $400
    assert!((stats.final_capital - 10_400.0).abs() < 1e-9);
}

#[test]
fn evaluation_uses_eligible_and_finite_rr_as_trade_denominator() -> Result<()> {
    let dir = tempdir()?;
    let csv_path = dir.path().join("eligibility.csv");
    let csv = "\
feature_a,highlow_or_atr,highlow_or_atr_eligible,rr_highlow_or_atr\n\
true,true,true,2.0\n\
true,false,true,-1.0\n\
true,false,false,NaN\n\
true,false,false,NaN\n\
true,true,true,2.0\n";
    std::fs::write(&csv_path, csv)?;

    let data = Arc::new(ColumnarData::load(&csv_path)?);
    let mut config = dummy_config("highlow_or_atr", Direction::Long);
    config.stats_detail = StatsDetail::Full;
    let mask_cache = Arc::new(MaskCache::with_max_entries(128));
    let ctx = EvaluationContext::new(
        Arc::clone(&data),
        mask_cache,
        &config,
        Arc::new(HashMap::new()),
    )?;

    let features = vec![FeatureDescriptor::boolean("feature_a", "test")];
    let bitsets = build_bitset_catalog(&ctx, &features)?;
    let combo = vec![features[0].clone()];
    let stats = evaluate_combination(&combo, &ctx, &bitsets, 3)?;

    assert_eq!(stats.mask_hits, 5, "combo mask should match all 5 rows");
    assert_eq!(
        stats.total_bars, 3,
        "only eligible rows with finite RR count as trades"
    );
    assert_eq!(stats.profitable_bars, 2);
    assert_eq!(stats.unprofitable_bars, 1);
    assert_eq!(stats.label_hits, 2);
    assert_eq!(stats.label_misses, 1);
    Ok(())
}

#[test]
fn evaluation_no_stacking_skips_overlapping_trades_using_exit_indices() -> Result<()> {
    let dir = tempdir()?;
    let csv_path = dir.path().join("no_stacking.csv");
    let csv = "\
feature_a,highlow_or_atr,highlow_or_atr_eligible,rr_highlow_or_atr,highlow_or_atr_exit_i\n\
true,false,true,1.0,4\n\
true,false,true,1.0,4\n\
true,false,true,1.0,4\n\
true,false,true,1.0,4\n\
true,false,true,1.0,4\n";
    std::fs::write(&csv_path, csv)?;

    let data = Arc::new(ColumnarData::load(&csv_path)?);
    let mask_cache = Arc::new(MaskCache::with_max_entries(128));
    let features = vec![FeatureDescriptor::boolean("feature_a", "test")];

    let mut config_stacking = dummy_config("highlow_or_atr", Direction::Long);
    config_stacking.stats_detail = StatsDetail::Full;
    config_stacking.stacking_mode = StackingMode::Stacking;
    let ctx_stacking = EvaluationContext::new(
        Arc::clone(&data),
        Arc::clone(&mask_cache),
        &config_stacking,
        Arc::new(HashMap::new()),
    )?;
    let bitsets_stacking = build_bitset_catalog(&ctx_stacking, &features)?;
    let combo = vec![features[0].clone()];
    let stats_stacking = evaluate_combination(&combo, &ctx_stacking, &bitsets_stacking, 1)?;

    let mut config_no_stacking = dummy_config("highlow_or_atr", Direction::Long);
    config_no_stacking.stats_detail = StatsDetail::Full;
    config_no_stacking.stacking_mode = StackingMode::NoStacking;
    let ctx_no_stacking = EvaluationContext::new(
        Arc::clone(&data),
        mask_cache,
        &config_no_stacking,
        Arc::new(HashMap::new()),
    )?;
    let bitsets_no_stacking = build_bitset_catalog(&ctx_no_stacking, &features)?;
    let stats_no_stacking =
        evaluate_combination(&combo, &ctx_no_stacking, &bitsets_no_stacking, 1)?;

    assert_eq!(stats_stacking.mask_hits, 5);
    assert_eq!(stats_stacking.total_bars, 5);
    assert_eq!(stats_no_stacking.mask_hits, 5);
    assert_eq!(
        stats_no_stacking.total_bars, 2,
        "expected only idx=0 and idx=4 to be eligible under no-stacking"
    );
    Ok(())
}
