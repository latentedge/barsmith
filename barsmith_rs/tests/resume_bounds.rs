use std::collections::HashMap;
use std::fs;

use anyhow::Result;
use barsmith_rs::{
    Config, Direction, FeatureDescriptor, PermutationPipeline, ReportMetricsMode,
    combinator::total_combinations,
    config::{EvalProfileMode, PositionSizingMode, StackingMode, StatsDetail, StopDistanceUnit},
};
use tempfile::tempdir;

/// A resume offset beyond the catalog should warn and enumerate nothing.
#[test]
fn pipeline_with_resume_offset_beyond_total_enumerates_nothing() -> Result<()> {
    let temp_dir = tempdir()?;
    let csv_path = temp_dir.path().join("data.csv");
    fs::write(
        &csv_path,
        "\
timestamp,is_green,feat_a,feat_b
2024-01-01T00:00:00Z,true,true,false
2024-01-01T00:30:00Z,false,false,true
",
    )?;

    let output_dir = temp_dir.path().join("out");
    let config = Config {
        input_csv: csv_path.clone(),
        source_csv: Some(csv_path),
        direction: Direction::Long,
        target: "is_green".to_string(),
        output_dir: output_dir.clone(),
        max_depth: 1,
        min_sample_size: 1,
        min_sample_size_report: 1,
        include_date_start: None,
        include_date_end: None,
        batch_size: 4,
        n_workers: 1,
        auto_batch: false,
        // With two boolean features at depth 1, the theoretical total is 2.
        // Setting resume_offset to a much larger value ensures we hit the
        // "resume offset exceeds theoretical combination count" path.
        resume_offset: 10,
        explicit_resume_offset: false,
        max_combos: None,
        dry_run: false,
        quiet: true,
        report_metrics: ReportMetricsMode::Off,
        report_top: 5,
        force_recompute: true,
        max_drawdown: 50.0,
        max_drawdown_report: None,
        min_calmar_report: None,
        strict_min_pruning: true,
        enable_feature_pairs: false,
        feature_pairs_limit: None,
        enable_subset_pruning: false,
        catalog_hash: None,
        stats_detail: StatsDetail::Full,
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
        position_sizing: PositionSizingMode::Fractional,
        stop_distance_column: None,
        stop_distance_unit: StopDistanceUnit::Points,
        min_contracts: 1,
        max_contracts: None,
        point_value: None,
        tick_value: None,
        margin_per_contract_dollar: None,
        require_any_features: Vec::new(),
    };

    let features = vec![
        FeatureDescriptor::boolean("feat_a", "test"),
        FeatureDescriptor::boolean("feat_b", "test"),
    ];
    let specs: HashMap<String, barsmith_rs::feature::ComparisonSpec> = HashMap::new();
    let mut pipeline = PermutationPipeline::new(config, features, specs);
    pipeline.run()?; // should complete without writing any result batches

    let results_dir = output_dir.join("results_parquet");
    assert!(
        results_dir.exists(),
        "results_parquet directory should exist even when no batches are written"
    );
    let parts: Vec<_> = fs::read_dir(&results_dir)?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("part-"))
        .collect();
    assert!(
        parts.is_empty(),
        "no Parquet result batches should be written when resume_offset exceeds total combinations"
    );

    Ok(())
}

/// Large catalogs need u128 combination counts before the pipeline can decide
/// whether a resume offset still fits in u64.
#[test]
fn total_combinations_can_exceed_u64_max() {
    let feature_count = 100_000;

    let depth_4 = total_combinations(feature_count, 4);
    assert!(
        depth_4 < u64::MAX as u128,
        "depth 4 with 100k features should be under u64::MAX, got {}",
        depth_4
    );

    let depth_5 = total_combinations(feature_count, 5);
    assert!(
        depth_5 > u64::MAX as u128,
        "depth 5 with 100k features should exceed u64::MAX, got {}",
        depth_5
    );

    let exceeds_u64 = depth_5 > u64::MAX as u128;
    assert!(
        exceeds_u64,
        "overflow check should detect depth 5 exceeds u64::MAX"
    );
}

/// With ~12,000 features (booleans + thresholds + feature-pairs),
/// depth 7+ will overflow u64.
#[test]
fn realistic_catalog_overflow_at_high_depth() {
    // Approximate a large run after boolean, threshold, and feature-pair expansion.
    // Total: ~13,000 features
    let feature_count = 13_000;

    // Depth 6: close to the edge
    let depth_6 = total_combinations(feature_count, 6);
    println!("13k features, depth 6: {}", depth_6);

    // Depth 7: should overflow
    let depth_7 = total_combinations(feature_count, 7);
    println!("13k features, depth 7: {}", depth_7);

    // Depth 7 should exceed u64::MAX
    assert!(
        depth_7 > u64::MAX as u128,
        "depth 7 with 13k features should exceed u64::MAX, got {} (u64::MAX = {})",
        depth_7,
        u64::MAX
    );
}
