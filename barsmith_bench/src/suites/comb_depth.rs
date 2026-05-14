use anyhow::Result;
use barsmith_rs::benchmark::BenchmarkCombSearch;

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;
use crate::runner::{BenchmarkSpec, RegressionPolicy, measure};

pub fn run_depth5(args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    let feature_count = 96;
    let max_depth = args.max_depth.max(5);
    let combinations = args.max_combos.max(10_000);
    let fixture = BenchmarkCombSearch::new(16_384, feature_count, max_depth);
    let start_offset = fixture.depth_start_offset(5);

    Ok(vec![measure(
        BenchmarkSpec {
            suite: "comb-depth5".to_string(),
            name: "synthetic-depth5-hot-path".to_string(),
            fixture_tier: "synthetic".to_string(),
            fixture_label: format!(
                "rows=16384,features={feature_count},max_depth={max_depth},start_depth=5,combinations={combinations}"
            ),
            fixture_sha256: None,
            command: None,
            iterations_per_sample: combinations as u64,
            regression_policy: RegressionPolicy::HardGate,
            notes: vec![
                "Starts enumeration at depth 5 so the benchmark covers the higher-cardinality combination path directly."
                    .to_string(),
            ],
        },
        args,
        || {
            fixture.scan_checksum_from(start_offset, combinations);
            Ok(combinations as u64)
        },
    )?])
}
