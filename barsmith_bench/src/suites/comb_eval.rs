use anyhow::Result;
use barsmith_rs::benchmark::BenchmarkCombSearch;

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;
use crate::runner::{BenchmarkSpec, RegressionPolicy, measure};

pub fn run(args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    let combinations = args.max_combos.max(20_000);
    let fixture = BenchmarkCombSearch::new(16_384, 96, args.max_depth.max(3));
    Ok(vec![measure(
        BenchmarkSpec {
            suite: "comb-eval".to_string(),
            name: "synthetic-depth3-hot-path".to_string(),
            fixture_tier: "synthetic".to_string(),
            fixture_label: format!(
                "rows=16384,features=96,max_depth={},combinations={combinations}",
                args.max_depth.max(3)
            ),
            fixture_sha256: None,
            command: None,
            iterations_per_sample: combinations as u64,
            regression_policy: RegressionPolicy::HardGate,
            notes: vec![
                "Exercises combination enumeration, bitset lookup/sort, and gated scan without filesystem noise."
                    .to_string(),
            ],
        },
        args,
        || {
            fixture.scan_checksum(combinations);
            Ok(combinations as u64)
        },
    )?])
}
