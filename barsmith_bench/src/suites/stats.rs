use std::hint::black_box;

use anyhow::Result;
use barsmith_rs::stats::{
    benchmark_core_statistics_checksum_for_returns, benchmark_core_statistics_fixture,
};

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;
use crate::runner::{BenchmarkSpec, RegressionPolicy, measure};

pub fn run(args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    const REPEATS: usize = 512;

    let returns = benchmark_core_statistics_fixture(8_192);
    Ok(vec![measure(
        BenchmarkSpec {
            suite: "stats".to_string(),
            name: "core-statistics-8192x512".to_string(),
            fixture_tier: "synthetic".to_string(),
            fixture_label: format!("rows=8192,repeats={REPEATS}"),
            fixture_sha256: None,
            command: None,
            iterations_per_sample: REPEATS as u64,
            regression_policy: RegressionPolicy::HardGate,
            notes: vec![
                "Exercises the core statistics path used during high-throughput sweeps."
                    .to_string(),
            ],
        },
        args,
        || {
            let checksum = benchmark_core_statistics_checksum_for_returns(&returns, REPEATS);
            black_box(checksum);
            Ok(REPEATS as u64)
        },
    )?])
}
