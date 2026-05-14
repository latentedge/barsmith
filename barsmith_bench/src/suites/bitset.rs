use anyhow::Result;
use barsmith_rs::benchmark::BenchmarkBitsetGatedScan;

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;
use crate::runner::{BenchmarkSpec, RegressionPolicy, measure};

pub fn run(args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    let scan_fixture = BenchmarkBitsetGatedScan::new(65_536);
    Ok(vec![measure(
        BenchmarkSpec {
            suite: "bitset".to_string(),
            name: "gated-scan-65536x3".to_string(),
            fixture_tier: "synthetic".to_string(),
            fixture_label: "rows=65536,masks=3,repeats=1024".to_string(),
            fixture_sha256: None,
            command: None,
            iterations_per_sample: 1_024,
            regression_policy: RegressionPolicy::HardGate,
            notes: vec![
                "Uses the same gated bitset scan path as combination evaluation.".to_string(),
            ],
        },
        args,
        || {
            scan_fixture.scan_checksum(1_024);
            Ok(1_024)
        },
    )?])
}
