use anyhow::Result;

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;
use crate::runner::{BenchmarkSpec, RegressionPolicy, measure};

pub fn run(args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    let rows = args.batch_size.max(16_384);
    let repeats = args.max_combos.max(25);

    Ok(vec![measure(
        BenchmarkSpec {
            suite: "target-generation".to_string(),
            name: "2x-atr-tp-atr-stop".to_string(),
            fixture_tier: "synthetic".to_string(),
            fixture_label: format!("rows={rows},repeats={repeats},direction=both,tick=0.25"),
            fixture_sha256: None,
            command: None,
            iterations_per_sample: rows.saturating_mul(repeats) as u64,
            regression_policy: RegressionPolicy::HardGate,
            notes: vec![
                "Exercises ATR target generation, cutoff resolution, tick rounding, and exit-index assignment without filesystem noise."
                    .to_string(),
            ],
        },
        args,
        || {
            custom_rs::benchmark_2x_atr_tp_atr_stop_checksum(rows, repeats);
            Ok(rows.saturating_mul(repeats) as u64)
        },
    )?])
}
