use std::hint::black_box;

use anyhow::Result;
use barsmith_rs::combinator::{
    SeekableIndexIterator, combinations_for_depth, rank_combination, unrank_combination,
};

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;
use crate::runner::{BenchmarkSpec, RegressionPolicy, measure};

pub fn run(args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    Ok(vec![
        measure(
            BenchmarkSpec {
                suite: "combinator".to_string(),
                name: "rank-unrank-n256-k3".to_string(),
                fixture_tier: "synthetic".to_string(),
                fixture_label: "n=256,k=3,iterations=200000".to_string(),
                fixture_sha256: None,
                command: None,
                iterations_per_sample: 200_000,
                regression_policy: RegressionPolicy::HardGate,
                notes: vec!["Exercises combinatorial rank/unrank arithmetic.".to_string()],
            },
            args,
            || rank_unrank_roundtrip(256, 3, 200_000),
        )?,
        measure(
            BenchmarkSpec {
                suite: "combinator".to_string(),
                name: "index-iterator-n512-depth3".to_string(),
                fixture_tier: "synthetic".to_string(),
                fixture_label: "n=512,max_depth=3,iterations=20000000".to_string(),
                fixture_sha256: None,
                command: None,
                iterations_per_sample: 20_000_000,
                regression_policy: RegressionPolicy::HardGate,
                notes: vec![
                    "Measures seekable index iteration without descriptor cloning.".to_string(),
                ],
            },
            args,
            || iterate_indices(512, 3, 20_000_000),
        )?,
    ])
}

fn rank_unrank_roundtrip(n: usize, k: usize, iterations: u64) -> Result<u64> {
    let total = combinations_for_depth(n, k);
    let mut checksum = 0u128;
    for step in 0..iterations {
        let rank = ((step as u128 * 104_729) + 17) % total;
        let combo = unrank_combination(rank, n, k);
        checksum = checksum.wrapping_add(rank_combination(&combo, n));
    }
    black_box(checksum);
    Ok(iterations)
}

fn iterate_indices(n: usize, max_depth: usize, iterations: u64) -> Result<u64> {
    let mut iter = SeekableIndexIterator::starting_at(n, max_depth, 0);
    let mut checksum = 0usize;
    let mut observed = 0u64;
    for _ in 0..iterations {
        let Some(combo) = iter.next() else {
            break;
        };
        checksum = checksum.wrapping_add(combo.iter().sum::<usize>());
        observed += 1;
    }
    black_box(checksum);
    Ok(observed)
}
