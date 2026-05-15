mod bitset;
mod cli;
mod comb_depth;
mod comb_eval;
mod combinator;
mod stats;
#[cfg(feature = "target-generation")]
mod target_generation;

use anyhow::{Result, anyhow};

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;

pub fn run_suite(args: &RunArgs, warnings: &mut Vec<String>) -> Result<Vec<BenchmarkResult>> {
    match args.suite.as_str() {
        "smoke" => run_many(
            args,
            warnings,
            &[
                Suite::Combinator,
                Suite::CombEval,
                Suite::Bitset,
                Suite::Stats,
            ],
        ),
        "all" => run_many(
            args,
            warnings,
            &[
                Suite::Combinator,
                Suite::CombEval,
                Suite::Bitset,
                Suite::Stats,
                Suite::CombCli,
                Suite::ResultsCli,
                Suite::StrictEval,
                Suite::SelectValidate,
                Suite::CombDepth5,
            ],
        ),
        "combinator" => combinator::run(args),
        "bitset" => bitset::run(args),
        "stats" => stats::run(args),
        "comb-eval" => comb_eval::run(args),
        "comb-depth5" => comb_depth::run_depth5(args),
        "target-generation" => run_target_generation(args),
        "comb-cli" => cli::run_comb_cli(args, warnings),
        "results-cli" => cli::run_results_cli(args, warnings),
        "strict-eval" | "formula-eval" => cli::run_strict_eval(args, warnings),
        "select-validate" | "selection-workflow" => cli::run_select_validate(args, warnings),
        other => Err(anyhow!(
            "unknown suite '{other}'; expected smoke, all, combinator, comb-eval, comb-depth5, target-generation, bitset, stats, comb-cli, results-cli, strict-eval, or select-validate"
        )),
    }
}

#[derive(Clone, Copy)]
enum Suite {
    Combinator,
    CombEval,
    CombDepth5,
    Bitset,
    Stats,
    CombCli,
    ResultsCli,
    StrictEval,
    SelectValidate,
}

fn run_many(
    args: &RunArgs,
    warnings: &mut Vec<String>,
    suites: &[Suite],
) -> Result<Vec<BenchmarkResult>> {
    let mut results = Vec::new();
    for suite in suites {
        match suite {
            Suite::Combinator => results.extend(combinator::run(args)?),
            Suite::CombEval => results.extend(comb_eval::run(args)?),
            Suite::CombDepth5 => results.extend(comb_depth::run_depth5(args)?),
            Suite::Bitset => results.extend(bitset::run(args)?),
            Suite::Stats => results.extend(stats::run(args)?),
            Suite::CombCli => results.extend(cli::run_comb_cli(args, warnings)?),
            Suite::ResultsCli => results.extend(cli::run_results_cli(args, warnings)?),
            Suite::StrictEval => results.extend(cli::run_strict_eval(args, warnings)?),
            Suite::SelectValidate => results.extend(cli::run_select_validate(args, warnings)?),
        }
    }
    Ok(results)
}

#[cfg(feature = "target-generation")]
fn run_target_generation(args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    target_generation::run(args)
}

#[cfg(not(feature = "target-generation"))]
fn run_target_generation(_args: &RunArgs) -> Result<Vec<BenchmarkResult>> {
    Err(anyhow!(
        "suite 'target-generation' requires building barsmith_bench with --features target-generation"
    ))
}
