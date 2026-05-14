mod bitset;
mod cli;
mod comb_eval;
mod combinator;
mod stats;

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
            ],
        ),
        "combinator" => combinator::run(args),
        "bitset" => bitset::run(args),
        "stats" => stats::run(args),
        "comb-eval" => comb_eval::run(args),
        "comb-cli" => cli::run_comb_cli(args, warnings),
        "results-cli" => cli::run_results_cli(args, warnings),
        "strict-eval" | "formula-eval" => cli::run_strict_eval(args, warnings),
        other => Err(anyhow!(
            "unknown suite '{other}'; expected smoke, all, combinator, comb-eval, bitset, stats, comb-cli, results-cli, or strict-eval"
        )),
    }
}

#[derive(Clone, Copy)]
enum Suite {
    Combinator,
    CombEval,
    Bitset,
    Stats,
    CombCli,
    ResultsCli,
    StrictEval,
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
            Suite::Bitset => results.extend(bitset::run(args)?),
            Suite::Stats => results.extend(stats::run(args)?),
            Suite::CombCli => results.extend(cli::run_comb_cli(args, warnings)?),
            Suite::ResultsCli => results.extend(cli::run_results_cli(args, warnings)?),
            Suite::StrictEval => results.extend(cli::run_strict_eval(args, warnings)?),
        }
    }
    Ok(results)
}
