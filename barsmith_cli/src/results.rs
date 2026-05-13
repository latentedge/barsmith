use anyhow::Result;
use barsmith_rs::storage::{ResultQuery, ResultRankBy, ResultRow, query_result_store};

use crate::cli::ResultsArgs;

pub fn run(args: ResultsArgs) -> Result<()> {
    let query = ResultQuery {
        output_dir: args.output_dir,
        direction: format!("{:?}", args.direction.to_direction()).to_ascii_lowercase(),
        target: normalize_target(&args.target),
        min_sample_size: args.min_samples,
        min_win_rate: args.min_win_rate,
        max_drawdown: args.max_drawdown,
        min_calmar: args.min_calmar,
        rank_by: args.rank_by.to_rank_by(),
        limit: args.limit,
    };

    let rows = query_result_store(&query)?;
    if rows.is_empty() {
        println!("No results matched the given filters.");
        return Ok(());
    }

    println!(
        "Top {} combinations for direction={}, target={}, rank_by={}",
        rows.len(),
        query.direction,
        query.target,
        rank_by_label(query.rank_by)
    );
    println!("{}", "=".repeat(80));
    for (idx, row) in rows.iter().enumerate() {
        print_result_row(idx + 1, row);
    }

    Ok(())
}

fn rank_by_label(rank_by: ResultRankBy) -> &'static str {
    match rank_by {
        ResultRankBy::CalmarRatio => "calmar-ratio",
        ResultRankBy::TotalReturn => "total-return",
    }
}

fn normalize_target(target: &str) -> String {
    if target == "atr_stop" {
        "2x_atr_tp_atr_stop".to_string()
    } else {
        target.to_string()
    }
}

fn print_result_row(rank: usize, row: &ResultRow) {
    println!();
    println!("Rank {rank}: {}", row.combination);
    println!(
        "  Depth: {} | Bars: {}/{} | Win rate: {}%",
        row.depth,
        row.profitable_bars,
        row.total_bars,
        f2(row.win_rate)
    );
    println!(
        "  Total R: {} | Max DD: {}R | Calmar: {} | Resume offset: {}",
        f2(row.total_return),
        f2(row.max_drawdown),
        f2(row.calmar_ratio),
        row.resume_offset
    );
}

fn f2(value: f64) -> String {
    format_float(value, 2)
}

fn format_float(value: f64, decimals: usize) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "Inf".to_string()
    } else if value.is_infinite() && value.is_sign_negative() {
        "-Inf".to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else {
        format!("{value:.decimals$}")
    }
}
