use std::fs::{self, File};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use barsmith_indicators::{FLOAT_TOLERANCE, atr, build_long_levels, build_short_levels};
use barsmith_rs::Direction;
use barsmith_rs::backtest::{BacktestInputs, BacktestOutputs, TradeDirection, run_backtest};
use chrono::{DateTime, Datelike, Utc};
use polars::prelude::*;

use super::FeatureEngineer;
use super::io::{bool_column, column_with_nans, timestamp_column};

#[derive(Clone, Copy, Debug)]
pub enum BacktestTargetKind {
    TribarWeekly,
    TribarMonthly,
}

pub struct BacktestConfig {
    pub csv_path: PathBuf,
    pub output_dir: PathBuf,
    pub direction: Direction,
    pub target_kind: BacktestTargetKind,
    pub features_expr: String,
    pub tp_multiple: f64,
    pub max_trades_per_period: u8,
}

pub fn run_backtest_with_target(config: &BacktestConfig) -> Result<PathBuf> {
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("Unable to create {}", config.output_dir.display()))?;
    let output_path = config.output_dir.join("barsmith_backtest.csv");

    let mut engineer = FeatureEngineer::from_csv(&config.csv_path)?;
    engineer.compute_features()?;

    let frame_len = engineer.frame.height();
    if frame_len == 0 {
        let mut file = File::create(&output_path)
            .with_context(|| format!("Unable to create {}", output_path.display()))?;
        CsvWriter::new(&mut file)
            .include_header(true)
            .finish(engineer.data_frame_mut())
            .with_context(|| "Failed to persist empty backtest dataset")?;
        return Ok(output_path);
    }

    let high = column_with_nans(&engineer.frame, "high")?;
    let low = column_with_nans(&engineer.frame, "low")?;
    let close = column_with_nans(&engineer.frame, "close")?;
    let timestamps = timestamp_column(&engineer.frame)?;

    let len = close.len();
    if high.len() != len || low.len() != len || timestamps.len() != len {
        return Err(anyhow!(
            "Inconsistent series lengths when preparing backtest inputs"
        ));
    }

    let atr_values = atr(&high, &low, &close, 14);

    let base_entry = build_entry_mask(&engineer.frame, &config.features_expr)?;
    if base_entry.len() != len {
        return Err(anyhow!(
            "Entry mask length {} does not match price series length {}",
            base_entry.len(),
            len
        ));
    }

    // Optional trend filter: for longs, close should be above 200sma;
    // for shorts, close should be below 200sma. If the columns are
    // missing, we fall back to not filtering.
    let above_200 = match bool_column(&engineer.frame, "is_close_above_200sma") {
        Ok(mask) if mask.len() == len => mask,
        _ => vec![true; len],
    };
    let below_200 = match bool_column(&engineer.frame, "is_close_below_200sma") {
        Ok(mask) if mask.len() == len => mask,
        _ => above_200.iter().map(|flag| !*flag).collect(),
    };

    let (mut entry_long, mut entry_short) = match config.direction {
        Direction::Long => (
            base_entry
                .iter()
                .zip(above_200.iter())
                .map(|(b, a)| *b && *a)
                .collect(),
            vec![false; len],
        ),
        Direction::Short => (
            vec![false; len],
            base_entry
                .iter()
                .zip(below_200.iter())
                .map(|(b, d)| *b && *d)
                .collect(),
        ),
        Direction::Both => (
            base_entry
                .iter()
                .zip(above_200.iter())
                .map(|(b, a)| *b && *a)
                .collect(),
            base_entry
                .iter()
                .zip(below_200.iter())
                .map(|(b, d)| *b && *d)
                .collect(),
        ),
    };

    // Period indices and per-period caps from the target kind.
    let (period_index, period_end_index, default_tp_multiple, default_max_trades) =
        period_indices_for_target(config.target_kind, &timestamps);

    if period_index.len() != len || period_end_index.len() != len {
        return Err(anyhow!(
            "Period index lengths do not match price series length when preparing backtest inputs"
        ));
    }

    let tp_multiple = if config.tp_multiple > 0.0 {
        config.tp_multiple
    } else {
        default_tp_multiple
    };
    let max_trades = if config.max_trades_per_period > 0 {
        config.max_trades_per_period
    } else {
        default_max_trades
    };

    let (stop_long, tp_long) = build_long_levels(&close, &low, &atr_values, tp_multiple);
    let (stop_short, tp_short) = build_short_levels(&close, &high, &atr_values, tp_multiple);

    // Disable entries and caps per direction when not requested.
    let (max_trades_long, max_trades_short) = match config.direction {
        Direction::Long => (max_trades, 0),
        Direction::Short => (0, max_trades),
        Direction::Both => (max_trades, max_trades),
    };
    if max_trades_long == 0 {
        entry_long.fill(false);
    }
    if max_trades_short == 0 {
        entry_short.fill(false);
    }

    let inputs = BacktestInputs {
        high,
        low,
        close: close.clone(),
        entry_long,
        entry_short,
        stop_long,
        tp_long,
        stop_short,
        tp_short,
        period_index,
        period_end_index,
        max_trades_per_period_long: max_trades_long,
        max_trades_per_period_short: max_trades_short,
        stop_after_first_winning_trade_long: true,
        stop_after_first_winning_trade_short: true,
    };

    let outputs: BacktestOutputs = run_backtest(&inputs);

    // Attach per-bar targets and RR back onto the engineered frame.

    let prefix = match config.target_kind {
        BacktestTargetKind::TribarWeekly => "tribar_weekly",
        BacktestTargetKind::TribarMonthly => "tribar_monthly",
    };

    if max_trades_long > 0 {
        let target_name = format!("backtest_{}_long", prefix);
        let rr_name = format!("rr_backtest_{}_long", prefix);
        engineer.replace_bool_column(&target_name, outputs.target_long.clone())?;
        engineer.replace_float_column(&rr_name, outputs.rr_long.clone())?;
    }

    if max_trades_short > 0 {
        let target_name = format!("backtest_{}_short", prefix);
        let rr_name = format!("rr_backtest_{}_short", prefix);
        engineer.replace_bool_column(&target_name, outputs.target_short.clone())?;
        engineer.replace_float_column(&rr_name, outputs.rr_short.clone())?;
    }

    // Summarise and print trade-level information so CLI users can
    // inspect the most recent executions without opening the CSV.
    if !outputs.trades.is_empty() {
        let mut total_trades = 0usize;
        let mut wins = 0usize;
        let mut losses = 0usize;
        let mut rr_sum = 0.0_f64;
        let mut rr_win_sum = 0.0_f64;
        let mut rr_loss_sum = 0.0_f64;
        let mut long_trades = 0usize;
        let mut short_trades = 0usize;
        let mut best_rr = f64::NEG_INFINITY;
        let mut worst_rr = f64::INFINITY;

        for trade in &outputs.trades {
            if !trade.rr.is_finite() {
                continue;
            }
            total_trades += 1;
            rr_sum += trade.rr;
            if trade.rr > 0.0 {
                wins += 1;
                rr_win_sum += trade.rr;
            } else {
                losses += 1;
                rr_loss_sum += trade.rr;
            }
            match trade.direction {
                TradeDirection::Long => long_trades += 1,
                TradeDirection::Short => short_trades += 1,
            }
            if trade.rr > best_rr {
                best_rr = trade.rr;
            }
            if trade.rr < worst_rr {
                worst_rr = trade.rr;
            }
        }

        if total_trades > 0 {
            let win_rate = wins as f64 / total_trades as f64 * 100.0;
            let avg_rr = rr_sum / total_trades as f64;
            let avg_rr_win = if wins > 0 {
                rr_win_sum / wins as f64
            } else {
                f64::NAN
            };
            let avg_rr_loss = if losses > 0 {
                rr_loss_sum / losses as f64
            } else {
                f64::NAN
            };

            let (period_start_label, period_end_label) = match config.target_kind {
                BacktestTargetKind::TribarWeekly => ("Week start", "Week end"),
                BacktestTargetKind::TribarMonthly => ("Month start", "Month end"),
            };

            println!();
            println!(
                "Backtest summary for {} ({:?}, {} trades):",
                match config.target_kind {
                    BacktestTargetKind::TribarWeekly => "tribar-weekly",
                    BacktestTargetKind::TribarMonthly => "tribar-monthly",
                },
                config.direction,
                total_trades,
            );
            println!(
                "  Long trades: {}, Short trades: {}",
                long_trades, short_trades
            );
            println!(
                "  Wins: {}  Losses: {}  Win rate: {:.2}%",
                wins, losses, win_rate
            );
            println!(
                "  Avg RR: {:.2}  Avg RR (wins): {:.2}  Avg RR (losses): {:.2}",
                avg_rr, avg_rr_win, avg_rr_loss,
            );
            if best_rr.is_finite() && worst_rr.is_finite() {
                println!("  Best RR: {:.2}  Worst RR: {:.2}", best_rr, worst_rr);
            }

            // Print the last 20 trades in chronological order.
            let count_to_show = outputs.trades.len().min(20);
            println!();
            println!("Last {} trades:", count_to_show);
            println!(
                "  {:>4}  {:>5}  {:>25}  {:>25}  {:>25}  {:>25}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6}",
                "Idx",
                "Side",
                "Entry time",
                "Exit time",
                period_start_label,
                period_end_label,
                "Entry",
                "Exit",
                "Stop",
                "Target",
                "RR",
            );

            let start = outputs.trades.len() - count_to_show;
            for (idx, trade) in outputs.trades[start..].iter().enumerate() {
                let entry_ts = timestamps
                    .get(trade.entry_index)
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "-".to_string());
                let exit_ts = timestamps
                    .get(trade.exit_index)
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "-".to_string());

                // Derive period start/end from the period index and end-index
                // masks used by the backtest engine.
                let period_id = trade.period;
                let mut period_start_idx = trade.entry_index;
                while period_start_idx > 0 && inputs.period_index[period_start_idx - 1] == period_id
                {
                    period_start_idx -= 1;
                }
                let period_end_idx = inputs.period_end_index[trade.entry_index];
                let period_start_ts = timestamps
                    .get(period_start_idx)
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "-".to_string());
                let period_end_ts = timestamps
                    .get(period_end_idx)
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "-".to_string());

                let side = match trade.direction {
                    TradeDirection::Long => "LONG",
                    TradeDirection::Short => "SHORT",
                };
                println!(
                    "  {:>4}  {:>5}  {:>25}  {:>25}  {:>25}  {:>25}  {:>10.2}  {:>10.2}  {:>10.2}  {:>10.2}  {:>6.2}",
                    start + idx + 1,
                    side,
                    entry_ts,
                    exit_ts,
                    period_start_ts,
                    period_end_ts,
                    trade.entry_price,
                    trade.exit_price,
                    trade.stop_price,
                    trade.tp_price,
                    trade.rr,
                );
            }
            println!();
        }
    }

    let mut file = File::create(&output_path)
        .with_context(|| format!("Unable to create {}", output_path.display()))?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .finish(engineer.data_frame_mut())
        .with_context(|| "Failed to persist backtest dataset")?;
    Ok(output_path)
}

fn compute_week_indices(timestamps: &[DateTime<Utc>]) -> (Vec<i64>, Vec<usize>) {
    let len = timestamps.len();
    if len == 0 {
        return (Vec::new(), Vec::new());
    }

    // Anchor Sunday 22:00 UTC so weekly rolling windows stay stable.
    const ANCHOR_SECS: i64 = (3 * 24 * 60 * 60) + (22 * 60 * 60);
    const WEEK_SECS: i64 = 7 * 24 * 60 * 60;
    let mut week_index = Vec::with_capacity(len);
    for ts in timestamps {
        let secs = ts.timestamp().saturating_sub(ANCHOR_SECS).max(0);
        week_index.push(secs / WEEK_SECS);
    }

    // Record the last index in each weekly bucket.
    let mut week_end_index = vec![0usize; len];
    let mut current_week = week_index[len - 1];
    let mut current_end = len - 1;
    let mut i = len;
    while i > 0 {
        i -= 1;
        if week_index[i] != current_week {
            current_week = week_index[i];
            current_end = i;
        }
        week_end_index[i] = current_end;
    }

    (week_index, week_end_index)
}

fn compute_month_indices(timestamps: &[DateTime<Utc>]) -> (Vec<i64>, Vec<usize>) {
    let len = timestamps.len();
    if len == 0 {
        return (Vec::new(), Vec::new());
    }

    let mut month_index = Vec::with_capacity(len);
    for ts in timestamps {
        let year = ts.year() as i64;
        let month = ts.month() as i64; // 1..=12
        month_index.push(year * 12 + (month - 1));
    }

    let mut month_end_index = vec![0usize; len];
    let mut current_month = month_index[len - 1];
    let mut current_end = len - 1;
    let mut i = len;
    while i > 0 {
        i -= 1;
        if month_index[i] != current_month {
            current_month = month_index[i];
            current_end = i;
        }
        month_end_index[i] = current_end;
    }

    (month_index, month_end_index)
}

fn period_indices_for_target(
    target: BacktestTargetKind,
    timestamps: &[DateTime<Utc>],
) -> (Vec<i64>, Vec<usize>, f64, u8) {
    match target {
        BacktestTargetKind::TribarWeekly => {
            let (idx, end) = compute_week_indices(timestamps);
            (idx, end, 2.0, 2)
        }
        BacktestTargetKind::TribarMonthly => {
            let (idx, end) = compute_month_indices(timestamps);
            (idx, end, 2.0, 2)
        }
    }
}

fn build_entry_mask(frame: &DataFrame, expr: &str) -> Result<Vec<bool>> {
    let len = frame.height();
    if len == 0 {
        return Ok(Vec::new());
    }

    // This parser is intentionally small because backtest entry expressions are
    // internal fixtures, not the public formula language.
    let mut entry = vec![true; len];
    for raw_term in expr.split("&&") {
        let term = raw_term.trim();
        if term.is_empty() {
            continue;
        }
        let parts: Vec<&str> = term.split_whitespace().collect();
        let mask = if parts.len() == 1 {
            bool_column(frame, parts[0])?
        } else if parts.len() == 3 {
            let col_name = parts[0];
            let op = parts[1];
            let value: f64 = parts[2].parse().with_context(|| {
                format!(
                    "Unable to parse numeric literal '{}' in features expression",
                    parts[2]
                )
            })?;
            let series = column_with_nans(frame, col_name)?;
            series
                .iter()
                .map(|v| match v {
                    x if !x.is_finite() => false,
                    x => match op {
                        ">" => *x > value,
                        "<" => *x < value,
                        ">=" => *x >= value,
                        "<=" => *x <= value,
                        "==" => (*x - value).abs() <= FLOAT_TOLERANCE,
                        "!=" => (*x - value).abs() > FLOAT_TOLERANCE,
                        _ => false,
                    },
                })
                .collect()
        } else {
            return Err(anyhow!(
                "Unsupported features expression term '{}'; expected 'flag' or 'feature OP value'",
                term
            ));
        };

        if mask.len() != len {
            return Err(anyhow!(
                "Features expression term '{}' produced mask of length {}, expected {}",
                term,
                mask.len(),
                len
            ));
        }
        for (idx, flag) in mask.into_iter().enumerate() {
            entry[idx] = entry[idx] && flag;
        }
    }
    Ok(entry)
}
