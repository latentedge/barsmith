use std::fmt::Write;

use tracing::info;

use crate::config::PositionSizingMode;
use crate::pipeline::format_int;
use crate::storage::ResultRow;

#[derive(Clone, Copy)]
struct TopResultRenderContext {
    dataset_rows: usize,
    position_sizing: PositionSizingMode,
    has_dollar_model: bool,
    dollars_per_r: f64,
    cost_per_trade_r: f64,
    cost_per_trade_dollar: f64,
}

pub(super) fn log_top_results(
    rows: &[ResultRow],
    dataset_rows: usize,
    position_sizing: PositionSizingMode,
    dollars_per_r: Option<f64>,
    cost_per_trade_r: Option<f64>,
    cost_per_trade_dollar: Option<f64>,
) {
    if rows.is_empty() {
        info!("No cumulative permutation results available yet");
        return;
    }

    let mut buffer = String::new();
    let direction = rows
        .first()
        .map(|row| row.direction.to_uppercase())
        .unwrap_or_else(|| "N/A".to_string());
    let _ = writeln!(
        buffer,
        "\nTOP {} COMBINATIONS ({}) - Sorted by EQUITY CALMAR:",
        format_int(rows.len() as u128),
        direction
    );
    let _ = writeln!(
        buffer,
        "======================================================================\n"
    );

    let dollars_per_r = dollars_per_r.unwrap_or(0.0);
    let cost_per_trade_r = cost_per_trade_r.unwrap_or(0.0);
    let context = TopResultRenderContext {
        dataset_rows,
        position_sizing,
        has_dollar_model: dollars_per_r > 0.0,
        dollars_per_r,
        cost_per_trade_r,
        cost_per_trade_dollar: cost_per_trade_dollar.unwrap_or(0.0),
    };

    for (idx, row) in rows.iter().enumerate() {
        write_top_result_row(&mut buffer, idx, row, context);
        if idx + 1 < rows.len() {
            let _ = writeln!(buffer);
        }
    }

    let _ = writeln!(
        buffer,
        "\n======================================================================"
    );
    info!("{}", buffer);
}

fn write_top_result_row(
    buffer: &mut String,
    idx: usize,
    row: &ResultRow,
    context: TopResultRenderContext,
) {
    let mask_hits = if row.mask_hits > 0 {
        row.mask_hits
    } else {
        row.total_bars
    };
    let (matched_bars, dataset_bars, coverage_pct) = if context.dataset_rows > 0 {
        let pct = (mask_hits as f64 / context.dataset_rows as f64) * 100.0;
        (mask_hits, context.dataset_rows, pct)
    } else {
        (mask_hits, 0, 0.0)
    };

    let _ = writeln!(buffer, "Rank {}: {}", idx + 1, row.combination);
    let _ = writeln!(
        buffer,
        "  Offset: {}",
        format_int(row.resume_offset as u128)
    );
    let _ = writeln!(
        buffer,
        "  Bars matching combo mask: {} ({:.2}% of dataset)",
        format_int(matched_bars as u128),
        coverage_pct
    );
    let _ = writeln!(
        buffer,
        "  Trades (eligible & finite RR): {}",
        format_int(row.total_bars as u128)
    );
    let _ = writeln!(
        buffer,
        "  Win Rate: {:.2}% ({}/{} bars)",
        row.win_rate,
        format_int(row.profitable_bars as u128),
        format_int(row.total_bars as u128)
    );
    let _ = writeln!(
        buffer,
        "  Target hit-rate: {:.2}% ({}/{} bars)",
        row.label_hit_rate,
        format_int(row.label_hits as u128),
        format_int(row.total_bars as u128)
    );
    let _ = writeln!(
        buffer,
        "  Expectancy: {:.3}R | Avg win: {:.3}R | Avg loss: {:.3}R",
        row.expectancy, row.avg_winning_rr, row.avg_losing_rr
    );
    let _ = writeln!(
        buffer,
        "  Total R: {:.1}R | Max DD: {:.1}R | Profit factor: {:.3}",
        row.total_return, row.max_drawdown, row.profit_factor
    );
    let _ = writeln!(
        buffer,
        "  R-dist: median {:.3}R | p05 {:.3}R | p95 {:.3}R | avg loss {:.3}R",
        row.median_rr, row.p05_rr, row.p95_rr, row.avg_losing_rr
    );
    write_cost_model(
        buffer,
        context.position_sizing,
        context.has_dollar_model,
        context.dollars_per_r,
        context.cost_per_trade_r,
        context.cost_per_trade_dollar,
    );
    write_equity_metrics(buffer, row);
    let _ = writeln!(buffer, "  Win/Loss: {:.2}", row.win_loss_ratio);
    let _ = writeln!(
        buffer,
        "  Drawdown shape: Pain {:.2} | Ulcer {:.2}",
        row.pain_ratio, row.ulcer_index
    );
    let _ = writeln!(
        buffer,
        "  Recall: {} / {} bars ({:.2}% of dataset)",
        format_int(matched_bars as u128),
        format_int(dataset_bars as u128),
        coverage_pct
    );
    let trades_per_1000 = if dataset_bars > 0 {
        (row.total_bars as f64 * 1000.0) / dataset_bars as f64
    } else {
        0.0
    };
    let _ = writeln!(buffer, "  Density: {:.2} trades/1000 bars", trades_per_1000);
    let _ = writeln!(
        buffer,
        "  Streaks W/L: {}/{} (avg {:.2}/{:.2}) | Largest Win/Loss: {:.2}R / {:.2}R",
        format_int(row.max_consecutive_wins as u128),
        format_int(row.max_consecutive_losses as u128),
        row.avg_win_streak,
        row.avg_loss_streak,
        row.largest_win,
        row.largest_loss
    );
}

fn write_cost_model(
    buffer: &mut String,
    position_sizing: PositionSizingMode,
    has_dollar_model: bool,
    dollars_per_r: f64,
    cost_per_trade_r: f64,
    cost_per_trade_dollar: f64,
) {
    match position_sizing {
        PositionSizingMode::Fractional => {
            if has_dollar_model && cost_per_trade_r > 0.0 {
                let cost_dollar = cost_per_trade_r * dollars_per_r;
                let _ = writeln!(
                    buffer,
                    "  Cost model: {:.3}R/trade (~${:.2})",
                    cost_per_trade_r, cost_dollar
                );
            }
        }
        PositionSizingMode::Contracts => {
            if cost_per_trade_dollar > 0.0 {
                let _ = writeln!(
                    buffer,
                    "  Cost model: ${:.2}/contract round-trip",
                    cost_per_trade_dollar
                );
            }
        }
    }
}

fn write_equity_metrics(buffer: &mut String, row: &ResultRow) {
    if row.final_capital <= 0.0 || row.total_return_pct == 0.0 {
        return;
    }
    let final_capital_str = format_int(row.final_capital.round() as u128);
    let _ = writeln!(
        buffer,
        "  Equity: Final ${} | Total {:.1}% | CAGR {:.2}%",
        final_capital_str, row.total_return_pct, row.cagr_pct
    );
    let _ = writeln!(
        buffer,
        "  Equity DD: Max {:.1}% | Calmar (equity): {:.2}",
        row.max_drawdown_pct_equity, row.calmar_equity
    );
    let _ = writeln!(
        buffer,
        "  Equity Sharpe/Sortino: {:.2} / {:.2}",
        row.sharpe_equity, row.sortino_equity
    );
}

pub(super) fn log_top_formulas(rows: &[ResultRow]) {
    if rows.is_empty() {
        info!("No cumulative permutation results available yet");
        return;
    }

    let mut buffer = String::new();
    let direction = rows
        .first()
        .map(|row| row.direction.to_uppercase())
        .unwrap_or_else(|| "N/A".to_string());
    let _ = writeln!(
        buffer,
        "\nTOP {} FORMULAS ({}) - Sorted by CALMAR RATIO:",
        rows.len(),
        direction
    );
    let _ = writeln!(
        buffer,
        "======================================================================\n"
    );

    for (idx, row) in rows.iter().enumerate() {
        let _ = writeln!(buffer, "Rank {}: {}", idx + 1, row.combination);
    }

    let _ = writeln!(
        buffer,
        "\n======================================================================"
    );
    info!("{}", buffer);
}
