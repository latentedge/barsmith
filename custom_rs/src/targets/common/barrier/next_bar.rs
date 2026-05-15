use barsmith_rs::Direction;

use super::{
    SMALL_DIVISOR,
    tick::{TickRoundMode, quantize_distance_to_tick},
};

pub(crate) fn compute_next_bar_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    wicks_diff_sma14: &[f64],
    sl_multiplier: f64,
    tick_size: Option<f64>,
    direction: Direction,
) -> (
    Vec<bool>,
    Vec<bool>,
    Vec<f64>,
    Vec<f64>,
    Vec<Option<usize>>,
    Vec<Option<usize>>,
) {
    let len = open
        .len()
        .min(high.len())
        .min(low.len())
        .min(close.len())
        .min(wicks_diff_sma14.len());
    let mut long = vec![false; len];
    let mut short = vec![false; len];
    let mut long_rr = vec![f64::NAN; len];
    let mut short_rr = vec![f64::NAN; len];
    let mut exit_i_long = vec![None; len];
    let mut exit_i_short = vec![None; len];
    if len < 2 {
        return (long, short, long_rr, short_rr, exit_i_long, exit_i_short);
    }

    let want_long = matches!(direction, Direction::Long | Direction::Both);
    let want_short = matches!(direction, Direction::Short | Direction::Both);

    for idx in 0..(len - 1) {
        let next = idx + 1;
        let entry = open[next];
        let high_next = high[next];
        let low_next = low[next];
        let close_next = close[next];
        let wick = wicks_diff_sma14[idx];
        if !entry.is_finite()
            || !high_next.is_finite()
            || !low_next.is_finite()
            || !close_next.is_finite()
            || !wick.is_finite()
        {
            continue;
        }
        let sl_distance_raw = (wick * sl_multiplier).abs();
        let sl_distance = if let Some(ts) = tick_size {
            quantize_distance_to_tick(sl_distance_raw, ts, TickRoundMode::Ceil)
        } else {
            sl_distance_raw
        };
        if sl_distance <= SMALL_DIVISOR {
            continue;
        }

        if want_long {
            let long_sl = entry - sl_distance;
            let long_sl_hit = low_next <= long_sl;
            long[idx] = close_next > entry && !long_sl_hit;
            let long_exit = if long_sl_hit { long_sl } else { close_next };
            long_rr[idx] = (long_exit - entry) / sl_distance;
            exit_i_long[idx] = Some(next);
        }

        if want_short {
            let short_sl = entry + sl_distance;
            let short_sl_hit = high_next >= short_sl;
            short[idx] = close_next < entry && !short_sl_hit;
            let short_exit = if short_sl_hit { short_sl } else { close_next };
            short_rr[idx] = (entry - short_exit) / sl_distance;
            exit_i_short[idx] = Some(next);
        }
    }

    (long, short, long_rr, short_rr, exit_i_long, exit_i_short)
}

#[cfg(test)]
pub(crate) fn compute_wicks_kf_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    kf_wicks_smooth: &[f64],
    sl_multiplier: f64,
    tick_size: Option<f64>,
    direction: Direction,
) -> (
    Vec<bool>,
    Vec<bool>,
    Vec<f64>,
    Vec<f64>,
    Vec<Option<usize>>,
    Vec<Option<usize>>,
) {
    let len = open
        .len()
        .min(high.len())
        .min(low.len())
        .min(close.len())
        .min(kf_wicks_smooth.len());
    let mut long = vec![false; len];
    let mut short = vec![false; len];
    let mut long_rr = vec![f64::NAN; len];
    let mut short_rr = vec![f64::NAN; len];
    let mut exit_i_long = vec![None; len];
    let mut exit_i_short = vec![None; len];
    if len < 2 {
        return (long, short, long_rr, short_rr, exit_i_long, exit_i_short);
    }

    let want_long = matches!(direction, Direction::Long | Direction::Both);
    let want_short = matches!(direction, Direction::Short | Direction::Both);

    for idx in 0..(len - 1) {
        let next = idx + 1;
        let entry = open[next];
        let high_next = high[next];
        let low_next = low[next];
        let close_next = close[next];
        let wick = kf_wicks_smooth[idx];
        if !entry.is_finite()
            || !high_next.is_finite()
            || !low_next.is_finite()
            || !close_next.is_finite()
            || !wick.is_finite()
        {
            continue;
        }

        let sl_distance_raw = (wick * sl_multiplier).abs();
        let sl_distance = if let Some(ts) = tick_size {
            quantize_distance_to_tick(sl_distance_raw, ts, TickRoundMode::Ceil)
        } else {
            sl_distance_raw
        };
        if sl_distance <= SMALL_DIVISOR {
            continue;
        }

        if want_long {
            let long_sl = entry - sl_distance;
            let long_sl_hit = low_next <= long_sl;
            long[idx] = close_next > entry && !long_sl_hit;
            let long_exit = if long_sl_hit { long_sl } else { close_next };
            long_rr[idx] = (long_exit - entry) / sl_distance;
            exit_i_long[idx] = Some(next);
        }

        if want_short {
            let short_sl = entry + sl_distance;
            let short_sl_hit = high_next >= short_sl;
            short[idx] = close_next < entry && !short_sl_hit;
            let short_exit = if short_sl_hit { short_sl } else { close_next };
            short_rr[idx] = (entry - short_exit) / sl_distance;
            exit_i_short[idx] = Some(next);
        }
    }

    (long, short, long_rr, short_rr, exit_i_long, exit_i_short)
}
