use barsmith_rs::Direction;

use super::{
    SMALL_DIVISOR,
    resolution::{TargetResolution, TargetTuple},
    tick::{TickRoundMode, quantize_price_to_tick},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HighlowOrAtrStopMode {
    /// Wider (higher-risk) stop:
    /// - long: min(low, entry - 1x ATR)
    /// - short: max(high, entry + 1x ATR)
    Wide,
    /// High/low only stop:
    /// - long: low
    /// - short: high
    HighlowOnly,
    /// ATR only stop:
    /// - long: entry - 1x ATR
    /// - short: entry + 1x ATR
    AtrOnly,
    /// Tighter (lower-risk) stop:
    /// - long: highest stop < entry from {low, entry - 1x ATR}
    /// - short: lowest stop > entry from {high, entry + 1x ATR}
    Tightest,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum HighlowOrAtrTpMode {
    AtrMultiple(f64),
    RiskMultiple(f64),
}

fn highlow_or_atr_stop_long(entry: f64, low: f64, atr: f64, mode: HighlowOrAtrStopMode) -> f64 {
    match mode {
        HighlowOrAtrStopMode::Wide => low.min(entry - atr),
        HighlowOrAtrStopMode::HighlowOnly => low,
        HighlowOrAtrStopMode::AtrOnly => entry - atr,
        HighlowOrAtrStopMode::Tightest => {
            let mut stop_raw = f64::NAN;
            if low < entry {
                stop_raw = low;
            }
            let atr_stop = entry - atr;
            if atr_stop < entry && (!stop_raw.is_finite() || atr_stop > stop_raw) {
                stop_raw = atr_stop;
            }
            stop_raw
        }
    }
}

fn highlow_or_atr_stop_short(entry: f64, high: f64, atr: f64, mode: HighlowOrAtrStopMode) -> f64 {
    match mode {
        HighlowOrAtrStopMode::Wide => high.max(entry + atr),
        HighlowOrAtrStopMode::HighlowOnly => high,
        HighlowOrAtrStopMode::AtrOnly => entry + atr,
        HighlowOrAtrStopMode::Tightest => {
            let mut stop_raw = f64::NAN;
            if high > entry {
                stop_raw = high;
            }
            let atr_stop = entry + atr;
            if atr_stop > entry && (!stop_raw.is_finite() || atr_stop < stop_raw) {
                stop_raw = atr_stop;
            }
            stop_raw
        }
    }
}

fn compute_highlow_or_atr_targets_and_rr_with_stop_mode(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
    stop_mode: HighlowOrAtrStopMode,
    tp_mode: HighlowOrAtrTpMode,
    min_tp_rr: Option<f64>,
    include_risk: bool,
) -> TargetResolution {
    let requires_atr = matches!(
        stop_mode,
        HighlowOrAtrStopMode::Wide | HighlowOrAtrStopMode::AtrOnly | HighlowOrAtrStopMode::Tightest
    ) || matches!(tp_mode, HighlowOrAtrTpMode::AtrMultiple(_));

    let mut len = open.len().min(high.len()).min(low.len()).min(close.len());
    if requires_atr {
        len = len.min(atr.len());
    }
    let mut out = TargetResolution::new(len, include_risk);
    if len < 2 {
        return out;
    }

    let cutoff_horizon = resolve_end_idx.unwrap_or(len - 1).min(len - 1);

    let want_long = matches!(direction, Direction::Long | Direction::Both);
    let want_short = matches!(direction, Direction::Short | Direction::Both);

    for idx in 0..(len - 1) {
        let cap_to_cutoff = resolve_end_idx.is_some() && idx <= cutoff_horizon;
        let local_horizon = if cap_to_cutoff {
            cutoff_horizon
        } else {
            len - 1
        };
        if idx >= local_horizon {
            // Do not open trades that have no future bars available for TP/SL resolution.
            continue;
        }

        let open_idx = open[idx];
        let close_idx = close[idx];
        let high_idx = high[idx];
        let low_idx = low[idx];
        let atr_idx = if requires_atr { atr[idx] } else { f64::NAN };

        if !open_idx.is_finite()
            || !close_idx.is_finite()
            || !high_idx.is_finite()
            || !low_idx.is_finite()
            || (requires_atr && !atr_idx.is_finite())
        {
            continue;
        }

        let body = close_idx - open_idx;
        if body.abs() <= f64::EPSILON {
            continue;
        }

        // Entry at signal bar close.
        let entry = close_idx;

        if body > 0.0 {
            if !want_long {
                continue;
            }
            let stop_raw = highlow_or_atr_stop_long(entry, low_idx, atr_idx, stop_mode);
            let stop = if let Some(ts) = tick_size {
                quantize_price_to_tick(stop_raw, ts, TickRoundMode::Floor)
            } else {
                stop_raw
            };

            if !stop.is_finite() || stop >= entry {
                continue;
            }
            let risk = entry - stop;
            if risk <= SMALL_DIVISOR {
                continue;
            }

            let tp_raw = match tp_mode {
                HighlowOrAtrTpMode::AtrMultiple(m) => entry + m * atr_idx,
                HighlowOrAtrTpMode::RiskMultiple(m) => entry + m * risk,
            };
            let tp = if let Some(ts) = tick_size {
                quantize_price_to_tick(tp_raw, ts, TickRoundMode::Ceil)
            } else {
                tp_raw
            };
            if !tp.is_finite() {
                continue;
            }
            if let Some(min_rr) = min_tp_rr {
                let rr_at_tp = (tp - entry) / risk;
                if !rr_at_tp.is_finite() || rr_at_tp <= min_rr {
                    continue;
                }
            }

            let mut rr = f64::NAN;
            let mut hit_tp = false;
            let mut exit_idx: Option<usize> = None;
            for j in (idx + 1)..=local_horizon {
                let o = open[j];
                let h = high[j];
                let l = low[j];
                if !h.is_finite() || !l.is_finite() {
                    continue;
                }

                // Gap-aware fills: if the next bar opens beyond our stop/TP,
                // assume the fill happens at the open price (RR can be < -1 or > 2).
                if o.is_finite() {
                    if o <= stop {
                        rr = (o - entry) / risk;
                        hit_tp = false;
                        exit_idx = Some(j);
                        break;
                    }
                    if o >= tp {
                        rr = (o - entry) / risk;
                        hit_tp = true;
                        exit_idx = Some(j);
                        break;
                    }
                }

                // Conservative ordering: SL dominates if both touched.
                if l <= stop {
                    rr = -1.0;
                    hit_tp = false;
                    exit_idx = Some(j);
                    break;
                }
                if h >= tp {
                    rr = (tp - entry) / risk;
                    hit_tp = true;
                    exit_idx = Some(j);
                    break;
                }
            }

            if !rr.is_finite() && cap_to_cutoff {
                let exit = close[local_horizon];
                if exit.is_finite() {
                    rr = (exit - entry) / risk;
                    hit_tp = false;
                    exit_idx = Some(local_horizon);
                }
            }

            if rr.is_finite() {
                out.rr_long[idx] = rr;
                out.long[idx] = hit_tp;
                out.exit_i_long[idx] = exit_idx;
                if let Some(risk_long) = out.risk_long.as_mut() {
                    risk_long[idx] = risk;
                }
            }
        } else {
            if !want_short {
                continue;
            }
            let stop_raw = highlow_or_atr_stop_short(entry, high_idx, atr_idx, stop_mode);
            let stop = if let Some(ts) = tick_size {
                quantize_price_to_tick(stop_raw, ts, TickRoundMode::Ceil)
            } else {
                stop_raw
            };

            if !stop.is_finite() || stop <= entry {
                continue;
            }
            let risk = stop - entry;
            if risk <= SMALL_DIVISOR {
                continue;
            }

            let tp_raw = match tp_mode {
                HighlowOrAtrTpMode::AtrMultiple(m) => entry - m * atr_idx,
                HighlowOrAtrTpMode::RiskMultiple(m) => entry - m * risk,
            };
            let tp = if let Some(ts) = tick_size {
                quantize_price_to_tick(tp_raw, ts, TickRoundMode::Floor)
            } else {
                tp_raw
            };
            if !tp.is_finite() {
                continue;
            }
            if let Some(min_rr) = min_tp_rr {
                let rr_at_tp = (entry - tp) / risk;
                if !rr_at_tp.is_finite() || rr_at_tp <= min_rr {
                    continue;
                }
            }

            let mut rr = f64::NAN;
            let mut hit_tp = false;
            let mut exit_idx: Option<usize> = None;
            for j in (idx + 1)..=local_horizon {
                let o = open[j];
                let h = high[j];
                let l = low[j];
                if !h.is_finite() || !l.is_finite() {
                    continue;
                }

                // Gap-aware fills: if the next bar opens beyond our stop/TP,
                // assume the fill happens at the open price (RR can be < -1 or > 2).
                if o.is_finite() {
                    if o >= stop {
                        rr = (entry - o) / risk;
                        hit_tp = false;
                        exit_idx = Some(j);
                        break;
                    }
                    if o <= tp {
                        rr = (entry - o) / risk;
                        hit_tp = true;
                        exit_idx = Some(j);
                        break;
                    }
                }

                if h >= stop {
                    rr = -1.0;
                    hit_tp = false;
                    exit_idx = Some(j);
                    break;
                }
                if l <= tp {
                    rr = (entry - tp) / risk;
                    hit_tp = true;
                    exit_idx = Some(j);
                    break;
                }
            }

            if !rr.is_finite() && cap_to_cutoff {
                let exit = close[local_horizon];
                if exit.is_finite() {
                    rr = (entry - exit) / risk;
                    hit_tp = false;
                    exit_idx = Some(local_horizon);
                }
            }

            if rr.is_finite() {
                out.rr_short[idx] = rr;
                out.short[idx] = hit_tp;
                out.exit_i_short[idx] = exit_idx;
                if let Some(risk_short) = out.risk_short.as_mut() {
                    risk_short[idx] = risk;
                }
            }
        }
    }

    out
}

pub(crate) fn compute_highlow_or_atr_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::Wide,
        HighlowOrAtrTpMode::AtrMultiple(2.0),
        None,
        false,
    )
    .into_targets_and_rr()
}

pub(crate) fn compute_highlow_1r_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        &[],
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::HighlowOnly,
        HighlowOrAtrTpMode::RiskMultiple(1.0),
        None,
        false,
    )
    .into_targets_and_rr()
}

pub(crate) fn compute_2x_atr_tp_atr_stop_target_resolution(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetResolution {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::AtrOnly,
        HighlowOrAtrTpMode::AtrMultiple(2.0),
        None,
        true,
    )
}

#[cfg(any(test, feature = "bench-api"))]
pub(crate) fn compute_2x_atr_tp_atr_stop_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_2x_atr_tp_atr_stop_target_resolution(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
    )
    .into_targets_and_rr()
}

pub(crate) fn compute_3x_atr_tp_atr_stop_target_resolution(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetResolution {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::AtrOnly,
        HighlowOrAtrTpMode::AtrMultiple(3.0),
        None,
        true,
    )
}

#[cfg(test)]
pub(crate) fn compute_3x_atr_tp_atr_stop_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_3x_atr_tp_atr_stop_target_resolution(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
    )
    .into_targets_and_rr()
}

pub(crate) fn compute_atr_tp_atr_stop_target_resolution(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetResolution {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::AtrOnly,
        HighlowOrAtrTpMode::AtrMultiple(1.0),
        None,
        true,
    )
}

#[cfg(test)]
pub(crate) fn compute_atr_tp_atr_stop_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_atr_tp_atr_stop_target_resolution(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
    )
    .into_targets_and_rr()
}

pub(crate) fn compute_highlow_sl_2x_atr_tp_rr_gt_1_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::HighlowOnly,
        HighlowOrAtrTpMode::AtrMultiple(2.0),
        Some(1.0),
        false,
    )
    .into_targets_and_rr()
}

pub(crate) fn compute_highlow_sl_1x_atr_tp_rr_gt_1_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::HighlowOnly,
        HighlowOrAtrTpMode::AtrMultiple(1.0),
        Some(1.0),
        false,
    )
    .into_targets_and_rr()
}

pub(crate) fn compute_highlow_or_atr_tightest_stop_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
) -> TargetTuple {
    compute_highlow_or_atr_targets_and_rr_with_stop_mode(
        open,
        high,
        low,
        close,
        atr,
        tick_size,
        resolve_end_idx,
        direction,
        HighlowOrAtrStopMode::Tightest,
        HighlowOrAtrTpMode::AtrMultiple(2.0),
        None,
        false,
    )
    .into_targets_and_rr()
}

#[cfg(test)]
mod tests;
