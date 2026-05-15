use crate::NON_ZERO_RANGE_EPS;
use crate::math::{diff, percentile_rank};
use crate::moving_average::{ema, rma, sma};
use crate::rolling::rolling_std;

pub fn momentum(values: &[f64], period: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(i, &current)| {
            if i < period {
                0.0
            } else {
                current - values[i - period]
            }
        })
        .collect()
}

pub fn roc(values: &[f64], period: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(i, &current)| {
            if i < period || values[i - period].abs() < f64::EPSILON {
                f64::NAN
            } else {
                current / values[i - period] - 1.0
            }
        })
        .collect()
}

pub fn derivative(values: &[f64], lag: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(i, &val)| if i < lag { 0.0 } else { val - values[i - lag] })
        .collect()
}

pub fn rsi(close: &[f64], period: usize, start: usize) -> Vec<f64> {
    let len = close.len();
    let mut full = vec![f64::NAN; len];
    if start >= len {
        return full;
    }
    let partial = rsi_core(&close[start..], period);
    for (idx, value) in partial.into_iter().enumerate() {
        if start + idx < len {
            full[start + idx] = value;
        }
    }
    full
}

pub fn rsi_core(close: &[f64], period: usize) -> Vec<f64> {
    let len = close.len();
    let mut gains = vec![f64::NAN; len];
    let mut losses = vec![f64::NAN; len];
    for i in 1..len {
        let change = close[i] - close[i - 1];
        gains[i] = change.max(0.0);
        losses[i] = (-change).max(0.0);
    }
    let avg_gain = rma(&gains, period);
    let avg_loss = rma(&losses, period);
    avg_gain
        .iter()
        .zip(avg_loss.iter())
        .map(|(gain, loss)| {
            if *loss == 0.0 {
                100.0
            } else {
                100.0 - (100.0 / (1.0 + gain / loss))
            }
        })
        .collect()
}

pub fn macd(close: &[f64], start: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = close.len();
    let mut macd_full = vec![f64::NAN; len];
    let mut signal_full = vec![f64::NAN; len];
    let mut hist_full = vec![f64::NAN; len];
    if start >= len {
        return (macd_full, signal_full, hist_full);
    }
    let (macd_slice, signal_slice, hist_slice) = macd_core(&close[start..]);
    for (idx, value) in macd_slice.into_iter().enumerate() {
        if start + idx < macd_full.len() {
            macd_full[start + idx] = value;
        }
    }
    for (idx, value) in signal_slice.into_iter().enumerate() {
        if start + idx < signal_full.len() {
            signal_full[start + idx] = value;
        }
    }
    for (idx, value) in hist_slice.into_iter().enumerate() {
        if start + idx < hist_full.len() {
            hist_full[start + idx] = value;
        }
    }
    (macd_full, signal_full, hist_full)
}

pub fn macd_core(close: &[f64]) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    const FAST: usize = 12;
    const SLOW: usize = 26;
    const SIGNAL: usize = 9;

    let ema_fast = ema(close, FAST);
    let ema_slow = ema(close, SLOW);
    let mut macd_line = diff(&ema_fast, &ema_slow);

    for value in macd_line.iter_mut().take(SLOW - 1) {
        *value = f64::NAN;
    }

    let mut signal = vec![f64::NAN; macd_line.len()];
    if let Some(first_valid) = macd_line.iter().position(|v| v.is_finite()) {
        let signal_slice = &macd_line[first_valid..];
        let ema_values = ema(signal_slice, SIGNAL);
        for (offset, value) in ema_values.into_iter().enumerate() {
            if first_valid + offset < signal.len() {
                signal[first_valid + offset] = value;
            }
        }
    }

    let hist = macd_line
        .iter()
        .zip(signal.iter())
        .map(|(line, sig)| {
            if line.is_finite() && sig.is_finite() {
                line - sig
            } else {
                f64::NAN
            }
        })
        .collect();

    (macd_line, signal, hist)
}

pub fn stochastic(
    close: &[f64],
    high: &[f64],
    low: &[f64],
    period: usize,
    signal: usize,
    start: usize,
) -> (Vec<f64>, Vec<f64>) {
    let len = close.len();
    let mut k_full = vec![f64::NAN; len];
    let mut d_full = vec![f64::NAN; len];
    if start >= len {
        return (k_full, d_full);
    }
    let (k_slice, d_slice) = stochastic_core(
        &close[start..],
        &high[start..],
        &low[start..],
        period,
        signal,
    );
    for (idx, value) in k_slice.into_iter().enumerate() {
        if start + idx < len {
            k_full[start + idx] = value;
        }
    }
    for (idx, value) in d_slice.into_iter().enumerate() {
        if start + idx < len {
            d_full[start + idx] = value;
        }
    }
    (k_full, d_full)
}

pub fn stochastic_core(
    close: &[f64],
    high: &[f64],
    low: &[f64],
    period: usize,
    signal: usize,
) -> (Vec<f64>, Vec<f64>) {
    let len = close.len();
    let mut highest = vec![f64::NAN; len];
    let mut lowest = vec![f64::NAN; len];
    let mut ranges = vec![f64::NAN; len];
    for i in 0..len {
        if i + 1 < period {
            continue;
        }
        let slice_high = &high[i + 1 - period..=i];
        let slice_low = &low[i + 1 - period..=i];
        let high_val = slice_high.iter().cloned().fold(f64::MIN, f64::max);
        let low_val = slice_low.iter().cloned().fold(f64::MAX, f64::min);
        highest[i] = high_val;
        lowest[i] = low_val;
        ranges[i] = high_val - low_val;
    }
    let needs_eps = ranges
        .iter()
        .any(|value| value.is_finite() && *value == 0.0);
    if needs_eps {
        for value in ranges.iter_mut() {
            if value.is_finite() {
                *value += NON_ZERO_RANGE_EPS;
            }
        }
    }
    let mut raw_k = vec![f64::NAN; len];
    for i in 0..len {
        let range = ranges[i];
        let low_val = lowest[i];
        if !range.is_finite() || !low_val.is_finite() {
            continue;
        }
        let mut denom = range;
        if denom.abs() < NON_ZERO_RANGE_EPS {
            denom += NON_ZERO_RANGE_EPS;
        }
        if denom.abs() < NON_ZERO_RANGE_EPS {
            continue;
        }
        raw_k[i] = ((close[i] - low_val) / denom * 100.0).clamp(0.0, 100.0);
    }
    let mut smooth_k = vec![f64::NAN; len];
    if let Some(first_valid) = raw_k.iter().position(|v| v.is_finite()) {
        let slice = &raw_k[first_valid..];
        let slice_sma = sma(slice, signal);
        for (offset, value) in slice_sma.into_iter().enumerate() {
            if first_valid + offset < len {
                smooth_k[first_valid + offset] = value;
            }
        }
    }
    let mut d = vec![f64::NAN; len];
    if let Some(first_valid) = smooth_k.iter().position(|v| v.is_finite()) {
        let slice = &smooth_k[first_valid..];
        let slice_sma = sma(slice, signal);
        for (offset, value) in slice_sma.into_iter().enumerate() {
            if first_valid + offset < len {
                d[first_valid + offset] = value;
            }
        }
    }
    (smooth_k, d)
}

pub fn bollinger(
    close: &[f64],
    period: usize,
    std_mult: f64,
    start: usize,
) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = close.len();
    let mut mid_full = vec![f64::NAN; len];
    let mut upper_full = vec![f64::NAN; len];
    let mut lower_full = vec![f64::NAN; len];
    let mut std_full = vec![f64::NAN; len];
    if start >= len {
        return (mid_full, upper_full, lower_full, std_full);
    }
    let (mid_slice, upper_slice, lower_slice, std_slice) =
        bollinger_core(&close[start..], period, std_mult);
    for (idx, value) in mid_slice.into_iter().enumerate() {
        if start + idx < len {
            mid_full[start + idx] = value;
        }
    }
    for (idx, value) in upper_slice.into_iter().enumerate() {
        if start + idx < len {
            upper_full[start + idx] = value;
        }
    }
    for (idx, value) in lower_slice.into_iter().enumerate() {
        if start + idx < len {
            lower_full[start + idx] = value;
        }
    }
    for (idx, value) in std_slice.into_iter().enumerate() {
        if start + idx < len {
            std_full[start + idx] = value;
        }
    }
    (mid_full, upper_full, lower_full, std_full)
}

pub fn bollinger_core(
    close: &[f64],
    period: usize,
    std_mult: f64,
) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mid = sma(close, period);
    let std = rolling_std(close, period);
    let upper = mid
        .iter()
        .zip(std.iter())
        .map(|(m, s)| m + s * std_mult)
        .collect();
    let lower = mid
        .iter()
        .zip(std.iter())
        .map(|(m, s)| m - s * std_mult)
        .collect();
    (mid, upper, lower, std)
}

pub fn bollinger_position(close: &[f64], lower: &[f64], upper: &[f64]) -> Vec<f64> {
    close
        .iter()
        .zip(lower.iter().zip(upper.iter()))
        .map(|(c, (l, u))| {
            if (u - l).abs() < f64::EPSILON {
                0.5
            } else {
                (c - l) / (u - l)
            }
        })
        .collect()
}

pub fn extension(high: &[f64], low: &[f64], period: usize) -> Vec<f64> {
    high.iter()
        .enumerate()
        .map(|(i, &h)| {
            if i + 1 < period {
                0.0
            } else {
                let start = i + 1 - period;
                h - low[start..=i].iter().cloned().fold(f64::MAX, f64::min)
            }
        })
        .collect()
}

pub fn momentum_score(rsi14: &[f64], roc5: &[f64], roc10: &[f64]) -> Vec<f64> {
    let rsi_rank = percentile_rank(rsi14);
    let roc5_rank = percentile_rank(roc5);
    let roc10_rank = percentile_rank(roc10);

    rsi_rank
        .iter()
        .zip(roc5_rank.iter().zip(roc10_rank.iter()))
        .map(|(rsi, (r5, r10))| {
            if !rsi.is_finite() || !r5.is_finite() || !r10.is_finite() {
                f64::NAN
            } else {
                (rsi + r5 + r10) / 3.0
            }
        })
        .collect()
}
