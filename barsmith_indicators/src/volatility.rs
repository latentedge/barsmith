use crate::moving_average::rma;
use crate::rolling::rolling_min;

pub fn atr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; close.len()];
    atr_into(high, low, close, period, &mut out);
    out
}

pub fn atr_into(high: &[f64], low: &[f64], close: &[f64], period: usize, out: &mut [f64]) {
    assert_eq!(
        close.len(),
        out.len(),
        "atr output length must match close length"
    );
    let len = close.len().min(high.len()).min(low.len());
    out.fill(f64::NAN);
    if len == 0 {
        return;
    }
    let mut tr = vec![0.0; len];
    for i in 0..len {
        let high_low = high[i] - low[i];
        let high_close = if i == 0 {
            high_low
        } else {
            (high[i] - close[i - 1]).abs()
        };
        let low_close = if i == 0 {
            high_low
        } else {
            (low[i] - close[i - 1]).abs()
        };
        tr[i] = high_low.max(high_close).max(low_close);
    }
    let values = rma(&tr, period);
    out[..len].copy_from_slice(&values);
}

pub fn atr_close_to_close(close: &[f64], period: usize) -> Vec<f64> {
    let len = close.len();
    let mut result = vec![f64::NAN; len];
    if len == 0 || period == 0 {
        return result;
    }
    let mut ranges = vec![f64::NAN; len];
    for i in 1..len {
        let current = close[i];
        let previous = close[i - 1];
        if current.is_finite() && previous.is_finite() {
            ranges[i] = (current - previous).abs();
        }
    }
    for i in 0..len {
        let start = (i + 1).saturating_sub(period);
        let mut sum = 0.0;
        let mut count = 0usize;
        for value in &ranges[start..=i] {
            if value.is_finite() {
                sum += *value;
                count += 1;
            }
        }
        if count > 0 {
            result[i] = sum / count as f64;
        }
    }
    if len > period {
        let alpha = 2.0 / (period as f64 + 1.0);
        for i in period..len {
            let value = ranges[i];
            let prev = result[i - 1];
            if value.is_finite() && prev.is_finite() {
                result[i] = alpha * value + (1.0 - alpha) * prev;
            }
        }
    }
    result
}

pub fn squeeze(values: &[f64], period: usize, mult: f64) -> Vec<bool> {
    let rolling_minima = rolling_min(values, period);
    values
        .iter()
        .zip(rolling_minima.iter())
        .map(|(v, m)| {
            if !v.is_finite() || !m.is_finite() {
                false
            } else {
                *v < *m * mult
            }
        })
        .collect()
}

pub fn adx(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Vec<f64> {
    let len = close.len().min(high.len()).min(low.len());
    let mut plus_dm = vec![0.0; len];
    let mut minus_dm = vec![0.0; len];
    let mut tr = vec![0.0; len];

    for i in 1..len {
        let up_move = high[i] - high[i - 1];
        let down_move = low[i - 1] - low[i];
        plus_dm[i] = if up_move > down_move && up_move > 0.0 {
            up_move
        } else {
            0.0
        };
        minus_dm[i] = if down_move > up_move && down_move > 0.0 {
            down_move
        } else {
            0.0
        };
        let high_low = high[i] - low[i];
        let high_close = (high[i] - close[i - 1]).abs();
        let low_close = (low[i] - close[i - 1]).abs();
        tr[i] = high_low.max(high_close).max(low_close);
    }

    let atr_values = rma(&tr, period);
    let plus_smoothed = rma(&plus_dm, period);
    let minus_smoothed = rma(&minus_dm, period);
    let plus_di = plus_smoothed
        .iter()
        .zip(atr_values.iter())
        .map(|(p, atr)| {
            if atr.abs() < f64::EPSILON {
                0.0
            } else {
                (p / atr) * 100.0
            }
        })
        .collect::<Vec<_>>();
    let minus_di = minus_smoothed
        .iter()
        .zip(atr_values.iter())
        .map(|(m, atr)| {
            if atr.abs() < f64::EPSILON {
                0.0
            } else {
                (m / atr) * 100.0
            }
        })
        .collect::<Vec<_>>();
    let dx = plus_di
        .iter()
        .zip(minus_di.iter())
        .map(|(p, m)| {
            if (p + m).abs() < f64::EPSILON {
                0.0
            } else {
                ((p - m).abs() / (p + m)) * 100.0
            }
        })
        .collect::<Vec<_>>();
    let dx_clean = dx
        .into_iter()
        .map(|v| if v.is_finite() { v } else { 0.0 })
        .collect::<Vec<_>>();
    rma(&dx_clean, period)
}
