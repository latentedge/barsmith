use crate::moving_average::sma;

pub fn rolling_std(values: &[f64], period: usize) -> Vec<f64> {
    if period == 0 {
        return vec![f64::NAN; values.len()];
    }
    if period == 1 {
        return vec![0.0; values.len()];
    }
    let mean = sma(values, period);
    values
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if i + 1 < period {
                return f64::NAN;
            }
            let start = i + 1 - period;
            let slice = &values[start..=i];
            if slice.iter().any(|v| !v.is_finite()) {
                return f64::NAN;
            }
            let mean_val = mean[i];
            if !mean_val.is_finite() {
                return f64::NAN;
            }
            let variance_sum = slice.iter().map(|x| (x - mean_val).powi(2)).sum::<f64>();
            let denom = (period - 1) as f64;
            (variance_sum / denom).sqrt()
        })
        .collect()
}

pub fn rolling_min(values: &[f64], period: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if i + 1 < period {
                f64::NAN
            } else {
                values[i + 1 - period..=i]
                    .iter()
                    .cloned()
                    .fold(f64::MAX, f64::min)
            }
        })
        .collect()
}

pub fn rolling_coeff_var(values: &[f64], period: usize) -> Vec<f64> {
    let mean = sma(values, period);
    let std = rolling_std(values, period);
    std.iter()
        .zip(mean.iter())
        .map(|(s, m)| if m.abs() < f64::EPSILON { 0.0 } else { s / m })
        .collect()
}

pub fn rolling_bool_sum(values: &[bool], period: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if period == 0 || i + 1 < period {
                f64::NAN
            } else {
                values[i + 1 - period..=i]
                    .iter()
                    .fold(0.0, |acc, flag| acc + if *flag { 1.0 } else { 0.0 })
            }
        })
        .collect()
}

pub fn streak(values: &[bool], period: usize) -> Vec<bool> {
    values
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if i + 1 < period {
                false
            } else {
                values[i + 1 - period..=i].iter().all(|flag| *flag)
            }
        })
        .collect()
}

pub fn shift_bool(values: &[bool], lag: usize) -> Vec<bool> {
    let mut result = vec![false; values.len()];
    if lag < values.len() {
        result[lag..].copy_from_slice(&values[..values.len() - lag]);
    }
    result
}
