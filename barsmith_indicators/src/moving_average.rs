pub fn ema(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; values.len()];
    ema_into(values, period, &mut out);
    out
}

pub fn ema_into(values: &[f64], period: usize, out: &mut [f64]) {
    assert_eq!(
        values.len(),
        out.len(),
        "ema output length must match input length"
    );
    out.fill(f64::NAN);
    if period == 0 || values.is_empty() {
        return;
    }
    let len = values.len();
    if len < period {
        return;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let seed = values[..period].iter().sum::<f64>() / period as f64;
    out[period - 1] = seed;
    let mut prev = seed;
    for i in period..len {
        let val = values[i];
        prev = alpha * val + (1.0 - alpha) * prev;
        out[i] = prev;
    }
}

pub fn rma(values: &[f64], period: usize) -> Vec<f64> {
    let len = values.len();
    let mut result = vec![f64::NAN; len];
    if period == 0 || len == 0 {
        return result;
    }
    let alpha = 1.0 / period as f64;
    let mut prev: Option<f64> = None;
    for (idx, &value) in values.iter().enumerate() {
        if !value.is_finite() {
            if let Some(prev_val) = prev {
                result[idx] = prev_val;
            }
            continue;
        }
        let next = match prev {
            Some(prev_val) => alpha * value + (1.0 - alpha) * prev_val,
            None => value,
        };
        result[idx] = next;
        prev = Some(next);
    }
    result
}

pub fn sma(values: &[f64], period: usize) -> Vec<f64> {
    let len = values.len();
    let mut result = vec![f64::NAN; len];
    if period == 0 || period > len {
        return result;
    }
    let weight = 1.0 / period as f64;
    let conv_len = len + period - 1;
    let mut conv = vec![0.0; conv_len];
    let mut conv_invalid = vec![false; conv_len];
    for k in 0..period {
        for (j, value) in values.iter().enumerate().take(len) {
            let idx = k + j;
            if !value.is_finite() {
                conv_invalid[idx] = true;
            } else if !conv_invalid[idx] {
                conv[idx] += *value * weight;
            }
        }
    }
    for (flag, entry) in conv_invalid.iter().zip(conv.iter_mut()) {
        if *flag {
            *entry = f64::NAN;
        }
    }
    result[(period - 1)..len].copy_from_slice(&conv[(period - 1)..len]);
    result
}

pub fn sma_strict(values: &[f64], period: usize) -> Vec<f64> {
    sma(values, period)
}
