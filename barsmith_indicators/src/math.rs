use std::cmp::Ordering;

pub fn round_to_decimals(value: f64, decimals: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let factor = 10f64.powi(decimals as i32);
    (value * factor).round() / factor
}

pub fn diff(a: &[f64], b: &[f64]) -> Vec<f64> {
    a.iter().zip(b.iter()).map(|(x, y)| x - y).collect()
}

pub fn elementwise_max(a: &[f64], b: &[f64]) -> Vec<f64> {
    a.iter().zip(b.iter()).map(|(x, y)| x.max(*y)).collect()
}

pub fn vector_abs(values: &[f64]) -> Vec<f64> {
    values.iter().map(|v| v.abs()).collect()
}

pub fn add_scalar(values: &[f64], scalar: f64) -> Vec<f64> {
    values.iter().map(|v| v + scalar).collect()
}

pub fn range(high: &[f64], low: &[f64]) -> Vec<f64> {
    high.iter().zip(low.iter()).map(|(h, l)| h - l).collect()
}

pub fn ratio(num: &[f64], denom: &[f64]) -> Vec<f64> {
    num.iter()
        .zip(denom.iter())
        .map(|(n, d)| if d.abs() < f64::EPSILON { 0.0 } else { n / d })
        .collect()
}

pub fn ratio_with_eps(num: &[f64], denom: &[f64], eps: f64) -> Vec<f64> {
    num.iter()
        .zip(denom.iter())
        .map(|(n, d)| if d.abs() < eps { 0.0 } else { n / d })
        .collect()
}

pub fn deviation(values: &[f64], reference: &[f64]) -> Vec<f64> {
    values
        .iter()
        .zip(reference.iter())
        .map(|(v, r)| {
            if r.abs() < f64::EPSILON {
                0.0
            } else {
                (v - r) / r
            }
        })
        .collect()
}

pub fn percentile_rank(values: &[f64]) -> Vec<f64> {
    let mut finite = values
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, value)| value.is_finite())
        .collect::<Vec<_>>();
    let count = finite.len();
    if count == 0 {
        return vec![f64::NAN; values.len()];
    }
    finite.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    let mut ranks = vec![f64::NAN; values.len()];
    let denom = count as f64;
    let mut i = 0;
    while i < count {
        let mut j = i + 1;
        while j < count && finite[j].1.partial_cmp(&finite[i].1) == Some(Ordering::Equal) {
            j += 1;
        }
        let avg_rank = ((i + j - 1) as f64 / 2.0 + 1.0) / denom;
        for k in i..j {
            ranks[finite[k].0] = avg_rank;
        }
        i = j;
    }
    ranks
}

pub fn quantile(values: &[f64], q: f64) -> Option<f64> {
    let mut finite = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return None;
    }
    finite.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let clamped = q.clamp(0.0, 1.0);
    let pos = clamped * (finite.len() - 1) as f64;
    let lower = pos.floor() as usize;
    let upper = pos.ceil() as usize;
    if lower == upper {
        Some(finite[lower])
    } else {
        Some(finite[lower] + (finite[upper] - finite[lower]) * (pos - lower as f64))
    }
}
