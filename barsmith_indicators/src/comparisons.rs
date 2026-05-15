use crate::math::round_to_decimals;
use crate::oscillators::derivative;
use crate::{Comparison, FLOAT_TOLERANCE};

pub fn stoch_threshold(values: &[f64], target: f64, comparison: Comparison) -> Vec<bool> {
    values
        .iter()
        .map(|value| {
            if !value.is_finite() {
                return false;
            }
            let rounded = round_to_decimals(*value, 10);
            match comparison {
                Comparison::Greater => rounded > target,
                Comparison::Less => rounded < target,
            }
        })
        .collect()
}

pub fn threshold(values: &[f64], target: f64, comparison: Comparison) -> Vec<bool> {
    values
        .iter()
        .map(|v| match comparison {
            Comparison::Greater => *v > target,
            Comparison::Less => *v < target,
        })
        .collect()
}

pub fn threshold_compare(
    values: &[f64],
    baseline: &[f64],
    mult: f64,
    comparison: Comparison,
) -> Vec<bool> {
    values
        .iter()
        .zip(baseline.iter())
        .map(|(v, b)| match comparison {
            Comparison::Greater => *v > b * mult,
            Comparison::Less => *v < b * mult,
        })
        .collect()
}

pub fn zscore_compare(values: &[f64], mean: &[f64], std: &[f64], threshold: f64) -> Vec<bool> {
    values
        .iter()
        .zip(mean.iter().zip(std.iter()))
        .map(|(value, (m, s))| {
            if *s == 0.0 {
                false
            } else {
                ((*value - *m) / *s).abs() > threshold
            }
        })
        .collect()
}

pub fn derivative_threshold(values: &[f64], target: f64, comparison: Comparison) -> Vec<bool> {
    derivative(values, 1)
        .iter()
        .map(|v| match comparison {
            Comparison::Greater => *v > target,
            Comparison::Less => *v < target,
        })
        .collect()
}

pub fn recovery(values: &[f64], level: f64, comparison: Comparison) -> Vec<bool> {
    values
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            if i == 0 {
                false
            } else {
                let prev = values[i - 1];
                match comparison {
                    Comparison::Greater => prev < level && v > level,
                    Comparison::Less => prev > level && v < level,
                }
            }
        })
        .collect()
}

pub fn comparison(values: &[f64], lag: usize, cmp: Comparison) -> Vec<bool> {
    values
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            if i < lag {
                false
            } else {
                match cmp {
                    Comparison::Greater => v > values[i - lag],
                    Comparison::Less => v < values[i - lag],
                }
            }
        })
        .collect()
}

pub fn double_rising(values: &[f64]) -> Vec<bool> {
    values
        .iter()
        .enumerate()
        .map(|(i, &current)| {
            if i < 2 {
                return false;
            }
            let prev1 = values[i - 1];
            let prev2 = values[i - 2];
            if !current.is_finite() || !prev1.is_finite() || !prev2.is_finite() {
                false
            } else {
                current > prev1 && prev1 > prev2
            }
        })
        .collect()
}

pub fn momentum_acceleration(values: &[f64], direction: Comparison) -> Vec<bool> {
    values
        .iter()
        .enumerate()
        .map(|(i, &current)| {
            if i == 0 {
                return false;
            }
            let prev = values[i - 1];
            if !current.is_finite() || !prev.is_finite() {
                false
            } else {
                match direction {
                    Comparison::Greater => current > 0.0 && current > prev,
                    Comparison::Less => current < 0.0 && current < prev,
                }
            }
        })
        .collect()
}

pub fn comparison_series(values: &[f64], lag: usize, cmp: Comparison) -> Vec<bool> {
    comparison(values, lag, cmp)
}

pub fn compare_series(left: &[f64], right: &[f64], comparison: Comparison) -> Vec<bool> {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| {
            if !l.is_finite() || !r.is_finite() {
                false
            } else {
                match comparison {
                    Comparison::Greater => *l > *r + FLOAT_TOLERANCE,
                    Comparison::Less => *l < *r - FLOAT_TOLERANCE,
                }
            }
        })
        .collect()
}

pub fn cross(fast: &[f64], slow: &[f64], upward: bool) -> Vec<bool> {
    fast.iter()
        .enumerate()
        .map(|(i, &value)| {
            if i == 0 || !value.is_finite() {
                return false;
            }
            let prev_fast = fast[i - 1];
            let prev_slow = slow[i - 1];
            let current_slow = slow[i];
            if !prev_fast.is_finite() || !prev_slow.is_finite() || !current_slow.is_finite() {
                return false;
            }
            if upward {
                prev_fast <= prev_slow && value > current_slow
            } else {
                prev_fast >= prev_slow && value < current_slow
            }
        })
        .collect()
}

pub fn stoch_cross(
    _rounded_fast: &[f64],
    _rounded_slow: &[f64],
    raw_fast: &[f64],
    raw_slow: &[f64],
    upward: bool,
) -> Vec<bool> {
    raw_fast
        .iter()
        .enumerate()
        .map(|(i, &current_fast)| {
            if i == 0 || !current_fast.is_finite() {
                return false;
            }
            let current_slow = raw_slow[i];
            if !current_slow.is_finite() {
                return false;
            }
            let prev_fast = raw_fast[i - 1];
            let prev_slow = raw_slow[i - 1];
            if !prev_fast.is_finite() || !prev_slow.is_finite() {
                return false;
            }
            if upward {
                (current_fast > current_slow) && (prev_fast <= prev_slow)
            } else {
                (current_fast < current_slow) && (prev_fast >= prev_slow)
            }
        })
        .collect()
}

pub fn dual_condition(first: &[bool], second: &[bool]) -> Vec<bool> {
    first
        .iter()
        .zip(second.iter())
        .map(|(a, b)| *a && *b)
        .collect()
}
