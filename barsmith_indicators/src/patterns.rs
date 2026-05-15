use crate::BODY_SIZE_EPS;

pub fn upper_wick(open: &[f64], close: &[f64], high: &[f64]) -> Vec<f64> {
    high.iter()
        .zip(open.iter().zip(close.iter()))
        .map(|(h, (o, c))| h - o.max(*c))
        .collect()
}

pub fn lower_wick(open: &[f64], close: &[f64], low: &[f64]) -> Vec<f64> {
    open.iter()
        .zip(close.iter().zip(low.iter()))
        .map(|(o, (c, l))| o.min(*c) - l)
        .collect()
}

pub fn ema_alignment(close: &[f64], ema9: &[f64], ema20: &[f64], ema50: &[f64]) -> Vec<bool> {
    close
        .iter()
        .zip(ema9.iter().zip(ema20.iter().zip(ema50.iter())))
        .map(|(c, (e9, (e20, e50)))| c > e9 && e9 > e20 && e20 > e50)
        .collect()
}

pub fn ribbon_alignment(
    ema9: &[f64],
    ema20: &[f64],
    ema50: &[f64],
    sma200: &[f64],
    bullish: bool,
) -> Vec<bool> {
    ema9.iter()
        .zip(ema20.iter().zip(ema50.iter().zip(sma200.iter())))
        .map(|(e9, (e20, (e50, s200)))| {
            if !e9.is_finite() || !e20.is_finite() || !e50.is_finite() || !s200.is_finite() {
                false
            } else if bullish {
                e9 > e20 && e20 > e50 && e50 > s200
            } else {
                e9 < e20 && e20 < e50 && e50 < s200
            }
        })
        .collect()
}

pub fn hammer(body: &[f64], upper: &[f64], lower: &[f64]) -> Vec<bool> {
    body.iter()
        .zip(upper.iter().zip(lower.iter()))
        .map(|(b, (u, l))| b.abs() < *l * 0.5 && *l > *u * 2.0)
        .collect()
}

pub fn shooting_star(body: &[f64], upper: &[f64], lower: &[f64]) -> Vec<bool> {
    body.iter()
        .zip(upper.iter().zip(lower.iter()))
        .map(|(b, (u, l))| b.abs() < *u * 0.5 && *u > *l * 2.0)
        .collect()
}

pub fn engulfing(is_green: &[bool], is_red: &[bool], bodies: &[f64], bullish: bool) -> Vec<bool> {
    bodies
        .iter()
        .enumerate()
        .map(|(i, &body)| {
            if i == 0 {
                return false;
            }
            let prev_body = bodies[i - 1].abs();
            if bullish {
                is_green[i] && is_red[i - 1] && body.abs() > prev_body
            } else {
                is_red[i] && is_green[i - 1] && body.abs() > prev_body
            }
        })
        .collect()
}

pub fn large_colored_body(condition: &[bool], bodies: &[f64], atr: &[f64], mult: f64) -> Vec<bool> {
    bodies
        .iter()
        .zip(condition.iter().zip(atr.iter()))
        .map(|(body, (cond, atr_val))| *cond && body.abs() > atr_val * mult)
        .collect()
}

pub fn large_body_ratio(values: &[f64], baseline: &[f64], mult: f64) -> Vec<bool> {
    values
        .iter()
        .zip(baseline.iter())
        .map(|(value, mean)| {
            if !value.is_finite() || !mean.is_finite() {
                return false;
            }
            if *value <= BODY_SIZE_EPS {
                return false;
            }
            *value > *mean * mult
        })
        .collect()
}
