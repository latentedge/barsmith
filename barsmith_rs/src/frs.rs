use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ForwardRobustnessComponents {
    pub frs: f64,
    pub k: usize,
    pub p: f64,
    pub c: f64,
    pub c_plus: f64,
    pub dd_min: f64,
    pub dd_median: f64,
    pub dd_mean: f64,
    pub dd_max: f64,
    pub t: f64,
    pub tail_penalty: f64,
    pub r_min: f64,
    pub r_median: f64,
    pub r_max: f64,
    pub mu_r: f64,
    pub sigma_r: f64,
    pub stability: f64,
    pub n_med: f64,
    pub n_min: usize,
    pub trade_score: f64,
}

impl ForwardRobustnessComponents {
    fn empty(n_min: usize) -> Self {
        Self {
            frs: 0.0,
            k: 0,
            p: 0.0,
            c: 0.0,
            c_plus: 0.0,
            dd_min: 0.0,
            dd_median: 0.0,
            dd_mean: 0.0,
            dd_max: 0.0,
            t: 0.0,
            tail_penalty: 0.0,
            r_min: 0.0,
            r_median: 0.0,
            r_max: 0.0,
            mu_r: 0.0,
            sigma_r: 0.0,
            stability: 0.0,
            n_med: 0.0,
            n_min,
            trade_score: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FrsOptions {
    pub n_min: usize,
    pub alpha: f64,
    pub beta: f64,
    pub gamma: f64,
    pub delta: f64,
}

impl Default for FrsOptions {
    fn default() -> Self {
        Self {
            n_min: 30,
            alpha: 2.0,
            beta: 2.0,
            gamma: 1.0,
            delta: 1.0,
        }
    }
}

pub fn compute_frs(
    returns_r: &[f64],
    max_drawdowns_r: &[f64],
    trades: &[usize],
    options: FrsOptions,
) -> ForwardRobustnessComponents {
    let k = returns_r.len();
    if k == 0 || max_drawdowns_r.len() != k || trades.len() != k {
        return ForwardRobustnessComponents::empty(options.n_min);
    }

    let eps = 1e-9_f64;
    let returns = returns_r.to_vec();
    let drawdowns = max_drawdowns_r
        .iter()
        .map(|value| value.max(0.0))
        .collect::<Vec<_>>();
    let mut counts = trades.iter().map(|value| *value as f64).collect::<Vec<_>>();

    let p = returns.iter().filter(|value| **value > 0.0).count() as f64 / k as f64;

    let mut calmars = returns
        .iter()
        .zip(drawdowns.iter())
        .map(|(ret, dd)| *ret / (*dd + eps))
        .collect::<Vec<_>>();
    let c = median(&mut calmars);
    let c_plus = c.max(0.0);

    let dd_min = min_value(&drawdowns);
    let dd_median = median(&mut drawdowns.clone());
    let dd_mean = mean(&drawdowns);
    let dd_max = max_value(&drawdowns);
    let t = dd_max / (dd_median + eps);
    let tail_penalty = 1.0 / (1.0 + t);

    let r_min = min_value(&returns);
    let r_median = median(&mut returns.clone());
    let r_max = max_value(&returns);
    let mu_r = mean(&returns);
    let sigma_r = stddev_population(&returns, mu_r);

    let mut abs_deviation = returns
        .iter()
        .map(|value| (*value - r_median).abs())
        .collect::<Vec<_>>();
    let mad_r = median(&mut abs_deviation);
    let stability = 1.0 / (1.0 + (mad_r / (r_median.abs() + eps)));

    let n_med = median(&mut counts);
    let trade_score = if options.n_min == 0 {
        1.0
    } else {
        (n_med / options.n_min as f64).min(1.0)
    };

    let frs = p.powf(options.alpha)
        * c_plus
        * tail_penalty.powf(options.beta)
        * stability.powf(options.gamma)
        * trade_score.powf(options.delta);

    ForwardRobustnessComponents {
        frs,
        k,
        p,
        c,
        c_plus,
        dd_min,
        dd_median,
        dd_mean,
        dd_max,
        t,
        tail_penalty,
        r_min,
        r_median,
        r_max,
        mu_r,
        sigma_r,
        stability,
        n_med,
        n_min: options.n_min,
        trade_score,
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn min_value(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::INFINITY, f64::min)
}

fn max_value(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

fn stddev_population(values: &[f64], mean: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.max(0.0).sqrt()
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_inputs_return_zero_components() {
        let components = compute_frs(&[], &[], &[], FrsOptions::default());
        assert_eq!(components.frs, 0.0);
        assert_eq!(components.k, 0);
    }

    #[test]
    fn all_positive_windows_get_positive_score() {
        let components = compute_frs(
            &[10.0, 12.0, 9.0],
            &[2.0, 3.0, 2.0],
            &[50, 55, 60],
            FrsOptions::default(),
        );

        assert!(components.frs > 0.0);
        assert_eq!(components.k, 3);
        assert_eq!(components.p, 1.0);
        assert_eq!(components.trade_score, 1.0);
    }

    #[test]
    fn low_trade_count_reduces_score() {
        let high = compute_frs(&[10.0, 10.0], &[2.0, 2.0], &[30, 30], FrsOptions::default());
        let low = compute_frs(&[10.0, 10.0], &[2.0, 2.0], &[3, 3], FrsOptions::default());

        assert!(low.frs < high.frs);
        assert_eq!(low.trade_score, 0.1);
    }

    #[test]
    fn negative_windows_reduce_consistency() {
        let mixed = compute_frs(
            &[10.0, -1.0, 8.0],
            &[2.0, 3.0, 2.0],
            &[50, 50, 50],
            FrsOptions::default(),
        );

        assert!(mixed.p < 1.0);
        assert!(mixed.frs >= 0.0);
    }
}
