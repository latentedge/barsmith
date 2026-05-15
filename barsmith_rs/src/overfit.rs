use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchGateStatus {
    Pass,
    Fail,
    Warning,
    Unavailable,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverfitOptions {
    pub candidate_top_k: usize,
    pub cscv_blocks: usize,
    pub cscv_max_splits: usize,
    pub max_pbo: f64,
    pub min_psr: f64,
    pub min_dsr: f64,
    pub min_positive_window_ratio: f64,
    pub effective_trials: Option<usize>,
    pub effective_trials_source: Option<String>,
    pub effective_trials_warning: Option<String>,
    pub complexity_penalty: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverfitReport {
    pub schema_version: u32,
    pub status: ResearchGateStatus,
    pub candidate_count: usize,
    pub effective_trials: usize,
    pub effective_trials_source: String,
    pub cscv_blocks_requested: usize,
    pub cscv_blocks_applied: usize,
    pub cscv_splits: usize,
    pub pbo: Option<f64>,
    pub psr: Option<f64>,
    pub dsr: Option<f64>,
    pub selected_formula: Option<String>,
    pub selected_formula_sha256: Option<String>,
    pub selected_positive_window_ratio: Option<f64>,
    pub warnings: Vec<String>,
    pub decisions: Vec<CscvDecision>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CscvDecision {
    pub split_index: usize,
    pub train_blocks: Vec<String>,
    pub test_blocks: Vec<String>,
    pub selected_formula: String,
    pub train_metric: f64,
    pub test_metric: f64,
    pub test_rank: usize,
    pub candidate_count: usize,
    pub test_percentile: f64,
    pub logit: f64,
    pub overfit: bool,
}

pub fn probabilistic_sharpe_ratio(samples: &[f64], benchmark_sharpe: f64) -> Option<f64> {
    let n = samples.len();
    if n < 3 {
        return None;
    }
    let sharpe = sample_sharpe(samples)?;
    let skew = sample_skewness(samples)?;
    let kurtosis = sample_kurtosis(samples)?;
    let denom = (1.0 - skew * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe * sharpe).sqrt();
    if !denom.is_finite() || denom <= 0.0 {
        return None;
    }
    let z = (sharpe - benchmark_sharpe) * ((n as f64 - 1.0).sqrt()) / denom;
    Some(normal_cdf(z))
}

pub fn deflated_sharpe_ratio(samples: &[f64], effective_trials: usize) -> Option<f64> {
    let n_trials = effective_trials.max(1) as f64;
    let sr_std = sample_std(samples)? / (samples.len() as f64).sqrt();
    if !sr_std.is_finite() || sr_std <= 0.0 {
        return probabilistic_sharpe_ratio(samples, 0.0);
    }

    let euler_gamma = 0.577_215_664_901_532_9_f64;
    let first = inverse_normal_cdf(1.0 - 1.0 / n_trials).unwrap_or(0.0);
    let second = inverse_normal_cdf(1.0 - 1.0 / (n_trials * std::f64::consts::E)).unwrap_or(0.0);
    let expected_max_sharpe = sr_std * ((1.0 - euler_gamma) * first + euler_gamma * second);
    probabilistic_sharpe_ratio(samples, expected_max_sharpe)
}

pub fn sample_sharpe(samples: &[f64]) -> Option<f64> {
    let mean = sample_mean(samples)?;
    let std = sample_std(samples)?;
    if std <= 0.0 || !std.is_finite() {
        return None;
    }
    Some(mean / std)
}

pub fn sample_mean(samples: &[f64]) -> Option<f64> {
    let finite = samples
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return None;
    }
    Some(finite.iter().sum::<f64>() / finite.len() as f64)
}

pub fn sample_std(samples: &[f64]) -> Option<f64> {
    let finite = samples
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if finite.len() < 2 {
        return None;
    }
    let mean = finite.iter().sum::<f64>() / finite.len() as f64;
    let var = finite
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (finite.len() - 1) as f64;
    Some(var.max(0.0).sqrt())
}

fn sample_skewness(samples: &[f64]) -> Option<f64> {
    let finite = samples
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    let n = finite.len();
    if n < 3 {
        return None;
    }
    let mean = finite.iter().sum::<f64>() / n as f64;
    let std = sample_std(&finite)?;
    if std <= 0.0 {
        return None;
    }
    let m3 = finite
        .iter()
        .map(|value| ((value - mean) / std).powi(3))
        .sum::<f64>()
        / n as f64;
    Some(m3)
}

fn sample_kurtosis(samples: &[f64]) -> Option<f64> {
    let finite = samples
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    let n = finite.len();
    if n < 4 {
        return None;
    }
    let mean = finite.iter().sum::<f64>() / n as f64;
    let std = sample_std(&finite)?;
    if std <= 0.0 {
        return None;
    }
    let m4 = finite
        .iter()
        .map(|value| ((value - mean) / std).powi(4))
        .sum::<f64>()
        / n as f64;
    Some(m4)
}

pub fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / 2.0_f64.sqrt()))
}

fn erf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let y = 1.0
        - (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736) * t
            + 0.254_829_592)
            * t
            * (-x * x).exp();
    sign * y
}

fn inverse_normal_cdf(p: f64) -> Option<f64> {
    if !(0.0..=1.0).contains(&p) || p == 0.0 || p == 1.0 {
        return None;
    }

    let a = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    let b = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    let c = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    let d = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];

    let p_low = 0.024_25;
    let p_high = 1.0 - p_low;
    if p < p_low {
        let q = (-2.0 * p.ln()).sqrt();
        return Some(
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
                / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0),
        );
    }
    if p <= p_high {
        let q = p - 0.5;
        let r = q * q;
        return Some(
            (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
                / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0),
        );
    }

    let q = (-2.0 * (1.0 - p).ln()).sqrt();
    Some(
        -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn psr_increases_for_better_sample_mean() {
        let weak = probabilistic_sharpe_ratio(&[0.1, -0.1, 0.0, 0.1, -0.05], 0.0).unwrap();
        let strong = probabilistic_sharpe_ratio(&[1.0, 0.8, 0.9, 1.1, 0.7], 0.0).unwrap();
        assert!(strong > weak);
    }

    #[test]
    fn dsr_penalizes_more_trials() {
        let samples = [1.0, 0.8, 0.9, 1.1, 0.7, 1.2, 0.6, 0.9];
        let few = deflated_sharpe_ratio(&samples, 2).unwrap();
        let many = deflated_sharpe_ratio(&samples, 1_000).unwrap();
        assert!(few >= many);
    }
}
