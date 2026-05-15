//! Reusable indicator and rolling-window math.
//!
//! This crate intentionally stays free of Polars, CLI, storage, and strategy
//! dependencies. Strategy crates can use the `Vec`-returning helpers for
//! clarity, or the `*_into` variants when they already own reusable buffers.

mod comparisons;
mod kalman;
mod levels;
mod math;
mod moving_average;
mod oscillators;
mod patterns;
mod rolling;
mod volatility;

pub use comparisons::{
    compare_series, comparison, comparison_series, cross, derivative_threshold, double_rising,
    dual_condition, momentum_acceleration, recovery, stoch_cross, stoch_threshold, threshold,
    threshold_compare, zscore_compare,
};
pub use kalman::kalman_filter;
pub use levels::{build_long_levels, build_short_levels};
pub use math::{
    add_scalar, deviation, diff, elementwise_max, percentile_rank, quantile, range, ratio,
    ratio_with_eps, round_to_decimals, vector_abs,
};
pub use moving_average::{ema, ema_into, rma, sma, sma_strict};
pub use oscillators::{
    bollinger, bollinger_core, bollinger_position, derivative, extension, macd, macd_core,
    momentum, momentum_score, roc, rsi, rsi_core, stochastic, stochastic_core,
};
pub use patterns::{
    ema_alignment, engulfing, hammer, large_body_ratio, large_colored_body, lower_wick,
    ribbon_alignment, shooting_star, upper_wick,
};
pub use rolling::{
    rolling_bool_sum, rolling_coeff_var, rolling_min, rolling_std, shift_bool, streak,
};
pub use volatility::{adx, atr, atr_close_to_close, atr_into, squeeze};

pub const BODY_SIZE_EPS: f64 = 1e-9;
pub const FLOAT_TOLERANCE: f64 = 1e-10;
pub const NON_ZERO_RANGE_EPS: f64 = f64::EPSILON;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    Greater,
    Less,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ema_into_matches_allocating_api() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let expected = ema(&values, 3);
        let mut actual = vec![0.0; values.len()];
        ema_into(&values, 3, &mut actual);
        assert_series_eq(&actual, &expected);
    }

    #[test]
    fn atr_into_matches_allocating_api() {
        let high = [11.0, 12.0, 13.0, 14.0];
        let low = [9.0, 10.0, 11.0, 12.0];
        let close = [10.0, 11.0, 12.0, 13.0];
        let expected = atr(&high, &low, &close, 3);
        let mut actual = vec![0.0; close.len()];
        atr_into(&high, &low, &close, 3, &mut actual);
        assert_series_eq(&actual, &expected);
    }

    fn assert_series_eq(actual: &[f64], expected: &[f64]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            if expected.is_nan() {
                assert!(actual.is_nan());
            } else {
                assert_eq!(actual, expected);
            }
        }
    }
}
