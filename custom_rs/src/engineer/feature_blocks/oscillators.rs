use std::collections::HashMap;

use barsmith_indicators::{
    Comparison, bollinger_position, compare_series, cross, derivative, derivative_threshold,
    recovery, round_to_decimals, sma_strict, stoch_cross, stoch_threshold, threshold,
};

use super::{DerivedMetrics, PriceSeries};

pub(in crate::engineer) fn oscillator_features(
    _prices: &PriceSeries,
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
    floats: &mut HashMap<&'static str, Vec<f64>>,
) {
    const STOCH_PRECISION: u32 = 15;
    let stoch_k_raw = derived.stoch_k.clone();
    let stoch_d_raw = derived.stoch_d.clone();
    let stoch_k_export: Vec<f64> = stoch_k_raw
        .iter()
        .map(|value| round_to_decimals(*value, STOCH_PRECISION))
        .collect();
    let stoch_d_export: Vec<f64> = stoch_d_raw
        .iter()
        .map(|value| round_to_decimals(*value, STOCH_PRECISION))
        .collect();
    let stoch_k_logic = stoch_k_raw.clone();
    let stoch_d_logic = stoch_d_raw.clone();
    floats.insert("stoch_k", stoch_k_export);
    floats.insert("stoch_d", stoch_d_export);
    bools.insert(
        "is_rsi_oversold_recovery",
        recovery(&derived.rsi14, 30.0, Comparison::Greater),
    );
    bools.insert(
        "is_rsi_overbought_recovery",
        recovery(&derived.rsi14, 70.0, Comparison::Less),
    );
    bools.insert(
        "rsi_bullish",
        threshold(&derived.rsi14, 50.0, Comparison::Greater),
    );
    bools.insert(
        "rsi_bearish",
        threshold(&derived.rsi14, 50.0, Comparison::Less),
    );
    bools.insert(
        "rsi_very_bullish",
        threshold(&derived.rsi14, 60.0, Comparison::Greater),
    );
    bools.insert(
        "rsi_very_bearish",
        threshold(&derived.rsi14, 40.0, Comparison::Less),
    );

    bools.insert(
        "is_stoch_oversold",
        stoch_threshold(&stoch_k_logic, 20.0, Comparison::Less),
    );
    bools.insert(
        "is_stoch_overbought",
        stoch_threshold(&stoch_k_logic, 80.0, Comparison::Greater),
    );
    bools.insert(
        "stoch_bullish_cross",
        stoch_cross(
            &stoch_k_logic,
            &stoch_d_logic,
            &stoch_k_raw,
            &stoch_d_raw,
            true,
        ),
    );
    bools.insert(
        "stoch_bearish_cross",
        stoch_cross(
            &stoch_k_logic,
            &stoch_d_logic,
            &stoch_k_raw,
            &stoch_d_raw,
            false,
        ),
    );

    bools.insert(
        "is_strong_momentum_score",
        threshold(&derived.momentum_score, 0.75, Comparison::Greater),
    );
    bools.insert(
        "is_weak_momentum_score",
        threshold(&derived.momentum_score, 0.25, Comparison::Less),
    );
}

pub(in crate::engineer) fn macd_features(
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
    floats: &mut HashMap<&'static str, Vec<f64>>,
) {
    floats.insert("macd", derived.macd.clone());
    floats.insert("macd_signal", derived.macd_signal.clone());
    floats.insert("macd_hist", derived.macd_hist.clone());
    let macd_hist_delta_1 = derivative(&derived.macd_hist, 1);
    floats.insert("macd_hist_delta_1", macd_hist_delta_1);

    bools.insert(
        "macd_bullish",
        compare_series(&derived.macd, &derived.macd_signal, Comparison::Greater),
    );
    bools.insert(
        "macd_bearish",
        compare_series(&derived.macd, &derived.macd_signal, Comparison::Less),
    );
    bools.insert(
        "macd_cross_up",
        cross(&derived.macd, &derived.macd_signal, true),
    );
    bools.insert(
        "macd_cross_down",
        cross(&derived.macd, &derived.macd_signal, false),
    );
    bools.insert(
        "macd_histogram_increasing",
        derivative_threshold(&derived.macd_hist, 0.0, Comparison::Greater),
    );
    bools.insert(
        "macd_histogram_decreasing",
        derivative_threshold(&derived.macd_hist, 0.0, Comparison::Less),
    );
}

pub(in crate::engineer) fn bollinger_features(
    prices: &PriceSeries,
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
    floats: &mut HashMap<&'static str, Vec<f64>>,
) {
    floats.insert(
        "bb_position",
        bollinger_position(&prices.close, &derived.bb_lower, &derived.bb_upper),
    );
    floats.insert("bb_std", derived.bb_std.clone());

    let bb_std_sma20 = sma_strict(&derived.bb_std, 20);
    let squeeze_flags: Vec<bool> = derived
        .bb_std
        .iter()
        .zip(bb_std_sma20.iter())
        .map(|(std, mean)| {
            if !std.is_finite() || !mean.is_finite() {
                false
            } else {
                *std < (*mean * 0.8)
            }
        })
        .collect();
    bools.insert("is_bb_squeeze", squeeze_flags);
    bools.insert(
        "above_bb_middle",
        compare_series(&prices.close, &derived.bb_mid, Comparison::Greater),
    );
    bools.insert(
        "below_bb_middle",
        compare_series(&prices.close, &derived.bb_mid, Comparison::Less),
    );

    floats.insert("ext", derived.ext.clone());
    floats.insert("ext_sma14", derived.ext_sma14.clone());
}
