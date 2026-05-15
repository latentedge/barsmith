use std::collections::HashMap;

use barsmith_indicators::{
    Comparison, compare_series, comparison, comparison_series, derivative_threshold, double_rising,
    dual_condition, ema_alignment, momentum_acceleration, quantile, ribbon_alignment, rolling_std,
    sma, threshold, threshold_compare,
};

use super::{DerivedMetrics, PriceSeries};

pub(in crate::engineer) fn ema_price_features(
    prices: &PriceSeries,
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
    floats: &mut HashMap<&'static str, Vec<f64>>,
) {
    floats.insert("9ema", derived.ema9.clone());
    floats.insert("20ema", derived.ema20.clone());
    floats.insert("50ema", derived.ema50.clone());
    floats.insert("200sma", derived.sma200.clone());
    floats.insert("rsi_14", derived.rsi14.clone());
    floats.insert("rsi_7", derived.rsi7.clone());
    floats.insert("rsi_21", derived.rsi21.clone());
    floats.insert("momentum_14", derived.momentum_14.clone());
    floats.insert("momentum_score", derived.momentum_score.clone());
    floats.insert("roc_5", derived.roc5.clone());
    floats.insert("roc_10", derived.roc10.clone());
    floats.insert("adx", derived.adx.clone());
    let trend_strength: Vec<f64> = prices
        .close
        .iter()
        .zip(derived.sma200.iter().zip(derived.adx.iter()))
        .map(|(price, (sma200, adx))| {
            if !price.is_finite()
                || !sma200.is_finite()
                || !adx.is_finite()
                || sma200.abs() < f64::EPSILON
            {
                f64::NAN
            } else {
                let adx_term = (adx / 100.0) * 0.4;
                let deviation_term = ((price / sma200) - 1.0).abs() * 10.0 * 0.6;
                adx_term + deviation_term
            }
        })
        .collect();
    floats.insert("trend_strength", trend_strength.clone());
    if let Some(threshold) = quantile(&trend_strength, 0.8) {
        let strong_trend_flags: Vec<bool> = trend_strength
            .iter()
            .map(|value| value.is_finite() && *value > threshold)
            .collect();
        bools.insert("is_very_strong_trend", strong_trend_flags);
    } else {
        bools.insert("is_very_strong_trend", vec![false; trend_strength.len()]);
    }
    floats.insert("adx_sma", sma(&derived.adx, 14));

    bools.insert(
        "all_emas_aligned",
        ema_alignment(&prices.close, &derived.ema9, &derived.ema20, &derived.ema50),
    );
    bools.insert(
        "all_emas_dealigned",
        ema_alignment(
            &prices.close.iter().map(|v| -v).collect::<Vec<_>>(),
            &derived.ema9.iter().map(|v| -v).collect::<Vec<_>>(),
            &derived.ema20.iter().map(|v| -v).collect::<Vec<_>>(),
            &derived.ema50.iter().map(|v| -v).collect::<Vec<_>>(),
        ),
    );
    bools.insert(
        "ema_ribbon_aligned",
        ribbon_alignment(
            &derived.ema9,
            &derived.ema20,
            &derived.ema50,
            &derived.sma200,
            true,
        ),
    );
    bools.insert(
        "ema_ribbon_dealigned",
        ribbon_alignment(
            &derived.ema9,
            &derived.ema20,
            &derived.ema50,
            &derived.sma200,
            false,
        ),
    );

    bools.insert(
        "is_close_above_200sma",
        compare_series(&prices.close, &derived.sma200, Comparison::Greater),
    );
    bools.insert(
        "is_close_below_200sma",
        compare_series(&prices.close, &derived.sma200, Comparison::Less),
    );
    bools.insert(
        "is_close_above_9ema",
        compare_series(&prices.close, &derived.ema9, Comparison::Greater),
    );
    bools.insert(
        "is_close_below_9ema",
        compare_series(&prices.close, &derived.ema9, Comparison::Less),
    );
    bools.insert(
        "is_close_above_kf_ma",
        compare_series(&prices.close, &derived.kf_ma, Comparison::Greater),
    );
    bools.insert(
        "is_close_below_kf_ma",
        compare_series(&prices.close, &derived.kf_ma, Comparison::Less),
    );

    floats.insert("kf_smooth", derived.kf_smooth.clone());
    floats.insert("kf_vs_9ema", derived.kf_vs_9ema.clone());
    floats.insert("kf_vs_200sma", derived.kf_vs_200sma.clone());
    floats.insert("kf_price_deviation", derived.kf_price_deviation.clone());
    floats.insert("kf_innovation_abs", derived.kf_innovation_abs.clone());
    floats.insert("kf_innovation", derived.kf_innovation.clone());
    floats.insert(
        "kf_adx_innovation_abs",
        derived.kf_adx_innovation_abs.clone(),
    );
    floats.insert("kf_adx", derived.kf_adx.clone());
    floats.insert("kf_trend_momentum", derived.kf_trend_momentum.clone());
    floats.insert(
        "kf_trend_volatility_ratio",
        derived.kf_trend_volatility_ratio.clone(),
    );
    floats.insert("kf_adx_deviation", derived.kf_adx_deviation.clone());
    floats.insert("kf_adx_momentum_5", derived.kf_adx_momentum_5.clone());

    floats.insert("price_vs_200sma_dev", derived.price_vs_200sma_dev.clone());
    floats.insert("price_vs_9ema_dev", derived.price_vs_9ema_dev.clone());
    floats.insert("9ema_to_200sma", derived.nine_to_two_hundred.clone());
}

pub(in crate::engineer) fn trend_state_features(
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
) {
    bools.insert(
        "adx_rising",
        derivative_threshold(&derived.adx, 0.0, Comparison::Greater),
    );
    bools.insert("adx_accelerating", double_rising(&derived.adx));

    bools.insert(
        "higher_high",
        comparison_series(&derived.adx, 1, Comparison::Greater),
    );
}

pub(in crate::engineer) fn kalman_features(
    prices: &PriceSeries,
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
) {
    let len = derived.kf_smooth.len();
    let kf_atr_mean20 = sma(&derived.kf_atr, 20);
    let kf_atr_std20 = rolling_std(&derived.kf_atr, 20);
    let kf_innovation_mean20 = sma(&derived.kf_innovation_abs, 20);
    let kf_innovation_std20 = rolling_std(&derived.kf_innovation, 20);
    let atr_expanding = bools
        .get("kf_atr_expanding")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);
    let atr_contracting = bools
        .get("kf_atr_contracting")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);
    let atr_c2c_expanding = bools
        .get("kf_atr_c2c_expanding")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);
    let atr_c2c_contracting = bools
        .get("kf_atr_c2c_contracting")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);
    let gap_volatility = bools
        .get("is_kf_gap_volatility")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);
    let continuous_volatility = bools
        .get("is_kf_continuous_volatility")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);
    let atr_c2c_high_flags = bools
        .get("is_kf_atr_c2c_high_volatility")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);
    let atr_c2c_low_flags = bools
        .get("is_kf_atr_c2c_low_volatility")
        .cloned()
        .unwrap_or_else(|| vec![false; len]);

    bools.insert(
        "kf_above_smooth",
        threshold(&derived.kf_price_deviation, 0.0, Comparison::Greater),
    );
    bools.insert(
        "kf_above_trend",
        compare_series(&prices.close, &derived.kf_trend, Comparison::Greater),
    );
    bools.insert(
        "kf_below_smooth",
        threshold(&derived.kf_price_deviation, 0.0, Comparison::Less),
    );
    bools.insert(
        "kf_below_trend",
        compare_series(&prices.close, &derived.kf_trend, Comparison::Less),
    );

    let kf_adx_above_25 = threshold(&derived.kf_adx, 25.0, Comparison::Greater);
    let kf_adx_above_40 = threshold(&derived.kf_adx, 40.0, Comparison::Greater);
    let kf_adx_below_20 = threshold(&derived.kf_adx, 20.0, Comparison::Less);
    let kf_adx_below_25 = threshold(&derived.kf_adx, 25.0, Comparison::Less);
    let kf_adx_increasing = comparison_series(&derived.kf_adx, 1, Comparison::Greater);
    let kf_adx_decreasing = comparison_series(&derived.kf_adx, 1, Comparison::Less);

    bools.insert("kf_adx_above_25", kf_adx_above_25.clone());
    bools.insert("kf_adx_above_40", kf_adx_above_40.clone());
    bools.insert("kf_adx_below_20", kf_adx_below_20.clone());
    bools.insert("kf_adx_increasing", kf_adx_increasing.clone());
    bools.insert("kf_adx_decreasing", kf_adx_decreasing.clone());
    bools.insert(
        "kf_adx_accelerating",
        momentum_acceleration(&derived.kf_adx_slope, Comparison::Greater),
    );
    bools.insert(
        "kf_adx_decelerating",
        momentum_acceleration(&derived.kf_adx_slope, Comparison::Less),
    );
    let kf_adx_innovation_mean20 = sma(&derived.kf_adx_innovation_abs, 20);
    let kf_adx_surprise_threshold: Vec<f64> = kf_adx_innovation_mean20
        .iter()
        .map(|mean| mean * 1.5)
        .collect();
    bools.insert(
        "is_kf_adx_surprise",
        compare_series(
            &derived.kf_adx_innovation_abs,
            &kf_adx_surprise_threshold,
            Comparison::Greater,
        ),
    );
    let mut adx_trend_emerging = vec![false; len];
    let mut adx_trend_fading = vec![false; len];
    for i in 5..len {
        if derived.kf_adx[i].is_finite() && derived.kf_adx[i - 5].is_finite() {
            adx_trend_emerging[i] = derived.kf_adx[i] > 20.0 && derived.kf_adx[i - 5] < 20.0;
            adx_trend_fading[i] = derived.kf_adx[i] < 25.0 && derived.kf_adx[i - 5] > 30.0;
        }
    }
    bools.insert("kf_adx_trend_emerging", adx_trend_emerging);
    bools.insert("kf_adx_trend_fading", adx_trend_fading);

    let atr_high_flags =
        threshold_compare(&derived.kf_atr, &kf_atr_mean20, 1.2, Comparison::Greater);
    let atr_low_flags = threshold_compare(&derived.kf_atr, &kf_atr_mean20, 0.8, Comparison::Less);

    bools.insert("is_kf_atr_high_volatility", atr_high_flags.clone());
    bools.insert("is_kf_atr_low_volatility", atr_low_flags.clone());

    let atr_very_high_flags: Vec<bool> = derived
        .kf_atr
        .iter()
        .zip(kf_atr_mean20.iter().zip(kf_atr_std20.iter()))
        .map(|(atr, (mean, std))| {
            atr.is_finite() && mean.is_finite() && std.is_finite() && *atr > *mean + 2.0 * *std
        })
        .collect();
    bools.insert("is_kf_atr_very_high_volatility", atr_very_high_flags);

    let mut atr_squeeze_flags = vec![false; len];
    for (i, flag) in atr_squeeze_flags.iter_mut().enumerate().take(len) {
        if i + 1 < 50 {
            continue;
        }
        let start = i + 1 - 50;
        let mut window_min = f64::INFINITY;
        for &value in &derived.kf_atr[start..=i] {
            if value.is_finite() {
                window_min = window_min.min(value);
            }
        }
        if window_min.is_finite()
            && derived.kf_atr[i].is_finite()
            && derived.kf_atr[i] < window_min * 1.1
        {
            *flag = true;
        }
    }
    bools.insert("is_kf_atr_squeeze", atr_squeeze_flags);

    bools.insert(
        "kf_c2c_dominance",
        compare_series(
            &derived.kf_atr_c2c,
            &derived
                .kf_atr
                .iter()
                .map(|value| value * 0.7)
                .collect::<Vec<_>>(),
            Comparison::Greater,
        ),
    );
    let divergence_flags: Vec<bool> = atr_expanding
        .iter()
        .zip(atr_c2c_contracting.iter())
        .zip(atr_contracting.iter().zip(atr_c2c_expanding.iter()))
        .map(|((atr_up, c2c_down), (atr_down, c2c_up))| {
            (*atr_up && *c2c_down) || (*atr_down && *c2c_up)
        })
        .collect();
    bools.insert("kf_volatility_divergence", divergence_flags);
    bools.insert(
        "kf_momentum_increasing",
        comparison(&derived.kf_close_momentum, 1, Comparison::Greater),
    );
    bools.insert(
        "kf_momentum_decreasing",
        comparison(&derived.kf_close_momentum, 1, Comparison::Less),
    );
    bools.insert(
        "kf_slope_increasing",
        comparison(&derived.kf_slope_5, 1, Comparison::Greater),
    );
    bools.insert(
        "kf_slope_decreasing",
        comparison(&derived.kf_slope_5, 1, Comparison::Less),
    );

    bools.insert(
        "kf_trending_volatile",
        dual_condition(&kf_adx_above_25, &atr_high_flags),
    );
    bools.insert(
        "kf_trending_quiet",
        dual_condition(&kf_adx_above_25, &atr_low_flags),
    );
    bools.insert(
        "kf_ranging_volatile",
        dual_condition(&kf_adx_below_20, &atr_high_flags),
    );
    bools.insert(
        "kf_ranging_quiet",
        dual_condition(&kf_adx_below_20, &atr_low_flags),
    );
    bools.insert(
        "is_kf_strong_trend_low_vol",
        dual_condition(
            &threshold(&derived.kf_adx, 35.0, Comparison::Greater),
            &threshold(&derived.kf_atr_pct, 0.012, Comparison::Less),
        ),
    );
    bools.insert(
        "is_kf_breakout_potential",
        dual_condition(&kf_adx_increasing, &atr_expanding),
    );
    bools.insert(
        "is_kf_consolidation",
        dual_condition(&kf_adx_decreasing, &atr_contracting),
    );
    bools.insert(
        "kf_trending_c2c_volatile",
        dual_condition(&kf_adx_above_25, &atr_c2c_high_flags),
    );
    bools.insert(
        "kf_trending_c2c_quiet",
        dual_condition(&kf_adx_above_25, &atr_c2c_low_flags),
    );
    let gap_opportunity = dual_condition(&gap_volatility, &kf_adx_below_25);
    bools.insert("is_kf_gap_trading_opportunity", gap_opportunity);
    bools.insert(
        "is_kf_smooth_trend",
        dual_condition(
            &continuous_volatility,
            &threshold(&derived.kf_adx, 30.0, Comparison::Greater),
        ),
    );
    let kf_close_above_ema9 =
        compare_series(&derived.kf_smooth, &derived.ema9, Comparison::Greater);
    let ema9_above_200 = compare_series(&derived.ema9, &derived.sma200, Comparison::Greater);
    bools.insert(
        "kf_ema_aligned",
        dual_condition(&kf_close_above_ema9, &ema9_above_200),
    );
    bools.insert(
        "kf_ema_divergence",
        dual_condition(
            &compare_series(&derived.kf_smooth, &derived.kf_trend, Comparison::Less),
            &compare_series(&derived.kf_trend, &derived.ema50, Comparison::Less),
        ),
    );
    bools.insert(
        "is_kf_positive_surprise",
        compare_series(
            &derived.kf_innovation,
            &kf_innovation_std20,
            Comparison::Greater,
        ),
    );
    let neg_innovation_std: Vec<f64> = kf_innovation_std20.iter().map(|std| -*std).collect();
    bools.insert(
        "is_kf_negative_surprise",
        compare_series(
            &derived.kf_innovation,
            &neg_innovation_std,
            Comparison::Less,
        ),
    );
    let innovation_large_threshold: Vec<f64> =
        kf_innovation_mean20.iter().map(|mean| mean * 1.5).collect();
    bools.insert(
        "is_kf_innovation_large",
        compare_series(
            &derived.kf_innovation_abs,
            &innovation_large_threshold,
            Comparison::Greater,
        ),
    );
}
