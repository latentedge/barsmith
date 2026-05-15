use std::collections::HashMap;

use barsmith_indicators::{
    Comparison, compare_series, comparison, derivative_threshold, rolling_std, sma, squeeze,
    threshold, threshold_compare, zscore_compare,
};

use super::{DerivedMetrics, PriceSeries};

pub(in crate::engineer) fn volatility_features(
    _prices: &PriceSeries,
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
    floats: &mut HashMap<&'static str, Vec<f64>>,
) {
    // `atr` is needed downstream for targets, but it is intentionally outside
    // the core NaN-trimming set so it does not move the warmup boundary.
    floats.insert("atr", derived.atr.clone());
    floats.insert("kf_atr", derived.kf_atr.clone());
    floats.insert("kf_atr_c2c", derived.kf_atr_c2c.clone());
    floats.insert("kf_atr_pct", derived.kf_atr_pct.clone());
    floats.insert("kf_atr_c2c_pct", derived.kf_atr_c2c_pct.clone());
    floats.insert("atr_c2c", derived.atr_c2c.clone());
    floats.insert("kf_atr_vs_c2c", derived.kf_atr_vs_c2c.clone());
    floats.insert("kf_atr_deviation", derived.kf_atr_deviation.clone());
    floats.insert("kf_atr_momentum_5", derived.kf_atr_momentum_5.clone());
    floats.insert(
        "kf_atr_c2c_momentum_5",
        derived.kf_atr_c2c_momentum_5.clone(),
    );
    floats.insert("kf_atr_innovation", derived.kf_atr_innovation.clone());
    floats.insert(
        "kf_atr_c2c_innovation",
        derived.kf_atr_c2c_innovation.clone(),
    );
    floats.insert("atr_pct", derived.atr_pct.clone());
    floats.insert("atr_c2c_pct", derived.atr_c2c_pct.clone());
    floats.insert("bar_range_pct", derived.bar_range_pct.clone());
    floats.insert("volatility_20_cv", derived.volatility_20_cv.clone());
    floats.insert("body_size_pct", derived.body_size_pct.clone());

    let atr_mean20 = sma(&derived.atr, 20);
    let kf_atr_mean20 = sma(&derived.kf_atr, 20);
    let kf_atr_std20 = rolling_std(&derived.kf_atr, 20);
    let kf_atr_c2c_mean20 = sma(&derived.kf_atr_c2c, 20);
    let kf_atr_c2c_std20 = rolling_std(&derived.kf_atr_c2c, 20);

    bools.insert(
        "is_high_volatility",
        threshold_compare(&derived.atr, &atr_mean20, 1.2, Comparison::Greater),
    );
    bools.insert(
        "is_low_volatility",
        threshold_compare(&derived.atr, &atr_mean20, 0.8, Comparison::Less),
    );
    bools.insert(
        "expanding_atr",
        comparison(&derived.atr, 1, Comparison::Greater),
    );
    bools.insert(
        "is_kf_atr_high_volatility",
        threshold_compare(&derived.kf_atr, &kf_atr_mean20, 1.2, Comparison::Greater),
    );
    bools.insert(
        "is_kf_atr_low_volatility",
        threshold_compare(&derived.kf_atr, &kf_atr_mean20, 0.8, Comparison::Less),
    );
    bools.insert("is_kf_atr_squeeze", squeeze(&derived.kf_atr_pct, 50, 1.1));
    bools.insert(
        "is_kf_atr_very_high_volatility",
        zscore_compare(&derived.kf_atr, &kf_atr_mean20, &kf_atr_std20, 2.0),
    );
    let atr_innovation_std20 = rolling_std(&derived.kf_atr_innovation, 20);
    let atr_innovation_spike: Vec<f64> = atr_innovation_std20.iter().map(|std| std * 2.0).collect();
    let atr_innovation_drop: Vec<f64> = atr_innovation_spike.iter().map(|value| -*value).collect();
    bools.insert(
        "is_kf_atr_volatility_spike",
        compare_series(
            &derived.kf_atr_innovation,
            &atr_innovation_spike,
            Comparison::Greater,
        ),
    );
    bools.insert(
        "is_kf_atr_volatility_drop",
        compare_series(
            &derived.kf_atr_innovation,
            &atr_innovation_drop,
            Comparison::Less,
        ),
    );
    bools.insert(
        "is_kf_atr_c2c_high_volatility",
        threshold_compare(
            &derived.kf_atr_c2c,
            &kf_atr_c2c_mean20,
            1.2,
            Comparison::Greater,
        ),
    );
    bools.insert(
        "is_kf_atr_c2c_low_volatility",
        threshold_compare(
            &derived.kf_atr_c2c,
            &kf_atr_c2c_mean20,
            0.8,
            Comparison::Less,
        ),
    );
    bools.insert(
        "is_kf_atr_c2c_squeeze",
        squeeze(&derived.kf_atr_c2c, 50, 1.1),
    );
    bools.insert(
        "is_kf_atr_c2c_very_high",
        zscore_compare(
            &derived.kf_atr_c2c,
            &kf_atr_c2c_mean20,
            &kf_atr_c2c_std20,
            2.0,
        ),
    );
    let atr_c2c_innovation_std20 = rolling_std(&derived.kf_atr_c2c_innovation, 20);
    let atr_c2c_innovation_spike: Vec<f64> = atr_c2c_innovation_std20
        .iter()
        .map(|std| std * 2.0)
        .collect();
    let atr_c2c_innovation_drop: Vec<f64> = atr_c2c_innovation_spike
        .iter()
        .map(|value| -*value)
        .collect();
    bools.insert(
        "is_kf_atr_c2c_spike",
        compare_series(
            &derived.kf_atr_c2c_innovation,
            &atr_c2c_innovation_spike,
            Comparison::Greater,
        ),
    );
    bools.insert(
        "is_kf_atr_c2c_drop",
        compare_series(
            &derived.kf_atr_c2c_innovation,
            &atr_c2c_innovation_drop,
            Comparison::Less,
        ),
    );

    bools.insert(
        "kf_atr_c2c_contracting",
        derivative_threshold(&derived.kf_atr_c2c, 0.0, Comparison::Less),
    );
    bools.insert(
        "kf_atr_c2c_expanding",
        derivative_threshold(&derived.kf_atr_c2c, 0.0, Comparison::Greater),
    );
    bools.insert(
        "kf_atr_contracting",
        derivative_threshold(&derived.kf_atr, 0.0, Comparison::Less),
    );
    bools.insert(
        "kf_atr_expanding",
        derivative_threshold(&derived.kf_atr, 0.0, Comparison::Greater),
    );
    bools.insert(
        "is_kf_gap_volatility",
        threshold(&derived.kf_atr_vs_c2c, 1.5, Comparison::Greater),
    );
    bools.insert(
        "is_kf_continuous_volatility",
        threshold(&derived.kf_atr_vs_c2c, 1.2, Comparison::Less),
    );

    floats.insert("atr_pct_mean50", derived.atr_mean50.clone());
    floats.insert("atr_c2c_mean50", derived.atr_c2c_mean50.clone());
}
