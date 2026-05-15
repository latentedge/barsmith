use std::collections::HashMap;

use barsmith_indicators::{
    Comparison, comparison, engulfing, hammer, large_body_ratio, large_colored_body,
    ratio_with_eps, shift_bool, shooting_star, sma, streak,
};

use super::{DerivedMetrics, PriceSeries, SMALL_DIVISOR};

pub(in crate::engineer) fn candle_features(
    prices: &PriceSeries,
    derived: &DerivedMetrics,
    bools: &mut HashMap<&'static str, Vec<bool>>,
    floats: &mut HashMap<&'static str, Vec<f64>>,
) {
    let len = prices.len();
    let mut is_green = vec![false; len];
    let mut is_red = vec![false; len];
    for i in 0..len {
        let body = prices.close[i] - prices.open[i];
        is_green[i] = body > 0.0;
        is_red[i] = body < 0.0;
    }

    bools.insert("is_green", is_green.clone());
    bools.insert("is_red", is_red.clone());

    let custom_high: Vec<f64> = prices
        .open
        .iter()
        .zip(prices.close.iter())
        .map(|(o, c)| o.max(*c))
        .collect();
    let custom_low: Vec<f64> = prices
        .open
        .iter()
        .zip(prices.close.iter())
        .map(|(o, c)| o.min(*c))
        .collect();

    floats.insert("upper_shadow_ratio", derived.upper_shadow_ratio.clone());
    floats.insert("lower_shadow_ratio", derived.lower_shadow_ratio.clone());
    floats.insert("wicks_diff", derived.wicks_diff.clone());
    floats.insert("wicks_diff_sma14", derived.wicks_diff_sma14.clone());
    floats.insert("kf_wicks_smooth", derived.kf_wicks_smooth.clone());
    floats.insert("body_to_total_wick", derived.body_to_total_wick.clone());
    floats.insert("body_atr_ratio", derived.body_atr_ratio.clone());
    let body_vs_max_wick_ratio =
        ratio_with_eps(&derived.abs_body, &derived.max_wick, SMALL_DIVISOR);
    floats.insert("body_vs_max_wick_ratio", body_vs_max_wick_ratio);

    let wick_max = &derived.max_wick;
    let multipliers = [0.5, 1.0, 1.5, 2.0, 2.5];
    for mult in multipliers {
        let key = match mult {
            0.5 => "body_dominant_0_5x",
            1.0 => "body_dominant_1_0x",
            1.5 => "body_dominant_1_5x",
            2.0 => "body_dominant_2_0x",
            _ => "body_dominant_2_5x",
        };
        let mut col = vec![false; len];
        for i in 0..len {
            col[i] = derived.abs_body[i] > wick_max[i] * mult;
        }
        bools.insert(key, col);
    }

    let mut tribar = vec![false; len];
    let mut tribar_green = vec![false; len];
    let mut tribar_red = vec![false; len];
    let mut tribar_hl = vec![false; len];
    let mut tribar_hl_green = vec![false; len];
    let mut tribar_hl_red = vec![false; len];
    for i in 2..len {
        let prev_high_1 = custom_high[i - 1];
        let prev_high_2 = custom_high[i - 2];
        let prev_low_1 = custom_low[i - 1];
        let prev_low_2 = custom_low[i - 2];

        let is_bullish =
            is_green[i] && prices.close[i] > prev_high_1 && prices.close[i] > prev_high_2;
        let is_bearish = is_red[i] && prices.close[i] < prev_low_1 && prices.close[i] < prev_low_2;
        tribar[i] = is_bullish || is_bearish;
        tribar_green[i] = is_bullish;
        tribar_red[i] = is_bearish;

        let bull_hl = is_green[i]
            && prices.close[i] > prices.high[i - 1]
            && prices.close[i] > prices.high[i - 2];
        let bear_hl =
            is_red[i] && prices.close[i] < prices.low[i - 1] && prices.close[i] < prices.low[i - 2];
        tribar_hl[i] = bull_hl || bear_hl;
        tribar_hl_green[i] = bull_hl;
        tribar_hl_red[i] = bear_hl;
    }
    bools.insert("is_tribar", tribar.clone());
    bools.insert("is_tribar_green", tribar_green);
    bools.insert("is_tribar_red", tribar_red);
    bools.insert("is_tribar_hl", tribar_hl.clone());
    bools.insert("is_tribar_hl_green", tribar_hl_green);
    bools.insert("is_tribar_hl_red", tribar_hl_red);

    bools.insert("prev_tribar", shift_bool(&tribar, 1));
    bools.insert("prev_green", shift_bool(&is_green, 1));

    bools.insert("consecutive_green_2", streak(&is_green, 2));
    bools.insert("consecutive_green_3", streak(&is_green, 3));
    bools.insert("consecutive_red_2", streak(&is_red, 2));
    bools.insert("consecutive_red_3", streak(&is_red, 3));

    bools.insert(
        "higher_high",
        comparison(&prices.high, 1, Comparison::Greater),
    );
    bools.insert(
        "higher_low",
        comparison(&prices.low, 1, Comparison::Greater),
    );
    bools.insert("lower_high", comparison(&prices.high, 1, Comparison::Less));
    bools.insert("lower_low", comparison(&prices.low, 1, Comparison::Less));

    bools.insert("bullish_bar_sequence", streak(&is_green, 3));
    bools.insert("bearish_bar_sequence", streak(&is_red, 3));

    bools.insert(
        "is_hammer",
        hammer(&derived.abs_body, &derived.upper_wick, &derived.lower_wick),
    );
    bools.insert(
        "is_shooting_star",
        shooting_star(&derived.abs_body, &derived.upper_wick, &derived.lower_wick),
    );
    bools.insert(
        "bullish_engulfing",
        engulfing(&is_green, &is_red, &derived.abs_body, true),
    );
    bools.insert(
        "bearish_engulfing",
        engulfing(&is_green, &is_red, &derived.abs_body, false),
    );

    bools.insert(
        "is_very_large_green",
        large_colored_body(&is_green, &derived.abs_body, &derived.atr, 1.5),
    );
    bools.insert(
        "is_very_large_red",
        large_colored_body(&is_red, &derived.abs_body, &derived.atr, 1.5),
    );

    let body_pct_mean20 = sma(&derived.body_size_pct, 20);
    bools.insert(
        "is_large_body",
        large_body_ratio(&derived.body_size_pct, &body_pct_mean20, 1.5),
    );
    bools.insert(
        "is_very_large_body",
        large_body_ratio(&derived.body_size_pct, &body_pct_mean20, 2.0),
    );
}
