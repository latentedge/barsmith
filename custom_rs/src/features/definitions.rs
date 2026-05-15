use std::collections::HashSet;

pub(super) const BOOLEAN_NOTE: &str = "Boolean feature exported by custom_rs";
pub(super) const CONTINUOUS_NOTE: &str =
    "Continuous feature exported by custom_rs; thresholds come from feature_ranges.json";

pub const BOOLEAN_FEATURES: &[&str] = &[
    // This is the boolean predicate search surface. Removing a name here
    // keeps the engineered column, but stops it from joining combinations.
    "is_tribar",
    "is_tribar_green",
    "is_tribar_red",
    "is_tribar_hl",
    "is_tribar_hl_green",
    "is_tribar_hl_red",
    "consecutive_green_2",
    "consecutive_green_3",
    "consecutive_red_2",
    "consecutive_red_3",
    "prev_tribar",
    "prev_green",
    "higher_high",
    "higher_low",
    "lower_high",
    "lower_low",
    "is_hammer",
    "is_shooting_star",
    "bullish_engulfing",
    "bearish_engulfing",
    "adx_rising",
    "adx_accelerating",
    "kf_adx_increasing",
    "kf_adx_decreasing",
    "kf_adx_accelerating",
    "kf_adx_decelerating",
    "kf_adx_trend_emerging",
    "kf_adx_trend_fading",
    "is_kf_atr_squeeze",
    "is_kf_atr_c2c_squeeze",
    "is_kf_atr_volatility_spike",
    "is_kf_atr_volatility_drop",
    "is_kf_atr_c2c_spike",
    "is_kf_atr_c2c_drop",
    "kf_atr_c2c_contracting",
    "kf_atr_c2c_expanding",
    "kf_atr_contracting",
    "kf_atr_expanding",
    "kf_volatility_divergence",
    "kf_momentum_increasing",
    "kf_momentum_decreasing",
    "kf_slope_increasing",
    "kf_slope_decreasing",
    "kf_trending_volatile",
    "kf_trending_quiet",
    "kf_ranging_volatile",
    "kf_ranging_quiet",
    "is_kf_strong_trend_low_vol",
    "is_kf_breakout_potential",
    "is_kf_consolidation",
    "kf_trending_c2c_volatile",
    "kf_trending_c2c_quiet",
    "is_kf_gap_trading_opportunity",
    "is_kf_smooth_trend",
    "is_kf_positive_surprise",
    "is_kf_negative_surprise",
    "is_rsi_oversold_recovery",
    "is_rsi_overbought_recovery",
    "stoch_bullish_cross",
    "stoch_bearish_cross",
    "macd_cross_up",
    "macd_cross_down",
    "is_bb_squeeze",
];

pub const CONTINUOUS_FEATURES: &[&str] = &[
    // feature_ranges.json owns threshold enumeration. This list keeps the
    // engineered numeric surface auditable and drives NaN trimming.
    "rsi_14",
    "rsi_7",
    "rsi_21",
    "momentum_14",
    "momentum_score",
    "roc_5",
    "roc_10",
    "adx",
    "kf_adx",
    "trend_strength",
    "adx_sma",
    "kf_trend_momentum",
    "kf_trend_volatility_ratio",
    "macd",
    "macd_signal",
    "macd_hist",
    "stoch_k",
    "stoch_d",
    "kf_price_deviation",
    "kf_vs_9ema",
    "kf_vs_200sma",
    "kf_innovation_abs",
    "kf_innovation",
    "kf_adx_deviation",
    "kf_adx_innovation_abs",
    "kf_adx_momentum_5",
    "kf_atr_pct",
    "kf_atr_c2c_pct",
    "kf_atr_vs_c2c",
    "kf_atr_deviation",
    "kf_atr_momentum_5",
    "kf_atr_c2c_momentum_5",
    "kf_atr_innovation",
    "kf_atr_c2c_innovation",
    "atr_pct",
    "atr_c2c_pct",
    "bar_range_pct",
    "volatility_20_cv",
    "body_size_pct",
    "body_vs_max_wick_ratio",
    "body_to_total_wick",
    "body_atr_ratio",
    "consecutive_green",
    "bb_position",
    "wicks_diff",
    "wicks_diff_sma14",
    "kf_wicks_smooth",
    "price_vs_200sma_dev",
    "price_vs_9ema_dev",
    "9ema_to_200sma",
    "upper_shadow_ratio",
    "lower_shadow_ratio",
    "atr_pct_mean50",
    "atr_c2c_mean50",
    "kf_atr",
    "kf_atr_c2c",
    "bb_std",
    "ext",
    "ext_sma14",
    "macd_hist_delta_1",
];

/// Core price and level columns used as default anchors for pairwise numeric
/// comparisons.
pub const PAIRWISE_BASE_NUMERIC_FEATURES: &[&str] = &["close", "open", "high", "low", "kf_smooth"];

/// Additional numeric columns that can form feature-to-feature predicates.
pub const PAIRWISE_EXTRA_NUMERIC_FEATURES: &[&str] = &[
    "9ema",
    "20ema",
    "50ema",
    "200sma",
    "atr_c2c",
    "atr_c2c_mean50",
    "bb_std",
    "kf_atr",
    "kf_atr_c2c",
    "wicks_diff",
    "wicks_diff_sma14",
    "lower_shadow_ratio",
    "upper_shadow_ratio",
    "price_vs_9ema_dev",
    "price_vs_200sma_dev",
    "9ema_to_200sma",
    "kf_price_deviation",
    "kf_vs_9ema",
    "kf_vs_200sma",
    "kf_innovation_abs",
];

#[derive(Debug)]
pub struct PairwiseRule {
    pub feature: &'static str,
    pub use_default: bool,
    pub include: &'static [&'static str],
    pub exclude: &'static [&'static str],
}

/// Unlisted features compare against the default anchors and exclude
/// themselves.
pub const PAIRWISE_NUMERIC_RULES: &[PairwiseRule] = &[
    PairwiseRule {
        feature: "close",
        use_default: true,
        include: &[],
        exclude: &["self"],
    },
    PairwiseRule {
        feature: "open",
        use_default: true,
        include: &[],
        exclude: &["self"],
    },
    PairwiseRule {
        feature: "high",
        use_default: true,
        include: &[],
        exclude: &["self"],
    },
    PairwiseRule {
        feature: "low",
        use_default: true,
        include: &[],
        exclude: &["self"],
    },
];

fn find_pairwise_rule(name: &str) -> Option<&'static PairwiseRule> {
    PAIRWISE_NUMERIC_RULES
        .iter()
        .find(|rule| rule.feature == name)
}

pub(crate) fn pairwise_declared_feature_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = Vec::new();
    names.extend_from_slice(PAIRWISE_BASE_NUMERIC_FEATURES);
    names.extend_from_slice(PAIRWISE_EXTRA_NUMERIC_FEATURES);

    for rule in PAIRWISE_NUMERIC_RULES {
        names.push(rule.feature);
        names.extend_from_slice(rule.include);
    }

    names.sort_unstable();
    names.dedup();
    names
}

pub(crate) fn allowed_partners_for(feature: &str) -> Vec<&'static str> {
    let rule = find_pairwise_rule(feature);
    let mut partners: Vec<&'static str> = Vec::new();

    if rule.map(|r| r.use_default).unwrap_or(true) {
        partners.extend_from_slice(PAIRWISE_BASE_NUMERIC_FEATURES);
    }
    if let Some(rule) = rule {
        partners.extend_from_slice(rule.include);
    }

    let mut excluded: HashSet<&str> = HashSet::new();
    if let Some(rule) = rule {
        for &raw in rule.exclude {
            excluded.insert(if raw == "self" { feature } else { raw });
        }
    } else {
        excluded.insert(feature);
    }

    partners
        .into_iter()
        .filter(|partner| !excluded.contains(partner))
        .collect()
}

pub(super) fn pair_allowed(left: &str, right: &str) -> bool {
    if left == right {
        return false;
    }

    let left_partners = allowed_partners_for(left);
    if left_partners.contains(&right) {
        return true;
    }

    let right_partners = allowed_partners_for(right);
    right_partners.contains(&left)
}
