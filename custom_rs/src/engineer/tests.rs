use super::{
    DerivedMetrics, FeatureEngineer, NEXT_BAR_SL_MULTIPLIER, PriceSeries, TickRoundMode,
    atr_close_to_close, candle_features, column_with_nans,
    compute_2x_atr_tp_atr_stop_target_resolution, compute_2x_atr_tp_atr_stop_targets_and_rr,
    compute_3x_atr_tp_atr_stop_targets_and_rr, compute_atr_tp_atr_stop_targets_and_rr,
    compute_highlow_1r_targets_and_rr, compute_highlow_or_atr_targets_and_rr,
    compute_highlow_or_atr_tightest_stop_targets_and_rr,
    compute_highlow_sl_1x_atr_tp_rr_gt_1_targets_and_rr,
    compute_highlow_sl_2x_atr_tp_rr_gt_1_targets_and_rr, compute_next_bar_targets_and_rr,
    compute_wicks_kf_targets_and_rr, quantize_distance_to_tick, quantize_price_to_tick, streak,
};
use barsmith_rs::Direction;
use polars::prelude::*;
use polars_io::prelude::CsvReadOptions;
use std::collections::HashMap;
use std::path::Path;

#[test]
fn streak_basic_behaves_like_shift_logic() {
    let values = vec![true, true, false, true];
    let result_two = streak(&values, 2);
    assert_eq!(result_two, vec![false, true, false, false]);
    let result_three = streak(&values, 3);
    assert_eq!(result_three, vec![false, false, false, false]);
}

#[test]
fn quantize_distance_to_tick_basic_modes() {
    let tick = 0.25;
    let dist = 0.26;

    let nearest = quantize_distance_to_tick(dist, tick, TickRoundMode::Nearest);
    let floor = quantize_distance_to_tick(dist, tick, TickRoundMode::Floor);
    let ceil = quantize_distance_to_tick(dist, tick, TickRoundMode::Ceil);

    assert!(
        (nearest - 0.25).abs() < 1e-9,
        "nearest should round to 0.25"
    );
    assert!(
        (floor - 0.25).abs() < 1e-9,
        "floor should round down to 0.25"
    );
    assert!((ceil - 0.50).abs() < 1e-9, "ceil should round up to 0.50");

    // Very small non-zero distances still map to one tick so that risk
    // is never zero when a stop is requested.
    let tiny = 0.01;
    let nearest_tiny = quantize_distance_to_tick(tiny, tick, TickRoundMode::Nearest);
    let floor_tiny = quantize_distance_to_tick(tiny, tick, TickRoundMode::Floor);
    let ceil_tiny = quantize_distance_to_tick(tiny, tick, TickRoundMode::Ceil);

    assert!((nearest_tiny - 0.25).abs() < 1e-9);
    assert!((floor_tiny - 0.25).abs() < 1e-9);
    assert!((ceil_tiny - 0.25).abs() < 1e-9);
}

#[test]
fn quantize_price_to_tick_basic_modes() {
    let tick = 0.25;
    let price = 100.26;

    let nearest = quantize_price_to_tick(price, tick, TickRoundMode::Nearest);
    let floor = quantize_price_to_tick(price, tick, TickRoundMode::Floor);
    let ceil = quantize_price_to_tick(price, tick, TickRoundMode::Ceil);

    assert!((nearest - 100.25).abs() < 1e-9);
    assert!((floor - 100.25).abs() < 1e-9);
    assert!((ceil - 100.50).abs() < 1e-9);
}

#[test]
fn atr_c2c_matches_expected_sample_values() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("tests/data/es_30m_sample.csv");
    let df = CsvReadOptions::default()
        .with_infer_schema_length(Some(1024))
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(path))
        .unwrap()
        .finish()
        .unwrap();
    let prices = PriceSeries::from_frame(&df).unwrap();
    let atr = atr_close_to_close(&prices.close, 14);
    let start = 195usize;
    let expected = [
        1.0042706809186688,
        0.9703679234628463,
        0.9076522003344668,
        0.786631906956538,
        0.8150809860289996,
        0.8397368545584663,
        0.894438607284004,
        0.8418467929794702,
        0.8629338872488741,
        1.0478760356156909,
    ];
    for (offset, &value) in expected.iter().enumerate() {
        let idx = start + offset;
        let actual = atr[idx];
        assert!(
            (actual - value).abs() < 1e-9,
            "idx {} expected {} got {}",
            idx,
            value,
            actual
        );
    }
}

#[test]
fn next_bar_color_and_wicks_open_equals_close_has_no_target_and_zero_rr() {
    // Flat follow-through should produce a defined RR but no color target.
    let open = vec![99.0, 100.0];
    let high = vec![100.0, 100.0];
    let low = vec![98.5, 100.0];
    let close = vec![99.5, 100.0];
    let wicks = vec![1.0, 1.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_next_bar_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &wicks,
            NEXT_BAR_SL_MULTIPLIER,
            None,
            Direction::Both,
        );

    assert_eq!(long.len(), 2);
    assert_eq!(short.len(), 2);
    // At idx 0, next bar has open == close, so no directional target.
    assert!(
        !long[0],
        "long target should be false when next_close == next_open"
    );
    assert!(
        !short[0],
        "short target should be false when next_close == next_open"
    );

    // RR is still defined and exactly 0R for both directions.
    assert!(rr_long[0].is_finite());
    assert!(rr_short[0].is_finite());
    assert!((rr_long[0]).abs() < 1e-9);
    assert!((rr_short[0]).abs() < 1e-9);
}

#[test]
fn next_bar_color_and_wicks_uses_tick_rounded_stop_distance_when_available() {
    // Raw risk is 0.30, but a 0.25 tick grid rounds it up to 0.50.
    // That changes RR from roughly 1.67R to exactly 1.0R.
    let open = vec![0.0, 100.0];
    let high = vec![0.0, 100.6];
    let low = vec![0.0, 99.8];
    let close = vec![0.0, 100.5];
    let wick_raw = 0.30 / NEXT_BAR_SL_MULTIPLIER;
    let wicks = vec![wick_raw, 0.0];

    let (_long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_next_bar_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &wicks,
            NEXT_BAR_SL_MULTIPLIER,
            Some(0.25),
            Direction::Long,
        );

    assert!(
        rr_long[0].is_finite(),
        "expected finite RR value with tick rounding applied"
    );
    assert!(
        (rr_long[0] - 1.0).abs() < 1e-6,
        "expected RR to reflect ceil tick-rounded stop distance (got {})",
        rr_long[0]
    );
}

#[test]
fn next_bar_color_and_wicks_uses_current_bar_wicks_for_stop_distance() {
    // Stop sizing uses the signal bar's wick, not the next bar's wick.
    let open = vec![0.0, 100.0];
    let high = vec![0.0, 101.0];
    let low = vec![0.0, 99.0];
    let close = vec![0.0, 101.0];
    let wicks = vec![1.0, 10.0];

    let (long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_next_bar_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &wicks,
            NEXT_BAR_SL_MULTIPLIER,
            None,
            Direction::Long,
        );

    assert!(long[0], "expected long target to be true");
    assert!(
        (rr_long[0] - (1.0 / 1.5)).abs() < 1e-9,
        "expected RR to reflect current-bar wick sizing (got {})",
        rr_long[0]
    );
}

#[test]
fn wicks_kf_open_equals_close_has_no_target_and_zero_rr() {
    let open = vec![99.0, 100.0];
    let high = vec![100.0, 100.0];
    let low = vec![98.5, 100.0];
    let close = vec![99.5, 100.0];
    let kf_wicks = vec![1.0, 1.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_wicks_kf_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &kf_wicks,
            NEXT_BAR_SL_MULTIPLIER,
            None,
            Direction::Both,
        );

    assert_eq!(long.len(), 2);
    assert_eq!(short.len(), 2);
    assert!(!long[0]);
    assert!(!short[0]);

    assert!(rr_long[0].is_finite());
    assert!(rr_short[0].is_finite());
    assert!(rr_long[0].abs() < 1e-9);
    assert!(rr_short[0].abs() < 1e-9);
}

#[test]
fn wicks_kf_uses_tick_rounded_stop_distance_when_available() {
    let open = vec![0.0, 100.0];
    let high = vec![0.0, 100.6];
    let low = vec![0.0, 99.8];
    let close = vec![0.0, 100.5];
    let wick_raw = 0.30 / NEXT_BAR_SL_MULTIPLIER;
    let kf_wicks = vec![wick_raw, 0.0];

    let (_long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_wicks_kf_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &kf_wicks,
            NEXT_BAR_SL_MULTIPLIER,
            Some(0.25),
            Direction::Long,
        );

    assert!(rr_long[0].is_finite());
    assert!(
        (rr_long[0] - 1.0).abs() < 1e-6,
        "expected RR to reflect ceil tick-rounded stop distance (got {})",
        rr_long[0]
    );
}

#[test]
fn wicks_kf_uses_current_bar_kf_wicks_for_stop_distance() {
    let open = vec![0.0, 100.0];
    let high = vec![0.0, 101.0];
    let low = vec![0.0, 99.0];
    let close = vec![0.0, 101.0];
    let kf_wicks = vec![1.0, 10.0];

    let (long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_wicks_kf_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &kf_wicks,
            NEXT_BAR_SL_MULTIPLIER,
            None,
            Direction::Long,
        );

    assert!(long[0], "expected long target to be true");
    assert!(
        (rr_long[0] - (1.0 / 1.5)).abs() < 1e-9,
        "expected RR to reflect current-bar kf wick sizing (got {})",
        rr_long[0]
    );
}

#[test]
fn highlow_or_atr_long_hits_tp_before_sl_and_returns_rr() {
    // idx 0 is green => long. Entry at close[0]=100. ATR[0]=1.
    // Stop=min(low[0]=99.5, entry-atr=99.0)=99.0. TP=102.0.
    // The following bar hits TP without touching SL => RR=2.0, label true.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 102.0, 100.0];
    let low = vec![99.5, 99.25, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![1.0, 1.0, 1.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(long[0]);
    assert!(!short[0]);
    assert!((rr_long[0] - 2.0).abs() < 1e-9, "got {}", rr_long[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn highlow_or_atr_tightest_stop_long_hits_tp_before_sl_and_returns_rr() {
    // Same as above, but with the tighter-stop variant:
    // Stop=tighter of low[0]=99.5 vs entry-atr=99.0 => 99.5. TP=102.0.
    // Risk=0.5, reward=2.0 => RR=4.0.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 102.0, 100.0];
    let low = vec![99.5, 99.75, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![1.0, 1.0, 1.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_tightest_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(long[0]);
    assert!(!short[0]);
    assert!((rr_long[0] - 4.0).abs() < 1e-9, "got {}", rr_long[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn highlow_1r_long_uses_signal_low_only_and_tp_is_1r() {
    // idx 0 is green => long. Entry at close[0]=100.
    // Stop=low[0]=99 => risk=1. TP=entry+risk=101.
    // The following bar opens above TP => gap-fill at open => RR=(111-100)/1=11.
    let open = vec![99.0, 111.0];
    let high = vec![101.0, 111.0];
    let low = vec![99.0, 110.0];
    let close = vec![100.0, 111.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_1r_targets_and_rr(&open, &high, &low, &close, None, None, Direction::Both);

    assert!(long[0]);
    assert!(!short[0]);
    assert!((rr_long[0] - 11.0).abs() < 1e-9, "got {}", rr_long[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn two_x_atr_tp_atr_stop_long_uses_entry_minus_atr_only_for_stop() {
    // idx 0 is green => long. Entry at close[0]=100. ATR=2.
    // Stop=98. TP=104. the following bar hits TP => RR=2.0.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 104.0, 100.0];
    let low = vec![80.0, 99.0, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![2.0, 2.0, 2.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_2x_atr_tp_atr_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(long[0]);
    assert!(!short[0]);
    assert!((rr_long[0] - 2.0).abs() < 1e-9, "got {}", rr_long[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn two_x_atr_tp_atr_stop_risk_uses_tick_rounded_stop_distance() {
    let open = vec![100.0, 100.4];
    let high = vec![100.2, 100.75];
    let low = vec![99.9, 100.3];
    let close = vec![100.1, 100.5];
    let atr = vec![0.30, 0.30];

    let target = compute_2x_atr_tp_atr_stop_target_resolution(
        &open,
        &high,
        &low,
        &close,
        &atr,
        Some(0.25),
        None,
        Direction::Long,
    );

    assert!(target.long[0]);
    let risk_long = target.risk_long_values();
    assert!(
        (risk_long[0] - 0.35).abs() < 1e-9,
        "expected risk from rounded stop, got {}",
        risk_long[0]
    );
    assert!(
        (risk_long[0] - atr[0]).abs() > 1e-9,
        "risk should not fall back to raw ATR when tick rounding changes the stop"
    );
    assert!(
        (target.rr_long[0] - ((100.75 - 100.1) / 0.35)).abs() < 1e-9,
        "RR should use the same realized risk as contract sizing"
    );
}

#[test]
fn two_x_atr_tp_atr_stop_short_risk_uses_tick_rounded_stop_distance() {
    let open = vec![100.2, 99.5];
    let high = vec![100.2, 99.8];
    let low = vec![100.0, 99.45];
    let close = vec![100.1, 99.3];
    let atr = vec![0.30, 0.30];

    let target = compute_2x_atr_tp_atr_stop_target_resolution(
        &open,
        &high,
        &low,
        &close,
        &atr,
        Some(0.25),
        None,
        Direction::Short,
    );

    assert!(target.short[0]);
    let risk_short = target.risk_short_values();
    assert!(
        (risk_short[0] - 0.40).abs() < 1e-9,
        "expected risk from rounded stop, got {}",
        risk_short[0]
    );
    assert!(
        (risk_short[0] - atr[0]).abs() > 1e-9,
        "risk should not fall back to raw ATR when tick rounding changes the stop"
    );
    assert!(
        (target.rr_short[0] - ((100.1 - 99.5) / 0.40)).abs() < 1e-9,
        "RR should use the same realized risk as contract sizing"
    );
}

#[test]
fn highlow_1r_short_uses_signal_high_only_and_tp_is_1r() {
    // idx 0 is red => short. Entry at close[0]=100.
    // Stop=high[0]=101 => risk=1. TP=entry-risk=99.
    // The following bar opens below TP => gap-fill at open => RR=(100-89)/1=11.
    let open = vec![101.0, 89.0];
    let high = vec![101.0, 100.0];
    let low = vec![99.0, 89.0];
    let close = vec![100.0, 90.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_1r_targets_and_rr(&open, &high, &low, &close, None, None, Direction::Both);

    assert!(short[0]);
    assert!((rr_short[0] - 11.0).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn two_x_atr_tp_atr_stop_short_uses_entry_plus_atr_only_for_stop() {
    // idx 0 is red => short. Entry at close[0]=100. ATR=2.
    // Stop=102. TP=96. the following bar hits TP => RR=2.0.
    let open = vec![101.0, 100.0, 100.0];
    let high = vec![101.0, 101.0, 100.0];
    let low = vec![99.0, 96.0, 100.0];
    let close = vec![100.0, 99.0, 100.0];
    let atr = vec![2.0, 2.0, 2.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_2x_atr_tp_atr_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(short[0]);
    assert!((rr_short[0] - 2.0).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn three_x_atr_tp_atr_stop_long_tp_is_3x_atr() {
    // idx 0 is green => long. Entry at close[0]=100. ATR=2.
    // Stop=98. TP=106. the following bar hits TP => RR=3.0.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 106.0, 100.0];
    let low = vec![80.0, 99.0, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![2.0, 2.0, 2.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_3x_atr_tp_atr_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(long[0]);
    assert!(!short[0]);
    assert!((rr_long[0] - 3.0).abs() < 1e-9, "got {}", rr_long[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn three_x_atr_tp_atr_stop_short_tp_is_3x_atr() {
    // idx 0 is red => short. Entry at close[0]=100. ATR=2.
    // Stop=102. TP=94. the following bar hits TP => RR=3.0.
    let open = vec![101.0, 100.0, 100.0];
    let high = vec![101.0, 101.0, 100.0];
    let low = vec![99.0, 94.0, 100.0];
    let close = vec![100.0, 99.0, 100.0];
    let atr = vec![2.0, 2.0, 2.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_3x_atr_tp_atr_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(short[0]);
    assert!((rr_short[0] - 3.0).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn atr_tp_atr_stop_long_tp_is_1x_atr() {
    // idx 0 is green => long. Entry at close[0]=100. ATR=2.
    // Stop=98. TP=102. the following bar hits TP => RR=1.0.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 102.0, 100.0];
    let low = vec![80.0, 99.0, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![2.0, 2.0, 2.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_atr_tp_atr_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(long[0]);
    assert!(!short[0]);
    assert!((rr_long[0] - 1.0).abs() < 1e-9, "got {}", rr_long[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn atr_tp_atr_stop_short_tp_is_1x_atr() {
    // idx 0 is red => short. Entry at close[0]=100. ATR=2.
    // Stop=102. TP=98. the following bar hits TP => RR=1.0.
    let open = vec![101.0, 100.0, 100.0];
    let high = vec![101.0, 101.0, 100.0];
    let low = vec![99.0, 98.0, 100.0];
    let close = vec![100.0, 99.0, 100.0];
    let atr = vec![2.0, 2.0, 2.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_atr_tp_atr_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(short[0]);
    assert!((rr_short[0] - 1.0).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn highlow_sl_2x_atr_tp_rr_gt_1_long_hits_tp_when_rr_at_tp_gt_1() {
    // idx 0 is green => long. Entry=close[0]=100. Stop=low[0]=99 => risk=1.
    // ATR=1 => TP=102 => RR_at_tp=2 (>1) => trade allowed and TP hit.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 102.0, 100.0];
    let low = vec![99.0, 99.25, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![1.0, 1.0, 1.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_sl_2x_atr_tp_rr_gt_1_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(long[0]);
    assert!(!short[0]);
    assert!((rr_long[0] - 2.0).abs() < 1e-9, "got {}", rr_long[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn highlow_sl_2x_atr_tp_rr_gt_1_long_is_rejected_when_rr_at_tp_is_1_after_tick_rounding() {
    // idx 0 is green => long. Entry=100. Stop=97.5 => risk=2.5.
    // ATR=1.25 => TP=102.5. With tick_size=0.25, stop and TP are already on-grid.
    // RR_at_tp=(102.5-100)/2.5 = 1.0 => strict gate (>1) rejects.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 103.0, 100.0];
    let low = vec![97.5, 99.0, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![1.25, 1.25, 1.25];

    let (long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_sl_2x_atr_tp_rr_gt_1_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            Some(0.25),
            None,
            Direction::Both,
        );

    assert!(!long[0]);
    assert!(rr_long[0].is_nan());
}

#[test]
fn highlow_sl_1x_atr_tp_rr_gt_1_short_hits_tp_when_rr_at_tp_gt_1() {
    // idx 0 is red => short. Entry=100. Stop=high[0]=101 => risk=1.
    // ATR=2 => TP=98 => RR_at_tp=2 (>1) => trade allowed and TP hit.
    let open = vec![101.0, 100.0, 100.0];
    let high = vec![101.0, 100.75, 100.0];
    let low = vec![99.0, 98.0, 100.0];
    let close = vec![100.0, 99.0, 100.0];
    let atr = vec![2.0, 2.0, 2.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_sl_1x_atr_tp_rr_gt_1_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(short[0]);
    assert!((rr_short[0] - 2.0).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn highlow_sl_1x_atr_tp_rr_gt_1_short_is_rejected_when_rr_at_tp_is_1_after_tick_rounding() {
    // idx 0 is red => short. Entry=100. Stop=101 => risk=1.
    // ATR=1 => TP=99. With tick_size=0.25, stop and TP are already on-grid.
    // RR_at_tp=(100-99)/1 = 1.0 => strict gate (>1) rejects.
    let open = vec![101.0, 100.0, 100.0];
    let high = vec![101.0, 101.0, 100.0];
    let low = vec![99.0, 98.0, 100.0];
    let close = vec![100.0, 99.0, 100.0];
    let atr = vec![1.0, 1.0, 1.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_sl_1x_atr_tp_rr_gt_1_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            Some(0.25),
            None,
            Direction::Both,
        );

    assert!(!short[0]);
    assert!(rr_short[0].is_nan());
}

#[test]
fn highlow_or_atr_long_sl_dominates_when_both_tp_and_sl_touch_same_bar() {
    // Same setup as above, but next bar touches both TP and SL.
    // Conservative ordering: SL dominates => RR=-1 and label false.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 102.0, 100.0];
    let low = vec![99.5, 98.0, 100.0];
    let close = vec![100.0, 101.0, 100.0];
    let atr = vec![1.0, 1.0, 1.0];

    let (long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(!long[0]);
    assert!((rr_long[0] + 1.0).abs() < 1e-9);
}

#[test]
fn highlow_or_atr_short_hits_tp_before_sl_and_tick_rounds_prices() {
    // idx 0 is red => short. Entry at close[0]=99.0. ATR=1.03.
    // Stop=max(high[0]=99.5, entry+atr=100.03)=100.03 => ceil tick(0.25)=100.25.
    // TP=entry-2*atr=96.94 => floor tick=96.75.
    // Risk=1.25, reward=2.25 => RR=1.8.
    let open = vec![100.0, 99.0, 99.0];
    let high = vec![99.5, 100.0, 99.0];
    let low = vec![98.5, 96.5, 99.0];
    let close = vec![99.0, 98.0, 99.0];
    let atr = vec![1.03, 1.03, 1.03];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            Some(0.25),
            None,
            Direction::Both,
        );

    assert!(short[0]);
    assert!((rr_short[0] - 1.8).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn highlow_or_atr_tightest_stop_short_hits_tp_before_sl_and_tick_rounds_prices() {
    // Same as above, but with the tighter-stop variant:
    // Stop=tighter of high[0]=99.5 vs entry+atr=100.03 => 99.5 => ceil tick=99.50.
    // Risk=0.5, reward=2.25 => RR=4.5.
    let open = vec![100.0, 99.0, 99.0];
    let high = vec![99.5, 99.25, 99.0];
    let low = vec![98.5, 96.5, 99.0];
    let close = vec![99.0, 98.0, 99.0];
    let atr = vec![1.03, 1.03, 1.03];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_tightest_stop_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            Some(0.25),
            None,
            Direction::Both,
        );

    assert!(short[0]);
    assert!((rr_short[0] - 4.5).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn highlow_or_atr_doji_signal_has_no_trade() {
    let open = vec![100.0, 100.0, 100.0];
    let high = vec![101.0, 101.0, 101.0];
    let low = vec![99.0, 99.0, 99.0];
    let close = vec![100.0, 100.5, 100.0];
    let atr = vec![1.0, 1.0, 1.0];

    let (long, short, rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(!long[0]);
    assert!(!short[0]);
    assert!(rr_long[0].is_nan());
    assert!(rr_short[0].is_nan());
}

#[test]
fn highlow_or_atr_long_gap_below_stop_fills_at_open() {
    // idx 0 is green => long. Entry at close[0]=100. ATR[0]=1.
    // Stop=tighter of low[0]=99.0 vs entry-atr=99.0 => 99.0. TP=102.0.
    // The following bar opens below stop at 98.5 => fill at open => RR=-1.5, label false.
    let open = vec![99.0, 98.5];
    let high = vec![100.5, 99.0];
    let low = vec![99.0, 98.0];
    let close = vec![100.0, 98.75];
    let atr = vec![1.0, 1.0];

    let (long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(!long[0]);
    assert!((rr_long[0] + 1.5).abs() < 1e-9, "got {}", rr_long[0]);
}

#[test]
fn highlow_or_atr_long_gap_above_tp_fills_at_open() {
    // idx 0 is green => long. Entry at close[0]=100. ATR[0]=1.
    // Stop=99.0. TP=102.0.
    // The following bar opens above TP at 103.0 => fill at open => RR=3.0, label true.
    let open = vec![99.0, 103.0];
    let high = vec![100.5, 104.0];
    let low = vec![99.0, 102.5];
    let close = vec![100.0, 103.5];
    let atr = vec![1.0, 1.0];

    let (long, _short, rr_long, _rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(long[0]);
    assert!((rr_long[0] - 3.0).abs() < 1e-9, "got {}", rr_long[0]);
}

#[test]
fn highlow_or_atr_short_gap_above_stop_fills_at_open() {
    // idx 0 is red => short. Entry at close[0]=100. ATR[0]=1.
    // Stop=tighter of high[0]=101.0 vs entry+atr=101.0 => 101.0. TP=98.0.
    // The following bar opens above stop at 102.0 => fill at open => RR=-2.0, label false.
    let open = vec![101.0, 102.0];
    let high = vec![101.0, 102.5];
    let low = vec![99.5, 99.0];
    let close = vec![100.0, 101.5];
    let atr = vec![1.0, 1.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(!short[0]);
    assert!((rr_short[0] + 2.0).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn highlow_or_atr_short_gap_below_tp_fills_at_open() {
    // idx 0 is red => short. Entry at close[0]=100. ATR[0]=1.
    // Stop=101.0. TP=98.0.
    // The following bar opens below TP at 97.0 => fill at open => RR=3.0, label true.
    let open = vec![101.0, 97.0];
    let high = vec![101.0, 100.0];
    let low = vec![99.5, 96.5];
    let close = vec![100.0, 98.0];
    let atr = vec![1.0, 1.0];

    let (_long, short, _rr_long, rr_short, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );

    assert!(short[0]);
    assert!((rr_short[0] - 3.0).abs() < 1e-9, "got {}", rr_short[0]);
}

#[test]
fn highlow_or_atr_forces_exit_at_cutoff_close_when_tp_after_cutoff() {
    // idx 0 is green => long. Entry at close[0]=100. ATR[0]=1.
    // Stop=tighter of low[0]=99.0 vs entry-atr=99.0 => 99.0. TP=102.0.
    // TP is only reached on bar 2, but with a cutoff horizon at bar 1
    // we force-exit at close[1]=100.5 => RR=0.5 and label false.
    let open = vec![99.0, 100.0, 100.0];
    let high = vec![100.5, 101.0, 102.0];
    let low = vec![99.0, 99.5, 100.0];
    let close = vec![100.0, 100.5, 101.0];
    let atr = vec![1.0, 1.0, 1.0];

    let (long_full, _short_full, rr_long_full, _rr_short_full, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            None,
            Direction::Both,
        );
    assert!(long_full[0]);
    assert!((rr_long_full[0] - 2.0).abs() < 1e-9);

    let (long_cut, _short_cut, rr_long_cut, _rr_short_cut, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            Some(1),
            Direction::Both,
        );
    assert!(!long_cut[0]);
    assert!(
        (rr_long_cut[0] - 0.5).abs() < 1e-9,
        "got {}",
        rr_long_cut[0]
    );
}

#[test]
fn highlow_or_atr_populates_rr_after_cutoff_horizon_for_post_period_entries() {
    // Entries after the cutoff still use the full prepared-data horizon.
    let open = vec![99.0, 100.0, 99.0, 100.0];
    let high = vec![100.5, 101.0, 100.5, 102.0];
    let low = vec![99.0, 99.5, 99.0, 99.5];
    let close = vec![100.0, 100.5, 100.0, 101.0];
    let atr = vec![1.0, 1.0, 1.0, 1.0];

    let (long_cut, _short_cut, rr_long_cut, _rr_short_cut, _exit_i_long, _exit_i_short) =
        compute_highlow_or_atr_targets_and_rr(
            &open,
            &high,
            &low,
            &close,
            &atr,
            None,
            Some(1),
            Direction::Both,
        );

    assert!(long_cut[2]);
    assert!(
        (rr_long_cut[2] - 2.0).abs() < 1e-9,
        "got {}",
        rr_long_cut[2]
    );
}

#[test]
fn tribar_variations_split_directionally() {
    // Construct a 5-bar series:
    // - idx 2 triggers bullish tribar + tribar_hl (green close breaks prior two highs)
    // - idx 4 triggers bearish tribar + tribar_hl (red close breaks prior two lows)
    let prices = PriceSeries {
        open: vec![100.0, 101.0, 102.0, 104.0, 103.0],
        high: vec![102.0, 103.0, 105.0, 106.0, 104.0],
        low: vec![99.0, 100.0, 101.0, 102.0, 99.0],
        close: vec![101.0, 102.0, 104.0, 103.0, 100.0],
    };
    let derived = DerivedMetrics::new(&prices);
    let mut bools: HashMap<&'static str, Vec<bool>> = HashMap::new();
    let mut floats: HashMap<&'static str, Vec<f64>> = HashMap::new();

    candle_features(&prices, &derived, &mut bools, &mut floats);

    let tribar_green = bools.get("is_tribar_green").unwrap();
    let tribar_red = bools.get("is_tribar_red").unwrap();
    let tribar_hl_green = bools.get("is_tribar_hl_green").unwrap();
    let tribar_hl_red = bools.get("is_tribar_hl_red").unwrap();
    let tribar = bools.get("is_tribar").unwrap();
    let tribar_hl = bools.get("is_tribar_hl").unwrap();

    assert!(tribar_green[2]);
    assert!(!tribar_red[2]);
    assert!(tribar_hl_green[2]);
    assert!(!tribar_hl_red[2]);
    assert!(tribar[2]);
    assert!(tribar_hl[2]);

    assert!(!tribar_green[4]);
    assert!(tribar_red[4]);
    assert!(!tribar_hl_green[4]);
    assert!(tribar_hl_red[4]);
    assert!(tribar[4]);
    assert!(tribar_hl[4]);
}

#[test]
fn atr_column_survives_nan_drop_and_matches_full_history() {
    // We want ATR values used by targets to be computed on the full
    // history and then filtered by the NaN-drop mask, rather than
    // being recomputed after trimming (which would reset the RMA state).
    let len = 210usize;
    let mut open = Vec::with_capacity(len);
    let mut high = Vec::with_capacity(len);
    let mut low = Vec::with_capacity(len);
    let mut close = Vec::with_capacity(len);
    for i in 0..len {
        let base = 100.0 + (i as f64) * 0.1;
        let wiggle = ((i % 7) as f64) * 0.05;
        let c = base + wiggle;
        let o = c - 0.2;
        let range_up = 0.3 + ((i % 5) as f64) * 0.02;
        let range_dn = 0.25 + ((i % 3) as f64) * 0.03;
        open.push(o);
        close.push(c);
        high.push(c + range_up);
        low.push(o - range_dn);
    }

    let prices = PriceSeries {
        open: open.clone(),
        high: high.clone(),
        low: low.clone(),
        close: close.clone(),
    };
    let derived_full = DerivedMetrics::new(&prices);
    let sma200_start = derived_full
        .sma200
        .iter()
        .position(|v| v.is_finite())
        .expect("sma200 should become finite for len >= 200");

    // Run the standard engineering pipeline with NaN-drop.
    let df = DataFrame::new_infer_height(vec![
        Series::new("open".into(), open).into(),
        Series::new("high".into(), high).into(),
        Series::new("low".into(), low).into(),
        Series::new("close".into(), close).into(),
    ])
    .unwrap();
    let mut engineer = FeatureEngineer { frame: df.clone() };
    engineer.compute_features_with_options(true).unwrap();

    // After NaN-drop, the first remaining row should line up with the first finite SMA200,
    // and ATR should match the full-history ATR at those original indices.
    let engineered_atr = column_with_nans(&engineer.frame, "atr").unwrap();
    let actual_start = len - engineered_atr.len();
    assert!(
        actual_start >= sma200_start,
        "expected NaN-drop start ({}) to be >= SMA200 warmup start ({})",
        actual_start,
        sma200_start
    );
    for (offset, &atr_val) in engineered_atr.iter().enumerate() {
        let orig_idx = actual_start + offset;
        let expected = derived_full.atr[orig_idx];
        assert!(
            (atr_val - expected).abs() < 1e-12,
            "ATR mismatch at orig_idx={}: got {}, expected {}",
            orig_idx,
            atr_val,
            expected
        );
    }

    // Demonstrate why recomputing ATR after trimming would be wrong:
    // the first ATR value would reset to the first TR of the trimmed slice.
    let trimmed_prices = PriceSeries {
        open: df
            .column("open")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .skip(actual_start)
            .collect(),
        high: df
            .column("high")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .skip(actual_start)
            .collect(),
        low: df
            .column("low")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .skip(actual_start)
            .collect(),
        close: df
            .column("close")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .skip(actual_start)
            .collect(),
    };
    let derived_trim = DerivedMetrics::new(&trimmed_prices);
    let full_first = derived_full.atr[actual_start];
    let trim_first = derived_trim.atr[0];
    assert!(
        (full_first - trim_first).abs() > 1e-6,
        "expected trimmed ATR to differ at first kept bar (full={}, trim={})",
        full_first,
        trim_first
    );
}
