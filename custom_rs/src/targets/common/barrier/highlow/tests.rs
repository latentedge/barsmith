use super::*;

#[test]
fn target_resolution_matches_linear_oracle() {
    let (open, high, low, close, atr) = target_fixture(512);
    let cases = [
        (
            HighlowOrAtrStopMode::Wide,
            HighlowOrAtrTpMode::AtrMultiple(2.0),
            None,
        ),
        (
            HighlowOrAtrStopMode::HighlowOnly,
            HighlowOrAtrTpMode::RiskMultiple(1.0),
            None,
        ),
        (
            HighlowOrAtrStopMode::AtrOnly,
            HighlowOrAtrTpMode::AtrMultiple(2.0),
            None,
        ),
        (
            HighlowOrAtrStopMode::AtrOnly,
            HighlowOrAtrTpMode::AtrMultiple(3.0),
            None,
        ),
        (
            HighlowOrAtrStopMode::Tightest,
            HighlowOrAtrTpMode::AtrMultiple(2.0),
            None,
        ),
        (
            HighlowOrAtrStopMode::HighlowOnly,
            HighlowOrAtrTpMode::AtrMultiple(2.0),
            Some(1.0),
        ),
    ];
    let directions = [Direction::Long, Direction::Short, Direction::Both];
    let tick_sizes = [None, Some(0.25)];
    let cutoffs = [None, Some(127), Some(300)];

    for (stop_mode, tp_mode, min_tp_rr) in cases {
        for direction in directions {
            for tick_size in tick_sizes {
                for cutoff in cutoffs {
                    let expected = linear_highlow_or_atr_targets_and_rr(
                        &open, &high, &low, &close, &atr, tick_size, cutoff, direction, stop_mode,
                        tp_mode, min_tp_rr,
                    );
                    let actual = compute_highlow_or_atr_targets_and_rr_with_stop_mode(
                        &open, &high, &low, &close, &atr, tick_size, cutoff, direction, stop_mode,
                        tp_mode, min_tp_rr, true,
                    );
                    assert_target_output_eq(
                        &actual, &expected, stop_mode, tp_mode, direction, tick_size, cutoff,
                    );
                }
            }
        }
    }
}

#[test]
fn target_resolution_covers_gap_both_touch_and_cutoff_edges() {
    let long_gap = compute_2x_atr_tp_atr_stop_targets_and_rr(
        &[100.0, 98.0],
        &[101.5, 99.0],
        &[99.5, 97.5],
        &[101.0, 98.5],
        &[2.0, 2.0],
        None,
        None,
        Direction::Long,
    );
    assert!(!long_gap.0[0]);
    assert_close(long_gap.2[0], -1.5, "long gap stop RR");
    assert_eq!(long_gap.4[0], Some(1));

    let short_gap = compute_2x_atr_tp_atr_stop_targets_and_rr(
        &[101.0, 103.0],
        &[101.5, 103.5],
        &[99.5, 102.5],
        &[100.0, 102.75],
        &[2.0, 2.0],
        None,
        None,
        Direction::Short,
    );
    assert!(!short_gap.1[0]);
    assert_close(short_gap.3[0], -1.5, "short gap stop RR");
    assert_eq!(short_gap.5[0], Some(1));

    let both_touched = compute_2x_atr_tp_atr_stop_targets_and_rr(
        &[100.0, 101.0],
        &[101.5, 104.0],
        &[99.5, 99.0],
        &[101.0, 101.5],
        &[1.0, 1.0],
        None,
        None,
        Direction::Long,
    );
    assert!(
        !both_touched.0[0],
        "stop loss wins when a future bar touches both sides"
    );
    assert_close(both_touched.2[0], -1.0, "long both-touch RR");
    assert_eq!(both_touched.4[0], Some(1));

    let cutoff_exit = compute_2x_atr_tp_atr_stop_targets_and_rr(
        &[100.0, 101.1, 101.4],
        &[101.2, 101.5, 101.8],
        &[99.8, 100.9, 101.0],
        &[101.0, 101.2, 101.5],
        &[2.0, 2.0, 2.0],
        None,
        Some(2),
        Direction::Long,
    );
    assert!(!cutoff_exit.0[0]);
    assert_close(cutoff_exit.2[0], 0.25, "cutoff exit RR");
    assert_eq!(cutoff_exit.4[0], Some(2));
}

fn target_fixture(rows: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut open = Vec::with_capacity(rows);
    let mut high = Vec::with_capacity(rows);
    let mut low = Vec::with_capacity(rows);
    let mut close = Vec::with_capacity(rows);
    let mut atr = Vec::with_capacity(rows);
    let mut price = 4_900.0;

    for idx in 0..rows {
        let drift = ((idx * 37 % 23) as f64 - 11.0) * 0.31;
        let impulse = match idx % 53 {
            0 => 5.5,
            1 => -4.75,
            _ => 0.0,
        };
        let o = price + drift * 0.4;
        let c = o + drift + impulse;
        let wick = 0.75 + (idx % 13) as f64 * 0.17;
        let mut h = o.max(c) + wick;
        let mut l = o.min(c) - wick * 1.1;
        let mut a = 2.5 + (idx % 19) as f64 * 0.11;
        let mut open_value = o;
        let mut close_value = c;

        if idx % 89 == 0 {
            open_value = f64::NAN;
        }
        if idx % 97 == 0 {
            close_value = f64::NAN;
        }
        if idx % 83 == 0 {
            h = f64::NAN;
        }
        if idx % 79 == 0 {
            l = f64::NAN;
        }
        if idx % 101 == 0 {
            a = f64::NAN;
        }

        open.push(open_value);
        high.push(h);
        low.push(l);
        close.push(close_value);
        atr.push(a);
        price = c;
    }

    (open, high, low, close, atr)
}

fn linear_highlow_or_atr_targets_and_rr(
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    atr: &[f64],
    tick_size: Option<f64>,
    resolve_end_idx: Option<usize>,
    direction: Direction,
    stop_mode: HighlowOrAtrStopMode,
    tp_mode: HighlowOrAtrTpMode,
    min_tp_rr: Option<f64>,
) -> TargetResolution {
    let requires_atr = matches!(
        stop_mode,
        HighlowOrAtrStopMode::Wide | HighlowOrAtrStopMode::AtrOnly | HighlowOrAtrStopMode::Tightest
    ) || matches!(tp_mode, HighlowOrAtrTpMode::AtrMultiple(_));

    let mut len = open.len().min(high.len()).min(low.len()).min(close.len());
    if requires_atr {
        len = len.min(atr.len());
    }
    let mut out = TargetResolution::new(len, true);
    if len < 2 {
        return out;
    }

    let cutoff_horizon = resolve_end_idx.unwrap_or(len - 1).min(len - 1);
    let want_long = matches!(direction, Direction::Long | Direction::Both);
    let want_short = matches!(direction, Direction::Short | Direction::Both);

    for idx in 0..(len - 1) {
        let cap_to_cutoff = resolve_end_idx.is_some() && idx <= cutoff_horizon;
        let local_horizon = if cap_to_cutoff {
            cutoff_horizon
        } else {
            len - 1
        };
        if idx >= local_horizon {
            continue;
        }

        let open_idx = open[idx];
        let close_idx = close[idx];
        let high_idx = high[idx];
        let low_idx = low[idx];
        let atr_idx = if requires_atr { atr[idx] } else { f64::NAN };

        if !open_idx.is_finite()
            || !close_idx.is_finite()
            || !high_idx.is_finite()
            || !low_idx.is_finite()
            || (requires_atr && !atr_idx.is_finite())
        {
            continue;
        }

        let body = close_idx - open_idx;
        if body.abs() <= f64::EPSILON {
            continue;
        }
        let entry = close_idx;

        if body > 0.0 {
            if !want_long {
                continue;
            }

            let stop_raw = highlow_or_atr_stop_long(entry, low_idx, atr_idx, stop_mode);
            let stop = if let Some(ts) = tick_size {
                quantize_price_to_tick(stop_raw, ts, TickRoundMode::Floor)
            } else {
                stop_raw
            };
            if !stop.is_finite() || stop >= entry {
                continue;
            }
            let risk = entry - stop;
            if risk <= SMALL_DIVISOR {
                continue;
            }

            let tp_raw = match tp_mode {
                HighlowOrAtrTpMode::AtrMultiple(m) => entry + m * atr_idx,
                HighlowOrAtrTpMode::RiskMultiple(m) => entry + m * risk,
            };
            let tp = if let Some(ts) = tick_size {
                quantize_price_to_tick(tp_raw, ts, TickRoundMode::Ceil)
            } else {
                tp_raw
            };
            if !tp.is_finite() {
                continue;
            }
            if let Some(min_rr) = min_tp_rr {
                let rr_at_tp = (tp - entry) / risk;
                if !rr_at_tp.is_finite() || rr_at_tp <= min_rr {
                    continue;
                }
            }

            let mut rr = f64::NAN;
            let mut hit_tp = false;
            let mut exit_idx = None;
            for j in (idx + 1)..=local_horizon {
                let o = open[j];
                let h = high[j];
                let l = low[j];
                if !h.is_finite() || !l.is_finite() {
                    continue;
                }
                if o.is_finite() {
                    if o <= stop {
                        rr = (o - entry) / risk;
                        exit_idx = Some(j);
                        break;
                    }
                    if o >= tp {
                        rr = (o - entry) / risk;
                        hit_tp = true;
                        exit_idx = Some(j);
                        break;
                    }
                }
                if l <= stop {
                    rr = -1.0;
                    exit_idx = Some(j);
                    break;
                }
                if h >= tp {
                    rr = (tp - entry) / risk;
                    hit_tp = true;
                    exit_idx = Some(j);
                    break;
                }
            }

            if !rr.is_finite() && cap_to_cutoff {
                let exit = close[local_horizon];
                if exit.is_finite() {
                    rr = (exit - entry) / risk;
                    exit_idx = Some(local_horizon);
                }
            }
            if rr.is_finite() {
                out.rr_long[idx] = rr;
                out.long[idx] = hit_tp;
                out.exit_i_long[idx] = exit_idx;
                out.risk_long.as_mut().expect("risk column")[idx] = risk;
            }
        } else {
            if !want_short {
                continue;
            }

            let stop_raw = highlow_or_atr_stop_short(entry, high_idx, atr_idx, stop_mode);
            let stop = if let Some(ts) = tick_size {
                quantize_price_to_tick(stop_raw, ts, TickRoundMode::Ceil)
            } else {
                stop_raw
            };
            if !stop.is_finite() || stop <= entry {
                continue;
            }
            let risk = stop - entry;
            if risk <= SMALL_DIVISOR {
                continue;
            }

            let tp_raw = match tp_mode {
                HighlowOrAtrTpMode::AtrMultiple(m) => entry - m * atr_idx,
                HighlowOrAtrTpMode::RiskMultiple(m) => entry - m * risk,
            };
            let tp = if let Some(ts) = tick_size {
                quantize_price_to_tick(tp_raw, ts, TickRoundMode::Floor)
            } else {
                tp_raw
            };
            if !tp.is_finite() {
                continue;
            }
            if let Some(min_rr) = min_tp_rr {
                let rr_at_tp = (entry - tp) / risk;
                if !rr_at_tp.is_finite() || rr_at_tp <= min_rr {
                    continue;
                }
            }

            let mut rr = f64::NAN;
            let mut hit_tp = false;
            let mut exit_idx = None;
            for j in (idx + 1)..=local_horizon {
                let o = open[j];
                let h = high[j];
                let l = low[j];
                if !h.is_finite() || !l.is_finite() {
                    continue;
                }
                if o.is_finite() {
                    if o >= stop {
                        rr = (entry - o) / risk;
                        exit_idx = Some(j);
                        break;
                    }
                    if o <= tp {
                        rr = (entry - o) / risk;
                        hit_tp = true;
                        exit_idx = Some(j);
                        break;
                    }
                }
                if h >= stop {
                    rr = -1.0;
                    exit_idx = Some(j);
                    break;
                }
                if l <= tp {
                    rr = (entry - tp) / risk;
                    hit_tp = true;
                    exit_idx = Some(j);
                    break;
                }
            }

            if !rr.is_finite() && cap_to_cutoff {
                let exit = close[local_horizon];
                if exit.is_finite() {
                    rr = (entry - exit) / risk;
                    exit_idx = Some(local_horizon);
                }
            }
            if rr.is_finite() {
                out.rr_short[idx] = rr;
                out.short[idx] = hit_tp;
                out.exit_i_short[idx] = exit_idx;
                out.risk_short.as_mut().expect("risk column")[idx] = risk;
            }
        }
    }

    out
}

fn assert_target_output_eq(
    actual: &TargetResolution,
    expected: &TargetResolution,
    stop_mode: HighlowOrAtrStopMode,
    tp_mode: HighlowOrAtrTpMode,
    direction: Direction,
    tick_size: Option<f64>,
    cutoff: Option<usize>,
) {
    let case = format!(
        "stop_mode={stop_mode:?}, tp_mode={tp_mode:?}, direction={direction:?}, tick_size={tick_size:?}, cutoff={cutoff:?}"
    );
    assert_eq!(actual.long, expected.long, "long labels differ for {case}");
    assert_eq!(
        actual.short, expected.short,
        "short labels differ for {case}"
    );
    assert_float_vec_eq(&actual.rr_long, &expected.rr_long, "rr_long", &case);
    assert_float_vec_eq(&actual.rr_short, &expected.rr_short, "rr_short", &case);
    assert_float_vec_eq(
        actual.risk_long_values(),
        expected.risk_long_values(),
        "risk_long",
        &case,
    );
    assert_float_vec_eq(
        actual.risk_short_values(),
        expected.risk_short_values(),
        "risk_short",
        &case,
    );
    assert_eq!(
        actual.exit_i_long, expected.exit_i_long,
        "long exits differ for {case}"
    );
    assert_eq!(
        actual.exit_i_short, expected.exit_i_short,
        "short exits differ for {case}"
    );
}

fn assert_float_vec_eq(actual: &[f64], expected: &[f64], name: &str, case: &str) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{name} length differs for {case}"
    );
    for (idx, (actual_value, expected_value)) in actual.iter().zip(expected.iter()).enumerate() {
        if actual_value.is_nan() && expected_value.is_nan() {
            continue;
        }
        assert!(
            (actual_value - expected_value).abs() <= 1e-10,
            "{name}[{idx}] differs for {case}: actual={actual_value}, expected={expected_value}"
        );
    }
}

fn assert_close(actual: f64, expected: f64, context: &str) {
    assert!(
        (actual - expected).abs() <= 1e-10,
        "{context}: actual={actual}, expected={expected}"
    );
}
