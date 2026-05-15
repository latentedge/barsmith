use std::collections::HashMap;

use anyhow::{Result, anyhow};
use barsmith_indicators::atr;
use barsmith_rs::Direction;
use barsmith_rs::config::Config;

use crate::targets::common::attach::{TargetFrame, compute_week_indices};

pub(crate) const ID: &str = "tribar_4h_2atr";
pub(crate) const SUPPORTS_BOTH_CANONICAL: bool = false;
pub(crate) const DEFAULT_STOP_DISTANCE_COLUMN: Option<&str> = None;

pub(crate) fn attach(frame: &mut TargetFrame<'_>, config: &Config) -> Result<()> {
    let file_name = config
        .input_csv
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    if !file_name.contains("_4h") {
        return Err(anyhow!(
            "{ID} target expects input CSV filename to contain '_4h' (got '{}')",
            file_name
        ));
    }

    if !matches!(config.direction, Direction::Long) {
        return Err(anyhow!(
            "{ID} target currently supports only Direction::Long (got {:?})",
            config.direction
        ));
    }

    let open = frame.column_with_nans("open")?;
    let high = frame.column_with_nans("high")?;
    let low = frame.column_with_nans("low")?;
    let close = frame.column_with_nans("close")?;
    let is_tribar = frame.bool_column("is_tribar")?;
    let is_close_above_kf_ma = frame.bool_column("is_close_above_kf_ma")?;
    let timestamps = frame.timestamps()?;

    let len = open.len();
    if high.len() != len
        || low.len() != len
        || close.len() != len
        || is_tribar.len() != len
        || is_close_above_kf_ma.len() != len
        || timestamps.len() != len
    {
        return Err(anyhow!(
            "Inconsistent series lengths when building {ID} target"
        ));
    }

    let atr_values = atr(&high, &low, &close, 14);
    let (week_index, week_end_index) = compute_week_indices(&timestamps);

    let mut label = vec![false; len];
    let mut rr = vec![f64::NAN; len];
    let mut exit_i_long: Vec<Option<i64>> = vec![None; len];
    let exit_i_short: Vec<Option<i64>> = vec![None; len];

    let mut trades_per_week: HashMap<i64, u8> = HashMap::new();
    let mut idx = 0usize;
    const ATR_MULTIPLE: f64 = 2.0;
    const SMALL_DIVISOR: f64 = 1e-9;

    while idx < len {
        let mut advanced = false;

        if is_tribar[idx] && is_close_above_kf_ma[idx] {
            let week = week_index[idx];
            let used = trades_per_week.get(&week).copied().unwrap_or(0);
            if used < 2 {
                let entry = close[idx];
                let atr = atr_values[idx];
                let bar_low = low[idx];

                if entry.is_finite() && atr.is_finite() && bar_low.is_finite() {
                    let atr_stop = entry - atr;
                    let stop = bar_low.min(atr_stop);
                    if stop.is_finite() && stop < entry {
                        let risk = entry - stop;
                        if risk.abs() > SMALL_DIVISOR {
                            let tp = entry + ATR_MULTIPLE * atr;
                            let last_idx = week_end_index[idx];

                            let mut exit_price = close[idx];
                            let mut exit_idx = idx;

                            for j in idx..=last_idx {
                                let h = high[j];
                                let l = low[j];
                                let c = close[j];
                                if !h.is_finite() || !l.is_finite() || !c.is_finite() {
                                    continue;
                                }

                                if l <= stop {
                                    exit_price = stop;
                                    exit_idx = j;
                                    break;
                                }
                                if h >= tp {
                                    exit_price = tp;
                                    exit_idx = j;
                                    break;
                                }

                                if j == last_idx {
                                    exit_price = c;
                                    exit_idx = j;
                                }
                            }

                            let trade_rr = (exit_price - entry) / risk;
                            rr[idx] = trade_rr;
                            label[idx] = trade_rr.is_finite() && trade_rr > 0.0;
                            exit_i_long[idx] = Some(exit_idx as i64);

                            trades_per_week.insert(week, used + 1);
                            idx = exit_idx.saturating_add(1);
                            advanced = true;
                        }
                    }
                }
            }
        }

        if !advanced {
            idx += 1;
        }
    }

    frame.replace_bool_column(ID, label)?;
    frame.replace_float_column("rr_long", rr)?;
    let rr_series = frame.clone_column_as("rr_long", "rr_tribar_4h_2atr")?;
    frame.replace_series(rr_series)?;

    frame.replace_i64_column("tribar_4h_2atr_exit_i_long", exit_i_long)?;
    frame.replace_i64_column("tribar_4h_2atr_exit_i_short", exit_i_short)?;
    let exit_series =
        frame.clone_column_as("tribar_4h_2atr_exit_i_long", "tribar_4h_2atr_exit_i")?;
    frame.replace_series(exit_series)?;

    Ok(())
}
