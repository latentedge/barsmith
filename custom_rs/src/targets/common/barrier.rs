#![allow(
    // Target geometry functions use parallel OHLCV slices to avoid allocating
    // structs in hot feature-engineering paths.
    clippy::too_many_arguments,
    clippy::type_complexity
)]

mod highlow;
mod next_bar;
mod resolution;
mod tick;

#[cfg(any(test, feature = "bench-api"))]
pub(crate) use highlow::compute_2x_atr_tp_atr_stop_targets_and_rr;
pub(crate) use highlow::compute_3x_atr_tp_atr_stop_target_resolution;
pub(crate) use highlow::{
    compute_2x_atr_tp_atr_stop_target_resolution, compute_atr_tp_atr_stop_target_resolution,
    compute_highlow_1r_targets_and_rr, compute_highlow_or_atr_targets_and_rr,
    compute_highlow_or_atr_tightest_stop_targets_and_rr,
    compute_highlow_sl_1x_atr_tp_rr_gt_1_targets_and_rr,
    compute_highlow_sl_2x_atr_tp_rr_gt_1_targets_and_rr,
};
#[cfg(test)]
pub(crate) use highlow::{
    compute_3x_atr_tp_atr_stop_targets_and_rr, compute_atr_tp_atr_stop_targets_and_rr,
};
pub(crate) use next_bar::compute_next_bar_targets_and_rr;
#[cfg(test)]
pub(crate) use next_bar::compute_wicks_kf_targets_and_rr;
pub(crate) use resolution::TargetResolution;
#[cfg(test)]
pub(crate) use tick::{TickRoundMode, quantize_distance_to_tick, quantize_price_to_tick};

const SMALL_DIVISOR: f64 = 1e-9;
