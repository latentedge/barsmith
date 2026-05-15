pub(crate) mod common;
pub mod registry;

pub(crate) mod highlow_1r;
pub(crate) mod highlow_or_atr;
pub(crate) mod highlow_or_atr_tightest_stop;
pub(crate) mod highlow_sl_1x_atr_tp_rr_gt_1;
pub(crate) mod highlow_sl_2x_atr_tp_rr_gt_1;
pub(crate) mod next_bar_color_and_wicks;
pub(crate) mod tribar_4h_2atr;
pub(crate) mod wicks_kf;

#[path = "2x_atr_tp_atr_stop/mod.rs"]
pub(crate) mod two_x_atr_tp_atr_stop;

#[path = "3x_atr_tp_atr_stop/mod.rs"]
pub(crate) mod three_x_atr_tp_atr_stop;

#[path = "atr_tp_atr_stop/mod.rs"]
pub(crate) mod atr_tp_atr_stop;
