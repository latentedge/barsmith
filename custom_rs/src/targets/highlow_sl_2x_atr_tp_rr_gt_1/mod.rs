use anyhow::Result;
use barsmith_rs::config::Config;

use crate::targets::common::{
    attach::{TargetFrame, attach_highlow_with_atr},
    barrier::compute_highlow_sl_2x_atr_tp_rr_gt_1_targets_and_rr,
};

pub(crate) const ID: &str = "highlow_sl_2x_atr_tp_rr_gt_1";
pub(crate) const SUPPORTS_BOTH_CANONICAL: bool = true;
pub(crate) const DEFAULT_STOP_DISTANCE_COLUMN: Option<&str> = None;

pub(crate) fn attach(frame: &mut TargetFrame<'_>, config: &Config) -> Result<()> {
    attach_highlow_with_atr(
        frame,
        config,
        ID,
        compute_highlow_sl_2x_atr_tp_rr_gt_1_targets_and_rr,
    )
}
