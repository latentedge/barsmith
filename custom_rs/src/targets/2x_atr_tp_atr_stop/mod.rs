use anyhow::Result;
use barsmith_rs::config::Config;

use crate::targets::common::{
    attach::{TargetFrame, attach_atr_stop},
    barrier::compute_2x_atr_tp_atr_stop_target_resolution,
};

pub(crate) const ID: &str = "2x_atr_tp_atr_stop";
pub(crate) const SUPPORTS_BOTH_CANONICAL: bool = false;
pub(crate) const DEFAULT_STOP_DISTANCE_COLUMN: Option<&str> = Some("2x_atr_tp_atr_stop_risk");

pub(crate) fn attach(frame: &mut TargetFrame<'_>, config: &Config) -> Result<()> {
    attach_atr_stop(
        frame,
        config,
        ID,
        compute_2x_atr_tp_atr_stop_target_resolution,
    )
}
