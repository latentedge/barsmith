use anyhow::Result;
use barsmith_rs::config::Config;

use crate::targets::common::attach::{TargetFrame, attach_next_bar_style};

pub(crate) const ID: &str = "wicks_kf";
pub(crate) const SUPPORTS_BOTH_CANONICAL: bool = true;
pub(crate) const DEFAULT_STOP_DISTANCE_COLUMN: Option<&str> = None;

pub(crate) fn attach(frame: &mut TargetFrame<'_>, config: &Config) -> Result<()> {
    attach_next_bar_style(frame, config, ID, "kf_wicks_smooth")
}
