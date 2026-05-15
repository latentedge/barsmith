use anyhow::{Result, anyhow};
use barsmith_rs::config::{Config, Direction, PositionSizingMode};

use super::{
    atr_tp_atr_stop, highlow_1r, highlow_or_atr, highlow_or_atr_tightest_stop,
    highlow_sl_1x_atr_tp_rr_gt_1, highlow_sl_2x_atr_tp_rr_gt_1, next_bar_color_and_wicks,
    three_x_atr_tp_atr_stop, tribar_4h_2atr, two_x_atr_tp_atr_stop, wicks_kf,
};
use crate::targets::common::attach::TargetFrame;

type AttachFn = for<'a> fn(&mut TargetFrame<'a>, &Config) -> Result<()>;

#[derive(Debug, Clone, Copy)]
pub struct TargetMetadata {
    pub id: &'static str,
    pub supports_both_canonical: bool,
    pub default_stop_distance_column: Option<&'static str>,
    attach: AttachFn,
}

impl TargetMetadata {
    const fn new(
        id: &'static str,
        supports_both_canonical: bool,
        default_stop_distance_column: Option<&'static str>,
        attach: AttachFn,
    ) -> Self {
        Self {
            id,
            supports_both_canonical,
            default_stop_distance_column,
            attach,
        }
    }
}

pub const TARGETS: &[TargetMetadata] = &[
    TargetMetadata::new(
        next_bar_color_and_wicks::ID,
        next_bar_color_and_wicks::SUPPORTS_BOTH_CANONICAL,
        next_bar_color_and_wicks::DEFAULT_STOP_DISTANCE_COLUMN,
        next_bar_color_and_wicks::attach,
    ),
    TargetMetadata::new(
        wicks_kf::ID,
        wicks_kf::SUPPORTS_BOTH_CANONICAL,
        wicks_kf::DEFAULT_STOP_DISTANCE_COLUMN,
        wicks_kf::attach,
    ),
    TargetMetadata::new(
        highlow_or_atr::ID,
        highlow_or_atr::SUPPORTS_BOTH_CANONICAL,
        highlow_or_atr::DEFAULT_STOP_DISTANCE_COLUMN,
        highlow_or_atr::attach,
    ),
    TargetMetadata::new(
        highlow_1r::ID,
        highlow_1r::SUPPORTS_BOTH_CANONICAL,
        highlow_1r::DEFAULT_STOP_DISTANCE_COLUMN,
        highlow_1r::attach,
    ),
    TargetMetadata::new(
        two_x_atr_tp_atr_stop::ID,
        two_x_atr_tp_atr_stop::SUPPORTS_BOTH_CANONICAL,
        two_x_atr_tp_atr_stop::DEFAULT_STOP_DISTANCE_COLUMN,
        two_x_atr_tp_atr_stop::attach,
    ),
    TargetMetadata::new(
        three_x_atr_tp_atr_stop::ID,
        three_x_atr_tp_atr_stop::SUPPORTS_BOTH_CANONICAL,
        three_x_atr_tp_atr_stop::DEFAULT_STOP_DISTANCE_COLUMN,
        three_x_atr_tp_atr_stop::attach,
    ),
    TargetMetadata::new(
        atr_tp_atr_stop::ID,
        atr_tp_atr_stop::SUPPORTS_BOTH_CANONICAL,
        atr_tp_atr_stop::DEFAULT_STOP_DISTANCE_COLUMN,
        atr_tp_atr_stop::attach,
    ),
    TargetMetadata::new(
        highlow_sl_2x_atr_tp_rr_gt_1::ID,
        highlow_sl_2x_atr_tp_rr_gt_1::SUPPORTS_BOTH_CANONICAL,
        highlow_sl_2x_atr_tp_rr_gt_1::DEFAULT_STOP_DISTANCE_COLUMN,
        highlow_sl_2x_atr_tp_rr_gt_1::attach,
    ),
    TargetMetadata::new(
        highlow_sl_1x_atr_tp_rr_gt_1::ID,
        highlow_sl_1x_atr_tp_rr_gt_1::SUPPORTS_BOTH_CANONICAL,
        highlow_sl_1x_atr_tp_rr_gt_1::DEFAULT_STOP_DISTANCE_COLUMN,
        highlow_sl_1x_atr_tp_rr_gt_1::attach,
    ),
    TargetMetadata::new(
        highlow_or_atr_tightest_stop::ID,
        highlow_or_atr_tightest_stop::SUPPORTS_BOTH_CANONICAL,
        highlow_or_atr_tightest_stop::DEFAULT_STOP_DISTANCE_COLUMN,
        highlow_or_atr_tightest_stop::attach,
    ),
    TargetMetadata::new(
        tribar_4h_2atr::ID,
        tribar_4h_2atr::SUPPORTS_BOTH_CANONICAL,
        tribar_4h_2atr::DEFAULT_STOP_DISTANCE_COLUMN,
        tribar_4h_2atr::attach,
    ),
];

pub fn default_target_id() -> &'static str {
    next_bar_color_and_wicks::ID
}

pub fn normalize_target(target: &str) -> String {
    target.to_string()
}

pub fn metadata_for(target: &str) -> Option<&'static TargetMetadata> {
    TARGETS.iter().find(|metadata| metadata.id == target)
}

pub fn supported_targets() -> Vec<&'static str> {
    TARGETS.iter().map(|metadata| metadata.id).collect()
}

pub fn ensure_supported_target(target: &str) -> Result<()> {
    if metadata_for(target).is_some() {
        return Ok(());
    }
    Err(anyhow!(
        "Unsupported target '{target}'. Supported targets: {}",
        supported_targets().join(", ")
    ))
}

pub(crate) fn attach_target(frame: &mut TargetFrame<'_>, config: &Config) -> Result<()> {
    let metadata = metadata_for(&config.target).ok_or_else(|| {
        anyhow!(
            "Unsupported target '{}'. Supported targets: {}",
            config.target,
            supported_targets().join(", ")
        )
    })?;
    (metadata.attach)(frame, config)
}

pub fn inferred_stop_distance_column(target: &str) -> Option<String> {
    metadata_for(target)
        .and_then(|metadata| metadata.default_stop_distance_column)
        .map(str::to_string)
}

pub fn reject_ambiguous_direction(target: &str, direction: Direction) -> Result<()> {
    if matches!(direction, Direction::Both) {
        if let Some(metadata) = metadata_for(target) {
            if !metadata.supports_both_canonical {
                return Err(anyhow!(
                    "--direction both is not supported for canonical target {}. Run separate long and short searches, or use --direction long / --direction short.",
                    metadata.id
                ));
            }
        }
    }
    Ok(())
}

pub fn reject_ambiguous_direction_label(target: &str, direction: Option<&str>) -> Result<()> {
    if matches!(direction.map(str::trim), Some(label) if label.eq_ignore_ascii_case("both")) {
        if let Some(metadata) = metadata_for(target) {
            if !metadata.supports_both_canonical {
                return Err(anyhow!(
                    "direction 'both' is not supported for canonical target {}. Run separate long and short searches, or use direction 'long' / 'short'.",
                    metadata.id
                ));
            }
        }
    }
    Ok(())
}

pub fn risk_model(
    position_sizing: PositionSizingMode,
    stop_distance_column: Option<&str>,
) -> &'static str {
    match position_sizing {
        PositionSizingMode::Fractional => "fractional_r",
        PositionSizingMode::Contracts => match stop_distance_column {
            Some(column) if canonical_risk_column(column).is_some() => {
                "realized_tick_rounded_target_risk"
            }
            _ => "raw_stop_distance_column",
        },
    }
}

pub fn is_target_output_column(name: &str) -> bool {
    if name == "rr_long" || name == "rr_short" || name.starts_with("rr_") {
        return true;
    }

    TARGETS.iter().any(|metadata| {
        name == metadata.id
            || name
                .strip_prefix(metadata.id)
                .is_some_and(|suffix| TARGET_SUFFIXES.contains(&suffix))
    })
}

fn canonical_risk_column(column: &str) -> Option<&'static str> {
    TARGETS
        .iter()
        .find(|metadata| metadata.default_stop_distance_column == Some(column))
        .map(|metadata| metadata.id)
}

const TARGET_SUFFIXES: &[&str] = &[
    "_long",
    "_short",
    "_eligible",
    "_eligible_long",
    "_eligible_short",
    "_exit_i",
    "_exit_i_long",
    "_exit_i_short",
    "_risk",
    "_risk_long",
    "_risk_short",
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn target_ids_are_unique() {
        let mut seen = HashSet::new();
        for metadata in TARGETS {
            assert!(
                seen.insert(metadata.id),
                "duplicate target id in registry: {}",
                metadata.id
            );
        }
    }

    #[test]
    fn default_risk_columns_are_treated_as_target_outputs() {
        for metadata in TARGETS {
            if let Some(column) = metadata.default_stop_distance_column {
                assert!(
                    is_target_output_column(column),
                    "default risk column should be excluded from the searchable catalog: {column}"
                );
            }
        }
    }

    #[test]
    fn unknown_targets_are_not_supported() {
        assert!(metadata_for("__unknown_target__").is_none());
    }
}
