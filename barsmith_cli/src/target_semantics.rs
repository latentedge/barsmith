use anyhow::{Result, anyhow};
use barsmith_rs::config::{Direction, PositionSizingMode};

pub(crate) fn normalize_target(target: &str) -> String {
    match target {
        "atr_stop" => "2x_atr_tp_atr_stop".to_string(),
        _ => target.to_string(),
    }
}

pub(crate) fn inferred_stop_distance_column(target: &str) -> Option<String> {
    canonical_atr_stop_target(target).map(|target| format!("{target}_risk"))
}

pub(crate) fn reject_ambiguous_direction(target: &str, direction: Direction) -> Result<()> {
    if matches!(direction, Direction::Both) {
        if let Some(canonical) = canonical_atr_stop_target(target) {
            return Err(anyhow!(
                "--direction both is not supported for canonical target {canonical}. Run separate long and short searches, or use --direction long / --direction short."
            ));
        }
    }
    Ok(())
}

pub(crate) fn reject_ambiguous_direction_label(
    target: &str,
    direction: Option<&str>,
) -> Result<()> {
    if matches!(direction.map(str::trim), Some(label) if label.eq_ignore_ascii_case("both")) {
        if let Some(canonical) = canonical_atr_stop_target(target) {
            return Err(anyhow!(
                "direction 'both' is not supported for canonical target {canonical}. Run separate long and short searches, or use direction 'long' / 'short'."
            ));
        }
    }
    Ok(())
}

pub(crate) fn risk_model(
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

fn canonical_atr_stop_target(target: &str) -> Option<&'static str> {
    match target {
        "2x_atr_tp_atr_stop" | "atr_stop" => Some("2x_atr_tp_atr_stop"),
        "3x_atr_tp_atr_stop" => Some("3x_atr_tp_atr_stop"),
        "atr_tp_atr_stop" => Some("atr_tp_atr_stop"),
        _ => None,
    }
}

fn canonical_risk_column(column: &str) -> Option<&'static str> {
    match column {
        "2x_atr_tp_atr_stop_risk" => Some("2x_atr_tp_atr_stop"),
        "3x_atr_tp_atr_stop_risk" => Some("3x_atr_tp_atr_stop"),
        "atr_tp_atr_stop_risk" => Some("atr_tp_atr_stop"),
        _ => None,
    }
}
