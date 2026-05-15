use polars::prelude::*;

pub(super) fn boolean_mask_from_series(series: &Column) -> Option<Vec<bool>> {
    match series.dtype() {
        DataType::Boolean => {
            let ca = series.bool().ok()?;
            Some(ca.into_iter().map(|value| value.unwrap_or(false)).collect())
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            if !is_binary_01_series(series) {
                return None;
            }
            let ca = series.i64().ok()?;
            Some(
                ca.into_iter()
                    .map(|value| matches!(value, Some(1)))
                    .collect(),
            )
        }
        DataType::Float32 | DataType::Float64 => {
            if !is_binary_01_series(series) {
                return None;
            }
            let ca = series.f64().ok()?;
            Some(
                ca.into_iter()
                    .map(|value| matches!(value, Some(1.0)))
                    .collect(),
            )
        }
        _ => None,
    }
}

/// Return true for numeric series that are really boolean masks stored as 0/1.
pub(super) fn is_binary_01_series(series: &Column) -> bool {
    match series.dtype() {
        DataType::Boolean => true,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            if let Ok(values) = series.i64() {
                let mut seen = false;
                for value in values.into_iter().flatten() {
                    if value != 0 && value != 1 {
                        return false;
                    }
                    seen = true;
                }
                seen
            } else {
                false
            }
        }
        DataType::Float32 | DataType::Float64 => {
            if let Ok(values) = series.f64() {
                let mut seen = false;
                for value in values.into_iter().flatten() {
                    if value != 0.0 && value != 1.0 {
                        return false;
                    }
                    seen = true;
                }
                seen
            } else {
                false
            }
        }
        _ => false,
    }
}
