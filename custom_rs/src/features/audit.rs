use std::collections::HashSet;

use polars::prelude::*;
use tracing::info;

use super::definitions::{BOOLEAN_FEATURES, CONTINUOUS_FEATURES};
use super::is_target_output_column;
use super::series_kind::is_binary_01_series;

pub(super) fn audit_boolean_coverage(df: &DataFrame) {
    let known: HashSet<&str> = BOOLEAN_FEATURES.iter().copied().collect();
    let mut unexpected = Vec::new();

    for series in df.columns() {
        let name = series.name().as_str();
        if known.contains(name) || is_target_output_column(name) {
            continue;
        }
        match series.dtype() {
            DataType::Boolean => unexpected.push(name.to_string()),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                if let Ok(values) = series.i64() {
                    let mut has_zero = false;
                    let mut has_one = false;
                    let mut other = false;
                    for value in values.into_iter().flatten() {
                        match value {
                            0 => has_zero = true,
                            1 => has_one = true,
                            _ => {
                                other = true;
                                break;
                            }
                        }
                    }
                    if !other && (has_zero || has_one) {
                        unexpected.push(name.to_string());
                    }
                }
            }
            _ => {}
        }
    }

    if unexpected.is_empty() {
        return;
    }

    unexpected.sort();
    info!(
        "Found {} boolean features (0/1 values) in dataframe but not in BOOLEAN_FEATURES:",
        unexpected.len()
    );
    for name in &unexpected {
        info!("   - {}", name);
    }
    info!("Add these to BOOLEAN_FEATURES in custom_rs::features if you want them in the catalog");
}

pub(super) fn audit_continuous_coverage(df: &DataFrame) {
    let known_ranges: HashSet<&str> = CONTINUOUS_FEATURES.iter().copied().collect();
    let mut unexpected = Vec::new();

    for series in df.columns() {
        let name = series.name().as_str();
        if known_ranges.contains(name) || is_target_output_column(name) {
            continue;
        }
        match series.dtype() {
            DataType::Float32 | DataType::Float64 => {
                let Some(values) = series.f64().ok() else {
                    continue;
                };
                let mut finite: Vec<f64> = values
                    .into_iter()
                    .flatten()
                    .filter(|value| value.is_finite())
                    .collect();
                if finite.len() < 2 {
                    continue;
                }
                finite.sort_by(f64::total_cmp);
                let n = finite.len();
                let p1 = finite[(0.01 * (n - 1) as f64) as usize];
                let p99 = finite[(0.99 * (n - 1) as f64) as usize];
                let data_min = finite[0];
                let data_max = finite[n - 1];
                unexpected.push((name.to_string(), p1, p99, data_min, data_max));
            }
            _ => {}
        }
    }

    if unexpected.is_empty() {
        return;
    }

    info!(
        "Found {} continuous features in dataframe but not in CONTINUOUS_FEATURES/feature_ranges.json:",
        unexpected.len()
    );
    for (name, p1, p99, data_min, data_max) in &unexpected {
        info!(
            "   - {:<34} P1-P99: [{:10.4}, {:10.4}]  Data: [{:10.4}, {:10.4}]",
            name, p1, p99, data_min, data_max
        );
    }
    info!("Add these to feature_ranges.json if you want to include them in threshold testing");
}

pub(super) fn audit_continuous_definitions(df: &DataFrame) {
    let mut missing = Vec::new();
    let mut non_numeric_or_binary = Vec::new();

    for &name in CONTINUOUS_FEATURES {
        match df.column(name) {
            Ok(series) => match series.dtype() {
                DataType::Float32 | DataType::Float64 => {
                    // Continuous catalog entries should carry real numeric
                    // variation, not boolean 0/1 masks.
                    if is_binary_01_series(series) {
                        non_numeric_or_binary.push((name, series.dtype().clone()));
                    }
                }
                _ => non_numeric_or_binary.push((name, series.dtype().clone())),
            },
            Err(_) => missing.push(name),
        }
    }

    if !missing.is_empty() {
        info!(
            "CONTINUOUS_FEATURES entries missing from engineered dataset (not present as columns):"
        );
        for name in &missing {
            info!("   - {}", name);
        }
        info!("Add these to engineer.rs or remove them from CONTINUOUS_FEATURES");
    }

    if !non_numeric_or_binary.is_empty() {
        info!(
            "CONTINUOUS_FEATURES entries are not suitable continuous numerics in the engineered dataset (non-float or effectively boolean 0/1):"
        );
        for (name, dtype) in &non_numeric_or_binary {
            info!("   - {} (dtype={:?})", name, dtype);
        }
        info!(
            "Export these columns as float32/float64 with rich value ranges (not just 0/1) if you want them in the continuous catalog"
        );
    }
}

pub(super) fn audit_boolean_definitions(df: &DataFrame) {
    let mut missing = Vec::new();
    let mut non_boolean = Vec::new();

    for &name in BOOLEAN_FEATURES {
        match df.column(name) {
            Ok(series) => match series.dtype() {
                DataType::Boolean => {}
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => {
                    let mut ok = false;
                    if let Ok(values) = series.i64() {
                        let mut has_zero_or_one = false;
                        let mut other = false;
                        for value in values.into_iter().flatten() {
                            match value {
                                0 | 1 => {
                                    has_zero_or_one = true;
                                }
                                _ => {
                                    other = true;
                                    break;
                                }
                            }
                        }
                        ok = has_zero_or_one && !other;
                    }
                    if !ok {
                        non_boolean.push((name, series.dtype().clone()));
                    }
                }
                _ => {
                    non_boolean.push((name, series.dtype().clone()));
                }
            },
            Err(_) => missing.push(name),
        }
    }

    if !missing.is_empty() {
        info!("BOOLEAN_FEATURES entries missing from engineered dataset (not present as columns):");
        for name in &missing {
            info!("   - {}", name);
        }
        info!("Add these flags to engineer.rs or remove them from BOOLEAN_FEATURES");
    }

    if !non_boolean.is_empty() {
        info!("BOOLEAN_FEATURES entries are not boolean/0-1 in the engineered dataset:");
        for (name, dtype) in &non_boolean {
            info!("   - {} (dtype={:?})", name, dtype);
        }
        info!(
            "Export these columns as booleans or 0/1 integers if you want them in the boolean catalog"
        );
    }
}
