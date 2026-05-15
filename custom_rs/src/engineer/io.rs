use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use polars::prelude::*;

pub(super) fn column_to_vec(frame: &DataFrame, name: &str) -> Result<Vec<f64>> {
    let series = frame
        .column(name)
        .with_context(|| format!("Missing required column {name}"))?;
    let chunk = series
        .f64()
        .with_context(|| format!("Column {name} must be float"))?;
    chunk
        .into_iter()
        .map(|opt| opt.ok_or_else(|| anyhow!("Column {name} contains nulls")))
        .collect()
}

pub(super) fn column_with_nans(frame: &DataFrame, name: &str) -> Result<Vec<f64>> {
    let series = frame
        .column(name)
        .with_context(|| format!("Missing required column {name}"))?;
    Ok(series
        .f64()
        .with_context(|| format!("Column {name} must be float"))?
        .into_iter()
        .map(|value| value.unwrap_or(f64::NAN))
        .collect())
}

pub(super) fn bool_column(frame: &DataFrame, name: &str) -> Result<Vec<bool>> {
    let series = frame
        .column(name)
        .with_context(|| format!("Missing required column {name}"))?;
    Ok(series
        .bool()
        .with_context(|| format!("Column {name} should be boolean"))?
        .into_iter()
        .map(|value| value.unwrap_or(false))
        .collect())
}

pub(super) fn timestamp_column(frame: &DataFrame) -> Result<Vec<DateTime<Utc>>> {
    // Prefer the canonical "timestamp" column; fall back to "datetime" or
    // "time" when present to support lean CSVs used for backtesting.
    let series = frame
        .column("timestamp")
        .or_else(|_| frame.column("datetime"))
        .or_else(|_| frame.column("time"))
        .with_context(|| {
            "Missing required timestamp/datetime column (expected 'timestamp', 'datetime', or 'time')"
        })?;

    let mut out = Vec::with_capacity(series.len());
    for value in series.as_materialized_series().iter() {
        use polars::prelude::AnyValue;

        let raw = match value {
            AnyValue::String(s) => s,
            AnyValue::StringOwned(ref s) => s.as_str(),
            AnyValue::Null => return Err(anyhow!("Timestamp column contains nulls")),
            other => {
                return Err(anyhow!(
                    "Timestamp column must be UTF-8 strings (got {:?})",
                    other.dtype()
                ));
            }
        };
        // Accept RFC3339-style strings like 2024-01-01T00:00:00Z.
        let parsed = DateTime::parse_from_rfc3339(raw)
            .with_context(|| format!("Failed to parse timestamp '{raw}' as RFC3339"))?;
        out.push(parsed.with_timezone(&Utc));
    }
    Ok(out)
}
