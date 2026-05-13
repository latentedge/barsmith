use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use polars_io::prelude::CsvReadOptions;

#[derive(Debug, Clone)]
pub struct DataSetMetadata {
    column_names: Arc<Vec<String>>,
    approx_rows: usize,
}

#[derive(Clone)]
pub struct ColumnarData {
    frame: Arc<DataFrame>,
    metadata: DataSetMetadata,
}

impl ColumnarData {
    pub fn from_frame(df: DataFrame) -> Self {
        let column_names = df
            .columns()
            .iter()
            .map(|series| series.name().to_string())
            .collect::<Vec<_>>();
        let metadata = DataSetMetadata {
            column_names: Arc::new(column_names),
            approx_rows: df.height(),
        };

        Self {
            frame: Arc::new(df),
            metadata,
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let display_path = path.display().to_string();
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .map_parse_options(|options| options.with_try_parse_dates(true))
            .with_ignore_errors(true)
            .try_into_reader_with_file_path(Some(path.to_path_buf()))
            .with_context(|| format!("Failed to initialize CSV reader for {display_path}"))?
            .finish()
            .with_context(|| format!("Failed to collect columnar data from {display_path}"))?;

        Ok(Self::from_frame(df))
    }

    pub fn metadata(&self) -> DataSetMetadata {
        self.metadata.clone()
    }

    pub fn column_names(&self) -> &[String] {
        self.metadata.column_names.as_ref()
    }

    pub fn approx_rows(&self) -> usize {
        self.metadata.approx_rows
    }

    pub fn data_frame(&self) -> Arc<DataFrame> {
        Arc::clone(&self.frame)
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.metadata.column_names.iter().any(|col| col == name)
    }

    pub fn boolean_column(&self, name: &str) -> Result<BooleanChunked> {
        self.frame
            .column(name)
            .with_context(|| format!("Missing boolean column '{name}'"))?
            .bool()
            .cloned()
            .context("Failed to interpret column as boolean")
    }

    pub fn float_column(&self, name: &str) -> Result<Float64Chunked> {
        let series = self
            .frame
            .column(name)
            .with_context(|| format!("Missing float column '{name}'"))?;
        let float_series = if matches!(series.dtype(), DataType::Float64) {
            series.clone()
        } else {
            series
                .cast(&DataType::Float64)
                .with_context(|| format!("Failed to cast column '{name}' to Float64"))?
        };
        float_series
            .f64()
            .cloned()
            .context("Failed to interpret column as float")
    }

    pub fn i64_column(&self, name: &str) -> Result<Int64Chunked> {
        self.frame
            .column(name)
            .with_context(|| format!("Missing int column '{name}'"))?
            .i64()
            .cloned()
            .context("Failed to interpret column as i64")
    }

    /// Slice rows by calendar date using the timestamp/datetime column.
    ///
    /// With no bounds, this returns the original dataset unchanged.
    pub fn filter_by_date_range(
        &self,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<Self> {
        if start.is_none() && end.is_none() {
            return Ok(self.clone());
        }

        // A literal "timestamp" column wins; otherwise use the first datetime
        // column Polars found.
        let df = self.data_frame();
        let frame = df.as_ref();

        let mut series_opt = if self.has_column("timestamp") {
            frame.column("timestamp").ok()
        } else {
            None
        };

        if series_opt.is_none() {
            for candidate in frame.columns() {
                if matches!(candidate.dtype(), DataType::Datetime(_, _)) {
                    series_opt = Some(candidate);
                    break;
                }
            }
        }

        let series = series_opt
            .with_context(|| "Missing required timestamp/datetime column for date filtering")?;

        let mut keep: Vec<bool> = Vec::with_capacity(series.len());

        match series.dtype() {
            DataType::Datetime(unit, _) => {
                let ca = series
                    .datetime()
                    .with_context(|| "Failed to interpret timestamp column as datetime")?;
                for opt_v in ca.physical().iter() {
                    let ts = match opt_v {
                        Some(v) => v,
                        None => {
                            keep.push(false);
                            continue;
                        }
                    };
                    // Polars stores datetime values as integers plus a time unit.
                    let (secs, nsecs) = match unit {
                        TimeUnit::Nanoseconds => {
                            let secs = ts / 1_000_000_000;
                            let nsecs = (ts % 1_000_000_000) as u32;
                            (secs, nsecs)
                        }
                        TimeUnit::Microseconds => {
                            let secs = ts / 1_000_000;
                            let nsecs = (ts % 1_000_000) as u32 * 1_000;
                            (secs, nsecs)
                        }
                        TimeUnit::Milliseconds => {
                            let secs = ts / 1_000;
                            let nsecs = (ts % 1_000) as u32 * 1_000_000;
                            (secs, nsecs)
                        }
                    };
                    let dt = match DateTime::<Utc>::from_timestamp(secs, nsecs) {
                        Some(v) => v,
                        None => {
                            keep.push(false);
                            continue;
                        }
                    };
                    let d = dt.date_naive();

                    let mut ok = true;
                    if let Some(s) = start {
                        if d < s {
                            ok = false;
                        }
                    }
                    if let Some(e) = end {
                        if d > e {
                            ok = false;
                        }
                    }
                    keep.push(ok);
                }
            }
            _ => {
                for value in series.as_materialized_series().iter() {
                    use polars::prelude::AnyValue;

                    let raw = match value {
                        AnyValue::String(s) => s,
                        AnyValue::StringOwned(ref s) => s.as_str(),
                        AnyValue::Null => {
                            keep.push(false);
                            continue;
                        }
                        other => {
                            return Err(anyhow::anyhow!(
                                "Timestamp column must be UTF-8 strings for date filtering (got {:?})",
                                other.dtype()
                            ));
                        }
                    };
                    let parsed = chrono::DateTime::parse_from_rfc3339(raw)
                        .with_context(|| format!("Failed to parse timestamp '{raw}' as RFC3339"))?;
                    let d = parsed.date_naive();

                    let mut ok = true;
                    if let Some(s) = start {
                        if d < s {
                            ok = false;
                        }
                    }
                    if let Some(e) = end {
                        if d > e {
                            ok = false;
                        }
                    }
                    keep.push(ok);
                }
            }
        }

        let mask = BooleanChunked::from_slice("date_filter".into(), &keep);
        let mut filtered = frame
            .filter(&mask)
            .with_context(|| "Failed to filter dataframe to requested date range")?;

        if let Some(start_offset) = keep.iter().position(|flag| *flag) {
            if start_offset > 0 {
                remap_exit_indices(&mut filtered, start_offset as i64)?;
            }
        }

        Ok(Self::from_frame(filtered))
    }

    pub fn slice_rows(&self, offset: usize, length: usize) -> Result<Self> {
        if offset == 0 && length >= self.approx_rows() {
            return Ok(self.clone());
        }
        let available = self.approx_rows().saturating_sub(offset);
        let length = length.min(available);
        let mut sliced = self.frame.as_ref().slice(offset as i64, length);
        if offset > 0 {
            remap_exit_indices(&mut sliced, offset as i64)?;
        }
        Ok(Self::from_frame(sliced))
    }

    /// Return a new frame with only the selected columns.
    pub fn prune_to_columns<S: AsRef<str>>(&self, keep: &[S]) -> Result<Self> {
        if keep.is_empty() {
            return Ok(self.clone());
        }
        let names: Vec<&str> = keep.iter().map(|s| s.as_ref()).collect();
        let df = self
            .frame
            .select(&names)
            .with_context(|| "Failed to prune dataframe to selected columns")?;

        Ok(Self::from_frame(df))
    }
}

fn remap_exit_indices(frame: &mut DataFrame, offset: i64) -> Result<()> {
    let names: Vec<String> = frame
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    for name in names {
        if !(name.ends_with("_exit_i")
            || name.ends_with("_exit_i_long")
            || name.ends_with("_exit_i_short"))
        {
            continue;
        }
        let Ok(col) = frame.column(&name) else {
            continue;
        };
        if !matches!(col.dtype(), DataType::Int64) {
            continue;
        }
        let ca = col
            .i64()
            .with_context(|| format!("Failed to interpret '{name}' as i64"))?
            .clone();
        let adjusted = Int64Chunked::from_iter_options(
            name.as_str().into(),
            ca.into_iter().map(|opt| match opt {
                Some(v) if v >= offset => Some(v - offset),
                _ => None,
            }),
        )
        .into_series();
        if frame.column(&name).is_ok() {
            *frame = frame
                .drop(&name)
                .with_context(|| format!("Failed to drop '{name}' for remapping"))?;
        }
        frame
            .with_column(adjusted.into())
            .with_context(|| format!("Failed to update remapped exit index column '{name}'"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use tempfile::tempdir;

    #[test]
    fn prune_to_columns_keeps_requested_columns_and_rows() -> Result<()> {
        let temp_dir = tempdir()?;
        let csv_path = temp_dir.path().join("sample.csv");
        std::fs::write(
            &csv_path,
            "timestamp,a,b,c\n2024-01-01T00:00:00Z,1,2,3\n2024-01-01T00:30:00Z,4,5,6\n",
        )?;

        let original = ColumnarData::load(&csv_path)?;
        assert_eq!(original.column_names().len(), 4);
        assert_eq!(original.approx_rows(), 2);

        let pruned = original.prune_to_columns(&["timestamp", "a"])?;
        let cols = pruned.column_names();
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"timestamp".to_string()));
        assert!(cols.contains(&"a".to_string()));
        assert_eq!(pruned.approx_rows(), 2);

        Ok(())
    }

    #[test]
    fn float_column_casts_integer_numeric_columns() -> Result<()> {
        let temp_dir = tempdir()?;
        let csv_path = temp_dir.path().join("sample_int_float.csv");
        std::fs::write(
            &csv_path,
            "timestamp,volume\n2024-01-01T00:00:00Z,1000\n2024-01-01T00:30:00Z,1250\n",
        )?;

        let original = ColumnarData::load(&csv_path)?;
        let volume = original.float_column("volume")?;
        assert_eq!(volume.get(0), Some(1000.0));
        assert_eq!(volume.get(1), Some(1250.0));

        Ok(())
    }

    #[test]
    fn filter_by_date_range_retains_only_dates_within_bounds() -> Result<()> {
        let temp_dir = tempdir()?;
        let csv_path = temp_dir.path().join("sample_dates.csv");
        std::fs::write(
            &csv_path,
            "timestamp,a\n\
             2023-12-31T23:30:00Z,1\n\
             2024-01-01T00:00:00Z,2\n\
             2024-06-01T00:00:00Z,3\n\
             2025-01-01T00:00:00Z,4\n",
        )?;

        let original = ColumnarData::load(&csv_path)?;
        assert_eq!(original.approx_rows(), 4);

        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let filtered = original.filter_by_date_range(Some(start), Some(end))?;
        assert_eq!(filtered.approx_rows(), 2);

        Ok(())
    }
}
