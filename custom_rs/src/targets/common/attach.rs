use anyhow::{Context, Result, anyhow};
use barsmith_rs::{Direction, config::Config};
use chrono::{DateTime, Datelike, Utc};
use polars::prelude::*;

use super::barrier::{TargetResolution, compute_next_bar_targets_and_rr};

pub(crate) const NEXT_BAR_SL_MULTIPLIER: f64 = 1.5;

pub(crate) type TargetTuple = (
    Vec<bool>,
    Vec<bool>,
    Vec<f64>,
    Vec<f64>,
    Vec<Option<usize>>,
    Vec<Option<usize>>,
);

pub(crate) type HighLowAtrTargetFn = fn(
    &[f64],
    &[f64],
    &[f64],
    &[f64],
    &[f64],
    Option<f64>,
    Option<usize>,
    Direction,
) -> TargetTuple;

pub(crate) type HighLowTargetFn =
    fn(&[f64], &[f64], &[f64], &[f64], Option<f64>, Option<usize>, Direction) -> TargetTuple;

pub(crate) type AtrStopTargetFn = fn(
    &[f64],
    &[f64],
    &[f64],
    &[f64],
    &[f64],
    Option<f64>,
    Option<usize>,
    Direction,
) -> TargetResolution;

pub(crate) struct TargetFrame<'a> {
    frame: &'a mut DataFrame,
}

impl<'a> TargetFrame<'a> {
    pub(crate) fn new(frame: &'a mut DataFrame) -> Self {
        Self { frame }
    }

    pub(crate) fn column_with_nans(&self, name: &str) -> Result<Vec<f64>> {
        let series = self
            .frame
            .column(name)
            .with_context(|| format!("Missing required column {name}"))?;
        Ok(series
            .f64()
            .with_context(|| format!("Column {name} must be float"))?
            .into_iter()
            .map(|value| value.unwrap_or(f64::NAN))
            .collect())
    }

    pub(crate) fn bool_column(&self, name: &str) -> Result<Vec<bool>> {
        let series = self
            .frame
            .column(name)
            .with_context(|| format!("Missing required column {name}"))?;
        Ok(series
            .bool()
            .with_context(|| format!("Column {name} should be boolean"))?
            .into_iter()
            .map(|value| value.unwrap_or(false))
            .collect())
    }

    pub(crate) fn timestamps(&self) -> Result<Vec<DateTime<Utc>>> {
        let series = self
            .frame
            .column("timestamp")
            .or_else(|_| self.frame.column("datetime"))
            .or_else(|_| self.frame.column("time"))
            .with_context(|| {
                "Missing required timestamp/datetime column (expected 'timestamp', 'datetime', or 'time')"
            })?;

        let mut out = Vec::with_capacity(series.len());
        for value in series.as_materialized_series().iter() {
            let raw = match value {
                AnyValue::String(s) => s,
                AnyValue::StringOwned(ref s) => s.as_str(),
                AnyValue::Null => return Err(anyhow!("Timestamp column contains nulls")),
                other => {
                    return Err(anyhow!(
                        "Timestamp column must be string-like; got {:?}",
                        other
                    ));
                }
            };
            let parsed = DateTime::parse_from_rfc3339(raw)
                .or_else(|_| DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f%z"))
                .or_else(|_| {
                    DateTime::parse_from_str(&format!("{raw}+00:00"), "%Y-%m-%d %H:%M:%S%.f%z")
                })
                .with_context(|| format!("Unable to parse timestamp '{raw}'"))?
                .with_timezone(&Utc);
            out.push(parsed);
        }

        Ok(out)
    }

    pub(crate) fn resolve_end_idx(&self, config: &Config) -> Result<Option<usize>> {
        if let Some(date_end) = config.include_date_end {
            let timestamps = self.timestamps()?;
            Ok(timestamps
                .iter()
                .rposition(|ts| ts.date_naive() <= date_end))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn replace_bool_column(&mut self, name: &str, values: Vec<bool>) -> Result<()> {
        if self.frame.column(name).is_ok() {
            *self.frame = self.frame.drop(name)?;
        }
        let series = Series::new(name.into(), values);
        self.frame
            .with_column(series.into())
            .with_context(|| format!("Failed to update column {name}"))?;
        Ok(())
    }

    pub(crate) fn replace_float_column(&mut self, name: &str, values: Vec<f64>) -> Result<()> {
        if self.frame.column(name).is_ok() {
            *self.frame = self.frame.drop(name)?;
        }
        let series = Series::new(name.into(), values);
        self.frame
            .with_column(series.into())
            .with_context(|| format!("Failed to update column {name}"))?;
        Ok(())
    }

    pub(crate) fn replace_i64_column(
        &mut self,
        name: &str,
        values: Vec<Option<i64>>,
    ) -> Result<()> {
        if self.frame.column(name).is_ok() {
            *self.frame = self.frame.drop(name)?;
        }
        let series = Series::new(name.into(), values);
        self.frame
            .with_column(series.into())
            .with_context(|| format!("Failed to update column {name}"))?;
        Ok(())
    }

    pub(crate) fn replace_series<C>(&mut self, series: C) -> Result<()>
    where
        C: IntoColumn,
    {
        let column = series.into_column();
        let name = column.name().to_string();
        if self.frame.column(&name).is_ok() {
            *self.frame = self.frame.drop(&name)?;
        }
        self.frame
            .with_column(column)
            .with_context(|| format!("Failed to update column {name}"))?;
        Ok(())
    }

    pub(crate) fn clone_column_as(&self, source: &str, target_name: &str) -> Result<Column> {
        let mut series = self.frame.column(source)?.clone();
        series.rename(target_name.into());
        Ok(series)
    }

    pub(crate) fn write_basic_target(
        &mut self,
        id: &str,
        direction: Direction,
        output: TargetTuple,
    ) -> Result<()> {
        let (long, short, rr_long, rr_short, exit_i_long, exit_i_short) = output;
        let long_col = format!("{id}_long");
        let short_col = format!("{id}_short");
        let exit_long_col = format!("{id}_exit_i_long");
        let exit_short_col = format!("{id}_exit_i_short");

        self.replace_bool_column(&long_col, long)?;
        self.replace_bool_column(&short_col, short)?;
        self.replace_float_column("rr_long", rr_long)?;
        self.replace_float_column("rr_short", rr_short)?;
        self.replace_i64_column(&exit_long_col, options_to_i64(exit_i_long))?;
        self.replace_i64_column(&exit_short_col, options_to_i64(exit_i_short))?;

        let (target_source, rr_source, exit_source) = match direction {
            Direction::Short => (short_col.as_str(), "rr_short", exit_short_col.as_str()),
            _ => (long_col.as_str(), "rr_long", exit_long_col.as_str()),
        };

        let target_series = self.clone_column_as(target_source, id)?;
        self.replace_series(target_series)?;

        let rr_name = format!("rr_{id}");
        let rr_series = self.clone_column_as(rr_source, &rr_name)?;
        self.replace_series(rr_series)?;

        let exit_name = format!("{id}_exit_i");
        let exit_series = self.clone_column_as(exit_source, &exit_name)?;
        self.replace_series(exit_series)?;
        Ok(())
    }

    pub(crate) fn write_eligible_target(
        &mut self,
        id: &str,
        direction: Direction,
        output: TargetTuple,
        open: &[f64],
        close: &[f64],
    ) -> Result<()> {
        let eligible_long: Vec<bool> = open
            .iter()
            .zip(close.iter())
            .map(|(o, c)| o.is_finite() && c.is_finite() && c > o)
            .collect();
        let eligible_short: Vec<bool> = open
            .iter()
            .zip(close.iter())
            .map(|(o, c)| o.is_finite() && c.is_finite() && c < o)
            .collect();

        self.write_basic_target(id, direction, output)?;

        let eligible_long_col = format!("{id}_eligible_long");
        let eligible_short_col = format!("{id}_eligible_short");
        self.replace_bool_column(&eligible_long_col, eligible_long)?;
        self.replace_bool_column(&eligible_short_col, eligible_short)?;

        let eligible_source = match direction {
            Direction::Short => eligible_short_col.as_str(),
            _ => eligible_long_col.as_str(),
        };
        let eligible_name = format!("{id}_eligible");
        let eligible_series = self.clone_column_as(eligible_source, &eligible_name)?;
        self.replace_series(eligible_series)?;
        Ok(())
    }

    pub(crate) fn write_atr_stop_target(
        &mut self,
        id: &str,
        direction: Direction,
        mut target: TargetResolution,
        open: &[f64],
        close: &[f64],
    ) -> Result<()> {
        let (risk_long, risk_short) = target.take_risk_columns().with_context(|| {
            format!("{id} target resolution did not return realized risk columns")
        })?;
        let output = (
            target.long,
            target.short,
            target.rr_long,
            target.rr_short,
            target.exit_i_long,
            target.exit_i_short,
        );

        self.write_eligible_target(id, direction, output, open, close)?;

        let risk_long_col = format!("{id}_risk_long");
        let risk_short_col = format!("{id}_risk_short");
        self.replace_float_column(&risk_long_col, risk_long)?;
        self.replace_float_column(&risk_short_col, risk_short)?;

        let risk_source = match direction {
            Direction::Short => risk_short_col.as_str(),
            _ => risk_long_col.as_str(),
        };
        let risk_name = format!("{id}_risk");
        let risk_series = self.clone_column_as(risk_source, &risk_name)?;
        self.replace_series(risk_series)?;
        Ok(())
    }
}

pub(crate) fn attach_next_bar_style(
    frame: &mut TargetFrame<'_>,
    config: &Config,
    id: &str,
    stop_distance_column: &str,
) -> Result<()> {
    let open = frame.column_with_nans("open")?;
    let high = frame.column_with_nans("high")?;
    let low = frame.column_with_nans("low")?;
    let close = frame.column_with_nans("close")?;
    let stop_distance = frame.column_with_nans(stop_distance_column)?;

    let output = compute_next_bar_targets_and_rr(
        &open,
        &high,
        &low,
        &close,
        &stop_distance,
        NEXT_BAR_SL_MULTIPLIER,
        config.tick_size,
        config.direction,
    );

    frame.write_basic_target(id, config.direction, output)
}

pub(crate) fn attach_highlow_with_atr(
    frame: &mut TargetFrame<'_>,
    config: &Config,
    id: &str,
    compute: HighLowAtrTargetFn,
) -> Result<()> {
    let open = frame.column_with_nans("open")?;
    let high = frame.column_with_nans("high")?;
    let low = frame.column_with_nans("low")?;
    let close = frame.column_with_nans("close")?;
    let atr_values = frame
        .column_with_nans("atr")
        .with_context(|| missing_atr_context(id))?;
    let resolve_end_idx = frame.resolve_end_idx(config)?;
    let output = compute(
        &open,
        &high,
        &low,
        &close,
        &atr_values,
        config.tick_size,
        resolve_end_idx,
        config.direction,
    );

    frame.write_eligible_target(id, config.direction, output, &open, &close)
}

pub(crate) fn attach_highlow_without_atr(
    frame: &mut TargetFrame<'_>,
    config: &Config,
    id: &str,
    compute: HighLowTargetFn,
) -> Result<()> {
    let open = frame.column_with_nans("open")?;
    let high = frame.column_with_nans("high")?;
    let low = frame.column_with_nans("low")?;
    let close = frame.column_with_nans("close")?;
    let resolve_end_idx = frame.resolve_end_idx(config)?;
    let output = compute(
        &open,
        &high,
        &low,
        &close,
        config.tick_size,
        resolve_end_idx,
        config.direction,
    );

    frame.write_eligible_target(id, config.direction, output, &open, &close)
}

pub(crate) fn attach_atr_stop(
    frame: &mut TargetFrame<'_>,
    config: &Config,
    id: &str,
    compute: AtrStopTargetFn,
) -> Result<()> {
    let open = frame.column_with_nans("open")?;
    let high = frame.column_with_nans("high")?;
    let low = frame.column_with_nans("low")?;
    let close = frame.column_with_nans("close")?;
    let atr_values = frame
        .column_with_nans("atr")
        .with_context(|| missing_atr_context(id))?;
    let resolve_end_idx = frame.resolve_end_idx(config)?;
    let target = compute(
        &open,
        &high,
        &low,
        &close,
        &atr_values,
        config.tick_size,
        resolve_end_idx,
        config.direction,
    );

    frame.write_atr_stop_target(id, config.direction, target, &open, &close)
}

pub(crate) fn options_to_i64(values: Vec<Option<usize>>) -> Vec<Option<i64>> {
    values
        .into_iter()
        .map(|value| value.map(|idx| idx as i64))
        .collect()
}

pub(crate) fn missing_atr_context(target_name: &str) -> String {
    format!(
        "Missing required 'atr' column for {target_name} target. Re-generate the engineered dataset \
         with --ack-new-df or choose a fresh --run-id so 'atr' is present."
    )
}

pub(crate) fn compute_week_indices(timestamps: &[DateTime<Utc>]) -> (Vec<i64>, Vec<usize>) {
    let mut week_index = Vec::with_capacity(timestamps.len());
    for ts in timestamps {
        let iso = ts.iso_week();
        week_index.push((iso.year() as i64) * 100 + i64::from(iso.week()));
    }
    let week_end_index = period_end_indices(&week_index);
    (week_index, week_end_index)
}

fn period_end_indices(period_index: &[i64]) -> Vec<usize> {
    let len = period_index.len();
    let mut out = vec![0usize; len];
    if len == 0 {
        return out;
    }

    let mut start = 0usize;
    while start < len {
        let value = period_index[start];
        let mut end = start;
        while end + 1 < len && period_index[end + 1] == value {
            end += 1;
        }
        for slot in out.iter_mut().take(end + 1).skip(start) {
            *slot = end;
        }
        start = end + 1;
    }

    out
}
