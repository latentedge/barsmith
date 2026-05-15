#![allow(
    // Feature blocks pass parallel column slices so preparation stays allocation-aware.
    clippy::too_many_arguments,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::path::Path;

use crate::features::{
    CONTINUOUS_FEATURES, PAIRWISE_BASE_NUMERIC_FEATURES, PAIRWISE_EXTRA_NUMERIC_FEATURES,
};
use crate::targets::common::attach::TargetFrame;
use crate::targets::registry::attach_target;
use anyhow::{Context, Result, anyhow};
#[cfg(test)]
pub(crate) use barsmith_indicators::atr_close_to_close;
pub(crate) use barsmith_indicators::streak;
use barsmith_indicators::{momentum_score, rolling_bool_sum, shift_bool};
use barsmith_rs::Config;
use polars::prelude::*;
use polars_io::prelude::CsvReadOptions;
use tracing::warn;

mod backtest;
#[cfg(feature = "bench-api")]
mod bench;
mod feature_blocks;
mod hashing;
mod io;
mod prepare;

pub use backtest::{BacktestConfig, BacktestTargetKind, run_backtest_with_target};
#[cfg(feature = "bench-api")]
pub use bench::benchmark_2x_atr_tp_atr_stop_checksum;
use feature_blocks::{DerivedMetrics, PriceSeries, candle_features};
use feature_blocks::{
    apply_indicator_warmups, bollinger_features, ema_price_features, kalman_features,
    macd_features, oscillator_features, trend_state_features, volatility_features,
};
use io::column_with_nans;
pub use prepare::{PrepareDatasetOptions, prepare_dataset, prepare_dataset_with_options};

struct FeatureEngineer {
    frame: DataFrame,
}

impl FeatureEngineer {
    fn from_csv(path: &Path) -> Result<Self> {
        let display_path = path.display().to_string();
        let mut df = CsvReadOptions::default()
            .with_infer_schema_length(Some(1024))
            .with_has_header(true)
            .with_ignore_errors(true)
            .try_into_reader_with_file_path(Some(path.to_path_buf()))
            .with_context(|| format!("Failed to initialize CSV reader for {display_path}"))?
            .finish()
            .with_context(|| "Unable to read CSV into DataFrame")?;
        // Input files sometimes include indicator columns from older scripts.
        // We recompute these under Barsmith's canonical names, so dropping the
        // old labels keeps the prepared frame unambiguous.
        const LEGACY_INDICATOR_COLUMNS: &[&str] = &["EMA 9", "SMA 200", "ADX", "ATR"];
        for name in LEGACY_INDICATOR_COLUMNS {
            if df.column(name).is_ok() {
                df = df
                    .drop(name)
                    .with_context(|| format!("Failed to drop legacy column {name}"))?;
            }
        }
        Ok(Self { frame: df })
    }

    fn data_frame_mut(&mut self) -> &mut DataFrame {
        &mut self.frame
    }

    fn compute_features(&mut self) -> Result<()> {
        self.compute_features_with_options(true)
    }

    fn compute_features_with_options(&mut self, drop_nan_rows_in_core: bool) -> Result<()> {
        let prices = PriceSeries::from_frame(&self.frame)?;
        let derived = DerivedMetrics::new(&prices);
        let mut bool_features = HashMap::<&'static str, Vec<bool>>::new();
        let mut float_features = HashMap::<&'static str, Vec<f64>>::new();

        candle_features(&prices, &derived, &mut bool_features, &mut float_features);
        ema_price_features(&prices, &derived, &mut bool_features, &mut float_features);
        volatility_features(&prices, &derived, &mut bool_features, &mut float_features);
        oscillator_features(&prices, &derived, &mut bool_features, &mut float_features);
        macd_features(&derived, &mut bool_features, &mut float_features);
        bollinger_features(&prices, &derived, &mut bool_features, &mut float_features);
        trend_state_features(&derived, &mut bool_features);
        kalman_features(&prices, &derived, &mut bool_features);

        // Indicators emit their own NaN warmups. A later pass drops rows that
        // still have NaNs in the core numeric feature set.
        apply_indicator_warmups(&mut bool_features, &mut float_features);

        let mut float_names: Vec<&'static str> = float_features.keys().copied().collect();
        float_names.sort_unstable();
        for name in float_names {
            let Some(values) = float_features.remove(name) else {
                return Err(anyhow!(
                    "internal feature assembly error: missing float values for {name}"
                ));
            };
            let series = Series::new(name.into(), values);
            self.frame
                .with_column(series.into())
                .with_context(|| format!("Failed to insert column {name}"))?;
        }

        let mut bool_names: Vec<&'static str> = bool_features.keys().copied().collect();
        bool_names.sort_unstable();
        for name in bool_names {
            let Some(values) = bool_features.remove(name) else {
                return Err(anyhow!(
                    "internal feature assembly error: missing boolean values for {name}"
                ));
            };
            let series = Series::new(name.into(), values);
            self.frame
                .with_column(series.into())
                .with_context(|| format!("Failed to insert boolean column {name}"))?;
        }

        // Compute any dependent features (streaks, alignment flags, momentum
        // scores) once, on the full engineered frame. These will be filtered
        // alongside the core numerics in the subsequent NaN-drop step.
        self.recompute_consecutive_columns()?;
        self.recompute_kf_alignment()?;
        self.recompute_high_low()?;
        self.recompute_momentum_scores()?;

        // Optionally drop any rows where the core numeric feature set still
        // contains NaNs. This leaves a clean dataset where all continuous
        // indicators are fully defined, and all boolean/derived features are
        // aligned to that trimmed history.
        if drop_nan_rows_in_core {
            self.drop_rows_with_nan_in_core()?;
        }

        Ok(())
    }

    fn drop_rows_with_nan_in_core(&mut self) -> Result<()> {
        let height = self.frame.height();
        if height == 0 {
            return Ok(());
        }

        // Core numeric columns that define the engineered feature space.
        let mut core_cols: Vec<&str> = Vec::new();
        core_cols.extend_from_slice(CONTINUOUS_FEATURES);
        core_cols.extend_from_slice(PAIRWISE_BASE_NUMERIC_FEATURES);
        core_cols.extend_from_slice(PAIRWISE_EXTRA_NUMERIC_FEATURES);
        core_cols.sort();
        core_cols.dedup();

        let mut mask_opt: Option<BooleanChunked> = None;
        let mut skipped_all_nan: Vec<&str> = Vec::new();

        for name in core_cols {
            let series = match self.frame.column(name) {
                Ok(s) => s,
                // Some configured features may not be present in a given
                // engineered dataset; skip them when building the mask.
                Err(_) => continue,
            };
            let col = match series.f64() {
                Ok(c) => c,
                // We only consider float-like series here; boolean columns
                // are handled separately in the boolean catalog.
                Err(_) => continue,
            };
            let col_mask = col.is_not_nan();
            // Skip core columns that are entirely NaN on this slice (typically
            // long-warmup indicators like 200sma on short histories). We warn
            // so this never hides data-quality regressions.
            if col_mask.sum().unwrap_or(0) == 0 {
                skipped_all_nan.push(name);
                continue;
            }
            mask_opt = Some(match mask_opt {
                None => col_mask,
                Some(prev) => prev & col_mask,
            });
        }

        if !skipped_all_nan.is_empty() {
            warn!(
                skipped = ?skipped_all_nan,
                "Skipping core numeric columns that are all NaN during NaN-drop"
            );
        }

        if let Some(mask) = mask_opt {
            if mask.sum().unwrap_or(0) == 0 {
                return Err(anyhow!(
                    "Dropping rows with NaNs in core indicator set would remove all rows. \
                    Dataset may be too short for overlapping warmups; \
                    for tests or diagnostics, call prepare_dataset_with_options with drop_nan_rows_in_core=false."
                ));
            }
            self.frame = self
                .frame
                .filter(&mask)
                .with_context(|| "Failed to drop rows with NaNs in core indicator set")?;
        }

        Ok(())
    }

    fn recompute_consecutive_columns(&mut self) -> Result<()> {
        fn series_to_bool_vec(series: &Column) -> Result<Vec<bool>> {
            Ok(series
                .bool()
                .context("Expected boolean series")?
                .into_iter()
                .map(|value| value.unwrap_or(false))
                .collect())
        }

        let is_green = series_to_bool_vec(self.frame.column("is_green")?)?;
        let is_red = if let Ok(series) = self.frame.column("is_red") {
            series_to_bool_vec(series)?
        } else {
            vec![false; self.frame.height()]
        };
        let is_tribar = if let Ok(series) = self.frame.column("is_tribar") {
            series_to_bool_vec(series)?
        } else {
            vec![false; self.frame.height()]
        };
        let prev_green = shift_bool(&is_green, 1);
        let prev_tribar = shift_bool(&is_tribar, 1);

        let updates = [
            ("consecutive_green_2", streak(&is_green, 2)),
            ("consecutive_green_3", streak(&is_green, 3)),
            ("consecutive_red_2", streak(&is_red, 2)),
            ("consecutive_red_3", streak(&is_red, 3)),
            ("prev_green", prev_green),
            ("prev_tribar", prev_tribar),
        ];

        for (name, values) in updates {
            if self.frame.column(name).is_ok() {
                self.frame = self
                    .frame
                    .drop(name)
                    .with_context(|| format!("Failed to remove existing column {name}"))?;
            }
            let series = Series::new(name.into(), values);
            self.frame
                .with_column(series.into())
                .with_context(|| format!("Failed to update column {name}"))?;
        }

        let consecutive_green_counts = rolling_bool_sum(&is_green, 3);
        if self.frame.column("consecutive_green").is_ok() {
            self.frame = self
                .frame
                .drop("consecutive_green")
                .with_context(|| "Failed to remove existing column consecutive_green")?;
        }
        let count_series = Series::new("consecutive_green".into(), consecutive_green_counts);
        self.frame
            .with_column(count_series.into())
            .with_context(|| "Failed to update column consecutive_green")?;
        Ok(())
    }

    fn attach_targets(&mut self, config: &Config) -> Result<()> {
        let mut target_frame = TargetFrame::new(&mut self.frame);
        attach_target(&mut target_frame, config)
    }

    fn recompute_kf_alignment(&mut self) -> Result<()> {
        let smooth = match column_with_nans(&self.frame, "kf_smooth") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };
        let ema9 = match column_with_nans(&self.frame, "9ema") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };
        let ema200 = match column_with_nans(&self.frame, "200sma") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };

        let mut values = Vec::with_capacity(smooth.len());
        for ((smooth_val, ema9_val), ema200_val) in
            smooth.iter().zip(ema9.iter()).zip(ema200.iter())
        {
            let aligned = smooth_val.is_finite()
                && ema9_val.is_finite()
                && ema200_val.is_finite()
                && smooth_val > ema9_val
                && ema9_val > ema200_val;
            values.push(aligned);
        }

        self.replace_bool_column("kf_ema_aligned", values)?;

        Ok(())
    }

    fn recompute_high_low(&mut self) -> Result<()> {
        let highs = match column_with_nans(&self.frame, "high") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };
        let lows = match column_with_nans(&self.frame, "low") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };
        let len = highs.len().min(lows.len());
        if len == 0 {
            return Ok(());
        }

        let mut higher_high = vec![false; len];
        let mut higher_low = vec![false; len];
        let mut lower_high = vec![false; len];
        let mut lower_low = vec![false; len];

        for i in 1..len {
            let high = highs[i];
            let prev_high = highs[i - 1];
            if high.is_finite() && prev_high.is_finite() {
                higher_high[i] = high > prev_high;
                lower_high[i] = high < prev_high;
            }
            let low = lows[i];
            let prev_low = lows[i - 1];
            if low.is_finite() && prev_low.is_finite() {
                higher_low[i] = low > prev_low;
                lower_low[i] = low < prev_low;
            }
        }

        self.replace_bool_column("higher_high", higher_high.clone())?;
        self.replace_bool_column("higher_low", higher_low.clone())?;
        self.replace_bool_column("lower_high", lower_high.clone())?;
        self.replace_bool_column("lower_low", lower_low.clone())?;

        let bullish: Vec<bool> = higher_high
            .iter()
            .zip(higher_low.iter())
            .map(|(hh, hl)| *hh && *hl)
            .collect();
        let bearish: Vec<bool> = lower_high
            .iter()
            .zip(lower_low.iter())
            .map(|(lh, ll)| *lh && *ll)
            .collect();
        self.replace_bool_column("bullish_bar_sequence", bullish)?;
        self.replace_bool_column("bearish_bar_sequence", bearish)?;

        Ok(())
    }

    fn replace_bool_column(&mut self, name: &str, values: Vec<bool>) -> Result<()> {
        if self.frame.column(name).is_ok() {
            self.frame = self.frame.drop(name)?;
        }
        let series = Series::new(name.into(), values);
        self.frame
            .with_column(series.into())
            .with_context(|| format!("Failed to update column {name}"))?;
        Ok(())
    }

    fn replace_float_column(&mut self, name: &str, values: Vec<f64>) -> Result<()> {
        if self.frame.column(name).is_ok() {
            self.frame = self.frame.drop(name)?;
        }
        let series = Series::new(name.into(), values);
        self.frame
            .with_column(series.into())
            .with_context(|| format!("Failed to update column {name}"))?;
        Ok(())
    }

    fn recompute_momentum_scores(&mut self) -> Result<()> {
        let rsi = match column_with_nans(&self.frame, "rsi_14") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };
        let roc5 = match column_with_nans(&self.frame, "roc_5") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };
        let roc10 = match column_with_nans(&self.frame, "roc_10") {
            Ok(values) => values,
            Err(_) => return Ok(()),
        };
        let score = momentum_score(&rsi, &roc5, &roc10);
        self.replace_float_column("momentum_score", score.clone())?;

        let strong: Vec<bool> = score
            .iter()
            .map(|val| val.is_finite() && *val > 0.75)
            .collect();
        let weak: Vec<bool> = score
            .iter()
            .map(|val| val.is_finite() && *val < 0.25)
            .collect();
        self.replace_bool_column("is_strong_momentum_score", strong)?;
        self.replace_bool_column("is_weak_momentum_score", weak)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;
