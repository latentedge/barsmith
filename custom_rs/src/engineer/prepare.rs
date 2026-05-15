use std::fs::{self, File};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use barsmith_rs::Config;
use polars::prelude::*;
use tracing::{info, warn};

use super::FeatureEngineer;
use super::hashing::{sha256_dataframe_as_csv, sha256_file};
use crate::targets::registry::ensure_supported_target;

pub struct PrepareDatasetOptions {
    pub drop_nan_rows_in_core: bool,
    pub ack_new_df: bool,
}

impl Default for PrepareDatasetOptions {
    fn default() -> Self {
        Self {
            drop_nan_rows_in_core: true,
            ack_new_df: false,
        }
    }
}

pub fn prepare_dataset(config: &Config) -> Result<PathBuf> {
    prepare_dataset_with_options(config, PrepareDatasetOptions::default())
}

pub fn prepare_dataset_with_options(
    config: &Config,
    options: PrepareDatasetOptions,
) -> Result<PathBuf> {
    ensure_supported_target(&config.target)?;
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("Unable to create {}", config.output_dir.display()))?;
    let output_path = config.output_dir.join("barsmith_prepared.csv");
    let mut engineer = FeatureEngineer::from_csv(&config.input_csv)?;
    engineer.compute_features_with_options(options.drop_nan_rows_in_core)?;
    engineer.attach_targets(config)?;

    if output_path.exists() {
        let old_hash = sha256_file(&output_path)?;
        let new_hash = sha256_dataframe_as_csv(engineer.data_frame_mut())?;
        if old_hash != new_hash {
            if !options.ack_new_df {
                return Err(anyhow!(
                    "Existing barsmith_prepared.csv differs from newly engineered dataframe.\n\
                     path: {}\n\
                     existing sha256: {}\n\
                     new sha256: {}\n\
                     Rerun with --ack-new-df to overwrite and continue, or choose a fresh --run-id to preserve prior results.",
                    output_path.display(),
                    old_hash,
                    new_hash
                ));
            }
            warn!(
                existing_hash = %old_hash,
                new_hash = %new_hash,
                path = %output_path.display(),
                "Prepared dataset hash mismatch; overwriting because ack_new_df=true"
            );
        } else {
            let row_count = engineer.data_frame_mut().height();
            info!(
                rows = row_count,
                path = %output_path.display(),
                "Prepared dataset unchanged; reusing existing barsmith_prepared.csv"
            );
            return Ok(output_path);
        }
    }

    let row_count = engineer.data_frame_mut().height();
    let mut file = File::create(&output_path)
        .with_context(|| format!("Unable to create {}", output_path.display()))?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .finish(engineer.data_frame_mut())
        .with_context(|| "Failed to persist engineered dataset")?;
    info!(
        rows = row_count,
        path = %output_path.display(),
        "Prepared engineered dataset written for combination run"
    );
    Ok(output_path)
}
