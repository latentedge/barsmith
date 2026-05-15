use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{Config, Direction};

pub(crate) const RUN_MANIFEST_FILE: &str = "run_manifest.json";
const RUN_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunManifest {
    schema_version: u32,
    run_identity_hash: String,
    csv_hash: String,
    created_at: String,
    updated_at: String,
    identity: RunIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunIdentity {
    schema_version: u32,
    csv_hash: String,
    direction: String,
    target: String,
    min_sample_size: usize,
    include_date_start: Option<String>,
    include_date_end: Option<String>,
    strict_min_pruning: bool,
    enable_subset_pruning: bool,
    enable_feature_pairs: bool,
    feature_pairs_limit: Option<usize>,
    catalog_hash: Option<String>,
    require_any_features: Vec<String>,
    stats_detail: crate::config::StatsDetail,
    max_drawdown: f64,
    stacking_mode: crate::config::StackingMode,
    position_sizing: crate::config::PositionSizingMode,
    stop_distance_column: Option<String>,
    stop_distance_unit: crate::config::StopDistanceUnit,
    min_contracts: usize,
    max_contracts: Option<usize>,
    capital_dollar: Option<f64>,
    risk_pct_per_trade: Option<f64>,
    equity_time_years: Option<f64>,
    asset: Option<String>,
    risk_per_trade_dollar: Option<f64>,
    cost_per_trade_dollar: Option<f64>,
    cost_per_trade_r: Option<f64>,
    dollars_per_r: Option<f64>,
    tick_size: Option<f64>,
    point_value: Option<f64>,
    tick_value: Option<f64>,
    margin_per_contract_dollar: Option<f64>,
}

impl RunIdentity {
    fn from_config(config: &Config, csv_hash: &str) -> Self {
        let mut require_any_features = config.require_any_features.clone();
        require_any_features.sort();
        require_any_features.dedup();

        Self {
            schema_version: RUN_MANIFEST_SCHEMA_VERSION,
            csv_hash: csv_hash.to_string(),
            direction: format_direction(config.direction),
            target: config.target.clone(),
            min_sample_size: config.min_sample_size,
            include_date_start: config.include_date_start.map(|value| value.to_string()),
            include_date_end: config.include_date_end.map(|value| value.to_string()),
            strict_min_pruning: config.strict_min_pruning,
            enable_subset_pruning: config.enable_subset_pruning,
            enable_feature_pairs: config.enable_feature_pairs,
            feature_pairs_limit: config.feature_pairs_limit,
            catalog_hash: config.catalog_hash.clone(),
            require_any_features,
            stats_detail: config.stats_detail,
            max_drawdown: config.max_drawdown,
            stacking_mode: config.stacking_mode,
            position_sizing: config.position_sizing,
            stop_distance_column: config.stop_distance_column.clone(),
            stop_distance_unit: config.stop_distance_unit,
            min_contracts: config.min_contracts,
            max_contracts: config.max_contracts,
            capital_dollar: config.capital_dollar,
            risk_pct_per_trade: config.risk_pct_per_trade,
            equity_time_years: config.equity_time_years,
            asset: config.asset.clone(),
            risk_per_trade_dollar: config.risk_per_trade_dollar,
            cost_per_trade_dollar: config.cost_per_trade_dollar,
            cost_per_trade_r: config.cost_per_trade_r,
            dollars_per_r: config.dollars_per_r,
            tick_size: config.tick_size,
            point_value: config.point_value,
            tick_value: config.tick_value,
            margin_per_contract_dollar: config.margin_per_contract_dollar,
        }
    }

    fn hash(&self) -> Result<String> {
        let serialized = serde_json::to_vec(self)?;
        Ok(hex::encode(Sha256::digest(serialized)))
    }
}

pub(crate) fn config_run_identity_hash(config: &Config, csv_hash: &str) -> Result<String> {
    RunIdentity::from_config(config, csv_hash).hash()
}

pub(crate) fn validate_or_write_run_manifest(
    output_dir: &Path,
    force_recompute: bool,
    has_existing_state: bool,
    config: &Config,
    run_identity_hash: &str,
    csv_hash: &str,
) -> Result<()> {
    let manifest_path = output_dir.join(RUN_MANIFEST_FILE);
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let identity = RunIdentity::from_config(config, csv_hash);

    let created_at = if manifest_path.exists() && !force_recompute {
        let raw = fs::read_to_string(&manifest_path)
            .with_context(|| format!("Unable to read {}", manifest_path.display()))?;
        let mut existing: RunManifest = serde_json::from_str(&raw)
            .with_context(|| format!("Unable to parse {}", manifest_path.display()))?;

        if existing.schema_version != RUN_MANIFEST_SCHEMA_VERSION {
            return Err(anyhow!(
                "{} uses unsupported schema_version {}. Run with --force-recompute or choose a fresh --run-id.",
                manifest_path.display(),
                existing.schema_version
            ));
        }

        if existing.run_identity_hash != run_identity_hash {
            return Err(anyhow!(
                "{} belongs to a different Barsmith run identity. Run with --force-recompute or choose a fresh --run-id.",
                manifest_path.display()
            ));
        }

        existing.updated_at = now.clone();
        write_run_manifest(&manifest_path, &existing)?;
        return Ok(());
    } else {
        now.clone()
    };

    if !force_recompute && has_existing_state {
        return Err(anyhow!(
            "Existing Barsmith state was found in {} but {} is missing. Run with --force-recompute or choose a fresh --run-id.",
            output_dir.display(),
            RUN_MANIFEST_FILE
        ));
    }

    let manifest = RunManifest {
        schema_version: RUN_MANIFEST_SCHEMA_VERSION,
        run_identity_hash: run_identity_hash.to_string(),
        csv_hash: csv_hash.to_string(),
        created_at,
        updated_at: now,
        identity,
    };
    write_run_manifest(&manifest_path, &manifest)
}

fn write_run_manifest(path: &Path, manifest: &RunManifest) -> Result<()> {
    let temp_path = path.with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(manifest)?;
    fs::write(&temp_path, payload)
        .with_context(|| format!("Unable to write {}", temp_path.display()))?;
    fs::rename(&temp_path, path)
        .with_context(|| format!("Unable to replace {}", path.display()))?;
    Ok(())
}

fn format_direction(direction: Direction) -> String {
    match direction {
        Direction::Long => "long",
        Direction::Short => "short",
        Direction::Both => "both",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        EvalProfileMode, PositionSizingMode, ReportMetricsMode, StackingMode, StatsDetail,
        StopDistanceUnit,
    };
    use std::path::PathBuf;

    fn config_with_stop_distance_column(stop_distance_column: &str) -> Config {
        Config {
            input_csv: PathBuf::from("dummy.csv"),
            source_csv: None,
            direction: Direction::Long,
            target: "2x_atr_tp_atr_stop".to_string(),
            output_dir: PathBuf::from("out"),
            max_depth: 2,
            min_sample_size: 1,
            min_sample_size_report: 1,
            include_date_start: None,
            include_date_end: None,
            batch_size: 100,
            n_workers: 1,
            auto_batch: false,
            resume_offset: 0,
            explicit_resume_offset: false,
            max_combos: None,
            dry_run: false,
            quiet: true,
            report_metrics: ReportMetricsMode::Off,
            report_top: 0,
            force_recompute: false,
            max_drawdown: 50.0,
            max_drawdown_report: None,
            min_calmar_report: None,
            strict_min_pruning: true,
            enable_subset_pruning: false,
            enable_feature_pairs: false,
            feature_pairs_limit: None,
            catalog_hash: None,
            stats_detail: StatsDetail::Core,
            eval_profile: EvalProfileMode::Off,
            eval_profile_sample_rate: 1,
            s3_output: None,
            s3_upload_each_batch: false,
            capital_dollar: Some(100_000.0),
            risk_pct_per_trade: Some(1.0),
            equity_time_years: None,
            asset: Some("MES".to_string()),
            risk_per_trade_dollar: Some(1_000.0),
            cost_per_trade_dollar: None,
            cost_per_trade_r: None,
            dollars_per_r: Some(1_000.0),
            tick_size: Some(0.25),
            stacking_mode: StackingMode::NoStacking,
            position_sizing: PositionSizingMode::Contracts,
            stop_distance_column: Some(stop_distance_column.to_string()),
            stop_distance_unit: StopDistanceUnit::Points,
            min_contracts: 1,
            max_contracts: None,
            point_value: Some(5.0),
            tick_value: Some(1.25),
            margin_per_contract_dollar: Some(2_000.0),
            require_any_features: Vec::new(),
        }
    }

    #[test]
    fn run_identity_changes_when_contract_risk_column_changes() {
        let raw_atr =
            config_run_identity_hash(&config_with_stop_distance_column("atr"), "same-csv-hash")
                .expect("raw atr identity");
        let realized_risk = config_run_identity_hash(
            &config_with_stop_distance_column("2x_atr_tp_atr_stop_risk"),
            "same-csv-hash",
        )
        .expect("realized risk identity");

        assert_ne!(
            raw_atr, realized_risk,
            "raw ATR sizing and realized-risk sizing must not resume into the same run identity"
        );
    }
}
