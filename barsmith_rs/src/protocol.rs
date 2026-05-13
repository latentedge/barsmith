use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

pub const PROTOCOL_SCHEMA_VERSION: u32 = 1;
pub const FORMULA_EXPORT_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResearchStage {
    Discovery,
    Validation,
    Lockbox,
    LiveShadow,
}

impl ResearchStage {
    pub fn is_lockbox_like(self) -> bool {
        matches!(self, Self::Lockbox | Self::LiveShadow)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discovery => "discovery",
            Self::Validation => "validation",
            Self::Lockbox => "lockbox",
            Self::LiveShadow => "live-shadow",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResearchWindow {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
}

impl ResearchWindow {
    pub fn new(start: Option<NaiveDate>, end: Option<NaiveDate>) -> Result<Self> {
        if let (Some(start), Some(end)) = (start, end) {
            if start > end {
                return Err(anyhow!("research window start {start} is after end {end}"));
            }
        }
        Ok(Self { start, end })
    }

    pub fn contains(&self, date: NaiveDate) -> bool {
        if self.start.is_some_and(|start| date < start) {
            return false;
        }
        if self.end.is_some_and(|end| date > end) {
            return false;
        }
        true
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        let left_end = self.end.unwrap_or(NaiveDate::MAX);
        let right_end = other.end.unwrap_or(NaiveDate::MAX);
        let left_start = self.start.unwrap_or(NaiveDate::MIN);
        let right_start = other.start.unwrap_or(NaiveDate::MIN);
        left_start <= right_end && right_start <= left_end
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchProtocol {
    pub schema_version: u32,
    pub protocol_id: Option<String>,
    pub dataset_id: String,
    pub target: String,
    pub direction: Option<String>,
    pub engine: Option<String>,
    pub strict: bool,
    pub discovery: ResearchWindow,
    pub validation: ResearchWindow,
    pub lockbox: ResearchWindow,
    pub live_shadow_min_days: Option<usize>,
    pub live_shadow_min_trades: Option<usize>,
    pub candidate_top_k: Option<usize>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResearchProtocolDraft {
    pub dataset_id: String,
    pub target: String,
    pub direction: Option<String>,
    pub engine: Option<String>,
    pub discovery: ResearchWindow,
    pub validation: ResearchWindow,
    pub lockbox: ResearchWindow,
    pub candidate_top_k: Option<usize>,
}

impl ResearchProtocol {
    pub fn from_draft(draft: ResearchProtocolDraft) -> Self {
        Self {
            schema_version: PROTOCOL_SCHEMA_VERSION,
            protocol_id: None,
            dataset_id: draft.dataset_id,
            target: draft.target,
            direction: draft.direction,
            engine: draft.engine,
            strict: true,
            discovery: draft.discovery,
            validation: draft.validation,
            lockbox: draft.lockbox,
            live_shadow_min_days: Some(30),
            live_shadow_min_trades: Some(100),
            candidate_top_k: draft.candidate_top_k,
            notes: vec![
                "Discovery may choose candidates; validation may only confirm or reject; lockbox must evaluate one frozen formula."
                    .to_string(),
            ],
        }
    }

    pub fn hash(&self) -> Result<String> {
        sha256_json(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormulaExportManifest {
    pub schema_version: u32,
    pub created_at: String,
    pub source_output_dir_sha256: String,
    pub source_run_manifest_sha256: Option<String>,
    pub source_run_identity_hash: Option<String>,
    pub source_date_start: Option<NaiveDate>,
    pub source_date_end: Option<NaiveDate>,
    pub target: String,
    pub direction: String,
    pub rank_by: String,
    pub min_sample_size: usize,
    pub min_win_rate: f64,
    pub max_drawdown: f64,
    pub min_calmar: Option<f64>,
    pub requested_limit: usize,
    pub exported_rows: usize,
    pub formulas_sha256: String,
    pub protocol_sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FormulaExportManifestDraft {
    pub source_output_dir_sha256: String,
    pub source_run_manifest_sha256: Option<String>,
    pub source_run_identity_hash: Option<String>,
    pub source_date_start: Option<NaiveDate>,
    pub source_date_end: Option<NaiveDate>,
    pub target: String,
    pub direction: String,
    pub rank_by: String,
    pub min_sample_size: usize,
    pub min_win_rate: f64,
    pub max_drawdown: f64,
    pub min_calmar: Option<f64>,
    pub requested_limit: usize,
    pub exported_rows: usize,
    pub formulas_sha256: String,
    pub protocol_sha256: Option<String>,
}

impl FormulaExportManifest {
    pub fn from_draft(draft: FormulaExportManifestDraft) -> Self {
        Self {
            schema_version: FORMULA_EXPORT_MANIFEST_SCHEMA_VERSION,
            created_at: Utc::now().to_rfc3339(),
            source_output_dir_sha256: draft.source_output_dir_sha256,
            source_run_manifest_sha256: draft.source_run_manifest_sha256,
            source_run_identity_hash: draft.source_run_identity_hash,
            source_date_start: draft.source_date_start,
            source_date_end: draft.source_date_end,
            target: draft.target,
            direction: draft.direction,
            rank_by: draft.rank_by,
            min_sample_size: draft.min_sample_size,
            min_win_rate: draft.min_win_rate,
            max_drawdown: draft.max_drawdown,
            min_calmar: draft.min_calmar,
            requested_limit: draft.requested_limit,
            exported_rows: draft.exported_rows,
            formulas_sha256: draft.formulas_sha256,
            protocol_sha256: draft.protocol_sha256,
        }
    }

    pub fn hash(&self) -> Result<String> {
        sha256_json(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrictProtocolValidation {
    pub stage: ResearchStage,
    pub strict: bool,
    pub protocol_sha256: Option<String>,
    pub formula_export_manifest_sha256: Option<String>,
    pub warnings: Vec<String>,
}

pub fn validate_strict_research_inputs(
    stage: ResearchStage,
    strict: bool,
    protocol: Option<&ResearchProtocol>,
    formula_manifest: Option<&FormulaExportManifest>,
    target: &str,
    cutoff: NaiveDate,
) -> Result<StrictProtocolValidation> {
    if !strict {
        return Ok(StrictProtocolValidation {
            stage,
            strict,
            protocol_sha256: protocol.map(ResearchProtocol::hash).transpose()?,
            formula_export_manifest_sha256: formula_manifest
                .map(FormulaExportManifest::hash)
                .transpose()?,
            warnings: vec!["strict protocol enforcement was disabled".to_string()],
        });
    }

    let protocol = protocol.ok_or_else(|| {
        anyhow!("--strict-protocol requires --research-protocol so stage windows are auditable")
    })?;
    let protocol_hash = protocol.hash()?;
    validate_protocol_binding(protocol, target, None)?;
    if protocol.discovery.overlaps(&protocol.validation) {
        return Err(anyhow!(
            "research protocol discovery and validation windows overlap"
        ));
    }
    if protocol.validation.overlaps(&protocol.lockbox) {
        return Err(anyhow!(
            "research protocol validation and lockbox windows overlap"
        ));
    }
    if protocol.discovery.overlaps(&protocol.lockbox) {
        return Err(anyhow!(
            "research protocol discovery and lockbox windows overlap"
        ));
    }

    let mut warnings = Vec::new();
    match stage {
        ResearchStage::Discovery => {
            if !protocol.discovery.contains(cutoff) {
                warnings.push(
                    "discovery stage cutoff is outside the protocol discovery window".to_string(),
                );
            }
        }
        ResearchStage::Validation => {
            if !protocol.validation.contains(cutoff) {
                warnings.push(
                    "validation cutoff is outside the protocol validation window".to_string(),
                );
            }
            validate_formula_manifest_for_protocol(
                protocol,
                &protocol_hash,
                formula_manifest,
                target,
            )?;
        }
        ResearchStage::Lockbox | ResearchStage::LiveShadow => {
            if !protocol.lockbox.contains(cutoff) {
                warnings.push(
                    "lockbox/live-shadow cutoff is outside the protocol lockbox window".to_string(),
                );
            }
            validate_formula_manifest_for_protocol(
                protocol,
                &protocol_hash,
                formula_manifest,
                target,
            )?;
        }
    }

    Ok(StrictProtocolValidation {
        stage,
        strict,
        protocol_sha256: Some(protocol_hash),
        formula_export_manifest_sha256: formula_manifest
            .map(FormulaExportManifest::hash)
            .transpose()?,
        warnings,
    })
}

pub fn validate_protocol_binding(
    protocol: &ResearchProtocol,
    target: &str,
    direction: Option<&str>,
) -> Result<()> {
    if protocol.schema_version != PROTOCOL_SCHEMA_VERSION {
        return Err(anyhow!(
            "research protocol schema_version {} is unsupported; expected {}",
            protocol.schema_version,
            PROTOCOL_SCHEMA_VERSION
        ));
    }
    if !protocol.strict {
        return Err(anyhow!(
            "strict eval requires a research protocol with strict=true"
        ));
    }
    if protocol.target != target {
        return Err(anyhow!(
            "research protocol target '{}' does not match evaluation target '{}'",
            protocol.target,
            target
        ));
    }
    if let Some(direction) = direction {
        let Some(protocol_direction) = protocol.direction.as_deref() else {
            return Err(anyhow!(
                "strict research protocol is missing direction; cannot bind formula provenance"
            ));
        };
        if !same_label(protocol_direction, direction) {
            return Err(anyhow!(
                "research protocol direction '{}' does not match formula manifest direction '{}'",
                protocol_direction,
                direction
            ));
        }
    }
    Ok(())
}

fn validate_formula_manifest_for_protocol(
    protocol: &ResearchProtocol,
    protocol_hash: &str,
    formula_manifest: Option<&FormulaExportManifest>,
    target: &str,
) -> Result<()> {
    let manifest = formula_manifest.ok_or_else(|| {
        anyhow!("--strict-protocol requires --formula-export-manifest for validation and lockbox")
    })?;
    if manifest.schema_version != FORMULA_EXPORT_MANIFEST_SCHEMA_VERSION {
        return Err(anyhow!(
            "formula manifest schema_version {} is unsupported; expected {}",
            manifest.schema_version,
            FORMULA_EXPORT_MANIFEST_SCHEMA_VERSION
        ));
    }
    validate_protocol_binding(protocol, target, Some(&manifest.direction))?;
    match manifest.protocol_sha256.as_deref() {
        Some(manifest_hash) if manifest_hash == protocol_hash => {}
        Some(manifest_hash) => {
            return Err(anyhow!(
                "formula manifest protocol_sha256 '{}' does not match research protocol sha256 '{}'",
                manifest_hash,
                protocol_hash
            ));
        }
        None => {
            return Err(anyhow!(
                "strict eval requires formula manifest protocol_sha256; export formulas with results --research-protocol"
            ));
        }
    }
    if manifest.target != target {
        return Err(anyhow!(
            "formula manifest target '{}' does not match evaluation target '{}'",
            manifest.target,
            target
        ));
    }
    let Some(source_end) = manifest.source_date_end else {
        return Err(anyhow!(
            "formula manifest is missing source_date_end; cannot prove discovery/pre-only provenance"
        ));
    };
    let discovery_end = protocol
        .discovery
        .end
        .ok_or_else(|| anyhow!("strict protocol discovery window must include an end date"))?;
    if source_end > discovery_end {
        return Err(anyhow!(
            "formula export source ended at {source_end}, after protocol discovery end {discovery_end}"
        ));
    }
    if let Some(validation_start) = protocol.validation.start {
        if source_end >= validation_start {
            return Err(anyhow!(
                "formula export source ended at {source_end}, overlapping validation start {validation_start}"
            ));
        }
    }
    if let Some(lockbox_start) = protocol.lockbox.start {
        if source_end >= lockbox_start {
            return Err(anyhow!(
                "formula export source ended at {source_end}, overlapping lockbox start {lockbox_start}"
            ));
        }
    }
    Ok(())
}

fn same_label(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

pub fn load_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

pub fn sha256_text(text: &str) -> String {
    sha256_bytes(text.as_bytes())
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn sha256_json<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(sha256_bytes(&bytes))
}

pub fn parse_optional_date(raw: Option<&str>, label: &str) -> Result<Option<NaiveDate>> {
    raw.map(|value| {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .with_context(|| format!("invalid {label} date '{value}'"))
    })
    .transpose()
}

pub fn parse_manifest_date(value: Option<&serde_json::Value>) -> Option<NaiveDate> {
    value
        .and_then(|value| value.as_str())
        .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn window(start: (i32, u32, u32), end: (i32, u32, u32)) -> ResearchWindow {
        ResearchWindow::new(
            Some(d(start.0, start.1, start.2)),
            Some(d(end.0, end.1, end.2)),
        )
        .unwrap()
    }

    fn protocol() -> ResearchProtocol {
        ResearchProtocol::from_draft(ResearchProtocolDraft {
            dataset_id: "es_30m_official_v2".to_string(),
            target: "2x_atr_tp_atr_stop".to_string(),
            direction: Some("long".to_string()),
            engine: Some("builtin".to_string()),
            discovery: window((2024, 1, 1), (2024, 6, 30)),
            validation: window((2024, 7, 1), (2024, 12, 31)),
            lockbox: window((2025, 1, 1), (2025, 3, 31)),
            candidate_top_k: Some(100),
        })
    }

    fn manifest(source_end: NaiveDate) -> FormulaExportManifest {
        FormulaExportManifest::from_draft(FormulaExportManifestDraft {
            source_output_dir_sha256: "source".to_string(),
            source_run_manifest_sha256: Some("run".to_string()),
            source_run_identity_hash: Some("identity".to_string()),
            source_date_start: Some(d(2024, 1, 1)),
            source_date_end: Some(source_end),
            target: "2x_atr_tp_atr_stop".to_string(),
            direction: "long".to_string(),
            rank_by: "calmar".to_string(),
            min_sample_size: 25,
            min_win_rate: 0.0,
            max_drawdown: 10.0,
            min_calmar: None,
            requested_limit: 100,
            exported_rows: 10,
            formulas_sha256: "formulas".to_string(),
            protocol_sha256: None,
        })
    }

    fn manifest_for_protocol(
        protocol: &ResearchProtocol,
        source_end: NaiveDate,
    ) -> FormulaExportManifest {
        let mut manifest = manifest(source_end);
        manifest.protocol_sha256 = Some(protocol.hash().unwrap());
        manifest
    }

    #[test]
    fn strict_protocol_rejects_discovery_validation_overlap() {
        let mut protocol = protocol();
        protocol.validation = window((2024, 6, 30), (2024, 12, 31));

        let err = validate_strict_research_inputs(
            ResearchStage::Validation,
            true,
            Some(&protocol),
            Some(&manifest_for_protocol(&protocol, d(2024, 6, 30))),
            "2x_atr_tp_atr_stop",
            d(2024, 12, 31),
        )
        .unwrap_err();

        assert!(err.to_string().contains("discovery and validation"));
    }

    #[test]
    fn strict_protocol_rejects_formula_manifest_after_discovery_window() {
        let protocol = protocol();

        let err = validate_strict_research_inputs(
            ResearchStage::Validation,
            true,
            Some(&protocol),
            Some(&manifest_for_protocol(&protocol, d(2024, 7, 1))),
            "2x_atr_tp_atr_stop",
            d(2024, 12, 31),
        )
        .unwrap_err();

        assert!(err.to_string().contains("after protocol discovery end"));
    }

    #[test]
    fn protocol_hash_changes_when_stage_windows_change() {
        let first = protocol();
        let mut second = protocol();
        second.lockbox = window((2025, 2, 1), (2025, 3, 31));

        assert_ne!(first.hash().unwrap(), second.hash().unwrap());
    }

    #[test]
    fn strict_protocol_rejects_non_strict_protocol_file() {
        let mut protocol = protocol();
        protocol.strict = false;

        let err = validate_strict_research_inputs(
            ResearchStage::Discovery,
            true,
            Some(&protocol),
            None,
            "2x_atr_tp_atr_stop",
            d(2024, 6, 30),
        )
        .unwrap_err();

        assert!(err.to_string().contains("strict=true"));
    }

    #[test]
    fn strict_protocol_rejects_formula_manifest_direction_mismatch() {
        let protocol = protocol();
        let mut manifest = manifest_for_protocol(&protocol, d(2024, 6, 30));
        manifest.direction = "short".to_string();

        let err = validate_strict_research_inputs(
            ResearchStage::Validation,
            true,
            Some(&protocol),
            Some(&manifest),
            "2x_atr_tp_atr_stop",
            d(2024, 12, 31),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("does not match formula manifest direction")
        );
    }

    #[test]
    fn strict_protocol_rejects_unbound_formula_manifest() {
        let protocol = protocol();
        let manifest = manifest(d(2024, 6, 30));

        let err = validate_strict_research_inputs(
            ResearchStage::Validation,
            true,
            Some(&protocol),
            Some(&manifest),
            "2x_atr_tp_atr_stop",
            d(2024, 12, 31),
        )
        .unwrap_err();

        assert!(err.to_string().contains("protocol_sha256"));
    }

    #[test]
    fn strict_protocol_rejects_stale_formula_manifest_binding() {
        let protocol = protocol();
        let mut manifest = manifest_for_protocol(&protocol, d(2024, 6, 30));
        manifest.protocol_sha256 = Some("stale".to_string());

        let err = validate_strict_research_inputs(
            ResearchStage::Validation,
            true,
            Some(&protocol),
            Some(&manifest),
            "2x_atr_tp_atr_stop",
            d(2024, 12, 31),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("does not match research protocol sha256")
        );
    }

    #[test]
    fn strict_protocol_rejects_unknown_schema_versions() {
        let mut protocol = protocol();
        protocol.schema_version = 999;

        let err = validate_strict_research_inputs(
            ResearchStage::Discovery,
            true,
            Some(&protocol),
            None,
            "2x_atr_tp_atr_stop",
            d(2024, 6, 30),
        )
        .unwrap_err();

        assert!(err.to_string().contains("schema_version"));
    }
}
