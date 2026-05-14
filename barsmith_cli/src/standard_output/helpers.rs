use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::SystemTime;

use anyhow::{Context, Result};
use barsmith_rs::config::Direction;
use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};

use crate::cli::DirectionValue;

pub(super) fn optional_metric(value: Option<f64>) -> String {
    value
        .map(format_metric)
        .unwrap_or_else(|| "n/a".to_string())
}

pub(super) fn relative_to(base: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(base).ok().map(path_for_json)
}

pub(super) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(&tmp_path, bytes)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| format!("failed to replace {}", path.display()))
}

pub(super) fn dataset_id_from_csv(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(sanitize_segment)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "dataset".to_string())
}

pub(super) fn dataset_id_from_prepared(path: &Path) -> String {
    let stem = path.file_stem().and_then(|stem| stem.to_str());
    if matches!(stem, Some("barsmith_prepared")) {
        if let Some(parent) = path.parent().and_then(|parent| parent.file_name()) {
            if let Some(name) = parent.to_str() {
                return sanitize_segment(name);
            }
        }
    }
    stem.map(sanitize_segment)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "dataset".to_string())
}

pub(super) fn normalize_target(target: &str) -> String {
    if target == "atr_stop" {
        "2x_atr_tp_atr_stop".to_string()
    } else {
        target.to_string()
    }
}

pub(super) fn sanitize_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_was_sep = false;
    for ch in raw.trim().chars() {
        let normalized = if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        if normalized == '_' {
            if !last_was_sep {
                out.push(normalized);
            }
            last_was_sep = true;
        } else {
            out.push(normalized);
            last_was_sep = false;
        }
    }
    let trimmed = out.trim_matches(['_', '.', '-']).to_string();
    if trimmed.is_empty() {
        "run".to_string()
    } else {
        trimmed
    }
}

pub(super) fn shell_join(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| {
            if arg
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || "-_./:=,+".contains(ch))
            {
                arg.clone()
            } else {
                format!("'{}'", arg.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn git_rev_parse<const N: usize>(args: [&str; N]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

pub(super) fn now_iso() -> String {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

pub(super) fn now_compact() -> String {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

pub(super) fn format_direction(direction: DirectionValue) -> &'static str {
    match direction {
        DirectionValue::Long => "long",
        DirectionValue::Short => "short",
        DirectionValue::Both => "both",
    }
}

pub(super) fn format_config_direction(direction: Direction) -> &'static str {
    match direction {
        Direction::Long => "long",
        Direction::Short => "short",
        Direction::Both => "both",
    }
}

pub(super) fn path_for_json(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

pub(super) fn serialize_metric<S>(
    value: &f64,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if value.is_finite() {
        serializer.serialize_f64(*value)
    } else {
        serializer.serialize_str(&format_metric(*value))
    }
}

pub(super) fn serialize_optional_metric<S>(
    value: &Option<f64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) if value.is_finite() => serializer.serialize_some(value),
        Some(value) => serializer.serialize_some(&format_metric(*value)),
        None => serializer.serialize_none(),
    }
}

pub(super) fn format_metric(value: f64) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "Inf".to_string()
    } else if value.is_infinite() && value.is_sign_negative() {
        "-Inf".to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else {
        value.to_string()
    }
}
