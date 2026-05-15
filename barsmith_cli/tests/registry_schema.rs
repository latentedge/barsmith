use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

const REGISTRY_SCHEMA_VERSION: u64 = 2;

#[test]
fn tracked_registry_records_match_current_schema() {
    let root = workspace_root();
    let registry_dir = root.join("runs/registry");
    let records = collect_json_files(&registry_dir).expect("registry records should be readable");

    assert!(
        !records.is_empty(),
        "runs/registry should contain at least one tracked example record"
    );

    for path in records {
        let raw = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let value: Value = serde_json::from_str(&raw)
            .unwrap_or_else(|err| panic!("invalid JSON in {}: {err}", path.display()));
        validate_common_record(&path, &value);
        match value.get("run_kind").and_then(Value::as_str) {
            Some("comb") => validate_comb_record(&path, &value),
            Some("forward-test") => validate_forward_record(&path, &value),
            other => panic!("{} has unsupported run_kind {other:?}", path.display()),
        }
    }
}

fn validate_common_record(path: &Path, value: &Value) {
    assert_eq!(
        value.get("schema_version").and_then(Value::as_u64),
        Some(REGISTRY_SCHEMA_VERSION),
        "{} should use registry schema version {REGISTRY_SCHEMA_VERSION}",
        path.display()
    );

    for field in [
        "run_kind",
        "run_id",
        "dataset_id",
        "target",
        "started_at",
        "completed_at",
        "run_path",
        "command_sha256",
        "checksum_file",
    ] {
        assert!(
            value.get(field).is_some(),
            "{} is missing required field {field}",
            path.display()
        );
    }

    let run_path = value
        .get("run_path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        !run_path.starts_with('/') && !run_path.contains(".."),
        "{} should store a portable run_path, got {run_path:?}",
        path.display()
    );
    assert!(
        !raw_json_contains_private_path(value),
        "{} should not store private local paths",
        path.display()
    );
}

fn validate_comb_record(path: &Path, value: &Value) {
    for field in [
        "direction",
        "position_sizing",
        "stop_distance_unit",
        "risk_model",
        "top_calmar",
        "top_total_r",
    ] {
        assert!(
            value.get(field).is_some(),
            "{} is missing comb registry field {field}",
            path.display()
        );
    }
}

fn validate_forward_record(path: &Path, value: &Value) {
    for field in [
        "cutoff",
        "prepared_sha256",
        "formulas_sha256",
        "position_sizing",
        "stop_distance_unit",
        "risk_model",
        "workflow_status",
        "strict_protocol",
        "artifact_files",
    ] {
        assert!(
            value.get(field).is_some(),
            "{} is missing forward-test registry field {field}",
            path.display()
        );
    }
}

fn raw_json_contains_private_path(value: &Value) -> bool {
    let raw = value.to_string();
    raw.contains("/Users/") || raw.contains("personal/algotrade") || raw.contains("../algotrade")
}

fn collect_json_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_json_files_inner(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_json_files_inner(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_json_files_inner(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "json") {
            files.push(path);
        }
    }
    Ok(())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("barsmith_cli should live under the workspace root")
        .to_path_buf()
}
