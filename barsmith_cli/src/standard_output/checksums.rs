use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use barsmith_rs::protocol::{
    sha256_bytes as protocol_sha256_bytes, sha256_file as protocol_sha256_file,
    sha256_text as protocol_sha256_text,
};

use super::StandardOutputPlan;

pub(super) fn write_forward_checksums(
    plan: &StandardOutputPlan,
    written_files: &[PathBuf],
) -> Result<PathBuf> {
    let mut paths = vec![
        plan.output_dir.join("command.txt"),
        plan.output_dir.join("command.json"),
        plan.output_dir.join("run_manifest.json"),
        plan.output_dir.join("reports").join("summary.md"),
    ];
    if plan.checksum_artifacts {
        paths.extend(
            written_files
                .iter()
                .filter(|path| path.starts_with(&plan.output_dir))
                .cloned(),
        );
        paths.push(plan.output_dir.join("barsmith.log"));
    }

    write_checksum_file(plan, paths)
}

pub(super) fn write_checksums(plan: &StandardOutputPlan) -> Result<PathBuf> {
    let mut paths = vec![
        plan.output_dir.join("command.txt"),
        plan.output_dir.join("command.json"),
        plan.output_dir.join("run_manifest.json"),
        plan.output_dir.join("reports").join("summary.md"),
    ];
    if plan.checksum_artifacts {
        paths.push(plan.output_dir.join("cumulative.duckdb"));
        paths.push(plan.output_dir.join("barsmith.log"));
        let results_dir = plan.output_dir.join("results_parquet");
        if results_dir.exists() {
            let mut parquet_parts = Vec::new();
            for entry in fs::read_dir(&results_dir)? {
                let path = entry?.path();
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("part-") && name.ends_with(".parquet"))
                {
                    parquet_parts.push(path);
                }
            }
            parquet_parts.sort();
            paths.extend(parquet_parts);
        }
    }

    write_checksum_file(plan, paths)
}

fn write_checksum_file(plan: &StandardOutputPlan, paths: Vec<PathBuf>) -> Result<PathBuf> {
    let mut lines = String::new();
    for path in paths {
        if path.is_file() {
            let digest = sha256_file(&path)?;
            let rel = path
                .strip_prefix(&plan.output_dir)
                .unwrap_or(&path)
                .display()
                .to_string();
            lines.push_str(&format!("{digest}  {rel}\n"));
        }
    }

    let checksum_path = plan.output_dir.join("checksums.sha256");
    fs::write(&checksum_path, lines).with_context(|| "failed to write checksums.sha256")?;
    Ok(checksum_path)
}

pub(super) fn sha256_text(value: &str) -> String {
    protocol_sha256_text(value)
}

pub(super) fn sha256_bytes(value: &[u8]) -> String {
    protocol_sha256_bytes(value)
}

pub(super) fn sha256_file(path: &Path) -> Result<String> {
    protocol_sha256_file(path)
}
