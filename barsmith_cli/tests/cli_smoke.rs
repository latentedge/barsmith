use std::path::{Path, PathBuf};
use std::process::Command;

use barsmith_rs::protocol::{ResearchProtocol, load_json};
use tempfile::tempdir;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn barsmith_cmd() -> Command {
    if let Some(bin) = option_env!("CARGO_BIN_EXE_barsmith_cli") {
        Command::new(bin)
    } else {
        let mut cmd = Command::new("cargo");
        cmd.arg("run")
            .arg("--manifest-path")
            .arg(workspace_root().join("Cargo.toml"))
            .args(["-p", "barsmith_cli", "--"]);
        cmd
    }
}

#[test]
fn cli_runs_on_sample_dataset() {
    let sample_csv = workspace_root()
        .join("tests")
        .join("data")
        .join("ohlcv_tiny.csv");
    assert!(
        sample_csv.exists(),
        "sample CSV missing at {}",
        sample_csv.display()
    );

    let temp_dir = tempdir().expect("temp run root");

    let mut cmd = barsmith_cmd();

    let status = cmd
        .args([
            "comb",
            "--csv",
            sample_csv.to_str().expect("sample"),
            "--direction",
            "long",
            "--target",
            "next_bar_color_and_wicks",
            "--position-sizing",
            "fractional",
            "--dataset-id",
            "tiny sample",
            "--run-id",
            "sample smoke",
            "--max-depth",
            "2",
            "--min-samples",
            "25",
            "--batch-size",
            "25",
            "--workers",
            "1",
            "--max-combos",
            "10",
            "--dry-run",
        ])
        .current_dir(temp_dir.path())
        .status()
        .expect("failed to spawn barsmith_cli");

    assert!(status.success(), "barsmith_cli exited with {status:?}");

    let output_dir = temp_dir
        .path()
        .join("runs")
        .join("artifacts")
        .join("comb")
        .join("next_bar_color_and_wicks")
        .join("long")
        .join("tiny_sample")
        .join("sample_smoke");
    for path in [
        output_dir.join("barsmith_prepared.csv"),
        output_dir.join("command.txt"),
        output_dir.join("command.json"),
    ] {
        assert!(path.exists(), "expected {}", path.display());
    }
}

#[test]
fn cli_results_queries_real_run_output() {
    let sample_csv = workspace_root()
        .join("tests")
        .join("data")
        .join("ohlcv_tiny.csv");
    let temp_dir = tempdir().expect("temp run root");
    let output_dir = temp_dir
        .path()
        .join("runs")
        .join("artifacts")
        .join("comb")
        .join("next_bar_color_and_wicks")
        .join("long")
        .join("tiny_sample")
        .join("results_smoke");

    let mut run_cmd = barsmith_cmd();
    let run_status = run_cmd
        .args([
            "comb",
            "--csv",
            sample_csv.to_str().expect("sample"),
            "--direction",
            "long",
            "--target",
            "next_bar_color_and_wicks",
            "--position-sizing",
            "fractional",
            "--dataset-id",
            "tiny sample",
            "--run-id",
            "results smoke",
            "--max-depth",
            "2",
            "--min-samples",
            "25",
            "--batch-size",
            "50",
            "--workers",
            "1",
            "--max-combos",
            "200",
            "--stats-detail",
            "core",
            "--report",
            "off",
            "--force",
        ])
        .current_dir(temp_dir.path())
        .status()
        .expect("failed to spawn barsmith_cli comb");
    assert!(run_status.success(), "comb exited with {run_status:?}");

    let mut results_cmd = barsmith_cmd();
    let output = results_cmd
        .args([
            "results",
            "--output-dir",
            output_dir.to_str().expect("output"),
            "--direction",
            "long",
            "--target",
            "next_bar_color_and_wicks",
            "--min-samples",
            "25",
            "--limit",
            "3",
        ])
        .current_dir(workspace_root())
        .output()
        .expect("failed to spawn barsmith_cli results");

    assert!(
        output.status.success(),
        "results exited with {:?}",
        output.status
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Top") || stdout.contains("No results matched"),
        "unexpected results stdout: {stdout}"
    );

    let mut total_return_cmd = barsmith_cmd();
    let export_path = temp_dir.path().join("formulas.txt");
    let total_return_output = total_return_cmd
        .args([
            "results",
            "--output-dir",
            output_dir.to_str().expect("output"),
            "--direction",
            "long",
            "--target",
            "next_bar_color_and_wicks",
            "--min-samples",
            "25",
            "--rank-by",
            "total-return",
            "--limit",
            "3",
            "--export-formulas",
            export_path.to_str().expect("export path"),
        ])
        .current_dir(workspace_root())
        .output()
        .expect("failed to spawn barsmith_cli results ranked by total return");

    assert!(
        total_return_output.status.success(),
        "total-return results exited with {:?}",
        total_return_output.status
    );
    let total_return_stdout = String::from_utf8_lossy(&total_return_output.stdout);
    assert!(
        total_return_stdout.contains("rank_by=total-return"),
        "unexpected total-return results stdout: {total_return_stdout}"
    );
    assert!(
        total_return_stdout.contains("Total R:"),
        "total-return output should show total R: {total_return_stdout}"
    );
    assert!(
        total_return_stdout.contains("discovery/pre-only"),
        "formula export should print the holdout provenance note: {total_return_stdout}"
    );
    let exported = std::fs::read_to_string(&export_path).expect("exported formulas");
    assert!(
        exported.contains("# Barsmith ranked formula export"),
        "formula export should include metadata comments: {exported}"
    );
    assert!(
        exported.contains("discovery/pre-only"),
        "formula export should include the holdout provenance note: {exported}"
    );
    assert!(
        exported.contains("Rank 1:") || exported.lines().all(|line| line.starts_with('#')),
        "unexpected formula export: {exported}"
    );
    let export_manifest_path = temp_dir.path().join("formula_export_manifest.json");
    let export_manifest =
        std::fs::read_to_string(&export_manifest_path).expect("formula export manifest");
    assert!(
        export_manifest.contains("\"target\": \"next_bar_color_and_wicks\""),
        "unexpected formula export manifest: {export_manifest}"
    );

    let protocol_path = temp_dir.path().join("research_protocol.json");
    std::fs::write(
        &protocol_path,
        r#"{
  "schema_version": 1,
  "protocol_id": "results-export-smoke",
  "dataset_id": "tiny_sample",
  "target": "next_bar_color_and_wicks",
  "direction": "long",
  "engine": "builtin",
  "strict": true,
  "discovery": {"start": "2024-01-01", "end": "2024-06-30"},
  "validation": {"start": "2024-07-01", "end": "2024-12-31"},
  "lockbox": {"start": "2025-01-01", "end": "2025-03-31"},
  "live_shadow_min_days": 30,
  "live_shadow_min_trades": 100,
  "candidate_top_k": 3,
  "notes": []
}"#,
    )
    .expect("protocol");
    let protocol_hash = load_json::<ResearchProtocol>(&protocol_path)
        .expect("load results protocol")
        .hash()
        .expect("results protocol hash");
    let protocol_export_path = temp_dir.path().join("protocol_formulas.txt");
    let protocol_manifest_path = temp_dir
        .path()
        .join("protocol_formula_export_manifest.json");
    let mut protocol_results_cmd = barsmith_cmd();
    let protocol_results_output = protocol_results_cmd
        .args([
            "results",
            "--output-dir",
            output_dir.to_str().expect("output"),
            "--direction",
            "long",
            "--target",
            "next_bar_color_and_wicks",
            "--min-samples",
            "25",
            "--rank-by",
            "total-return",
            "--limit",
            "3",
            "--export-formulas",
            protocol_export_path.to_str().expect("protocol export path"),
            "--export-formula-manifest",
            protocol_manifest_path
                .to_str()
                .expect("protocol manifest path"),
            "--research-protocol",
            protocol_path.to_str().expect("protocol path"),
        ])
        .current_dir(workspace_root())
        .output()
        .expect("failed to spawn protocol-bound results export");

    assert!(
        protocol_results_output.status.success(),
        "protocol-bound results export exited with {:?}",
        protocol_results_output.status
    );
    let protocol_manifest =
        std::fs::read_to_string(&protocol_manifest_path).expect("protocol manifest");
    assert!(
        protocol_manifest.contains(&format!("\"protocol_sha256\": \"{protocol_hash}\"")),
        "protocol-bound formula export should include the protocol hash: {protocol_manifest}"
    );
}

#[test]
fn cli_eval_formulas_strict_protocol_writes_overfit_and_stress_reports() {
    let temp_dir = tempdir().expect("temp run root");
    let prepared = workspace_root()
        .join("barsmith_rs")
        .join("tests")
        .join("fixtures")
        .join("formula_eval_prepared.csv");
    let formulas = workspace_root()
        .join("barsmith_rs")
        .join("tests")
        .join("fixtures")
        .join("formula_eval_formulas.txt");
    let protocol = temp_dir.path().join("research_protocol.json");
    let formula_manifest = temp_dir.path().join("formula_export_manifest.json");
    std::fs::write(
        &protocol,
        r#"{
  "schema_version": 1,
  "protocol_id": "strict-smoke",
  "dataset_id": "tiny_forward",
  "target": "2x_atr_tp_atr_stop",
  "direction": "long",
  "engine": "custom",
  "strict": true,
  "discovery": {"start": "2024-12-29", "end": "2024-12-31"},
  "validation": {"start": "2025-01-01", "end": "2025-01-03"},
  "lockbox": {"start": "2025-01-04", "end": "2025-01-05"},
  "live_shadow_min_days": 30,
  "live_shadow_min_trades": 100,
  "candidate_top_k": 3,
  "notes": []
}"#,
    )
    .expect("protocol");
    let protocol_hash = load_json::<ResearchProtocol>(&protocol)
        .expect("load protocol")
        .hash()
        .expect("protocol hash");
    std::fs::write(
        &formula_manifest,
        format!(
            r#"{{
  "schema_version": 2,
  "created_at": "2026-05-14T00:00:00Z",
  "source_output_dir_path_sha256": "source",
  "source_run_manifest_sha256": null,
  "source_run_identity_hash": null,
  "source_date_start": "2024-12-29",
  "source_date_end": "2024-12-31",
  "target": "2x_atr_tp_atr_stop",
  "direction": "long",
  "rank_by": "total-return",
  "min_sample_size": 1,
  "min_win_rate": 0.0,
  "max_drawdown": 1000.0,
  "min_calmar": null,
  "requested_limit": 3,
  "exported_rows": 3,
  "formulas_sha256": "fixture",
  "protocol_sha256": "{protocol_hash}"
}}"#
        ),
    )
    .expect("formula manifest");

    let runs_root = temp_dir.path().join("artifacts");
    let registry_dir = temp_dir.path().join("registry");
    let mut cmd = barsmith_cmd();
    let status = cmd
        .args([
            "eval-formulas",
            "--prepared",
            prepared.to_str().expect("prepared"),
            "--formulas",
            formulas.to_str().expect("formulas"),
            "--target",
            "2x_atr_tp_atr_stop",
            "--stacking-mode",
            "no-stacking",
            "--cutoff",
            "2024-12-31",
            "--stage",
            "validation",
            "--strict-protocol",
            "--research-protocol",
            protocol.to_str().expect("protocol"),
            "--formula-export-manifest",
            formula_manifest.to_str().expect("manifest"),
            "--candidate-top-k",
            "3",
            "--pre-min-trades",
            "1",
            "--post-min-trades",
            "1",
            "--post-warn-below-trades",
            "1",
            "--max-contracts",
            "2",
            "--runs-root",
            runs_root.to_str().expect("runs"),
            "--dataset-id",
            "tiny forward",
            "--run-id",
            "strict smoke",
            "--registry-dir",
            registry_dir.to_str().expect("registry"),
        ])
        .current_dir(workspace_root())
        .status()
        .expect("failed to spawn strict eval");

    assert!(status.success(), "strict eval exited with {status:?}");
    let output_dir = runs_root
        .join("forward-test")
        .join("2x_atr_tp_atr_stop")
        .join("tiny_forward")
        .join("2024-12-31")
        .join("strict_smoke");
    for path in [
        output_dir.join("protocol_validation.json"),
        output_dir.join("overfit_report.json"),
        output_dir.join("stress_report.json"),
        output_dir.join("reports").join("overfit.md"),
        output_dir.join("reports").join("stress.md"),
    ] {
        assert!(path.exists(), "expected {}", path.display());
    }
    let registry = std::fs::read_to_string(
        registry_dir
            .join("forward-test")
            .join("2x_atr_tp_atr_stop")
            .join("tiny_forward")
            .join("2024-12-31")
            .join("strict_smoke.json"),
    )
    .expect("registry");
    assert!(registry.contains("\"protocol_sha256\""));
    assert!(registry.contains("\"overfit_status\""));
    assert!(registry.contains("\"stress_status\""));
    let stress_matrix =
        std::fs::read_to_string(output_dir.join("stress_matrix.csv")).expect("stress matrix");
    assert!(stress_matrix.contains("half_max_contracts"));
}

#[test]
fn cli_eval_formulas_lockbox_rejects_multi_formula_files() {
    let temp_dir = tempdir().expect("temp run root");
    let prepared = workspace_root()
        .join("barsmith_rs")
        .join("tests")
        .join("fixtures")
        .join("formula_eval_prepared.csv");
    let formulas = workspace_root()
        .join("barsmith_rs")
        .join("tests")
        .join("fixtures")
        .join("formula_eval_formulas.txt");

    let mut cmd = barsmith_cmd();
    let output = cmd
        .args([
            "eval-formulas",
            "--prepared",
            prepared.to_str().expect("prepared"),
            "--formulas",
            formulas.to_str().expect("formulas"),
            "--target",
            "2x_atr_tp_atr_stop",
            "--stage",
            "lockbox",
            "--run-id",
            "lockbox_rejects_multi_formula_files",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("failed to spawn lockbox eval");

    assert!(
        !output.status.success(),
        "lockbox should reject multi-formula files"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("requires exactly one frozen formula"),
        "unexpected lockbox stderr: {stderr}"
    );
}

#[test]
fn cli_comb_writes_standard_output_metadata_and_registry() {
    let sample_csv = workspace_root()
        .join("tests")
        .join("data")
        .join("ohlcv_tiny.csv");
    let temp_dir = tempdir().expect("temp run root");

    let mut cmd = barsmith_cmd();
    let status = cmd
        .args([
            "comb",
            "--csv",
            sample_csv.to_str().expect("sample"),
            "--direction",
            "long",
            "--target",
            "next_bar_color_and_wicks",
            "--position-sizing",
            "fractional",
            "--dataset-id",
            "tiny sample",
            "--run-id",
            "metadata smoke",
            "--max-depth",
            "2",
            "--min-samples",
            "25",
            "--batch-size",
            "50",
            "--workers",
            "1",
            "--max-combos",
            "50",
            "--stats-detail",
            "core",
            "--report",
            "off",
            "--force",
        ])
        .current_dir(temp_dir.path())
        .status()
        .expect("failed to spawn barsmith_cli comb");

    assert!(status.success(), "comb exited with {status:?}");

    let output_dir = temp_dir
        .path()
        .join("runs")
        .join("artifacts")
        .join("comb")
        .join("next_bar_color_and_wicks")
        .join("long")
        .join("tiny_sample")
        .join("metadata_smoke");
    let registry_path = temp_dir
        .path()
        .join("runs")
        .join("registry")
        .join("comb")
        .join("next_bar_color_and_wicks")
        .join("long")
        .join("tiny_sample")
        .join("metadata_smoke.json");
    for path in [
        output_dir.join("command.txt"),
        output_dir.join("command.json"),
        output_dir.join("run_manifest.json"),
        output_dir.join("checksums.sha256"),
        output_dir.join("reports").join("summary.md"),
        registry_path.clone(),
    ] {
        assert!(path.exists(), "expected {}", path.display());
    }

    let registry = std::fs::read_to_string(registry_path).expect("registry should be readable");
    assert!(registry.contains("\"run_id\": \"metadata_smoke\""));
    assert!(
        registry.contains(
            "\"run_path\": \"comb/next_bar_color_and_wicks/long/tiny_sample/metadata_smoke\""
        ),
        "registry should keep a portable run path: {registry}"
    );
    assert!(
        !registry.contains(temp_dir.path().to_str().expect("temp path")),
        "registry should not expose local temp paths: {registry}"
    );
    assert!(registry.contains("\"top_calmar\""));
    assert!(registry.contains("\"top_total_r\""));
    assert!(registry.contains("\"formula_sha256\""));
    assert!(registry.contains("\"total_return_r\""));

    let checksums =
        std::fs::read_to_string(output_dir.join("checksums.sha256")).expect("checksums");
    assert!(
        checksums.contains("reports/summary.md"),
        "checksums should cover the closeout summary: {checksums}"
    );
}

#[test]
fn cli_eval_formulas_writes_outputs_and_plot() {
    let fixture_dir = workspace_root()
        .join("barsmith_rs")
        .join("tests")
        .join("fixtures");
    let prepared_csv = fixture_dir.join("formula_eval_prepared.csv");
    let formulas = fixture_dir.join("formula_eval_formulas.txt");
    assert!(prepared_csv.exists(), "missing {}", prepared_csv.display());
    assert!(formulas.exists(), "missing {}", formulas.display());

    let temp_dir = tempdir().expect("temp run root");
    let csv_out = temp_dir.path().join("formula_results.csv");
    let json_out = temp_dir.path().join("formula_results.json");
    let selection_out = temp_dir.path().join("selection_report.json");
    let selection_decisions_out = temp_dir.path().join("selection_decisions.csv");
    let selected_formulas_out = temp_dir.path().join("selected_formulas.txt");
    let frs_out = temp_dir.path().join("frs.csv");
    let frs_windows_out = temp_dir.path().join("frs_windows.csv");
    let curves_out = temp_dir.path().join("curves.csv");
    let plot_out = temp_dir.path().join("curves.png");

    let mut cmd = barsmith_cmd();

    let status = cmd
        .args([
            "eval-formulas",
            "--prepared",
            prepared_csv.to_str().expect("prepared"),
            "--formulas",
            formulas.to_str().expect("formulas"),
            "--target",
            "2x_atr_tp_atr_stop",
            "--stacking-mode",
            "no-stacking",
            "--cutoff",
            "2024-12-31",
            "--dataset-id",
            "tiny forward",
            "--run-id",
            "explicit outputs",
            "--report-top",
            "2",
            "--csv-out",
            csv_out.to_str().expect("csv"),
            "--json-out",
            json_out.to_str().expect("json"),
            "--selection-out",
            selection_out.to_str().expect("selection"),
            "--selection-decisions-out",
            selection_decisions_out
                .to_str()
                .expect("selection decisions"),
            "--selected-formulas-out",
            selected_formulas_out.to_str().expect("selected formulas"),
            "--frs-out",
            frs_out.to_str().expect("frs"),
            "--frs-windows-out",
            frs_windows_out.to_str().expect("frs windows"),
            "--equity-curves-out",
            curves_out.to_str().expect("curves"),
            "--plot",
            "--plot-mode",
            "combined",
            "--plot-out",
            plot_out.to_str().expect("plot"),
        ])
        .current_dir(temp_dir.path())
        .status()
        .expect("failed to spawn barsmith_cli eval-formulas");

    assert!(status.success(), "barsmith_cli exited with {status:?}");
    for path in [
        &csv_out,
        &json_out,
        &selection_out,
        &selection_decisions_out,
        &selected_formulas_out,
        &frs_out,
        &frs_windows_out,
        &curves_out,
        &plot_out,
    ] {
        let metadata = std::fs::metadata(path).expect("output should exist");
        assert!(metadata.len() > 0, "{} should not be empty", path.display());
    }
    let standard_output_dir = temp_dir
        .path()
        .join("runs")
        .join("artifacts")
        .join("forward-test")
        .join("2x_atr_tp_atr_stop")
        .join("tiny_forward")
        .join("2024-12-31")
        .join("explicit_outputs");
    for path in [
        standard_output_dir.join("command.txt"),
        standard_output_dir.join("command.json"),
        standard_output_dir.join("run_manifest.json"),
        temp_dir
            .path()
            .join("runs")
            .join("registry")
            .join("forward-test")
            .join("2x_atr_tp_atr_stop")
            .join("tiny_forward")
            .join("2024-12-31")
            .join("explicit_outputs.json"),
    ] {
        assert!(path.exists(), "expected {}", path.display());
    }
}

#[test]
fn cli_eval_formulas_writes_standard_forward_test_folder_and_registry() {
    let fixture_dir = workspace_root()
        .join("barsmith_rs")
        .join("tests")
        .join("fixtures");
    let prepared_csv = fixture_dir.join("formula_eval_prepared.csv");
    let formulas = fixture_dir.join("formula_eval_formulas.txt");

    let temp_dir = tempdir().expect("temp run root");

    let mut cmd = barsmith_cmd();
    let status = cmd
        .args([
            "eval-formulas",
            "--prepared",
            prepared_csv.to_str().expect("prepared"),
            "--formulas",
            formulas.to_str().expect("formulas"),
            "--target",
            "2x_atr_tp_atr_stop",
            "--stacking-mode",
            "no-stacking",
            "--cutoff",
            "2024-12-31",
            "--report-top",
            "2",
            "--dataset-id",
            "tiny forward",
            "--run-id",
            "forward smoke",
            "--checksum-artifacts",
            "--plot",
            "--plot-mode",
            "combined",
        ])
        .current_dir(temp_dir.path())
        .status()
        .expect("failed to spawn barsmith_cli eval-formulas");

    assert!(status.success(), "eval-formulas exited with {status:?}");

    let output_dir = temp_dir
        .path()
        .join("runs")
        .join("artifacts")
        .join("forward-test")
        .join("2x_atr_tp_atr_stop")
        .join("tiny_forward")
        .join("2024-12-31")
        .join("forward_smoke");
    let registry_path = temp_dir
        .path()
        .join("runs")
        .join("registry")
        .join("forward-test")
        .join("2x_atr_tp_atr_stop")
        .join("tiny_forward")
        .join("2024-12-31")
        .join("forward_smoke.json");

    for path in [
        output_dir.join("command.txt"),
        output_dir.join("command.json"),
        output_dir.join("run_manifest.json"),
        output_dir.join("checksums.sha256"),
        output_dir.join("reports").join("summary.md"),
        output_dir.join("reports").join("selection.md"),
        output_dir.join("formula_results.csv"),
        output_dir.join("formula_results.json"),
        output_dir.join("selection_report.json"),
        output_dir.join("selection_decisions.csv"),
        output_dir.join("selected_formulas.txt"),
        output_dir.join("frs_summary.csv"),
        output_dir.join("frs_windows.csv"),
        output_dir.join("equity_curves.csv"),
        output_dir.join("plots").join("equity_curves.png"),
        registry_path.clone(),
    ] {
        assert!(path.exists(), "expected {}", path.display());
    }

    let registry = std::fs::read_to_string(registry_path).expect("registry should be readable");
    assert!(registry.contains("\"run_kind\": \"forward-test\""));
    assert!(registry.contains("\"run_id\": \"forward_smoke\""));
    assert!(
        registry.contains(
            "\"run_path\": \"forward-test/2x_atr_tp_atr_stop/tiny_forward/2024-12-31/forward_smoke\""
        ),
        "registry should keep a portable run path: {registry}"
    );
    assert!(
        !registry.contains(temp_dir.path().to_str().expect("temp path")),
        "registry should not expose local temp paths: {registry}"
    );
    assert!(registry.contains("\"formula_sha256\""));
    assert!(registry.contains("\"prepared_sha256\""));
    assert!(registry.contains("\"formulas_sha256\""));
    assert!(registry.contains("\"selection_status\""));
    assert!(registry.contains("\"diagnostic_top_post_formula_sha256\""));
    assert!(
        !registry.contains("\"calmar_equity\": null"),
        "registry should preserve non-finite metric values explicitly: {registry}"
    );
    assert!(
        registry.contains("\"calmar_equity\": \"Inf\"")
            || registry.contains("\"calmar_equity\": \"-Inf\"")
            || registry.contains("\"calmar_equity\":")
    );

    let checksums =
        std::fs::read_to_string(output_dir.join("checksums.sha256")).expect("checksums");
    assert!(checksums.contains("reports/summary.md"));
    assert!(checksums.contains("reports/selection.md"));
    assert!(checksums.contains("selection_report.json"));
    assert!(checksums.contains("formula_results.csv"));
    assert!(checksums.contains("plots/equity_curves.png"));
}
