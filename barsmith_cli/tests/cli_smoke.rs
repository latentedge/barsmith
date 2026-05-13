use std::path::{Path, PathBuf};
use std::process::Command;

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
        cmd.args(["run", "-p", "barsmith_cli", "--"]);
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

    let temp_dir = tempdir().expect("temp output dir");
    let output_dir = temp_dir.path().join("barsmith_output");

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
            "--output-dir",
            output_dir.to_str().expect("output"),
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
        .current_dir(workspace_root())
        .status()
        .expect("failed to spawn barsmith_cli");

    assert!(status.success(), "barsmith_cli exited with {status:?}");

    let prepared_csv = output_dir.join("barsmith_prepared.csv");
    assert!(
        prepared_csv.exists(),
        "expected engineered CSV at {}",
        prepared_csv.display()
    );
}

#[test]
fn cli_results_queries_real_run_output() {
    let sample_csv = workspace_root()
        .join("tests")
        .join("data")
        .join("ohlcv_tiny.csv");
    let temp_dir = tempdir().expect("temp output dir");
    let output_dir = temp_dir.path().join("barsmith_output");

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
            "--output-dir",
            output_dir.to_str().expect("output"),
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
        .current_dir(workspace_root())
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
}

#[test]
fn cli_comb_writes_standard_output_metadata_and_registry() {
    let sample_csv = workspace_root()
        .join("tests")
        .join("data")
        .join("ohlcv_tiny.csv");
    let temp_dir = tempdir().expect("temp output dir");
    let runs_root = temp_dir.path().join("artifacts");
    let registry_dir = temp_dir.path().join("registry");

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
            "--runs-root",
            runs_root.to_str().expect("runs root"),
            "--dataset-id",
            "tiny sample",
            "--run-id",
            "metadata smoke",
            "--registry-dir",
            registry_dir.to_str().expect("registry"),
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
        .current_dir(workspace_root())
        .status()
        .expect("failed to spawn barsmith_cli comb");

    assert!(status.success(), "comb exited with {status:?}");

    let output_dir = runs_root
        .join("comb")
        .join("next_bar_color_and_wicks")
        .join("long")
        .join("tiny_sample")
        .join("metadata_smoke");
    let registry_path = registry_dir
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

    let temp_dir = tempdir().expect("temp output dir");
    let csv_out = temp_dir.path().join("formula_results.csv");
    let json_out = temp_dir.path().join("formula_results.json");
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
            "--report-top",
            "2",
            "--csv-out",
            csv_out.to_str().expect("csv"),
            "--json-out",
            json_out.to_str().expect("json"),
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
        .current_dir(workspace_root())
        .status()
        .expect("failed to spawn barsmith_cli eval-formulas");

    assert!(status.success(), "barsmith_cli exited with {status:?}");
    for path in [
        &csv_out,
        &json_out,
        &frs_out,
        &frs_windows_out,
        &curves_out,
        &plot_out,
    ] {
        let metadata = std::fs::metadata(path).expect("output should exist");
        assert!(metadata.len() > 0, "{} should not be empty", path.display());
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

    let temp_dir = tempdir().expect("temp output dir");
    let runs_root = temp_dir.path().join("artifacts");
    let registry_dir = temp_dir.path().join("registry");

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
            "--runs-root",
            runs_root.to_str().expect("runs root"),
            "--dataset-id",
            "tiny forward",
            "--run-id",
            "forward smoke",
            "--registry-dir",
            registry_dir.to_str().expect("registry"),
            "--checksum-artifacts",
            "--plot",
            "--plot-mode",
            "combined",
        ])
        .current_dir(workspace_root())
        .status()
        .expect("failed to spawn barsmith_cli eval-formulas");

    assert!(status.success(), "eval-formulas exited with {status:?}");

    let output_dir = runs_root
        .join("forward-test")
        .join("2x_atr_tp_atr_stop")
        .join("tiny_forward")
        .join("2024-12-31")
        .join("forward_smoke");
    let registry_path = registry_dir
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
        output_dir.join("formula_results.csv"),
        output_dir.join("formula_results.json"),
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
    assert!(checksums.contains("formula_results.csv"));
    assert!(checksums.contains("plots/equity_curves.png"));
}
