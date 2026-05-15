use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow};
use barsmith_rs::protocol::{
    FormulaExportManifest, FormulaExportManifestDraft, ResearchProtocol, ResearchProtocolDraft,
    ResearchWindow, sha256_file, write_json_pretty,
};
use chrono::NaiveDate;

use crate::cli::RunArgs;
use crate::model::BenchmarkResult;
use crate::runner::{BenchmarkSpec, RegressionPolicy, measure};

pub fn run_comb_cli(args: &RunArgs, _warnings: &mut Vec<String>) -> Result<Vec<BenchmarkResult>> {
    let binary = ensure_barsmith_cli(args)?;
    let fixture_sha = sha256_file(&args.fixture_csv).ok();
    let mut sample = 0usize;
    let command = comb_command_string(&binary, args, "<sample>");

    Ok(vec![measure(
        BenchmarkSpec {
            suite: "comb-cli".to_string(),
            name: "tiny-comb-core".to_string(),
            fixture_tier: "A".to_string(),
            fixture_label: args.fixture_csv.display().to_string(),
            fixture_sha256: fixture_sha,
            command: Some(command),
            iterations_per_sample: args.max_combos as u64,
            regression_policy: RegressionPolicy::ReviewOnly,
            notes: vec!["Release CLI path on the committed tiny fixture.".to_string()],
        },
        args,
        || {
            sample += 1;
            let runs_root = args
                .work_dir
                .join("comb-cli")
                .join(format!("sample-{sample}"));
            run_comb_fixture(&binary, args, &runs_root, "bench_comb", "timed")?;
            Ok(args.max_combos as u64)
        },
    )?])
}

pub fn run_results_cli(
    args: &RunArgs,
    _warnings: &mut Vec<String>,
) -> Result<Vec<BenchmarkResult>> {
    let binary = ensure_barsmith_cli(args)?;
    let fixture_sha = sha256_file(&args.fixture_csv).ok();
    let fixture_root = args.work_dir.join("results-cli-fixture");
    let output_dir = run_comb_fixture(&binary, args, &fixture_root, "bench_results", "seed")?;
    let command_args = results_args(&output_dir);
    let command = command_string(&binary, &command_args);

    Ok(vec![measure(
        BenchmarkSpec {
            suite: "results-cli".to_string(),
            name: "tiny-results-query".to_string(),
            fixture_tier: "A".to_string(),
            fixture_label: args.fixture_csv.display().to_string(),
            fixture_sha256: fixture_sha,
            command: Some(command),
            iterations_per_sample: 1,
            regression_policy: RegressionPolicy::ReviewOnly,
            notes: vec!["Queries a prepared tiny result store through the CLI.".to_string()],
        },
        args,
        || {
            run_child(&binary, &command_args)?;
            Ok(1)
        },
    )?])
}

pub fn run_strict_eval(
    args: &RunArgs,
    _warnings: &mut Vec<String>,
) -> Result<Vec<BenchmarkResult>> {
    let binary = ensure_barsmith_cli(args)?;
    let prepared = PathBuf::from("barsmith_rs/tests/fixtures/formula_eval_prepared.csv");
    let formulas = PathBuf::from("barsmith_rs/tests/fixtures/formula_eval_formulas.txt");
    let fixture_sha = sha256_file(&prepared).ok();
    let protocol_dir = args.work_dir.join("strict-eval-fixture");
    fs::create_dir_all(&protocol_dir)
        .with_context(|| format!("failed to create {}", protocol_dir.display()))?;
    let protocol_path = protocol_dir.join("research_protocol.json");
    let manifest_path = protocol_dir.join("formula_export_manifest.json");
    write_strict_eval_fixture(&protocol_path, &manifest_path)?;

    let mut sample = 0usize;
    let command = strict_eval_command_string(&binary, args, &prepared, &formulas, "<sample>");
    Ok(vec![measure(
        BenchmarkSpec {
            suite: "strict-eval".to_string(),
            name: "formula-fixture-validation".to_string(),
            fixture_tier: "A".to_string(),
            fixture_label: prepared.display().to_string(),
            fixture_sha256: fixture_sha,
            command: Some(command),
            iterations_per_sample: 1,
            regression_policy: RegressionPolicy::ReviewOnly,
            notes: vec!["Strict protocol formula evaluation without plotting.".to_string()],
        },
        args,
        || {
            sample += 1;
            let runs_root = args
                .work_dir
                .join("strict-eval")
                .join(format!("sample-{sample}"));
            let command_args = strict_eval_args(
                &prepared,
                &formulas,
                &protocol_path,
                &manifest_path,
                &runs_root,
                &runs_root.join("registry"),
            );
            run_child(&binary, &command_args)?;
            Ok(1)
        },
    )?])
}

pub fn run_select_validate(
    args: &RunArgs,
    _warnings: &mut Vec<String>,
) -> Result<Vec<BenchmarkResult>> {
    let binary = ensure_barsmith_cli(args)?;
    let fixture_sha = sha256_file(&args.fixture_csv).ok();
    let fixture_root = args.work_dir.join("select-validate-fixture");
    let output_dir = run_select_discovery_fixture(
        &binary,
        args,
        &fixture_root,
        "select_bench_source",
        "discovery",
    )?;
    let protocol_path = fixture_root.join("research_protocol.json");
    write_select_validate_protocol(&protocol_path)?;

    let mut sample = 0usize;
    let command = select_validate_command_string(
        &binary,
        args,
        &output_dir,
        &output_dir.join("barsmith_prepared.csv"),
        &protocol_path,
        "<sample>",
    );
    Ok(vec![measure(
        BenchmarkSpec {
            suite: "select-validate".to_string(),
            name: "tiny-strict-selection-workflow".to_string(),
            fixture_tier: "A".to_string(),
            fixture_label: args.fixture_csv.display().to_string(),
            fixture_sha256: fixture_sha,
            command: Some(command),
            iterations_per_sample: 1,
            regression_policy: RegressionPolicy::ReviewOnly,
            notes: vec![
                "Runs the strict select validate wrapper over a prepared tiny discovery result store."
                    .to_string(),
            ],
        },
        args,
        || {
            sample += 1;
            let runs_root = args
                .work_dir
                .join("select-validate")
                .join(format!("sample-{sample}"));
            let command_args = select_validate_args(
                args,
                SelectValidateFixture {
                    source_output: &output_dir,
                    prepared: &output_dir.join("barsmith_prepared.csv"),
                    protocol: &protocol_path,
                    runs_root: &runs_root,
                    registry_dir: &runs_root.join("registry"),
                    dataset_id: "select_bench_source",
                    run_id: "validation",
                },
            );
            run_child(&binary, &command_args)?;
            Ok(1)
        },
    )?])
}

fn ensure_barsmith_cli(args: &RunArgs) -> Result<PathBuf> {
    if let Some(binary) = &args.barsmith_bin {
        if binary.is_file() {
            return Ok(binary.clone());
        }
        return Err(anyhow!(
            "--barsmith-bin points to a missing file: {}",
            binary.display()
        ));
    }

    let binary = default_binary_path();
    if !binary.is_file() {
        return Err(anyhow!(
            "CLI benchmark suites need an existing barsmith_cli binary. Build it with `cargo build --release -p barsmith_cli` or pass --barsmith-bin; expected default path {}",
            binary.display(),
        ));
    }
    Ok(binary)
}

fn default_binary_path() -> PathBuf {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    target_dir
        .join("release")
        .join(format!("barsmith_cli{}", std::env::consts::EXE_SUFFIX))
}

fn run_select_discovery_fixture(
    binary: &Path,
    args: &RunArgs,
    runs_root: &Path,
    dataset_id: &str,
    run_id: &str,
) -> Result<PathBuf> {
    let output_dir = runs_root
        .join("comb")
        .join("next_bar_color_and_wicks")
        .join("long")
        .join(dataset_id)
        .join(run_id);
    if runs_root.exists() {
        fs::remove_dir_all(runs_root)
            .with_context(|| format!("failed to remove {}", runs_root.display()))?;
    }
    let registry_dir = runs_root.join("registry");
    let mut command_args = comb_args(args, runs_root, &registry_dir, dataset_id, run_id);
    set_arg_value(&mut command_args, "--min-samples", "1");
    command_args.extend(["--date-end".to_string(), "2024-02-29".to_string()]);
    run_child(binary, &command_args)?;
    Ok(output_dir)
}

fn run_comb_fixture(
    binary: &Path,
    args: &RunArgs,
    runs_root: &Path,
    dataset_id: &str,
    run_id: &str,
) -> Result<PathBuf> {
    let output_dir = runs_root
        .join("comb")
        .join("next_bar_color_and_wicks")
        .join("long")
        .join(dataset_id)
        .join(run_id);
    if runs_root.exists() {
        fs::remove_dir_all(runs_root)
            .with_context(|| format!("failed to remove {}", runs_root.display()))?;
    }
    let registry_dir = runs_root.join("registry");
    let command_args = comb_args(args, runs_root, &registry_dir, dataset_id, run_id);
    run_child(binary, &command_args)?;
    Ok(output_dir)
}

fn comb_args(
    args: &RunArgs,
    runs_root: &Path,
    registry_dir: &Path,
    dataset_id: &str,
    run_id: &str,
) -> Vec<String> {
    vec![
        "comb".to_string(),
        "--csv".to_string(),
        args.fixture_csv.display().to_string(),
        "--direction".to_string(),
        "long".to_string(),
        "--target".to_string(),
        "next_bar_color_and_wicks".to_string(),
        "--position-sizing".to_string(),
        "fractional".to_string(),
        "--runs-root".to_string(),
        runs_root.display().to_string(),
        "--registry-dir".to_string(),
        registry_dir.display().to_string(),
        "--dataset-id".to_string(),
        dataset_id.to_string(),
        "--run-id".to_string(),
        run_id.to_string(),
        "--max-depth".to_string(),
        args.max_depth.to_string(),
        "--min-samples".to_string(),
        "1".to_string(),
        "--batch-size".to_string(),
        args.batch_size.to_string(),
        "--workers".to_string(),
        args.workers.to_string(),
        "--max-combos".to_string(),
        args.max_combos.to_string(),
        "--stats-detail".to_string(),
        "core".to_string(),
        "--report".to_string(),
        "off".to_string(),
        "--no-file-log".to_string(),
        "--force".to_string(),
    ]
}

fn set_arg_value(args: &mut [String], flag: &str, value: &str) {
    if let Some(index) = args.iter().position(|arg| arg == flag) {
        if let Some(slot) = args.get_mut(index + 1) {
            *slot = value.to_string();
        }
    }
}

fn results_args(output_dir: &Path) -> Vec<String> {
    vec![
        "results".to_string(),
        "--output-dir".to_string(),
        output_dir.display().to_string(),
        "--direction".to_string(),
        "long".to_string(),
        "--target".to_string(),
        "next_bar_color_and_wicks".to_string(),
        "--min-samples".to_string(),
        "25".to_string(),
        "--rank-by".to_string(),
        "total-return".to_string(),
        "--limit".to_string(),
        "3".to_string(),
    ]
}

fn write_strict_eval_fixture(protocol_path: &Path, manifest_path: &Path) -> Result<()> {
    let protocol = ResearchProtocol::from_draft(ResearchProtocolDraft {
        dataset_id: "formula_fixture".to_string(),
        target: "2x_atr_tp_atr_stop".to_string(),
        direction: Some("long".to_string()),
        engine: Some("custom".to_string()),
        discovery: window(2024, 12, 29, 2024, 12, 31)?,
        validation: window(2025, 1, 1, 2025, 1, 3)?,
        lockbox: window(2025, 1, 4, 2025, 1, 5)?,
        candidate_top_k: Some(3),
    });
    let protocol_hash = protocol.hash()?;
    write_json_pretty(protocol_path, &protocol)?;

    let manifest = FormulaExportManifest::from_draft(FormulaExportManifestDraft {
        source_output_dir_path_sha256: "benchmark-source-path".to_string(),
        source_run_manifest_sha256: None,
        source_run_identity_hash: None,
        source_date_start: Some(date(2024, 12, 29)?),
        source_date_end: Some(date(2024, 12, 31)?),
        target: "2x_atr_tp_atr_stop".to_string(),
        direction: "long".to_string(),
        rank_by: "total-return".to_string(),
        min_sample_size: 1,
        min_win_rate: 0.0,
        max_drawdown: 1_000.0,
        min_calmar: None,
        requested_limit: 3,
        exported_rows: 3,
        source_processed_combinations: None,
        source_stored_combinations: None,
        formulas_sha256: "benchmark-formula-fixture".to_string(),
        protocol_sha256: Some(protocol_hash),
    });
    write_json_pretty(manifest_path, &manifest)
}

fn write_select_validate_protocol(protocol_path: &Path) -> Result<()> {
    let protocol = ResearchProtocol::from_draft(ResearchProtocolDraft {
        dataset_id: "select_bench_source".to_string(),
        target: "next_bar_color_and_wicks".to_string(),
        direction: Some("long".to_string()),
        engine: Some("builtin".to_string()),
        discovery: window(2024, 1, 1, 2024, 2, 29)?,
        validation: window(2024, 3, 1, 2024, 3, 31)?,
        lockbox: window(2024, 4, 1, 2024, 4, 14)?,
        candidate_top_k: Some(3),
    });
    write_json_pretty(protocol_path, &protocol)
}

fn strict_eval_args(
    prepared: &Path,
    formulas: &Path,
    protocol: &Path,
    manifest: &Path,
    runs_root: &Path,
    registry_dir: &Path,
) -> Vec<String> {
    vec![
        "eval-formulas".to_string(),
        "--prepared".to_string(),
        prepared.display().to_string(),
        "--formulas".to_string(),
        formulas.display().to_string(),
        "--target".to_string(),
        "2x_atr_tp_atr_stop".to_string(),
        "--position-sizing".to_string(),
        "fractional".to_string(),
        "--stacking-mode".to_string(),
        "no-stacking".to_string(),
        "--cutoff".to_string(),
        "2024-12-31".to_string(),
        "--stage".to_string(),
        "validation".to_string(),
        "--strict-protocol".to_string(),
        "--research-protocol".to_string(),
        protocol.display().to_string(),
        "--formula-export-manifest".to_string(),
        manifest.display().to_string(),
        "--candidate-top-k".to_string(),
        "3".to_string(),
        "--pre-min-trades".to_string(),
        "1".to_string(),
        "--post-min-trades".to_string(),
        "1".to_string(),
        "--report-top".to_string(),
        "2".to_string(),
        "--runs-root".to_string(),
        runs_root.display().to_string(),
        "--registry-dir".to_string(),
        registry_dir.display().to_string(),
        "--dataset-id".to_string(),
        "formula_fixture".to_string(),
        "--run-id".to_string(),
        "benchmark_validation".to_string(),
        "--no-file-log".to_string(),
    ]
}

struct SelectValidateFixture<'a> {
    source_output: &'a Path,
    prepared: &'a Path,
    protocol: &'a Path,
    runs_root: &'a Path,
    registry_dir: &'a Path,
    dataset_id: &'a str,
    run_id: &'a str,
}

fn select_validate_args(args: &RunArgs, fixture: SelectValidateFixture<'_>) -> Vec<String> {
    vec![
        "select".to_string(),
        "validate".to_string(),
        "--source-output-dir".to_string(),
        fixture.source_output.display().to_string(),
        "--prepared".to_string(),
        fixture.prepared.display().to_string(),
        "--target".to_string(),
        "next_bar_color_and_wicks".to_string(),
        "--direction".to_string(),
        "long".to_string(),
        "--cutoff".to_string(),
        "2024-02-29".to_string(),
        "--research-protocol".to_string(),
        fixture.protocol.display().to_string(),
        "--preset".to_string(),
        "exploratory".to_string(),
        "--candidate-top-k".to_string(),
        "3".to_string(),
        "--min-samples".to_string(),
        args.min_samples.to_string(),
        "--pre-min-trades".to_string(),
        "1".to_string(),
        "--post-min-trades".to_string(),
        "0".to_string(),
        "--post-warn-below-trades".to_string(),
        "0".to_string(),
        "--runs-root".to_string(),
        fixture.runs_root.display().to_string(),
        "--registry-dir".to_string(),
        fixture.registry_dir.display().to_string(),
        "--dataset-id".to_string(),
        fixture.dataset_id.to_string(),
        "--run-id".to_string(),
        fixture.run_id.to_string(),
        "--no-file-log".to_string(),
    ]
}

fn run_child(binary: &Path, args: &[String]) -> Result<()> {
    let output = Command::new(binary)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to spawn {}", binary.display()))?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(anyhow!(
        "{} failed with status {:?}: {}",
        command_string(binary, args),
        output.status.code(),
        stderr.trim()
    ))
}

fn comb_command_string(binary: &Path, args: &RunArgs, sample: &str) -> String {
    let runs_root = args.work_dir.join("comb-cli").join(sample);
    let registry_dir = runs_root.join("registry");
    command_string(
        binary,
        &comb_args(args, &runs_root, &registry_dir, "bench_comb", "timed"),
    )
}

fn strict_eval_command_string(
    binary: &Path,
    args: &RunArgs,
    prepared: &Path,
    formulas: &Path,
    sample: &str,
) -> String {
    let root = args.work_dir.join("strict-eval").join(sample);
    let registry_dir = root.join("registry");
    command_string(
        binary,
        &strict_eval_args(
            prepared,
            formulas,
            Path::new("<protocol>"),
            Path::new("<manifest>"),
            &root,
            &registry_dir,
        ),
    )
}

fn select_validate_command_string(
    binary: &Path,
    args: &RunArgs,
    source_output: &Path,
    prepared: &Path,
    protocol: &Path,
    sample: &str,
) -> String {
    let root = args.work_dir.join("select-validate").join(sample);
    let registry_dir = root.join("registry");
    command_string(
        binary,
        &select_validate_args(
            args,
            SelectValidateFixture {
                source_output,
                prepared,
                protocol,
                runs_root: &root,
                registry_dir: &registry_dir,
                dataset_id: "select_bench_source",
                run_id: "validation",
            },
        ),
    )
}

fn command_string(binary: &Path, args: &[String]) -> String {
    std::iter::once(binary.display().to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}

fn window(
    start_year: i32,
    start_month: u32,
    start_day: u32,
    end_year: i32,
    end_month: u32,
    end_day: u32,
) -> Result<ResearchWindow> {
    ResearchWindow::new(
        Some(date(start_year, start_month, start_day)?),
        Some(date(end_year, end_month, end_day)?),
    )
}

fn date(year: i32, month: u32, day: u32) -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| anyhow!("invalid benchmark date {year:04}-{month:02}-{day:02}"))
}
