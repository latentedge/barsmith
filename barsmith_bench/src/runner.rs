use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;

use crate::cli::RunArgs;
use crate::model::{
    BENCH_REPORT_SCHEMA_VERSION, BenchmarkEnvironment, BenchmarkReport, BenchmarkResult,
    BenchmarkStatus, CargoProfile,
};
use crate::suites;

pub use crate::model::RegressionPolicy;

pub struct Measurement {
    pub samples_ms: Vec<f64>,
    pub iterations_per_sample: u64,
    pub throughput_per_second: Option<f64>,
}

pub struct BenchmarkSpec {
    pub suite: String,
    pub name: String,
    pub fixture_tier: String,
    pub fixture_label: String,
    pub fixture_sha256: Option<String>,
    pub command: Option<String>,
    pub iterations_per_sample: u64,
    pub regression_policy: RegressionPolicy,
    pub notes: Vec<String>,
}

pub fn run(args: RunArgs) -> Result<()> {
    if args.samples == 0 {
        return Err(anyhow!("--samples must be greater than zero"));
    }

    let mut warnings = Vec::new();
    let benchmarks = suites::run_suite(&args, &mut warnings)?;
    let report = BenchmarkReport {
        schema_version: BENCH_REPORT_SCHEMA_VERSION,
        generated_at: Utc::now().to_rfc3339(),
        environment: collect_environment(),
        profile: collect_profile(&args),
        benchmarks,
        warnings,
    };

    let out = args
        .out
        .unwrap_or_else(|| PathBuf::from("target/barsmith-bench/current.json"));
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(&report)?;
    fs::write(&out, bytes).with_context(|| format!("failed to write {}", out.display()))?;

    println!("Benchmark report written: {}", out.display());
    println!(
        "Benchmarks: {} | warnings: {}",
        report.benchmarks.len(),
        report.warnings.len()
    );
    for benchmark in &report.benchmarks {
        println!(
            "{}::{} median={:.3}ms p95={:.3}ms",
            benchmark.suite, benchmark.name, benchmark.median_ms, benchmark.p95_ms
        );
    }

    Ok(())
}

pub fn measure<F>(spec: BenchmarkSpec, args: &RunArgs, mut run_once: F) -> Result<BenchmarkResult>
where
    F: FnMut() -> Result<u64>,
{
    for _ in 0..args.warmups {
        let _ = run_once()?;
    }

    let mut samples_ms = Vec::with_capacity(args.samples);
    let mut total_work = 0u64;
    for _ in 0..args.samples {
        let started = Instant::now();
        total_work = total_work.saturating_add(run_once()?);
        let elapsed = started.elapsed();
        samples_ms.push(elapsed.as_secs_f64() * 1_000.0);
    }

    let measurement = Measurement {
        throughput_per_second: throughput(total_work, &samples_ms),
        samples_ms,
        iterations_per_sample: spec.iterations_per_sample,
    };
    summarize(spec, args, measurement)
}

fn summarize(
    spec: BenchmarkSpec,
    args: &RunArgs,
    mut measurement: Measurement,
) -> Result<BenchmarkResult> {
    if measurement.samples_ms.is_empty() {
        return Err(anyhow!("benchmark {} produced no samples", spec.name));
    }

    measurement.samples_ms.sort_by(f64::total_cmp);
    let min_ms = measurement.samples_ms[0];
    let max_ms = *measurement.samples_ms.last().expect("non-empty samples");
    let median_ms = percentile_sorted(&measurement.samples_ms, 0.50);
    let p95_ms = percentile_sorted(&measurement.samples_ms, 0.95);
    let mean_ms = mean(&measurement.samples_ms);
    let stddev_ms = stddev(&measurement.samples_ms, mean_ms);

    Ok(BenchmarkResult {
        suite: spec.suite,
        name: spec.name,
        fixture_tier: spec.fixture_tier,
        fixture_label: spec.fixture_label,
        fixture_sha256: spec.fixture_sha256,
        command: spec.command,
        samples: args.samples,
        warmups: args.warmups,
        iterations_per_sample: measurement.iterations_per_sample,
        median_ms,
        p95_ms,
        min_ms,
        max_ms,
        mean_ms,
        stddev_ms,
        throughput_per_second: measurement.throughput_per_second,
        median_budget_pct: args.median_budget_pct,
        p95_budget_pct: args.p95_budget_pct,
        regression_policy: spec.regression_policy,
        status: BenchmarkStatus::Pass,
        notes: spec.notes,
    })
}

fn percentile_sorted(values: &[f64], percentile: f64) -> f64 {
    let len = values.len();
    if len == 1 {
        return values[0];
    }
    let rank = (percentile * (len as f64 - 1.0)).ceil() as usize;
    values[rank.min(len - 1)]
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn stddev(values: &[f64], mean: f64) -> f64 {
    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

fn throughput(total_work: u64, samples_ms: &[f64]) -> Option<f64> {
    let elapsed_seconds = samples_ms.iter().sum::<f64>() / 1_000.0;
    if elapsed_seconds > 0.0 && total_work > 0 {
        Some(total_work as f64 / elapsed_seconds)
    } else {
        None
    }
}

fn collect_profile(args: &RunArgs) -> CargoProfile {
    CargoProfile {
        label: args.profile_label.clone(),
        cargo_profile: "release".to_string(),
        rustflags: std::env::var("RUSTFLAGS").ok(),
        cargo_target_dir: std::env::var("CARGO_TARGET_DIR").ok(),
        features: vec![
            "barsmith_rs/bench-api".to_string(),
            "barsmith_rs/simd-eval".to_string(),
        ],
    }
}

fn collect_environment() -> BenchmarkEnvironment {
    BenchmarkEnvironment {
        git_sha: command_stdout("git", &["rev-parse", "--short=12", "HEAD"]),
        git_dirty: command_stdout("git", &["status", "--short"])
            .is_some_and(|status| !status.trim().is_empty()),
        rustc_version: command_stdout("rustc", &["-V"]),
        target_triple: rustc_host_triple(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        cpu_model: cpu_model(),
    }
}

fn rustc_host_triple() -> Option<String> {
    let verbose = command_stdout("rustc", &["-vV"])?;
    verbose
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(str::to_string)
}

fn cpu_model() -> Option<String> {
    if cfg!(target_os = "macos") {
        command_stdout("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else if cfg!(target_os = "linux") {
        fs::read_to_string("/proc/cpuinfo").ok().and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("model name"))
                .and_then(|line| line.split_once(':').map(|(_, model)| model.trim()))
                .map(str::to_string)
        })
    } else {
        None
    }
}

fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
