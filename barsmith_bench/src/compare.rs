use std::collections::{HashMap, HashSet};
use std::fs;

use anyhow::{Context, Result, bail};
use chrono::Utc;

use crate::cli::CompareArgs;
use crate::model::{
    BENCH_COMPARISON_SCHEMA_VERSION, BenchmarkComparison, BenchmarkComparisonReport,
    BenchmarkReport, BenchmarkResult, ComparisonReportStatus, ComparisonStatus,
};

pub fn run(args: CompareArgs) -> Result<()> {
    let baseline_text = fs::read_to_string(&args.baseline)
        .with_context(|| format!("failed to read {}", args.baseline.display()))?;
    let candidate_text = fs::read_to_string(&args.candidate)
        .with_context(|| format!("failed to read {}", args.candidate.display()))?;
    let baseline: BenchmarkReport = serde_json::from_str(&baseline_text)
        .with_context(|| format!("failed to parse {}", args.baseline.display()))?;
    let candidate: BenchmarkReport = serde_json::from_str(&candidate_text)
        .with_context(|| format!("failed to parse {}", args.candidate.display()))?;

    let baseline_by_key = baseline
        .benchmarks
        .iter()
        .map(|benchmark| (bench_key(benchmark), benchmark))
        .collect::<HashMap<_, _>>();

    let mut results = Vec::new();
    let candidate_keys = candidate
        .benchmarks
        .iter()
        .map(bench_key)
        .collect::<HashSet<_>>();

    let mut has_blocking_issue = false;
    let mut needs_review = false;
    for benchmark in &candidate.benchmarks {
        let baseline = baseline_by_key.get(&bench_key(benchmark)).copied();
        let comparison = compare_one(
            baseline,
            benchmark,
            args.median_budget_pct,
            args.p95_budget_pct,
        );
        has_blocking_issue |= comparison.status.is_blocking();
        needs_review |= comparison.status.needs_review();
        results.push(comparison);
    }

    for benchmark in &baseline.benchmarks {
        if !candidate_keys.contains(&bench_key(benchmark)) {
            let comparison = missing_candidate(benchmark);
            has_blocking_issue |= comparison.status.is_blocking();
            needs_review |= comparison.status.needs_review();
            results.push(comparison);
        }
    }

    let status = if has_blocking_issue {
        ComparisonReportStatus::Fail
    } else if needs_review {
        ComparisonReportStatus::Review
    } else {
        ComparisonReportStatus::Pass
    };
    let report = BenchmarkComparisonReport {
        schema_version: BENCH_COMPARISON_SCHEMA_VERSION,
        generated_at: Utc::now().to_rfc3339(),
        baseline: args.baseline.display().to_string(),
        candidate: args.candidate.display().to_string(),
        median_budget_pct: args.median_budget_pct,
        p95_budget_pct: args.p95_budget_pct,
        status,
        results,
    };

    print_summary(&report);

    if let Some(path) = &args.out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(path, serde_json::to_vec_pretty(&report)?)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }

    if let Some(path) = &args.markdown_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(path, markdown_summary(&report))
            .with_context(|| format!("failed to write {}", path.display()))?;
    }

    if has_blocking_issue && args.fail_on_regression {
        bail!("benchmark comparison failed the hard gate");
    }

    Ok(())
}

fn bench_key(benchmark: &BenchmarkResult) -> (String, String) {
    (benchmark.suite.clone(), benchmark.name.clone())
}

fn compare_one(
    baseline: Option<&BenchmarkResult>,
    candidate: &BenchmarkResult,
    median_budget_pct: f64,
    p95_budget_pct: f64,
) -> BenchmarkComparison {
    let Some(baseline) = baseline else {
        return BenchmarkComparison {
            suite: candidate.suite.clone(),
            name: candidate.name.clone(),
            regression_policy: candidate.regression_policy,
            baseline_median_ms: None,
            candidate_median_ms: Some(candidate.median_ms),
            median_delta_pct: None,
            baseline_p95_ms: None,
            candidate_p95_ms: Some(candidate.p95_ms),
            p95_delta_pct: None,
            baseline_mean_ms: None,
            candidate_mean_ms: Some(candidate.mean_ms),
            mean_delta_pct: None,
            status: missing_baseline_status(candidate),
        };
    };

    let median_delta_pct = pct_delta(baseline.median_ms, candidate.median_ms);
    let p95_delta_pct = pct_delta(baseline.p95_ms, candidate.p95_ms);
    let mean_delta_pct = pct_delta(baseline.mean_ms, candidate.mean_ms);
    let median_regressed = median_delta_pct.is_some_and(|delta| delta > median_budget_pct);
    let p95_regressed = p95_delta_pct.is_some_and(|delta| delta > p95_budget_pct);
    let mean_regressed = mean_delta_pct.is_some_and(|delta| delta > median_budget_pct);
    let hard_regressed = median_regressed || (p95_regressed && mean_regressed);
    let status = if hard_regressed {
        if candidate.regression_policy.is_hard_gate() {
            ComparisonStatus::Regression
        } else {
            ComparisonStatus::ReviewRequired
        }
    } else if p95_regressed {
        ComparisonStatus::ReviewRequired
    } else {
        ComparisonStatus::Pass
    };

    BenchmarkComparison {
        suite: candidate.suite.clone(),
        name: candidate.name.clone(),
        regression_policy: candidate.regression_policy,
        baseline_median_ms: Some(baseline.median_ms),
        candidate_median_ms: Some(candidate.median_ms),
        median_delta_pct,
        baseline_p95_ms: Some(baseline.p95_ms),
        candidate_p95_ms: Some(candidate.p95_ms),
        p95_delta_pct,
        baseline_mean_ms: Some(baseline.mean_ms),
        candidate_mean_ms: Some(candidate.mean_ms),
        mean_delta_pct,
        status,
    }
}

fn missing_candidate(baseline: &BenchmarkResult) -> BenchmarkComparison {
    BenchmarkComparison {
        suite: baseline.suite.clone(),
        name: baseline.name.clone(),
        regression_policy: baseline.regression_policy,
        baseline_median_ms: Some(baseline.median_ms),
        candidate_median_ms: None,
        median_delta_pct: None,
        baseline_p95_ms: Some(baseline.p95_ms),
        candidate_p95_ms: None,
        p95_delta_pct: None,
        baseline_mean_ms: Some(baseline.mean_ms),
        candidate_mean_ms: None,
        mean_delta_pct: None,
        status: if baseline.regression_policy.is_hard_gate() {
            ComparisonStatus::MissingCandidate
        } else {
            ComparisonStatus::ReviewRequired
        },
    }
}

fn missing_baseline_status(candidate: &BenchmarkResult) -> ComparisonStatus {
    if candidate.regression_policy.is_hard_gate() {
        ComparisonStatus::MissingBaseline
    } else {
        ComparisonStatus::ReviewRequired
    }
}

fn pct_delta(baseline: f64, candidate: f64) -> Option<f64> {
    if baseline <= f64::EPSILON {
        None
    } else {
        Some(((candidate - baseline) / baseline) * 100.0)
    }
}

fn print_summary(report: &BenchmarkComparisonReport) {
    println!("Benchmark comparison: {}", report.status);
    for result in &report.results {
        println!(
            "{}::{} median_delta={} p95_delta={} mean_delta={} status={}",
            result.suite,
            result.name,
            fmt_pct(result.median_delta_pct),
            fmt_pct(result.p95_delta_pct),
            fmt_pct(result.mean_delta_pct),
            result.status
        );
    }
}

fn markdown_summary(report: &BenchmarkComparisonReport) -> String {
    let mut text = String::new();
    text.push_str("# Barsmith Benchmark Comparison\n\n");
    text.push_str(&format!("Status: `{}`\n\n", report.status));
    text.push_str("| Suite | Benchmark | Median Delta | p95 Delta | Mean Delta | Status |\n");
    text.push_str("| --- | --- | ---: | ---: | ---: | --- |\n");
    for result in &report.results {
        text.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            result.suite,
            result.name,
            fmt_pct(result.median_delta_pct),
            fmt_pct(result.p95_delta_pct),
            fmt_pct(result.mean_delta_pct),
            result.status
        ));
    }
    text
}

fn fmt_pct(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}%"))
        .unwrap_or_else(|| "n/a".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BenchmarkStatus, RegressionPolicy};

    fn bench(name: &str, median_ms: f64, p95_ms: f64, policy: RegressionPolicy) -> BenchmarkResult {
        BenchmarkResult {
            suite: "suite".to_string(),
            name: name.to_string(),
            fixture_tier: "A".to_string(),
            fixture_label: "fixture".to_string(),
            fixture_sha256: None,
            command: None,
            samples: 21,
            warmups: 2,
            iterations_per_sample: 1,
            median_ms,
            p95_ms,
            min_ms: median_ms,
            max_ms: p95_ms,
            mean_ms: median_ms,
            stddev_ms: 0.0,
            throughput_per_second: None,
            median_budget_pct: 3.0,
            p95_budget_pct: 5.0,
            regression_policy: policy,
            status: BenchmarkStatus::Pass,
            notes: Vec::new(),
        }
    }

    #[test]
    fn hard_gate_regression_is_blocking() {
        let baseline = bench("hot-loop", 100.0, 110.0, RegressionPolicy::HardGate);
        let candidate = bench("hot-loop", 104.0, 111.0, RegressionPolicy::HardGate);

        let comparison = compare_one(Some(&baseline), &candidate, 3.0, 5.0);

        assert_eq!(comparison.status, ComparisonStatus::Regression);
        assert!(comparison.status.is_blocking());
    }

    #[test]
    fn review_only_regression_requires_review_without_blocking() {
        let baseline = bench("cli", 100.0, 110.0, RegressionPolicy::ReviewOnly);
        let candidate = bench("cli", 104.0, 111.0, RegressionPolicy::ReviewOnly);

        let comparison = compare_one(Some(&baseline), &candidate, 3.0, 5.0);

        assert_eq!(comparison.status, ComparisonStatus::ReviewRequired);
        assert!(!comparison.status.is_blocking());
        assert!(comparison.status.needs_review());
    }

    #[test]
    fn p95_only_regression_requires_review_without_blocking() {
        let baseline = bench("hot-loop", 100.0, 110.0, RegressionPolicy::HardGate);
        let mut candidate = bench("hot-loop", 101.0, 117.0, RegressionPolicy::HardGate);
        candidate.mean_ms = 101.0;

        let comparison = compare_one(Some(&baseline), &candidate, 3.0, 5.0);

        assert_eq!(comparison.status, ComparisonStatus::ReviewRequired);
        assert!(!comparison.status.is_blocking());
        assert!(comparison.status.needs_review());
    }

    #[test]
    fn hard_gate_missing_candidate_is_blocking() {
        let baseline = bench("hot-loop", 100.0, 110.0, RegressionPolicy::HardGate);

        let comparison = missing_candidate(&baseline);

        assert_eq!(comparison.status, ComparisonStatus::MissingCandidate);
        assert!(comparison.status.is_blocking());
    }
}
