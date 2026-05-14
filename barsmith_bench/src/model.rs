use serde::{Deserialize, Serialize};

pub const BENCH_REPORT_SCHEMA_VERSION: u32 = 1;
pub const BENCH_COMPARISON_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RegressionPolicy {
    HardGate,
    ReviewOnly,
}

impl RegressionPolicy {
    pub fn is_hard_gate(self) -> bool {
        matches!(self, Self::HardGate)
    }
}

impl std::fmt::Display for RegressionPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HardGate => formatter.write_str("hard-gate"),
            Self::ReviewOnly => formatter.write_str("review-only"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BenchmarkStatus {
    Pass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ComparisonReportStatus {
    Pass,
    Review,
    Fail,
}

impl std::fmt::Display for ComparisonReportStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => formatter.write_str("pass"),
            Self::Review => formatter.write_str("review"),
            Self::Fail => formatter.write_str("fail"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ComparisonStatus {
    Pass,
    Regression,
    ReviewRequired,
    MissingBaseline,
    MissingCandidate,
}

impl ComparisonStatus {
    pub fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::Regression | Self::MissingBaseline | Self::MissingCandidate
        )
    }

    pub fn needs_review(self) -> bool {
        matches!(self, Self::ReviewRequired)
    }
}

impl std::fmt::Display for ComparisonStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => formatter.write_str("pass"),
            Self::Regression => formatter.write_str("regression"),
            Self::ReviewRequired => formatter.write_str("review-required"),
            Self::MissingBaseline => formatter.write_str("missing-baseline"),
            Self::MissingCandidate => formatter.write_str("missing-candidate"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub schema_version: u32,
    pub generated_at: String,
    pub environment: BenchmarkEnvironment,
    pub profile: CargoProfile,
    pub benchmarks: Vec<BenchmarkResult>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkEnvironment {
    pub git_sha: Option<String>,
    pub git_dirty: bool,
    pub rustc_version: Option<String>,
    pub target_triple: Option<String>,
    pub os: String,
    pub arch: String,
    pub cpu_model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CargoProfile {
    pub label: String,
    pub cargo_profile: String,
    pub rustflags: Option<String>,
    pub cargo_target_dir: Option<String>,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub suite: String,
    pub name: String,
    pub fixture_tier: String,
    pub fixture_label: String,
    pub fixture_sha256: Option<String>,
    pub command: Option<String>,
    pub samples: usize,
    pub warmups: usize,
    pub iterations_per_sample: u64,
    pub median_ms: f64,
    pub p95_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub stddev_ms: f64,
    pub throughput_per_second: Option<f64>,
    pub median_budget_pct: f64,
    pub p95_budget_pct: f64,
    pub regression_policy: RegressionPolicy,
    pub status: BenchmarkStatus,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkComparisonReport {
    pub schema_version: u32,
    pub generated_at: String,
    pub baseline: String,
    pub candidate: String,
    pub median_budget_pct: f64,
    pub p95_budget_pct: f64,
    pub status: ComparisonReportStatus,
    pub results: Vec<BenchmarkComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkComparison {
    pub suite: String,
    pub name: String,
    pub regression_policy: RegressionPolicy,
    pub baseline_median_ms: Option<f64>,
    pub candidate_median_ms: Option<f64>,
    pub median_delta_pct: Option<f64>,
    pub baseline_p95_ms: Option<f64>,
    pub candidate_p95_ms: Option<f64>,
    pub p95_delta_pct: Option<f64>,
    pub baseline_mean_ms: Option<f64>,
    pub candidate_mean_ms: Option<f64>,
    pub mean_delta_pct: Option<f64>,
    pub status: ComparisonStatus,
}
