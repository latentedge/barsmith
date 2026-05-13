use serde::Serialize;

use crate::overfit::ResearchGateStatus;

#[derive(Debug, Clone, Serialize)]
pub struct StressOptions {
    pub min_total_r: f64,
    pub min_expectancy: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StressReport {
    pub schema_version: u32,
    pub status: ResearchGateStatus,
    pub selected_formula: Option<String>,
    pub selected_formula_sha256: Option<String>,
    pub scenarios: Vec<StressScenarioResult>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StressScenarioResult {
    pub scenario: String,
    pub cost_multiplier: f64,
    pub extra_cost_per_trade_r: f64,
    pub extra_cost_per_trade_dollar: f64,
    pub max_contracts_override: Option<usize>,
    pub pre_trades: usize,
    pub post_trades: usize,
    pub pre_total_r: f64,
    pub post_total_r: f64,
    pub pre_expectancy: f64,
    pub post_expectancy: f64,
    pub post_max_drawdown_r: f64,
    pub pass: bool,
}
