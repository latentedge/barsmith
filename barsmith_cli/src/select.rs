use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use barsmith_rs::protocol::{
    ResearchProtocol, load_json, parse_manifest_date, validate_protocol, validate_protocol_binding,
};
use barsmith_rs::storage::{ResultQuery, query_result_store};

use crate::cli::{
    DEFAULT_CAPITAL_DOLLAR, DEFAULT_RISK_PCT_PER_TRADE, EvalFormulasArgs, LockboxStageValue,
    PlotMetricValue, PlotXValue, ResearchStageValue, SelectCommand, SelectExplainArgs,
    SelectLockboxArgs, SelectValidateArgs, SelectionModeValue, SelectionPresetValue,
};
use crate::{eval_formulas, results, standard_output};

pub fn run(command: SelectCommand) -> Result<()> {
    match command {
        SelectCommand::Validate(args) => run_validate(*args),
        SelectCommand::Lockbox(args) => run_lockbox(*args),
        SelectCommand::Explain(args) => run_explain(args),
    }
}

fn run_validate(args: SelectValidateArgs) -> Result<()> {
    let protocol = load_protocol(&args.research_protocol)?;
    validate_protocol_binding(
        &protocol,
        &results::normalize_target(&args.target),
        Some(&format!("{:?}", args.direction.to_direction()).to_ascii_lowercase()),
    )?;

    let policy = ResolvedSelectionPolicy::for_validate(&args, protocol.candidate_top_k);
    let query = ResultQuery {
        output_dir: args.source_output_dir.clone(),
        direction: format!("{:?}", args.direction.to_direction()).to_ascii_lowercase(),
        target: results::normalize_target(&args.target),
        min_sample_size: args.min_samples,
        min_win_rate: args.min_win_rate,
        max_drawdown: args.source_max_drawdown,
        min_calmar: args.source_min_calmar,
        rank_by: args.source_rank_by.to_rank_by(),
        limit: policy.candidate_top_k,
    };

    let mut eval_args =
        eval_args_for_validate(&args, &policy, placeholder_path(), placeholder_path());
    let argv: Vec<OsString> = std::env::args_os().collect();
    let output_plan = standard_output::resolve_forward_output(&eval_args, &argv)?;
    let formulas_path = output_plan.output_dir.join("candidate_formulas.txt");
    let manifest_path = output_plan.output_dir.join("formula_export_manifest.json");
    eval_args.formulas_path = formulas_path.clone();
    eval_args.formula_export_manifest = Some(manifest_path.clone());

    if args.dry_run {
        crate::init_tracing(None)?;
        print_validate_plan(&args, &protocol, &query, &output_plan, &policy)?;
        return Ok(());
    }

    ensure_source_run_is_discovery_only(&args.source_output_dir, &protocol)?;
    let rows = query_result_store(&query)?;
    if rows.is_empty() {
        return Err(anyhow!(
            "source comb run had no candidates after filters; lower source filters or inspect the comb run first"
        ));
    }

    let log_file = if args.no_file_log {
        None
    } else {
        Some(output_plan.output_dir.join("barsmith.log"))
    };
    crate::init_tracing(log_file.clone())?;
    crate::log_invocation(log_file.as_ref());
    standard_output::write_start_files(&output_plan)?;

    results::export_ranked_formulas(
        &query,
        &rows,
        &formulas_path,
        &manifest_path,
        Some(&protocol),
    )?;

    standard_output::apply_forward_output_defaults(&mut eval_args, &output_plan);
    let run_result = eval_formulas::run(&eval_args);
    if let Ok(result) = run_result.as_ref() {
        standard_output::write_forward_closeout_files(
            &output_plan,
            &eval_args,
            &result.report,
            &result.written_files,
        )?;
    }

    run_result.map(|_| ())
}

fn run_lockbox(args: SelectLockboxArgs) -> Result<()> {
    let protocol = load_protocol(&args.research_protocol)?;
    validate_protocol_binding(&protocol, &results::normalize_target(&args.target), None)?;

    let mut eval_args = eval_args_for_lockbox(&args);
    let argv: Vec<OsString> = std::env::args_os().collect();
    let output_plan = standard_output::resolve_forward_output(&eval_args, &argv)?;
    let log_file = if args.no_file_log {
        None
    } else {
        Some(output_plan.output_dir.join("barsmith.log"))
    };

    crate::init_tracing(log_file.clone())?;
    crate::log_invocation(log_file.as_ref());
    standard_output::write_start_files(&output_plan)?;
    standard_output::apply_forward_output_defaults(&mut eval_args, &output_plan);

    let run_result = eval_formulas::run(&eval_args);
    if let Ok(result) = run_result.as_ref() {
        standard_output::write_forward_closeout_files(
            &output_plan,
            &eval_args,
            &result.report,
            &result.written_files,
        )?;
    }

    run_result.map(|_| ())
}

fn run_explain(args: SelectExplainArgs) -> Result<()> {
    crate::init_tracing(None)?;
    let protocol = load_protocol(&args.protocol)?;
    let protocol_hash = protocol.hash()?;

    println!("Barsmith strict selection workflow");
    println!("Protocol: {}", args.protocol.display());
    println!("Protocol SHA-256: {protocol_hash}");
    println!("Dataset: {}", protocol.dataset_id);
    println!("Target: {}", protocol.target);
    println!(
        "Direction: {}",
        protocol.direction.as_deref().unwrap_or("not-bound")
    );
    println!(
        "Discovery: {} -> {}",
        display_date(protocol.discovery.start),
        display_date(protocol.discovery.end)
    );
    println!(
        "Validation: {} -> {}",
        display_date(protocol.validation.start),
        display_date(protocol.validation.end)
    );
    println!(
        "Lockbox: {} -> {}",
        display_date(protocol.lockbox.start),
        display_date(protocol.lockbox.end)
    );
    println!();
    println!("Recommended path:");
    println!("1. Run comb only on the discovery/pre window.");
    println!(
        "2. Run `barsmith_cli select validate` with this protocol and the discovery run folder."
    );
    println!("3. Review `reports/selection.md`, `reports/overfit.md`, and `reports/stress.md`.");
    println!("4. Run `barsmith_cli select lockbox` once with the frozen selected formula.");
    println!();
    println!("Institutional preset defaults are conservative and can reject many candidates.");
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct ResolvedSelectionPolicy {
    preset: SelectionPresetValue,
    candidate_top_k: usize,
    pre_min_trades: usize,
    post_min_trades: usize,
    post_warn_below_trades: usize,
    pre_min_total_r: f64,
    post_min_total_r: f64,
    pre_min_expectancy: f64,
    post_min_expectancy: f64,
    selection_max_drawdown_r: Option<f64>,
    min_pre_frs: f64,
    max_return_degradation: f64,
    max_single_trade_contribution: Option<f64>,
    max_formula_depth: Option<usize>,
    min_density_per_1000_bars: Option<f64>,
    overfit_candidate_top_k: usize,
}

impl ResolvedSelectionPolicy {
    fn for_validate(args: &SelectValidateArgs, protocol_candidate_top_k: Option<usize>) -> Self {
        let base = match args.preset {
            SelectionPresetValue::Exploratory => Self {
                preset: args.preset,
                candidate_top_k: protocol_candidate_top_k.unwrap_or(1_000),
                pre_min_trades: 100,
                post_min_trades: 30,
                post_warn_below_trades: 50,
                pre_min_total_r: 0.0,
                post_min_total_r: 0.0,
                pre_min_expectancy: 0.0,
                post_min_expectancy: 0.0,
                selection_max_drawdown_r: None,
                min_pre_frs: 0.0,
                max_return_degradation: 0.25,
                max_single_trade_contribution: None,
                max_formula_depth: None,
                min_density_per_1000_bars: None,
                overfit_candidate_top_k: 100,
            },
            SelectionPresetValue::Institutional => Self {
                preset: args.preset,
                candidate_top_k: protocol_candidate_top_k.unwrap_or(1_000),
                pre_min_trades: 4_000,
                post_min_trades: 50,
                post_warn_below_trades: 100,
                pre_min_total_r: 0.0,
                post_min_total_r: 0.0,
                pre_min_expectancy: 0.0,
                post_min_expectancy: 0.0,
                selection_max_drawdown_r: Some(25.0),
                min_pre_frs: 0.0,
                max_return_degradation: 0.25,
                max_single_trade_contribution: Some(0.25),
                max_formula_depth: Some(5),
                min_density_per_1000_bars: Some(0.25),
                overfit_candidate_top_k: 100,
            },
            SelectionPresetValue::Custom => Self {
                preset: args.preset,
                candidate_top_k: protocol_candidate_top_k.unwrap_or(1_000),
                pre_min_trades: 100,
                post_min_trades: 30,
                post_warn_below_trades: 50,
                pre_min_total_r: 0.0,
                post_min_total_r: 0.0,
                pre_min_expectancy: 0.0,
                post_min_expectancy: 0.0,
                selection_max_drawdown_r: None,
                min_pre_frs: 0.0,
                max_return_degradation: 0.25,
                max_single_trade_contribution: None,
                max_formula_depth: None,
                min_density_per_1000_bars: None,
                overfit_candidate_top_k: 100,
            },
        };

        Self {
            candidate_top_k: args.candidate_top_k.unwrap_or(base.candidate_top_k).max(1),
            pre_min_trades: args.pre_min_trades.unwrap_or(base.pre_min_trades),
            post_min_trades: args.post_min_trades.unwrap_or(base.post_min_trades),
            post_warn_below_trades: args
                .post_warn_below_trades
                .unwrap_or(base.post_warn_below_trades),
            pre_min_total_r: args.pre_min_total_r.unwrap_or(base.pre_min_total_r),
            post_min_total_r: args.post_min_total_r.unwrap_or(base.post_min_total_r),
            pre_min_expectancy: args.pre_min_expectancy.unwrap_or(base.pre_min_expectancy),
            post_min_expectancy: args.post_min_expectancy.unwrap_or(base.post_min_expectancy),
            selection_max_drawdown_r: args
                .selection_max_drawdown_r
                .or(base.selection_max_drawdown_r),
            min_pre_frs: args.min_pre_frs.unwrap_or(base.min_pre_frs),
            max_return_degradation: args
                .max_return_degradation
                .unwrap_or(base.max_return_degradation),
            max_single_trade_contribution: args
                .max_single_trade_contribution
                .or(base.max_single_trade_contribution),
            max_formula_depth: args.max_formula_depth.or(base.max_formula_depth),
            min_density_per_1000_bars: args
                .min_density_per_1000_bars
                .or(base.min_density_per_1000_bars),
            overfit_candidate_top_k: args
                .overfit_candidate_top_k
                .unwrap_or(base.overfit_candidate_top_k)
                .max(1),
            ..base
        }
    }
}

fn eval_args_for_validate(
    args: &SelectValidateArgs,
    policy: &ResolvedSelectionPolicy,
    formulas_path: PathBuf,
    manifest_path: PathBuf,
) -> EvalFormulasArgs {
    EvalFormulasArgs {
        prepared_path: args.prepared_path.clone(),
        formulas_path,
        target: args.target.clone(),
        stage: ResearchStageValue::Validation,
        research_protocol: Some(args.research_protocol.clone()),
        strict_protocol: true,
        formula_export_manifest: Some(manifest_path),
        ack_rerun_lockbox: false,
        rr_column: args.rr_column.clone(),
        stacking_mode: args.stacking_mode,
        cutoff: args.cutoff.clone(),
        capital: DEFAULT_CAPITAL_DOLLAR,
        risk_pct_per_trade: DEFAULT_RISK_PCT_PER_TRADE,
        report_top: args.report_top,
        runs_root: args.runs_root.clone(),
        dataset_id: args.dataset_id.clone(),
        run_id: args.run_id.clone(),
        run_slug: args
            .run_slug
            .clone()
            .or_else(|| Some("select_validation".to_string())),
        registry_dir: args.registry_dir.clone(),
        artifact_uri: args.artifact_uri.clone(),
        checksum_artifacts: args.checksum_artifacts,
        no_file_log: args.no_file_log,
        csv_out: None,
        json_out: None,
        selection_out: None,
        selection_decisions_out: None,
        selected_formulas_out: None,
        protocol_validation_out: None,
        rank_by: args.rank_by,
        selection_mode: SelectionModeValue::HoldoutConfirm,
        selection_preset: Some(policy.preset),
        candidate_top_k: policy.candidate_top_k,
        pre_min_trades: policy.pre_min_trades,
        post_min_trades: policy.post_min_trades,
        post_warn_below_trades: policy.post_warn_below_trades,
        pre_min_total_r: policy.pre_min_total_r,
        post_min_total_r: policy.post_min_total_r,
        pre_min_expectancy: policy.pre_min_expectancy,
        post_min_expectancy: policy.post_min_expectancy,
        selection_max_drawdown_r: policy.selection_max_drawdown_r,
        min_pre_frs: policy.min_pre_frs,
        max_return_degradation: policy.max_return_degradation,
        max_single_trade_contribution: policy.max_single_trade_contribution,
        max_formula_depth: policy.max_formula_depth,
        min_density_per_1000_bars: policy.min_density_per_1000_bars,
        complexity_penalty: args.complexity_penalty,
        embargo_bars: args.embargo_bars,
        no_purge_cross_boundary_exits: args.no_purge_cross_boundary_exits,
        no_frs: args.no_frs,
        frs_scope: args.frs_scope,
        frs_nmin: args.frs_nmin,
        frs_alpha: 2.0,
        frs_beta: 2.0,
        frs_gamma: 1.0,
        frs_delta: 1.0,
        frs_out: None,
        frs_windows_out: None,
        overfit_report: true,
        overfit_out: None,
        overfit_decisions_out: None,
        cscv_blocks: args.cscv_blocks,
        cscv_max_splits: args.cscv_max_splits,
        overfit_candidate_top_k: policy.overfit_candidate_top_k,
        max_pbo: args.max_pbo,
        min_psr: args.min_psr,
        min_dsr: args.min_dsr,
        min_positive_window_ratio: args.min_positive_window_ratio,
        effective_trials: args.effective_trials,
        stress_report: true,
        stress_out: None,
        stress_matrix_out: None,
        stress_min_total_r: args.stress_min_total_r,
        stress_min_expectancy: args.stress_min_expectancy,
        equity_curves_top_k: args.equity_curves_top_k,
        equity_curves_rank_by: crate::cli::EquityCurveRankByValue::Post,
        equity_curves_window: crate::cli::EquityCurveWindowValue::Both,
        equity_curves_out: None,
        plot: args.plot,
        plot_mode: args.plot_mode,
        plot_out: None,
        plot_dir: None,
        plot_x: PlotXValue::Timestamp,
        plot_metric: PlotMetricValue::Dollar,
        max_drawdown: args.max_drawdown,
        min_calmar: args.min_calmar,
        asset: args.asset.clone(),
        position_sizing: args.position_sizing,
        stop_distance_column: args.stop_distance_column.clone(),
        stop_distance_unit: args.stop_distance_unit,
        min_contracts: args.min_contracts,
        max_contracts: args.max_contracts,
        margin_per_contract_dollar: args.margin_per_contract_dollar,
        commission_per_trade_dollar: args.commission_per_trade_dollar,
        slippage_per_trade_dollar: args.slippage_per_trade_dollar,
        cost_per_trade_dollar: args.cost_per_trade_dollar,
        no_costs: args.no_costs,
    }
}

fn eval_args_for_lockbox(args: &SelectLockboxArgs) -> EvalFormulasArgs {
    EvalFormulasArgs {
        prepared_path: args.prepared_path.clone(),
        formulas_path: args.formulas_path.clone(),
        target: args.target.clone(),
        stage: lockbox_stage(args.stage),
        research_protocol: Some(args.research_protocol.clone()),
        strict_protocol: true,
        formula_export_manifest: Some(args.formula_export_manifest.clone()),
        ack_rerun_lockbox: args.ack_rerun_lockbox,
        rr_column: args.rr_column.clone(),
        stacking_mode: args.stacking_mode,
        cutoff: args.cutoff.clone(),
        capital: DEFAULT_CAPITAL_DOLLAR,
        risk_pct_per_trade: DEFAULT_RISK_PCT_PER_TRADE,
        report_top: args.report_top,
        runs_root: args.runs_root.clone(),
        dataset_id: args.dataset_id.clone(),
        run_id: args.run_id.clone(),
        run_slug: args
            .run_slug
            .clone()
            .or_else(|| Some("select_lockbox".to_string())),
        registry_dir: args.registry_dir.clone(),
        artifact_uri: args.artifact_uri.clone(),
        checksum_artifacts: args.checksum_artifacts,
        no_file_log: args.no_file_log,
        csv_out: None,
        json_out: None,
        selection_out: None,
        selection_decisions_out: None,
        selected_formulas_out: None,
        protocol_validation_out: None,
        rank_by: args.rank_by,
        selection_mode: SelectionModeValue::Off,
        selection_preset: None,
        candidate_top_k: 1,
        pre_min_trades: 1,
        post_min_trades: 1,
        post_warn_below_trades: 1,
        pre_min_total_r: 0.0,
        post_min_total_r: 0.0,
        pre_min_expectancy: 0.0,
        post_min_expectancy: 0.0,
        selection_max_drawdown_r: None,
        min_pre_frs: 0.0,
        max_return_degradation: 0.25,
        max_single_trade_contribution: None,
        max_formula_depth: None,
        min_density_per_1000_bars: None,
        complexity_penalty: 0.0,
        embargo_bars: 0,
        no_purge_cross_boundary_exits: false,
        no_frs: args.no_frs,
        frs_scope: args.frs_scope,
        frs_nmin: args.frs_nmin,
        frs_alpha: 2.0,
        frs_beta: 2.0,
        frs_gamma: 1.0,
        frs_delta: 1.0,
        frs_out: None,
        frs_windows_out: None,
        overfit_report: true,
        overfit_out: None,
        overfit_decisions_out: None,
        cscv_blocks: args.cscv_blocks,
        cscv_max_splits: args.cscv_max_splits,
        overfit_candidate_top_k: args.overfit_candidate_top_k,
        max_pbo: args.max_pbo,
        min_psr: args.min_psr,
        min_dsr: args.min_dsr,
        min_positive_window_ratio: args.min_positive_window_ratio,
        effective_trials: args.effective_trials,
        stress_report: true,
        stress_out: None,
        stress_matrix_out: None,
        stress_min_total_r: args.stress_min_total_r,
        stress_min_expectancy: args.stress_min_expectancy,
        equity_curves_top_k: args.equity_curves_top_k,
        equity_curves_rank_by: crate::cli::EquityCurveRankByValue::Post,
        equity_curves_window: crate::cli::EquityCurveWindowValue::Both,
        equity_curves_out: None,
        plot: args.plot,
        plot_mode: args.plot_mode,
        plot_out: None,
        plot_dir: None,
        plot_x: PlotXValue::Timestamp,
        plot_metric: PlotMetricValue::Dollar,
        max_drawdown: args.max_drawdown,
        min_calmar: args.min_calmar,
        asset: args.asset.clone(),
        position_sizing: args.position_sizing,
        stop_distance_column: args.stop_distance_column.clone(),
        stop_distance_unit: args.stop_distance_unit,
        min_contracts: args.min_contracts,
        max_contracts: args.max_contracts,
        margin_per_contract_dollar: args.margin_per_contract_dollar,
        commission_per_trade_dollar: args.commission_per_trade_dollar,
        slippage_per_trade_dollar: args.slippage_per_trade_dollar,
        cost_per_trade_dollar: args.cost_per_trade_dollar,
        no_costs: args.no_costs,
    }
}

fn print_validate_plan(
    args: &SelectValidateArgs,
    protocol: &ResearchProtocol,
    query: &ResultQuery,
    output_plan: &standard_output::StandardOutputPlan,
    policy: &ResolvedSelectionPolicy,
) -> Result<()> {
    let source_end = ensure_source_run_is_discovery_only(&args.source_output_dir, protocol)?;
    let rows = query_result_store(query)?;

    println!("Barsmith select validate dry run");
    println!("Source output: {}", args.source_output_dir.display());
    println!("Prepared CSV: {}", args.prepared_path.display());
    println!("Target: {}", query.target);
    println!("Direction: {}", query.direction);
    println!("Cutoff: {}", args.cutoff);
    println!("Protocol dataset: {}", protocol.dataset_id);
    println!("Policy preset: {}", policy.preset.to_preset().as_str());
    println!("Candidate cap: {}", policy.candidate_top_k);
    println!(
        "Source rank metric: {}",
        results::rank_by_label(query.rank_by)
    );
    println!("Source candidates matched: {}", rows.len());
    println!("Planned output: {}", output_plan.output_dir.display());
    println!(
        "Planned formula export: {}",
        output_plan
            .output_dir
            .join("candidate_formulas.txt")
            .display()
    );
    println!(
        "Planned manifest: {}",
        output_plan
            .output_dir
            .join("formula_export_manifest.json")
            .display()
    );
    println!("Source run date end: {source_end}");
    println!("Dry run wrote no evaluation artifacts.");
    Ok(())
}

fn ensure_source_run_is_discovery_only(
    output_dir: &Path,
    protocol: &ResearchProtocol,
) -> Result<chrono::NaiveDate> {
    let source_end = source_date_end(output_dir)?.ok_or_else(|| {
        anyhow!(
            "source run is missing date_end metadata; strict selection cannot prove discovery/pre-only provenance"
        )
    })?;
    let discovery_end = protocol.discovery.end.ok_or_else(|| {
        anyhow!("strict selection protocol discovery window must include an end date")
    })?;
    if source_end > discovery_end {
        return Err(anyhow!(
            "source run date end {source_end} is after protocol discovery end {discovery_end}"
        ));
    }
    Ok(source_end)
}

fn source_date_end(output_dir: &Path) -> Result<Option<chrono::NaiveDate>> {
    let path = output_dir.join("run_manifest.json");
    if !path.is_file() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let json: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(json
        .pointer("/identity/include_date_end")
        .and_then(|value| parse_manifest_date(Some(value))))
}

fn load_protocol(path: &Path) -> Result<ResearchProtocol> {
    let protocol: ResearchProtocol = load_json(path)?;
    validate_protocol(&protocol)?;
    Ok(protocol)
}

fn lockbox_stage(stage: LockboxStageValue) -> ResearchStageValue {
    stage.to_stage_value()
}

fn display_date(date: Option<chrono::NaiveDate>) -> String {
    date.map(|date| date.to_string())
        .unwrap_or_else(|| "open".to_string())
}

fn placeholder_path() -> PathBuf {
    PathBuf::from("__barsmith_select_pending__")
}
