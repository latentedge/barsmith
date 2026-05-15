use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::Result;

use crate::cli::{CombArgs, EvalFormulasArgs, PlotModeValue};

use super::helpers::{
    dataset_id_from_csv, dataset_id_from_prepared, format_direction, git_rev_parse,
    normalize_target, now_compact, now_iso, sanitize_segment, shell_join,
};
use super::{RunKind, StandardOutputPlan};

pub fn resolve_comb_output(args: &CombArgs, argv: &[OsString]) -> Result<StandardOutputPlan> {
    let target = sanitize_segment(&normalize_target(&args.target));
    let direction = format_direction(args.direction);
    let dataset_id = args
        .dataset_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| dataset_id_from_csv(&args.csv_path));

    let git_sha = git_rev_parse(["rev-parse", "HEAD"]);
    let git_short_sha = git_rev_parse(["rev-parse", "--short=12", "HEAD"]);
    let created_at = now_compact();
    let slug = args
        .run_slug
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| "run".to_string());
    let run_id = args
        .run_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| {
            let sha = git_short_sha.as_deref().unwrap_or("nogit");
            sanitize_segment(&format!("{created_at}_{sha}_{slug}"))
        });

    let run_path = PathBuf::from(RunKind::Comb.as_str())
        .join(&target)
        .join(direction)
        .join(&dataset_id)
        .join(&run_id);
    let output_dir = args.runs_root.join(&run_path);

    let command_argv: Vec<String> = argv
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let command_line = shell_join(&command_argv);

    Ok(StandardOutputPlan {
        run_kind: RunKind::Comb,
        output_dir,
        run_path,
        run_id,
        dataset_id,
        target,
        direction: direction.to_string(),
        cutoff: None,
        created_at: now_iso(),
        git_sha,
        git_short_sha,
        registry_dir: Some(args.registry_dir.clone()),
        artifact_uri: args.artifact_uri.clone(),
        checksum_artifacts: args.checksum_artifacts,
        command_argv,
        command_line,
    })
}

pub fn resolve_forward_output(
    args: &EvalFormulasArgs,
    argv: &[OsString],
) -> Result<StandardOutputPlan> {
    let target = sanitize_segment(&normalize_target(&args.target));
    let cutoff = sanitize_segment(&args.cutoff);
    let dataset_id = args
        .dataset_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| dataset_id_from_prepared(&args.prepared_path));

    let git_sha = git_rev_parse(["rev-parse", "HEAD"]);
    let git_short_sha = git_rev_parse(["rev-parse", "--short=12", "HEAD"]);
    let created_at = now_compact();
    let slug = args
        .run_slug
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| "forward_test".to_string());
    let run_id = args
        .run_id
        .as_deref()
        .map(sanitize_segment)
        .unwrap_or_else(|| {
            let sha = git_short_sha.as_deref().unwrap_or("nogit");
            sanitize_segment(&format!("{created_at}_{sha}_{slug}"))
        });

    let run_path = PathBuf::from(RunKind::ForwardTest.as_str())
        .join(&target)
        .join(&dataset_id)
        .join(&cutoff)
        .join(&run_id);
    let output_dir = args.runs_root.join(&run_path);

    let command_argv: Vec<String> = argv
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let command_line = shell_join(&command_argv);

    Ok(StandardOutputPlan {
        run_kind: RunKind::ForwardTest,
        output_dir,
        run_path,
        run_id,
        dataset_id,
        target,
        direction: "n/a".to_string(),
        cutoff: Some(cutoff),
        created_at: now_iso(),
        git_sha,
        git_short_sha,
        registry_dir: Some(args.registry_dir.clone()),
        artifact_uri: args.artifact_uri.clone(),
        checksum_artifacts: args.checksum_artifacts,
        command_argv,
        command_line,
    })
}

pub fn apply_forward_output_defaults(args: &mut EvalFormulasArgs, plan: &StandardOutputPlan) {
    args.csv_out
        .get_or_insert_with(|| plan.output_dir.join("formula_results.csv"));
    args.json_out
        .get_or_insert_with(|| plan.output_dir.join("formula_results.json"));
    if args.selection_mode.to_mode().is_enabled() && !args.stage.to_stage().is_lockbox_like() {
        args.selection_out
            .get_or_insert_with(|| plan.output_dir.join("selection_report.json"));
        args.selection_decisions_out
            .get_or_insert_with(|| plan.output_dir.join("selection_decisions.csv"));
        args.selected_formulas_out
            .get_or_insert_with(|| plan.output_dir.join("selected_formulas.txt"));
    }
    if args.strict_protocol {
        args.protocol_validation_out
            .get_or_insert_with(|| plan.output_dir.join("protocol_validation.json"));
    }

    if !args.no_frs {
        args.frs_out
            .get_or_insert_with(|| plan.output_dir.join("frs_summary.csv"));
        args.frs_windows_out
            .get_or_insert_with(|| plan.output_dir.join("frs_windows.csv"));
    }
    if args.strict_protocol || args.overfit_report {
        args.overfit_out
            .get_or_insert_with(|| plan.output_dir.join("overfit_report.json"));
        args.overfit_decisions_out
            .get_or_insert_with(|| plan.output_dir.join("overfit_decisions.csv"));
    }
    if args.strict_protocol || args.stress_report {
        args.stress_out
            .get_or_insert_with(|| plan.output_dir.join("stress_report.json"));
        args.stress_matrix_out
            .get_or_insert_with(|| plan.output_dir.join("stress_matrix.csv"));
    }

    if args.equity_curves_top_k > 0 {
        args.equity_curves_out
            .get_or_insert_with(|| plan.output_dir.join("equity_curves.csv"));
    }

    if args.plot {
        match args.plot_mode {
            PlotModeValue::Combined => {
                args.plot_out
                    .get_or_insert_with(|| plan.output_dir.join("plots").join("equity_curves.png"));
            }
            PlotModeValue::Individual => {
                args.plot_dir
                    .get_or_insert_with(|| plan.output_dir.join("plots"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{
        DirectionValue, EvalProfileValue, PositionSizingValue, ReportMetricsValue,
        StackingModeValue, StopDistanceUnitValue,
    };
    use crate::stats_detail::StatsDetailValue;

    fn args() -> CombArgs {
        CombArgs {
            csv_path: PathBuf::from("data/ES 30m official.csv"),
            direction: DirectionValue::Long,
            target: "2x_atr_tp_atr_stop".to_string(),
            runs_root: PathBuf::from(crate::cli::DEFAULT_RUNS_ROOT),
            dataset_id: Some("ES 30m Official V2".to_string()),
            run_id: Some("Manual Run 01".to_string()),
            run_slug: None,
            registry_dir: PathBuf::from(crate::cli::DEFAULT_REGISTRY_DIR),
            artifact_uri: Some("s3://bucket/barsmith/run".to_string()),
            checksum_artifacts: false,
            s3_output: None,
            s3_upload_each_batch: false,
            max_depth: 3,
            min_samples: 100,
            min_samples_report: None,
            date_start: None,
            date_end: None,
            batch_size: 20_000,
            auto_batch: false,
            workers: Some(1),
            resume_from: 0,
            max_combos: None,
            dry_run: false,
            quiet: false,
            report_metrics: ReportMetricsValue::Full,
            top_k: 5,
            force: false,
            ack_new_df: false,
            feature_pairs: false,
            feature_pairs_limit: None,
            max_drawdown: 30.0,
            max_drawdown_report: None,
            min_calmar_report: None,
            no_file_log: false,
            subset_pruning: false,
            stats_detail: StatsDetailValue::Core,
            profile_eval: EvalProfileValue::Off,
            profile_eval_sample_rate: 1,
            capital: None,
            risk_pct_per_trade: None,
            asset: None,
            position_sizing: PositionSizingValue::Fractional,
            stop_distance_column: None,
            stop_distance_unit: StopDistanceUnitValue::Points,
            min_contracts: 1,
            max_contracts: None,
            margin_per_contract_dollar: None,
            commission_per_trade_dollar: None,
            slippage_per_trade_dollar: None,
            cost_per_trade_dollar: None,
            no_costs: false,
            require_any_features: Vec::new(),
            stacking_mode: StackingModeValue::NoStacking,
        }
    }

    #[test]
    fn runs_root_builds_canonical_run_path() {
        let plan = resolve_comb_output(
            &args(),
            &[OsString::from("barsmith"), OsString::from("comb")],
        )
        .expect("plan");

        assert_eq!(plan.dataset_id, "es_30m_official_v2");
        assert_eq!(plan.run_id, "manual_run_01");
        assert_eq!(
            plan.output_dir,
            PathBuf::from(
                "runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/manual_run_01"
            )
        );
        assert_eq!(
            plan.run_path,
            PathBuf::from("comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/manual_run_01")
        );
    }

    #[test]
    fn registry_dir_defaults_to_standard_root() {
        let plan = resolve_comb_output(
            &args(),
            &[OsString::from("barsmith"), OsString::from("comb")],
        )
        .expect("plan");

        assert_eq!(
            plan.registry_dir,
            Some(PathBuf::from(crate::cli::DEFAULT_REGISTRY_DIR))
        );
    }

    #[test]
    fn custom_roots_override_standard_defaults() {
        let mut args = args();
        args.runs_root = PathBuf::from("custom/artifacts");
        args.registry_dir = PathBuf::from("custom/registry");

        let plan =
            resolve_comb_output(&args, &[OsString::from("barsmith"), OsString::from("comb")])
                .expect("plan");

        assert_eq!(
            plan.output_dir,
            PathBuf::from(
                "custom/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/manual_run_01"
            )
        );
        assert_eq!(plan.registry_dir, Some(PathBuf::from("custom/registry")));
    }
}
