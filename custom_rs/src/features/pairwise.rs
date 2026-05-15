use std::collections::{HashMap, HashSet};

use barsmith_rs::config::Config;
use barsmith_rs::feature::{
    ComparisonOperator, ComparisonSpec, FeatureDescriptor, generate_unordered_feature_comparisons,
};
use polars::prelude::*;
use tracing::info;

use super::definitions::{pair_allowed, pairwise_declared_feature_names};
use super::is_target_output_column;
use super::series_kind::is_binary_01_series;

struct PairwiseCandidates<'a> {
    available: Vec<&'a str>,
    missing: Vec<&'a str>,
    non_numeric: Vec<&'a str>,
    declared_set: HashSet<&'a str>,
}

pub(super) fn append_pairwise_features(
    df: &DataFrame,
    dataset_display: &str,
    config: &Config,
    descriptors: &mut Vec<FeatureDescriptor>,
    comparison_specs: &mut HashMap<String, ComparisonSpec>,
) {
    let declared_pair_names = pairwise_declared_feature_names();
    let candidates = classify_pairwise_candidates(df, &declared_pair_names);
    log_pairwise_configuration_gaps(df, &candidates);
    log_unexpected_pairwise_candidates(df, &candidates.declared_set);

    if candidates.available.is_empty() {
        info!(
            dataset = %dataset_display,
            "Feature pairs enabled but no PAIRWISE_NUMERIC_FEATURES present in dataset; skipping feature-to-feature comparisons"
        );
        return;
    }

    info!("Feature-pair candidates:");
    for name in &candidates.available {
        info!("   - {}", name);
    }

    let ops = [
        ComparisonOperator::GreaterThan,
        ComparisonOperator::LessThan,
    ];
    let (pair_descriptors, pair_specs) = generate_unordered_feature_comparisons(
        &candidates.available,
        &ops,
        None,
        "Feature-to-feature comparison",
    );

    let (filtered_descriptors, filtered_specs) =
        filter_allowed_pairwise_specs(pair_descriptors, pair_specs, config.feature_pairs_limit);
    let pair_count = filtered_specs.len();
    let preview_names: Vec<String> = filtered_descriptors
        .iter()
        .take(5)
        .map(|descriptor| descriptor.name.clone())
        .collect();

    descriptors.extend(filtered_descriptors);
    for (key, spec) in filtered_specs {
        comparison_specs.entry(key).or_insert(spec);
    }

    log_pairwise_summary(
        dataset_display,
        candidates.available.len(),
        &ops,
        pair_count,
        config.feature_pairs_limit,
        &preview_names,
    );
}

fn classify_pairwise_candidates<'a>(
    df: &DataFrame,
    declared_pair_names: &'a [&'a str],
) -> PairwiseCandidates<'a> {
    let mut available = Vec::new();
    let mut missing = Vec::new();
    let mut non_numeric = Vec::new();
    let declared_set = declared_pair_names.iter().copied().collect();

    for &name in declared_pair_names {
        match df.column(name) {
            Ok(series) if is_pairwise_numeric_series(series) => available.push(name),
            Ok(_) => non_numeric.push(name),
            Err(_) => missing.push(name),
        }
    }

    PairwiseCandidates {
        available,
        missing,
        non_numeric,
        declared_set,
    }
}

fn is_pairwise_numeric_series(series: &Column) -> bool {
    matches!(series.dtype(), DataType::Float32 | DataType::Float64) && !is_binary_01_series(series)
}

fn log_pairwise_configuration_gaps(df: &DataFrame, candidates: &PairwiseCandidates<'_>) {
    if !candidates.missing.is_empty() {
        info!(
            "Pairwise numeric configuration entries missing from engineered dataset (not present as columns):"
        );
        for name in &candidates.missing {
            info!("   - {}", name);
        }
        info!("Add these to engineer.rs or remove them from the pairwise numeric configuration");
    }

    if !candidates.non_numeric.is_empty() {
        info!(
            "Pairwise numeric configuration entries are not numeric in the engineered dataset (skipping for feature-pairs):"
        );
        for name in &candidates.non_numeric {
            if let Ok(series) = df.column(name) {
                info!("   - {} (dtype={:?})", name, series.dtype());
            } else {
                info!("   - {}", name);
            }
        }
        info!(
            "Export these columns as float32/float64 if you want them in feature-to-feature comparisons"
        );
    }
}

fn log_unexpected_pairwise_candidates(df: &DataFrame, declared_pair_set: &HashSet<&str>) {
    let mut unexpected = Vec::new();

    for series in df.columns() {
        let name = series.name().as_str();
        if declared_pair_set.contains(name) || is_target_output_column(name) {
            continue;
        }
        if is_pairwise_numeric_series(series) {
            unexpected.push(name.to_string());
        }
    }

    if unexpected.is_empty() {
        return;
    }

    unexpected.sort();
    info!(
        "Found {} numeric features in dataframe but not in the pairwise numeric configuration (skipped for feature-to-feature predicates):",
        unexpected.len()
    );
    for name in &unexpected {
        info!("   - {}", name);
    }
    info!(
        "Add these to the pairwise numeric configuration in custom_rs::features if you want feature-to-feature comparisons for them"
    );
}

fn filter_allowed_pairwise_specs(
    pair_descriptors: Vec<FeatureDescriptor>,
    pair_specs: HashMap<String, ComparisonSpec>,
    limit: Option<usize>,
) -> (Vec<FeatureDescriptor>, HashMap<String, ComparisonSpec>) {
    let mut descriptor_by_name: HashMap<String, FeatureDescriptor> = pair_descriptors
        .into_iter()
        .map(|descriptor| (descriptor.name.clone(), descriptor))
        .collect();
    let mut filtered_descriptors = Vec::new();
    let mut filtered_specs = HashMap::new();
    let limit = limit.unwrap_or(usize::MAX);

    for (name, spec) in pair_specs {
        if filtered_specs.len() >= limit {
            break;
        }

        let Some(right) = spec.rhs_feature.as_deref() else {
            continue;
        };
        if !pair_allowed(spec.base_feature.as_str(), right) {
            continue;
        }

        if let Some(descriptor) = descriptor_by_name.remove(&name) {
            filtered_descriptors.push(descriptor);
        }
        filtered_specs.insert(name, spec);
    }

    (filtered_descriptors, filtered_specs)
}

fn log_pairwise_summary(
    dataset_display: &str,
    pair_feature_count: usize,
    ops: &[ComparisonOperator],
    pair_count: usize,
    pair_limit: Option<usize>,
    preview_names: &[String],
) {
    let op_symbols: Vec<&str> = ops
        .iter()
        .map(|op| match op {
            ComparisonOperator::GreaterThan => ">",
            ComparisonOperator::LessThan => "<",
            _ => "?",
        })
        .collect();
    let total_pairs = pair_feature_count * pair_feature_count.saturating_sub(1) / 2;
    let theoretical_predicates = total_pairs * op_symbols.len();
    info!(
        dataset = %dataset_display,
        pair_feature_count,
        operators = ?op_symbols,
        theoretical_predicates,
        pair_condition_count = pair_count,
        pair_limit = ?pair_limit,
        "Added feature-to-feature comparisons to catalog"
    );

    if pair_count == 0 {
        return;
    }

    info!("Sample feature-to-feature predicates:");
    for name in preview_names {
        info!("   - {}", name);
    }
    if pair_count > preview_names.len() {
        info!(
            "   ... and {} more",
            pair_count.saturating_sub(preview_names.len())
        );
    }
}
