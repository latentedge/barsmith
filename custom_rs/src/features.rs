use std::collections::HashMap;
use std::path::Path;

use crate::thresholds;
use anyhow::Result;
use barsmith_rs::feature::{ComparisonSpec, FeatureCategory, FeatureDescriptor};
use barsmith_rs::{config::Config, data::ColumnarData};
use tracing::info;

mod audit;
mod definitions;
mod pairwise;
mod pruning;
mod series_kind;
#[cfg(test)]
mod tests;

use audit::{
    audit_boolean_coverage, audit_boolean_definitions, audit_continuous_coverage,
    audit_continuous_definitions,
};
pub use definitions::{
    BOOLEAN_FEATURES, CONTINUOUS_FEATURES, PAIRWISE_BASE_NUMERIC_FEATURES,
    PAIRWISE_EXTRA_NUMERIC_FEATURES, PAIRWISE_NUMERIC_RULES, PairwiseRule,
};
use definitions::{BOOLEAN_NOTE, CONTINUOUS_NOTE};
use pairwise::append_pairwise_features;
use pruning::{prune_boolean_constants_and_duplicates, prune_duplicate_descriptor_names};

pub struct FeatureCatalog;

pub struct CatalogBuild {
    pub descriptors: Vec<FeatureDescriptor>,
    pub comparison_specs: HashMap<String, ComparisonSpec>,
}

impl FeatureCatalog {
    pub fn build_with_dataset(dataset_path: &Path, config: &Config) -> Result<CatalogBuild> {
        let dataset_display = dataset_path.display().to_string();
        let data = ColumnarData::load(dataset_path)?;
        let data = data.filter_by_date_range(config.include_date_start, config.include_date_end)?;
        let frame = data.data_frame();
        let df = frame.as_ref();
        audit_boolean_definitions(df);
        audit_boolean_coverage(df);
        audit_continuous_definitions(df);
        audit_continuous_coverage(df);

        // Constant flags and exact duplicate masks cannot add signal, but they
        // do multiply the search.
        let mut descriptors = Self::boolean_descriptors();
        descriptors = prune_boolean_constants_and_duplicates(descriptors, df);

        let threshold_catalog =
            thresholds::generate_threshold_catalog_from_frame(df, &dataset_display)?;
        let mut comparison_specs = threshold_catalog.specs;
        descriptors.extend(threshold_catalog.descriptors);

        if config.enable_feature_pairs {
            append_pairwise_features(
                df,
                &dataset_display,
                config,
                &mut descriptors,
                &mut comparison_specs,
            );
        }

        descriptors = prune_duplicate_descriptor_names(descriptors);

        info!(
            dataset = %dataset_display,
            boolean_features = descriptors
                .iter()
                .filter(|d| matches!(d.category, FeatureCategory::Boolean))
                .count(),
            feature_vs_constant = descriptors
                .iter()
                .filter(|d| matches!(d.category, FeatureCategory::FeatureVsConstant))
                .count(),
            feature_vs_feature = descriptors
                .iter()
                .filter(|d| matches!(d.category, FeatureCategory::FeatureVsFeature))
                .count(),
            catalog_total = descriptors.len(),
            "Feature catalog generated"
        );

        Ok(CatalogBuild {
            descriptors,
            comparison_specs,
        })
    }

    pub fn boolean_descriptors() -> Vec<FeatureDescriptor> {
        BOOLEAN_FEATURES
            .iter()
            .map(|name| FeatureDescriptor::boolean(name, BOOLEAN_NOTE))
            .collect()
    }

    pub fn boolean_names() -> &'static [&'static str] {
        BOOLEAN_FEATURES
    }

    pub fn descriptors_for(names: &[&str]) -> Vec<FeatureDescriptor> {
        names
            .iter()
            .map(|name| {
                if BOOLEAN_FEATURES.contains(name) {
                    FeatureDescriptor::boolean(name, BOOLEAN_NOTE)
                } else if CONTINUOUS_FEATURES.contains(name) {
                    FeatureDescriptor::new(*name, FeatureCategory::Continuous, CONTINUOUS_NOTE)
                } else {
                    // Some parity tests pass curated historical feature names
                    // that are not part of the current boolean catalog.
                    FeatureDescriptor::boolean(name, BOOLEAN_NOTE)
                }
            })
            .collect()
    }
}

pub(crate) fn is_target_output_column(name: &str) -> bool {
    crate::targets::registry::is_target_output_column(name)
}
