use std::collections::{HashMap, HashSet};

use barsmith_rs::feature::{FeatureCategory, FeatureDescriptor};
use polars::prelude::*;
use tracing::info;

use super::series_kind::boolean_mask_from_series;

pub(super) fn prune_boolean_constants_and_duplicates(
    descriptors: Vec<FeatureDescriptor>,
    df: &DataFrame,
) -> Vec<FeatureDescriptor> {
    let mut kept: Vec<FeatureDescriptor> = Vec::new();
    let mut mask_index: HashMap<Vec<bool>, String> = HashMap::new();
    let mut constant_dropped: Vec<String> = Vec::new();
    let mut duplicate_dropped: Vec<(String, String)> = Vec::new();

    for descriptor in descriptors.into_iter() {
        if descriptor.category != FeatureCategory::Boolean {
            kept.push(descriptor);
            continue;
        }

        let name = descriptor.name.clone();
        let series = match df.column(&name) {
            Ok(col) => col,
            Err(_) => {
                kept.push(descriptor);
                continue;
            }
        };

        let mask = match boolean_mask_from_series(series) {
            Some(mask) => mask,
            None => {
                kept.push(descriptor);
                continue;
            }
        };

        if mask.is_empty() {
            kept.push(descriptor);
            continue;
        }

        let all_true = mask.iter().all(|value| *value);
        let all_false = mask.iter().all(|value| !*value);
        if all_true || all_false {
            constant_dropped.push(name);
            continue;
        }

        if let Some(existing) = mask_index.get(&mask) {
            duplicate_dropped.push((name, existing.clone()));
            continue;
        }

        mask_index.insert(mask, descriptor.name.clone());
        kept.push(descriptor);
    }

    let kept_boolean = kept
        .iter()
        .filter(|descriptor| descriptor.category == FeatureCategory::Boolean)
        .count();
    if kept_boolean == 0 {
        return kept;
    }

    if !constant_dropped.is_empty() {
        constant_dropped.sort();
        info!(
            "Dropped {} constant boolean features for this dataset (mask always true/false):",
            constant_dropped.len()
        );
        for name in &constant_dropped {
            info!("   - {}", name);
        }
    }

    if !duplicate_dropped.is_empty() {
        info!(
            "Dropped {} duplicate boolean features with identical masks:",
            duplicate_dropped.len()
        );
        for (duplicate, canonical) in &duplicate_dropped {
            info!("   - {} (duplicate of {})", duplicate, canonical);
        }
    }

    kept
}

pub(super) fn prune_duplicate_descriptor_names(
    descriptors: Vec<FeatureDescriptor>,
) -> Vec<FeatureDescriptor> {
    let mut seen = HashSet::new();
    let mut kept = Vec::with_capacity(descriptors.len());
    let mut dropped = Vec::new();

    for descriptor in descriptors {
        if seen.insert(descriptor.name.clone()) {
            kept.push(descriptor);
        } else {
            dropped.push(descriptor.name);
        }
    }

    if !dropped.is_empty() {
        dropped.sort();
        info!(
            "Dropped {} duplicate feature descriptors with repeated names:",
            dropped.len()
        );
        for name in &dropped {
            info!("   - {}", name);
        }
    }

    kept
}
