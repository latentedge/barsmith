use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeatureCategory {
    Boolean,
    Continuous,
    /// Feature-vs-constant comparison (scalar threshold, e.g. rsi_14>20).
    FeatureVsConstant,
    /// Feature-vs-feature comparison (pairwise numeric predicate, e.g. 9ema>200sma).
    FeatureVsFeature,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeatureDescriptor {
    pub name: String,
    pub category: FeatureCategory,
    pub note: String,
}

impl FeatureDescriptor {
    pub fn new(
        name: impl Into<String>,
        category: FeatureCategory,
        note: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            category,
            note: note.into(),
        }
    }

    pub fn boolean(name: &str, note: &str) -> Self {
        Self::new(name, FeatureCategory::Boolean, note)
    }

    pub fn comparison(name: impl Into<String>, note: impl Into<String>) -> Self {
        Self::new(name, FeatureCategory::FeatureVsConstant, note)
    }

    /// Descriptor for feature-vs-constant scalar comparisons (e.g. rsi_14>20).
    pub fn feature_vs_constant(name: impl Into<String>, note: impl Into<String>) -> Self {
        Self::new(name, FeatureCategory::FeatureVsConstant, note)
    }

    /// Descriptor for feature-vs-feature numeric comparisons (e.g. 9ema>200sma).
    pub fn feature_vs_feature(name: impl Into<String>, note: impl Into<String>) -> Self {
        Self::new(name, FeatureCategory::FeatureVsFeature, note)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComparisonOperator {
    GreaterThan,
    LessThan,
    GreaterEqual,
    LessEqual,
}

#[derive(Clone, Debug)]
pub struct ComparisonSpec {
    pub base_feature: String,
    pub operator: ComparisonOperator,
    /// Threshold for feature-to-constant comparisons. When `rhs_feature` is
    /// `Some`, this may be `None`.
    pub threshold: Option<f64>,
    /// Optional right-hand side feature for feature-to-feature comparisons.
    /// When `rhs_feature` is `Some`, the comparison is evaluated as
    /// `base_feature (op) rhs_feature`.
    pub rhs_feature: Option<String>,
}

impl ComparisonSpec {
    pub fn threshold(
        base_feature: impl Into<String>,
        operator: ComparisonOperator,
        threshold: f64,
    ) -> Self {
        Self {
            base_feature: base_feature.into(),
            operator,
            threshold: Some(threshold),
            rhs_feature: None,
        }
    }

    pub fn pair(
        left_feature: impl Into<String>,
        operator: ComparisonOperator,
        right_feature: impl Into<String>,
    ) -> Self {
        Self {
            base_feature: left_feature.into(),
            operator,
            threshold: None,
            rhs_feature: Some(right_feature.into()),
        }
    }
}

fn operator_symbol(op: ComparisonOperator) -> &'static str {
    match op {
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::GreaterEqual => ">=",
        ComparisonOperator::LessEqual => "<=",
    }
}

/// Generate ordered feature-to-feature comparison predicates.
///
/// Each `(left, operator, right)` becomes a virtual boolean feature such as
/// `close>9ema`. `max_pairs` caps the emitted predicate count when provided.
pub fn generate_feature_comparisons(
    features: &[&str],
    operators: &[ComparisonOperator],
    max_pairs: Option<usize>,
    note: &str,
) -> (
    Vec<FeatureDescriptor>,
    std::collections::HashMap<String, ComparisonSpec>,
) {
    use std::collections::HashMap;

    let mut descriptors = Vec::new();
    let mut specs = HashMap::new();
    let mut emitted = 0usize;
    let limit = max_pairs.unwrap_or(usize::MAX);

    for &left in features {
        for &right in features {
            if left == right {
                continue;
            }
            for &op in operators {
                if emitted >= limit {
                    return (descriptors, specs);
                }
                let symbol = operator_symbol(op);
                let name = format!("{left}{symbol}{right}");
                if specs.contains_key(&name) {
                    continue;
                }
                let descriptor = FeatureDescriptor::feature_vs_feature(name.clone(), note);
                let spec = ComparisonSpec::pair(left, op, right);
                descriptors.push(descriptor);
                specs.insert(name, spec);
                emitted += 1;
            }
        }
    }

    (descriptors, specs)
}

/// Generate feature-to-feature predicates without inverted duplicates.
///
/// A pair such as `9ema` and `200sma` gets one canonical ordering instead of
/// equivalent inverted predicates.
pub fn generate_unordered_feature_comparisons(
    features: &[&str],
    operators: &[ComparisonOperator],
    max_pairs: Option<usize>,
    note: &str,
) -> (Vec<FeatureDescriptor>, HashMap<String, ComparisonSpec>) {
    let mut descriptors = Vec::new();
    let mut specs = HashMap::new();
    let mut emitted = 0usize;
    let limit = max_pairs.unwrap_or(usize::MAX);

    for (i, &left) in features.iter().enumerate() {
        for &right in features.iter().skip(i + 1) {
            for &op in operators {
                if emitted >= limit {
                    return (descriptors, specs);
                }
                let symbol = operator_symbol(op);
                let name = format!("{left}{symbol}{right}");
                if specs.contains_key(&name) {
                    continue;
                }
                let descriptor = FeatureDescriptor::feature_vs_feature(name.clone(), note);
                let spec = ComparisonSpec::pair(left, op, right);
                descriptors.push(descriptor);
                specs.insert(name, spec);
                emitted += 1;
            }
        }
    }

    (descriptors, specs)
}
