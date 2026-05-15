use super::definitions::{allowed_partners_for, pairwise_declared_feature_names};
use super::pruning::{prune_boolean_constants_and_duplicates, prune_duplicate_descriptor_names};
use super::*;
use polars::prelude::*;

fn df_from_series(columns: Vec<(&str, Series)>) -> DataFrame {
    let columns: Vec<Column> = columns
        .into_iter()
        .map(|(_, series)| series.into())
        .collect();
    DataFrame::new_infer_height(columns).expect("failed to build test DataFrame")
}

fn names(descriptors: &[FeatureDescriptor]) -> Vec<String> {
    descriptors
        .iter()
        .map(|descriptor| descriptor.name.clone())
        .collect()
}

#[test]
fn constant_true_boolean_is_pruned() {
    let s_true = Series::new("const_true".into(), &[true, true, true]);
    let s_mixed = Series::new("mixed".into(), &[true, false, true]);
    let df = df_from_series(vec![("const_true", s_true), ("mixed", s_mixed)]);

    let descriptors = vec![
        FeatureDescriptor::boolean("const_true", "test"),
        FeatureDescriptor::boolean("mixed", "test"),
    ];

    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        !names.contains(&"const_true".to_string()),
        "constant-true flag should be pruned"
    );
    assert!(
        names.contains(&"mixed".to_string()),
        "non-constant boolean flag should be kept"
    );
}

#[test]
fn constant_false_boolean_is_pruned() {
    let s_false = Series::new("const_false".into(), &[false, false, false]);
    let s_mixed = Series::new("mixed2".into(), &[false, true, false]);
    let df = df_from_series(vec![("const_false", s_false), ("mixed2", s_mixed)]);

    let descriptors = vec![
        FeatureDescriptor::boolean("const_false", "test"),
        FeatureDescriptor::boolean("mixed2", "test"),
    ];

    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        !names.contains(&"const_false".to_string()),
        "constant-false flag should be pruned"
    );
    assert!(
        names.contains(&"mixed2".to_string()),
        "non-constant boolean flag should be kept"
    );
}

#[test]
fn duplicate_boolean_masks_prune_to_single_canonical() {
    let s_a = Series::new("a".into(), &[true, false, true, false]);
    let s_b = Series::new("b".into(), &[true, false, true, false]);
    let s_c = Series::new("c".into(), &[false, false, true, true]);
    let df = df_from_series(vec![("a", s_a), ("b", s_b), ("c", s_c)]);

    let descriptors = vec![
        FeatureDescriptor::boolean("a", "test"),
        FeatureDescriptor::boolean("b", "test"),
        FeatureDescriptor::boolean("c", "test"),
    ];

    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        names.contains(&"a".to_string()) || names.contains(&"b".to_string()),
        "one of the duplicate flags should be kept"
    );
    assert!(
        !(names.contains(&"a".to_string()) && names.contains(&"b".to_string())),
        "both duplicate flags should not be kept simultaneously"
    );
    assert!(
        names.contains(&"c".to_string()),
        "independent boolean flag should be kept"
    );
}

#[test]
fn integer_binary_series_treated_as_boolean_and_pruned_when_constant() {
    let s_const = Series::new("ib_const".into(), &[0i64, 0, 0, 0]);
    let s_mixed = Series::new("ib_mixed".into(), &[0i64, 1, 0, 1]);
    let df = df_from_series(vec![("ib_const", s_const), ("ib_mixed", s_mixed)]);

    let descriptors = vec![
        FeatureDescriptor::boolean("ib_const", "test"),
        FeatureDescriptor::boolean("ib_mixed", "test"),
    ];

    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        !names.contains(&"ib_const".to_string()),
        "constant 0/1 integer flag should be pruned"
    );
    assert!(
        names.contains(&"ib_mixed".to_string()),
        "non-constant 0/1 integer flag should be kept"
    );
}

#[test]
fn float_binary_series_treated_as_boolean_and_pruned_when_constant() {
    let s_const = Series::new("fb_const".into(), &[1.0f64, 1.0, 1.0]);
    let s_mixed = Series::new("fb_mixed".into(), &[0.0f64, 1.0, 0.0]);
    let df = df_from_series(vec![("fb_const", s_const), ("fb_mixed", s_mixed)]);

    let descriptors = vec![
        FeatureDescriptor::boolean("fb_const", "test"),
        FeatureDescriptor::boolean("fb_mixed", "test"),
    ];

    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        !names.contains(&"fb_const".to_string()),
        "constant 0/1 float flag should be pruned"
    );
    assert!(
        names.contains(&"fb_mixed".to_string()),
        "non-constant 0/1 float flag should be kept"
    );
}

#[test]
fn non_binary_integer_series_not_treated_as_boolean() {
    let s_int = Series::new("int_other".into(), &[0i64, 1, 2, 3]);
    let df = df_from_series(vec![("int_other", s_int)]);

    let descriptors = vec![FeatureDescriptor::boolean("int_other", "test")];
    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        names.contains(&"int_other".to_string()),
        "non-binary integer column should not be treated as boolean for pruning"
    );
}

#[test]
fn non_binary_float_series_not_treated_as_boolean() {
    let s_float = Series::new("float_other".into(), &[0.1f64, 0.0, 1.0, 0.3]);
    let df = df_from_series(vec![("float_other", s_float)]);

    let descriptors = vec![FeatureDescriptor::boolean("float_other", "test")];
    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        names.contains(&"float_other".to_string()),
        "non-binary float column should not be treated as boolean for pruning"
    );
}

#[test]
fn missing_boolean_column_is_left_untouched() {
    let s_other = Series::new("other".into(), &[true, false, true]);
    let df = df_from_series(vec![("other", s_other)]);

    let descriptors = vec![FeatureDescriptor::boolean("missing_flag", "test")];
    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        names.contains(&"missing_flag".to_string()),
        "flags without a corresponding column in the dataframe should be preserved"
    );
}

#[test]
fn empty_dataframe_keeps_boolean_descriptors() {
    let s_empty_bool: Series = Series::new_empty("empty_flag".into(), &DataType::Boolean);
    let df = df_from_series(vec![("empty_flag", s_empty_bool)]);

    let descriptors = vec![FeatureDescriptor::boolean("empty_flag", "test")];
    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        names.contains(&"empty_flag".to_string()),
        "boolean flags over empty datasets should be preserved"
    );
}

#[test]
fn non_boolean_descriptors_are_not_affected_by_pruning() {
    let s_bool = Series::new("flag".into(), &[true, false, true]);
    let df = df_from_series(vec![("flag", s_bool)]);

    let descriptors = vec![
        FeatureDescriptor::boolean("flag", "test"),
        FeatureDescriptor::new("continuous_feature", FeatureCategory::Continuous, "note"),
    ];

    let pruned = prune_boolean_constants_and_duplicates(descriptors, &df);
    let names = names(&pruned);
    assert!(
        names.contains(&"continuous_feature".to_string()),
        "non-boolean descriptors should pass through pruning unchanged"
    );
}

#[test]
fn duplicate_descriptor_names_keep_first_definition() {
    let descriptors = vec![
        FeatureDescriptor::boolean("same_name", "first"),
        FeatureDescriptor::feature_vs_constant("same_name", "duplicate"),
        FeatureDescriptor::boolean("other_name", "other"),
    ];

    let pruned = prune_duplicate_descriptor_names(descriptors);
    let names = names(&pruned);

    assert_eq!(
        names,
        vec!["same_name".to_string(), "other_name".to_string()],
        "duplicate descriptor names should not create ambiguous stored formulas"
    );
    assert_eq!(
        pruned[0].note, "first",
        "the first descriptor keeps catalog order stable"
    );
}

#[test]
fn allowed_partners_apply_default_and_self_exclusion() {
    let partners = allowed_partners_for("momentum_score");
    assert!(
        partners.contains(&"close"),
        "default partners should include core price anchors"
    );
    assert!(
        !partners.contains(&"momentum_score"),
        "features should not list themselves as partners by default"
    );
}

#[test]
fn bb_position_and_consecutive_green_do_not_pair_with_each_other() {
    let bb_partners = allowed_partners_for("bb_position");
    assert!(
        bb_partners.contains(&"close"),
        "bb_position should still compare against core price anchors"
    );
    assert!(
        !bb_partners.contains(&"bb_position"),
        "bb_position should not list itself as a partner"
    );
    assert!(
        !bb_partners.contains(&"consecutive_green"),
        "bb_position should not pair directly with consecutive_green"
    );

    let cg_partners = allowed_partners_for("consecutive_green");
    assert!(
        cg_partners.contains(&"close"),
        "consecutive_green should still compare against core price anchors"
    );
    assert!(
        !cg_partners.contains(&"consecutive_green"),
        "consecutive_green should not list itself as a partner"
    );
    assert!(
        !cg_partners.contains(&"bb_position"),
        "consecutive_green should not pair directly with bb_position"
    );
}

#[test]
fn pairwise_declared_feature_names_cover_bases_and_rules() {
    let names = pairwise_declared_feature_names();
    assert!(
        names.contains(&"close"),
        "declared names should include base numeric anchors"
    );
    assert!(
        names.contains(&"9ema"),
        "declared names should include at least one extra numeric feature"
    );
}

#[test]
fn target_output_columns_are_not_research_features() {
    for name in [
        "2x_atr_tp_atr_stop",
        "2x_atr_tp_atr_stop_long",
        "2x_atr_tp_atr_stop_risk",
        "2x_atr_tp_atr_stop_risk_short",
        "2x_atr_tp_atr_stop_exit_i",
        "2x_atr_tp_atr_stop_eligible",
        "rr_2x_atr_tp_atr_stop",
        "rr_long",
        "tribar_4h_2atr",
    ] {
        assert!(
            is_target_output_column(name),
            "{name} should be treated as target output metadata"
        );
    }

    for name in ["atr_pct", "body_atr_ratio", "kf_atr", "close"] {
        assert!(
            !is_target_output_column(name),
            "{name} should remain eligible as normal feature data"
        );
    }
}
