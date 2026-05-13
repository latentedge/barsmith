# Engines and targets

Barsmith separates “core search + storage” from “feature engineering + target labeling”.

## Engine selection

`barsmith_cli comb` accepts `--engine auto|builtin|custom`.

- `auto` is the default. It uses the builtin engine for simple next-bar targets and the custom Rust engine for richer targets.
- `builtin` forces `barsmith_builtin`.
- `custom` forces `custom_rs`.

Use `builtin` when you want the smallest target surface and fastest preparation path. Use `custom` when you need ATR/High-Low/KF/tribar targets or the larger feature catalog.

## Builtin engine

The builtin engine uses `barsmith_builtin` to:

- load the raw OHLCV CSV,
- add a minimal set of engineered columns,
- emit a boolean target column and supporting columns required by the evaluator,
- build a small feature catalog for `comb`.

Supported targets (builtin engine):

- `next_bar_up`
- `next_bar_down`
- `next_bar_color_and_wicks` (compatibility alias for `next_bar_up`)

## Custom engines / prepared datasets

The custom engine uses `custom_rs` for richer targets and feature catalogs. It is available directly through the CLI:

```bash
barsmith_cli comb \
  --csv ../es_30m.csv \
  --target 2x_atr_tp_atr_stop \
  --engine auto \
  --runs-root runs/artifacts \
  --dataset-id es_30m_official_v2
```

Custom-engine targets currently include:

- `next_bar_color_and_wicks`
- `wicks_kf`
- `highlow_or_atr`
- `highlow_1r`
- `2x_atr_tp_atr_stop`
- `3x_atr_tp_atr_stop`
- `atr_tp_atr_stop`
- `highlow_sl_2x_atr_tp_rr_gt_1`
- `highlow_sl_1x_atr_tp_rr_gt_1`
- `highlow_or_atr_tightest_stop`
- `tribar_4h_2atr`

If you want to bring your own feature engineering or targets, you have two main options:

1. Use `barsmith_rs` as a library and provide your own “prepared dataset” that satisfies the contract in `docs/data-contract.md`.
2. Add a Rust engine crate that prepares the dataset and then calls the shared `barsmith_rs` pipeline.

When `--date-start` or `--date-end` is set, the custom engine computes raw engineered columns first and fits the searchable feature catalog from the filtered evaluation window. This keeps scalar threshold generation aligned with the in-sample search window instead of learning thresholds from future rows.

## Feature catalog types

Barsmith’s feature catalog is a list of `FeatureDescriptor`s that can represent:

- boolean predicates (pre-computed boolean columns)
- feature-vs-constant thresholds (numeric columns compared to a constant)
- feature-vs-feature comparisons (optional pairwise comparisons when enabled)

The evaluator combines predicates with AND logic up to `--max-depth`.
