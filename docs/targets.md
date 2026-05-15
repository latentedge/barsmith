# Targets and Strategy Preparation

Barsmith keeps the search engine strategy-agnostic. `comb` has one first-party
preparation path:

- `custom_rs` owns feature engineering, target labeling, target metadata, and
  feature-catalog construction.
- `barsmith_indicators` owns reusable indicator and rolling-window math. It has
  no Polars, CLI, storage, or strategy dependencies.
- `barsmith_rs` owns prepared-dataset loading, combination enumeration,
  evaluation, stats, resume, and persistence.

New first-party strategies should be implemented under `custom_rs`. Reusable
indicator math belongs in `barsmith_indicators` only when another strategy could
reasonably reuse it.

## Supported targets

`custom_rs` currently supports:

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

ATR-stop targets emit target-specific RR, eligibility, exit-index, and realized
risk columns. The realized risk columns are computed from the same rounded stop
used by target resolution, so `--position-sizing contracts --asset <CODE>` sizes
contracts from executable tick-rounded risk instead of raw ATR.

Run long and short ATR-stop searches separately. The CLI rejects `--direction
both` for canonical ATR-stop targets because the prepared dataset has one
canonical target/RR/risk column.

## Folder contract

Each public target has a folder under `custom_rs/src/targets/<target-id>/`.
Literal folder names should match the CLI target id. If a target id starts with
a number, use a valid Rust module alias in `custom_rs/src/targets/mod.rs`:

```rust
#[path = "2x_atr_tp_atr_stop/mod.rs"]
pub(crate) mod two_x_atr_tp_atr_stop;
```

A target folder owns:

- public target id and metadata constants,
- attach-time validation, such as direction or timeframe restrictions,
- target/RR/eligibility/exit/risk column attachment,
- target-specific tests or fixtures when behavior is unique.

Shared target resolution code belongs in `custom_rs/src/targets/common/`. The
`barrier.rs` facade keeps the stable helper names, while
`targets/common/barrier/` separates tick rounding, result storage, next-bar
targets, and high-low/ATR target geometry.

Shared indicators belong in `barsmith_indicators`. The searchable feature
surface is curated in `custom_rs/src/features/definitions.rs`; add a feature
there only when it should participate in combination search, not merely because
it exists in the prepared CSV. Preparation orchestration lives in
`custom_rs/src/engineer.rs`; reusable feature-block assembly is exposed through
`custom_rs/src/engineer/feature_blocks.rs` and implemented in the
`custom_rs/src/engineer/feature_blocks/` submodules.
Feature-catalog helpers are split under `custom_rs/src/features/` so target
authors can distinguish curated search definitions from audit logs, pairwise
rules, pruning, and series-type classification.

## Adding a target

1. Create `custom_rs/src/targets/<target-id>/mod.rs`.
2. Add the module to `custom_rs/src/targets/mod.rs`.
3. Add one registry entry in `custom_rs/src/targets/registry.rs`.
4. Implement `attach(frame: &mut TargetFrame<'_>, config: &Config)`.
5. Emit canonical columns required by `docs/data-contract.md`:
   - `<target>`
   - `rr_<target>`
   - `<target>_eligible` when only some rows are trade candidates
   - `<target>_exit_i` for no-stacking workflows
   - `<target>_risk` when contract sizing needs realized stop risk
6. Add tests for NaNs, same-bar stop/target touches, gap opens,
   cutoff-capped exits, direction restrictions, and tick rounding where they
   apply.
7. Run the full Rust gate plus the performance gate. For ATR/high-low target
   changes, also run the target-generation benchmark suite.

Use `custom_rs/src/targets/TEMPLATE.md` as the contributor checklist.

## Adding an indicator

Add a function to `barsmith_indicators` when the math is reusable outside one
target. Keep the API slice-based and Polars-free:

```rust
pub fn ema(values: &[f64], period: usize) -> Vec<f64>;
pub fn ema_into(values: &[f64], period: usize, out: &mut [f64]);
```

Prefer a `Vec`-returning helper for readability and a `*_into` variant when a
caller can reuse buffers in a high-volume preparation path. Indicator tests
should cover short histories, NaNs, flat values, zero denominators, and warmup
behavior.

## Prepared datasets

If you bring your own prepared dataset instead of using `comb`, it must satisfy
`docs/data-contract.md`. In particular, no-stacking evaluation requires a valid
integer `<target>_exit_i` column, and contract sizing needs an explicit
stop-distance column unless the target registry can infer one.
