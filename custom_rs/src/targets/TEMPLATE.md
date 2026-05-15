# Target Template

Use this checklist when adding a target under `custom_rs/src/targets/<target-id>/`.

## Files

- `custom_rs/src/targets/<target-id>/mod.rs`
- optional `custom_rs/src/targets/<target-id>/tests.rs` when the target needs
  focused tests beyond the shared engineer tests
- `custom_rs/src/targets/mod.rs`
- `custom_rs/src/targets/registry.rs`

## Metadata

Every target module should define:

```rust
pub(crate) const ID: &str = "<target-id>";
pub(crate) const SUPPORTS_BOTH_CANONICAL: bool = true;
pub(crate) const DEFAULT_STOP_DISTANCE_COLUMN: Option<&str> = None;
```

Use `SUPPORTS_BOTH_CANONICAL = false` when one canonical target/RR/risk column
cannot honestly represent combined long and short results.

## Attachment

Expose one setup-time function:

```rust
pub(crate) fn attach(frame: &mut TargetFrame<'_>, config: &Config) -> anyhow::Result<()> {
    // Read columns through TargetFrame helpers, call pure slice functions, and
    // write canonical output columns back to the frame.
}
```

Keep row loops in plain slice functions. Avoid trait objects, heap allocation,
hashing, logging, or schema inspection inside the row loop.

## Required Columns

Emit the prepared-dataset columns described in `docs/data-contract.md`:

- `<target>` as the canonical boolean label
- `rr_<target>` as canonical R-unit reward
- `<target>_exit_i` for no-stacking workflows
- `<target>_eligible` when some rows cannot be valid entries
- `<target>_risk` when contract sizing should use realized stop risk

Target output columns are excluded from the searchable feature catalog through
`custom_rs::targets::registry::is_target_output_column`.

## Tests

Cover the target's actual risk cases:

- NaN or missing input columns
- long, short, and rejected `both` semantics
- same-bar target/stop touches
- gap opens through stop or target
- cutoff-capped exits
- tick rounding
- realized risk columns for contract sizing

Run `scripts/performance_gate.sh` before closing any performance-sensitive
target change. For ATR/high-low target geometry changes, also run the
target-generation suite described in `docs/testing.md`.
