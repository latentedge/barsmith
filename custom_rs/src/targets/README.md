# Targets

Each folder here is named after the public CLI target id. Rust modules with
numeric prefixes use a valid Rust identifier in `targets/mod.rs`, but the
filesystem name still matches the target that users type.

Target modules own target metadata, attach-time validation, and the columns they
write into `barsmith_prepared.csv`. Shared stop/target geometry lives in
`common/`. Reusable indicator math belongs in `barsmith_indicators`, not in a
target folder.

When adding a target:

1. Create `custom_rs/src/targets/<target-id>/mod.rs`.
2. Register it in `targets/mod.rs` and `targets/registry.rs`.
3. Emit the prepared-dataset columns described in `docs/data-contract.md`.
4. Keep per-row logic as direct slice functions.
5. Add tests for the target's edge cases and run the benchmark gate.

Use `TEMPLATE.md` for the detailed checklist.
