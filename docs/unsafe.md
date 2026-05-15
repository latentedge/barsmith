# Unsafe Rust Inventory

Barsmith is performance sensitive, but first-party `unsafe` must stay rare, isolated, and reviewable.

This document covers only unsafe code written in this repository. It does not inventory unsafe internals in dependencies such as Polars, DuckDB, Arrow, or Tokio.

## Current first-party unsafe usage

| Location | Compiled When | Purpose | Why Unsafe Is Used | Safe Fallback |
| --- | --- | --- | --- | --- |
| `barsmith_rs/src/bitset.rs` | `target_arch = "aarch64"` and `simd-eval` feature | NEON bitset scanning for combination evaluation. | AArch64 NEON intrinsics and pointer loads require `unsafe`. | Scalar bitset scanners in the same module. |
| `barsmith_cli/src/main.rs` | All CLI builds | Reads the parent process ID for ancestor-process logging. | `libc::getppid()` is an FFI call. | If this helper fails later in the chain, ancestor logging degrades rather than affecting evaluation. |

No first-party `unsafe` is currently present in `barsmith_indicators` or `custom_rs`.

## `barsmith_rs/src/bitset.rs`

### Scope

Unsafe code is limited to the private AArch64 NEON scanner functions and the small public dispatch wrappers that call them:

- `scan_bitsets_neon_dyn`
- `scan_bitsets_neon_dyn_gated`
- `scan_bitsets_neon_fixed_gated`
- `scan_bitsets_simd_dyn`
- `scan_bitsets_simd_dyn_gated`

These functions are compiled only for AArch64 builds with the `simd-eval` feature. `barsmith_cli` enables `simd-eval` for `barsmith_rs`, so Apple Silicon CLI builds use this path.

`scan_bitsets_neon_fixed_gated` handles common depth-1 through depth-5 gated scans through the same safety model as the dynamic scanner, while avoiding the dynamic slice loop in the hottest max-depth-5 search path.

### Safety invariants

The NEON scanner relies on these invariants:

- Every `BitsetMask` in a combination is built by Barsmith and stores masks as `Vec<u64>`.
- Scanner loops load two `u64` lanes only when `word_index + 1 < words_len`.
- `words_len` is capped by `max_len` and the shortest mask in the combination before any two-lane load.
- Tail words are masked before counting/hit-callback traversal for non-multiple-of-64 row counts.
- Eligible and finite gates are optional masks; out-of-bounds eligible gates are treated as permissive, while out-of-bounds finite gates are treated as no hit.
- NEON intrinsics are behind `#[cfg(all(target_arch = "aarch64", feature = "simd-eval"))]`.

### Review requirements

Changes to this module must:

- Keep unsafe blocks inside `bitset.rs`; do not spread SIMD pointer logic into evaluator code.
- Preserve scalar fallback behavior.
- Include or update scalar/SIMD parity coverage where practical.
- Run the normal test gate plus release benchmark smoke.
- Record before/after timing for hot-path changes that alter scan loops or mask layout.

The 2026-05-14 fixed-depth NEON dispatch was accepted because scalar parity tests still cover depths 1 through 5 and the dedicated depth-5 benchmark improved median and p95 timing against the same-machine baseline.

## `barsmith_cli/src/main.rs`

### Scope

The CLI uses:

```rust
unsafe { libc::getppid() }
```

This is used only to begin collecting ancestor process metadata for logging and reproducibility hints.

### Safety invariants

- `getppid()` takes no pointer arguments and does not require repository-owned memory invariants.
- The returned process id is used as data for best-effort process inspection.
- Failure to inspect later ancestors must not affect combination evaluation or output correctness.

### Review requirements

Changes to this area must:

- Keep FFI isolated to a tiny helper.
- Avoid using process-inspection results to change evaluation semantics.
- Prefer a safe standard-library or well-maintained crate wrapper if one becomes available and does not add meaningful dependency risk.

## Project policy

- New first-party unsafe code is not allowed by default.
- If unsafe is necessary, it must have a narrow module boundary, a written rationale, local safety comments, and tests that exercise the safe behavior around it.
- Unsafe code must not own user input parsing, run identity, resume mutation, Parquet/DuckDB writes, or financial metric semantics unless a separate design explains why there is no safe alternative.
- Performance-motivated unsafe must have benchmark evidence.
- Any new unsafe site must update this document and `docs/review-checklist.md`.
