# Barsmith docs

Barsmith is currently marked **unstable**. Expect breaking changes.

## Contents

- `docs/quickstart.md` — install + first run (uses `tests/data/ohlcv_tiny.csv`)
- `docs/data-contract.md` — raw CSV + prepared dataset contract
- `docs/cli.md` — CLI flags and practical guidance for `comb`, `eval-formulas`, `results`, and `select`
- `docs/selection.md` — strict choose-the-best workflow after discovery runs
- `docs/research-protocol.md` — pre/post selection protocol, holdout confirmation, and overfit controls
- `docs/runs.md` — running long experiments (resume, standard run folders, batching)
- `docs/outputs.md` — what gets written + how to query results (DuckDB/Parquet)
- `docs/architecture.md` — crate layout + the `comb` pipeline at a high level
- `docs/engines.md` — builtin engine vs custom engines / prepared datasets
- `docs/performance.md` — performance knobs and build tips
- `docs/unsafe.md` — first-party unsafe Rust inventory and review policy
- `docs/testing.md` — local validation matrix and CI expectations
- `docs/migration.md` — breaking changes and resume/output migration notes
- `docs/stability.md` — project maturity, compatibility, and support boundary
- `docs/review-checklist.md` — correctness, resume, performance, and data hygiene review checklist
- `docs/troubleshooting.md` — common errors and fixes
- `docs/development.md` — contributing/dev workflows (tests, formatting, audit)
