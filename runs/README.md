# Run Registry

Use this folder for lightweight audit records, not full Barsmith run artifacts.

Recommended production pattern:

```text
runs/
  registry/
    comb/<target>/<direction>/<dataset-id>/<run-id>.json
    forward-test/<target>/<dataset-id>/<cutoff>/<run-id>.json
  artifacts/        # ignored by git; full local artifacts may live here
```

Full run folders contain private or heavy files such as prepared CSVs, Parquet batches,
DuckDB catalogs, logs, and plots. Keep those in `runs/artifacts/`, `tmp/`, or external
object storage. Commit only small registry JSON files when you need durable audit
traceability in Git.

Generate a standardized run folder and registry record with:

```bash
barsmith_cli comb \
  --csv ../es_30m.csv \
  --target 2x_atr_tp_atr_stop \
  --direction long \
  --runs-root runs/artifacts \
  --dataset-id es_30m_official_v2 \
  --run-slug no_stacking \
  --registry-dir runs/registry
```

Registry records intentionally store a portable run path and formula hash rather
than local artifact paths or formula text, so a public repo can keep audit
metadata without exposing private formulas. Non-finite metrics are encoded as
strings such as `Inf`, `-Inf`, or `NaN` so registry JSON does not lose meaning
through silent `null` values.

Forward-test runs use the same pattern:

```bash
barsmith_cli eval-formulas \
  --prepared runs/artifacts/comb/2x_atr_tp_atr_stop/long/es_30m_official_v2/<run-id>/barsmith_prepared.csv \
  --formulas formulas.txt \
  --target 2x_atr_tp_atr_stop \
  --cutoff 2024-12-31 \
  --runs-root runs/artifacts \
  --dataset-id es_30m_official_v2 \
  --run-slug no_stacking_forward \
  --registry-dir runs/registry \
  --plot \
  --plot-mode combined
```
