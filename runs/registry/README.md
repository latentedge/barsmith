# Run Registry Records

This directory stores lightweight Barsmith audit records that are safe to keep
in Git.

Registry records should contain portable run paths, hashes, schema versions,
summary metrics, and workflow status. They must not contain raw formulas,
private raw data, absolute local paths, logs, Parquet files, DuckDB files, or
full run artifacts.

Full artifacts belong under `runs/artifacts/`, which stays ignored by Git.

Validate tracked records with:

```bash
scripts/check_registry_schema.sh
```
