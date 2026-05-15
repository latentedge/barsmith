use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use duckdb::{Connection, params};
use polars::prelude::*;
use polars_io::prelude::{ParquetCompression, ParquetWriter};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::config::{Config, Direction};
use crate::run_identity::{
    RUN_MANIFEST_FILE, config_run_identity_hash, validate_or_write_run_manifest,
};
use crate::stats::StatSummary;

#[derive(Debug, Clone)]
pub struct ResultRow {
    pub direction: String,
    pub target: String,
    pub combination: String,
    pub resume_offset: u64,
    pub depth: u32,
    /// Raw combo-mask support for recall reporting.
    ///
    /// Full-stat recomputation populates this. Older rows may fall back to
    /// `total_bars`.
    pub mask_hits: u64,
    pub total_bars: u64,
    pub profitable_bars: u64,
    pub win_rate: f64,
    pub label_hit_rate: f64,
    pub label_hits: u64,
    pub label_misses: u64,
    pub expectancy: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub profit_factor: f64,
    pub calmar_ratio: f64,
    pub win_loss_ratio: f64,
    pub ulcer_index: f64,
    pub pain_ratio: f64,
    pub max_consecutive_wins: u64,
    pub max_consecutive_losses: u64,
    pub avg_winning_rr: f64,
    pub avg_win_streak: f64,
    pub avg_loss_streak: f64,
    pub median_rr: f64,
    pub avg_losing_rr: f64,
    pub p05_rr: f64,
    pub p95_rr: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub final_capital: f64,
    pub total_return_pct: f64,
    pub cagr_pct: f64,
    pub max_drawdown_pct_equity: f64,
    pub calmar_equity: f64,
    pub sharpe_equity: f64,
    pub sortino_equity: f64,
}

pub struct CumulativeStore {
    results_dir: PathBuf,
    duckdb_conn: Connection,
    run_identity_hash: String,
    csv_hash: String,
    direction: String,
    target: String,
    batch_counter: usize,
    // Run knobs copied into metadata for later inspection.
    min_sample_size: usize,
    strict_min_pruning: bool,
}

#[derive(Debug, Clone)]
pub struct ResultQuery {
    pub output_dir: PathBuf,
    pub direction: String,
    pub target: String,
    pub min_sample_size: usize,
    pub min_win_rate: f64,
    pub max_drawdown: f64,
    pub min_calmar: Option<f64>,
    pub rank_by: ResultRankBy,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultRankBy {
    CalmarRatio,
    TotalReturn,
}

#[derive(Debug, Clone)]
pub struct ResultStoreSummary {
    pub processed_combinations: Option<u64>,
    pub stored_combinations: Option<u64>,
}

impl ResultRankBy {
    fn order_expression(self) -> &'static str {
        match self {
            Self::CalmarRatio => "calmar_ratio",
            Self::TotalReturn => "total_return",
        }
    }
}

pub fn summarize_result_store(output_dir: &Path) -> Result<ResultStoreSummary> {
    let results_dir = output_dir.join("results_parquet");
    let duckdb_path = output_dir.join("cumulative.duckdb");
    if !duckdb_path.exists() {
        return Ok(ResultStoreSummary {
            processed_combinations: None,
            stored_combinations: None,
        });
    }

    let conn = Connection::open(&duckdb_path)
        .with_context(|| format!("Unable to open {}", duckdb_path.display()))?;
    conn.execute("SET max_expression_depth TO 100000", [])?;

    let processed_combinations = conn
        .query_row(
            "SELECT COALESCE(SUM(processed), 0) FROM metadata",
            [],
            |row| row.get::<_, i64>(0),
        )
        .ok()
        .map(|value| value.max(0) as u64);

    let stored_combinations = if has_parquet_files(&results_dir)? {
        refresh_results_view(&conn, &results_dir)?;
        conn.query_row("SELECT COUNT(*) FROM results", [], |row| {
            row.get::<_, i64>(0)
        })
        .ok()
        .map(|value| value.max(0) as u64)
    } else {
        Some(0)
    };

    Ok(ResultStoreSummary {
        processed_combinations,
        stored_combinations,
    })
}

pub fn query_result_store(query: &ResultQuery) -> Result<Vec<ResultRow>> {
    let results_dir = query.output_dir.join("results_parquet");
    if query.limit == 0 || !has_parquet_files(&results_dir)? {
        return Ok(Vec::new());
    }

    let duckdb_path = query.output_dir.join("cumulative.duckdb");
    if !duckdb_path.exists() {
        return Err(anyhow!(
            "cumulative.duckdb not found at {}",
            duckdb_path.display()
        ));
    }

    let conn = Connection::open(&duckdb_path)
        .with_context(|| format!("Unable to open {}", duckdb_path.display()))?;
    conn.execute("SET max_expression_depth TO 100000", [])?;
    refresh_results_view(&conn, &results_dir)?;
    let columns = describe_result_columns(&conn)?;

    let select_columns = [
        "direction".to_string(),
        "target".to_string(),
        "combination".to_string(),
        "resume_offset".to_string(),
        "depth".to_string(),
        if columns.contains("mask_hits") {
            "mask_hits".to_string()
        } else {
            "total_bars AS mask_hits".to_string()
        },
        "total_bars".to_string(),
        "profitable_bars".to_string(),
        "win_rate".to_string(),
        select_or_default(&columns, "label_hit_rate", "0.0"),
        select_or_default(&columns, "label_hits", "0"),
        select_or_default(&columns, "label_misses", "0"),
        select_or_default(&columns, "expectancy", "0.0"),
        select_or_default(&columns, "total_return", "0.0"),
        "max_drawdown".to_string(),
        select_or_default(&columns, "profit_factor", "0.0"),
        "calmar_ratio".to_string(),
    ]
    .join(",\n            ");

    let sql_base = format!(
        "
        SELECT
            {select_columns}
        FROM results
        WHERE total_bars >= ?
          AND win_rate >= ?
          AND max_drawdown <= ?
          AND direction = ?
          AND target = ?"
    );

    let order_expression = query.rank_by.order_expression();
    let (sql, min_calmar) = match query.min_calmar {
        Some(min_calmar) => (
            format!("{sql_base} AND calmar_ratio >= ? ORDER BY {order_expression} DESC LIMIT ?"),
            Some(min_calmar),
        ),
        None => (
            format!("{sql_base} ORDER BY {order_expression} DESC LIMIT ?"),
            None,
        ),
    };

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = if let Some(min_calmar) = min_calmar {
        stmt.query(params![
            query.min_sample_size as i64,
            query.min_win_rate,
            query.max_drawdown,
            query.direction,
            query.target,
            min_calmar,
            query.limit as i64,
        ])?
    } else {
        stmt.query(params![
            query.min_sample_size as i64,
            query.min_win_rate,
            query.max_drawdown,
            query.direction,
            query.target,
            query.limit as i64,
        ])?
    };

    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(ResultRow {
            direction: row.get(0)?,
            target: row.get(1)?,
            combination: row.get(2)?,
            resume_offset: row.get::<_, i64>(3)? as u64,
            depth: row.get::<_, i64>(4)? as u32,
            mask_hits: row.get::<_, i64>(5)? as u64,
            total_bars: row.get::<_, i64>(6)? as u64,
            profitable_bars: row.get::<_, i64>(7)? as u64,
            win_rate: row.get(8)?,
            label_hit_rate: row.get(9).unwrap_or(0.0),
            label_hits: row.get::<_, i64>(10).unwrap_or(0) as u64,
            label_misses: row.get::<_, i64>(11).unwrap_or(0) as u64,
            expectancy: row.get(12)?,
            total_return: row.get(13)?,
            max_drawdown: row.get(14)?,
            profit_factor: row.get(15)?,
            calmar_ratio: row.get(16)?,
            win_loss_ratio: 0.0,
            ulcer_index: 0.0,
            pain_ratio: 0.0,
            max_consecutive_wins: 0,
            max_consecutive_losses: 0,
            avg_winning_rr: 0.0,
            avg_win_streak: 0.0,
            avg_loss_streak: 0.0,
            median_rr: 0.0,
            avg_losing_rr: 0.0,
            p05_rr: 0.0,
            p95_rr: 0.0,
            largest_win: 0.0,
            largest_loss: 0.0,
            final_capital: 0.0,
            total_return_pct: 0.0,
            cagr_pct: 0.0,
            max_drawdown_pct_equity: 0.0,
            calmar_equity: 0.0,
            sharpe_equity: 0.0,
            sortino_equity: 0.0,
        });
    }

    Ok(out)
}

fn describe_result_columns(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("DESCRIBE results")?;
    let mut rows = stmt.query([])?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        columns.insert(name);
    }
    Ok(columns)
}

fn select_or_default(columns: &HashSet<String>, column: &str, default: &str) -> String {
    if columns.contains(column) {
        column.to_string()
    } else {
        format!("{default} AS {column}")
    }
}

fn refresh_results_view(conn: &Connection, results_dir: &Path) -> Result<()> {
    if !has_parquet_files(results_dir)? {
        return Ok(());
    }

    let mut candidates = Vec::new();
    for entry in fs::read_dir(results_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("part-") && name.ends_with(".parquet") {
            candidates.push(entry.path());
        }
    }
    candidates.sort();

    let mut good_paths = Vec::new();
    for path in candidates {
        let display_str = path.display().to_string();
        let escaped = display_str.replace('\'', "''");
        let probe_sql = format!("SELECT COUNT(*) FROM read_parquet('{escaped}')");
        match conn.prepare(&probe_sql)?.query([]) {
            Ok(_) => good_paths.push(escaped),
            Err(error) => {
                warn!(
                    file = %display_str,
                    ?error,
                    "Skipping corrupt or unreadable Parquet batch"
                );
            }
        }
    }

    if good_paths.is_empty() {
        conn.execute("DROP VIEW IF EXISTS results", [])?;
        return Ok(());
    }

    let parquet_list = good_paths
        .iter()
        .map(|path| format!("'{path}'"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(
        &format!(
            "CREATE OR REPLACE VIEW results AS \
             SELECT * FROM read_parquet([{parquet_list}], union_by_name = true)"
        ),
        [],
    )?;
    Ok(())
}

impl CumulativeStore {
    pub fn new(config: &Config) -> Result<(Self, u64)> {
        fs::create_dir_all(&config.output_dir)?;

        let results_dir = config.output_dir.join("results_parquet");
        let duckdb_path = config.output_dir.join("cumulative.duckdb");

        // A forced recompute starts this run folder from scratch.
        if config.force_recompute {
            if duckdb_path.exists() {
                let _ = fs::remove_file(&duckdb_path);
            }
            let manifest_path = config.output_dir.join(RUN_MANIFEST_FILE);
            if manifest_path.exists() {
                let _ = fs::remove_file(&manifest_path);
            }
            if results_dir.exists() {
                for entry in fs::read_dir(&results_dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        let _ = fs::remove_file(path);
                    }
                }
            }
        }

        fs::create_dir_all(&results_dir)?;

        let csv_path = config.source_csv.as_ref().unwrap_or(&config.input_csv);
        let csv_hash = csv_fingerprint(csv_path)?;
        let run_identity_hash = config_run_identity_hash(config, &csv_hash)?;
        let has_existing_state = has_existing_run_state(&results_dir, &duckdb_path)?;
        validate_or_write_run_manifest(
            &config.output_dir,
            config.force_recompute,
            has_existing_state,
            config,
            &run_identity_hash,
            &csv_hash,
        )?;

        let conn = Connection::open(&duckdb_path)
            .with_context(|| format!("Unable to open {}", duckdb_path.display()))?;

        info!(
            db_path = %duckdb_path.display(),
            "CumulativeStore::new opened DuckDB connection"
        );

        // Loosen DuckDB's default expression depth guard to handle the large
        // UNION view over many Parquet parts and complex predicates. This does
        // not change query semantics, only how deep an expression tree DuckDB
        // will accept before bailing out during parsing/optimization.
        conn.execute("SET max_expression_depth TO 100000", [])?;
        // Allow the DuckDB catalog backing the cumulative store to use up to
        // 30 GiB of memory for query execution. This helps avoid per-query
        // OOMs when scanning large Parquet-backed views, at the cost of a
        // higher process-level memory ceiling.
        conn.execute("SET memory_limit TO '30GB'", [])?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                config_hash TEXT NOT NULL,
                csv_hash TEXT NOT NULL,
                processed BIGINT NOT NULL,
                last_updated TIMESTAMP NOT NULL,
                PRIMARY KEY (config_hash, csv_hash)
            )",
            [],
        )?;

        // Older metadata tables may not have these diagnostic columns yet.
        conn.execute(
            "ALTER TABLE metadata ADD COLUMN IF NOT EXISTS min_sample_size INTEGER",
            [],
        )?;
        conn.execute(
            "ALTER TABLE metadata ADD COLUMN IF NOT EXISTS strict_min_pruning BOOLEAN",
            [],
        )?;

        // One run folder belongs to one input CSV unless recompute is explicit.
        enforce_csv_consistency(&conn, &csv_hash, config.force_recompute)?;

        let resume_offset = if config.force_recompute {
            0
        } else {
            query_resume_offset(&conn, &run_identity_hash, &csv_hash)?
        };

        let batch_counter = if config.force_recompute {
            0
        } else {
            existing_batch_count(&results_dir)?
        };

        let store = Self {
            results_dir: results_dir.clone(),
            duckdb_conn: conn,
            run_identity_hash,
            csv_hash,
            direction: format_direction(config.direction),
            target: config.target.clone(),
            batch_counter,
            min_sample_size: config.min_sample_size,
            strict_min_pruning: config.strict_min_pruning,
        };
        info!("CumulativeStore::new refreshing results view...");
        store.refresh_view()?;
        info!("CumulativeStore::new refresh_view completed");

        // Resume metadata without Parquet parts usually means someone deleted
        // batch files by hand. Warn before continuing from that offset.
        if resume_offset > 0 && !has_parquet_files(&results_dir)? {
            warn!(
                resume_offset,
                "Resume metadata reports processed combinations but no Parquet result parts were found on disk; \
                 prior batches may have been removed"
            );
        }

        Ok((store, resume_offset))
    }

    fn current_min_sample_size(&self) -> usize {
        self.min_sample_size
    }

    fn current_strict_min_pruning(&self) -> bool {
        self.strict_min_pruning
    }

    /// Expose the CSV fingerprint for this store so callers can bind
    /// membership lookups to the correct dataset.
    pub fn csv_hash(&self) -> &str {
        &self.csv_hash
    }

    pub fn ingest(&mut self, combinations: &[String], stats: &[StatSummary]) -> Result<()> {
        // Older call sites use this path. New pipeline code should prefer
        // `ingest_with_enumerated`, where skipped combinations are explicit.
        let _ = self.ingest_with_enumerated(combinations, stats, combinations.len(), 0)?;
        Ok(())
    }

    /// Ingest a batch of *evaluated* combinations, while also tracking how many
    /// combinations were *enumerated* in the global stream. This allows resume
    /// offsets to remain index-based even when some combinations are skipped
    /// due to prior evaluation (combination-key reuse).
    pub fn ingest_with_enumerated(
        &mut self,
        combinations: &[String],
        stats: &[StatSummary],
        enumerated_count: usize,
        batch_start_offset: u64,
    ) -> Result<Option<PathBuf>> {
        let mut build_ms: u64 = 0;
        let mut parquet_ms: u64 = 0;
        let mut meta_ms: u64 = 0;
        let mut parquet_path: Option<PathBuf> = None;

        if !combinations.is_empty() {
            let build_start = Instant::now();
            let mut df = self.build_batch_frame(combinations, stats, batch_start_offset)?;
            build_ms = (build_start.elapsed().as_secs_f32() * 1000.0).round() as u64;

            let filename = format!("part-{:016}.parquet", self.batch_counter);
            let file_path = self.results_dir.join(filename);
            let temp_path = file_path.with_extension("parquet.tmp");
            self.batch_counter += 1;

            let parquet_start = Instant::now();
            {
                let mut file = File::create(&temp_path)
                    .with_context(|| format!("Unable to create {}", temp_path.display()))?;
                ParquetWriter::new(&mut file)
                    .with_compression(ParquetCompression::Zstd(None))
                    .finish(&mut df)
                    .context("Failed to write Parquet batch")?;
                file.sync_all()
                    .with_context(|| format!("Unable to sync {}", temp_path.display()))?;
            }
            fs::rename(&temp_path, &file_path).with_context(|| {
                format!(
                    "Unable to atomically replace Parquet batch {}",
                    file_path.display()
                )
            })?;
            parquet_ms = (parquet_start.elapsed().as_secs_f32() * 1000.0).round() as u64;
            parquet_path = Some(file_path.clone());
        }

        if enumerated_count > 0 {
            let meta_start = Instant::now();
            self.update_metadata(enumerated_count as i64)?;
            meta_ms = (meta_start.elapsed().as_secs_f32() * 1000.0).round() as u64;
        }

        let total_ms = build_ms + parquet_ms + meta_ms;
        info!(
            ingest_build_ms = %build_ms,
            ingest_parquet_ms = %parquet_ms,
            ingest_meta_ms = %meta_ms,
            ingest_total_ms = %total_ms,
            stored = %combinations.len(),
            enumerated = %enumerated_count,
            "Ingest batch timing"
        );

        Ok(parquet_path)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.duckdb_conn.execute("CHECKPOINT", [])?;
        Ok(())
    }

    pub fn top_results(
        &self,
        limit: usize,
        min_sample_size: usize,
        max_drawdown: f64,
        min_calmar: Option<f64>,
    ) -> Result<Vec<ResultRow>> {
        if limit == 0 || !has_parquet_files(&self.results_dir)? {
            return Ok(Vec::new());
        }
        let sql_base = "\
            SELECT
                direction,
                target,
                combination,
                depth,
                total_bars,
                profitable_bars,
                win_rate,
                max_drawdown,
                calmar_ratio,
                resume_offset
            FROM results
            WHERE total_bars >= ?
              AND max_drawdown <= ?";

        let (sql, min_calmar) = match min_calmar {
            Some(min_calmar) => (
                format!("{sql_base} AND calmar_ratio >= ? ORDER BY calmar_ratio DESC LIMIT ?"),
                Some(min_calmar),
            ),
            None => (
                format!("{sql_base} ORDER BY calmar_ratio DESC LIMIT ?"),
                None,
            ),
        };
        let mut stmt = self.duckdb_conn.prepare(&sql)?;
        let mut rows = if let Some(min_calmar) = min_calmar {
            stmt.query(params![
                min_sample_size as i64,
                max_drawdown,
                min_calmar,
                limit as i64
            ])?
        } else {
            stmt.query(params![min_sample_size as i64, max_drawdown, limit as i64])?
        };

        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let total_bars = row.get::<_, i64>(4)? as u64;
            out.push(ResultRow {
                direction: row.get(0)?,
                target: row.get(1)?,
                combination: row.get(2)?,
                depth: row.get::<_, i32>(3)? as u32,
                mask_hits: total_bars,
                total_bars,
                profitable_bars: row.get::<_, i64>(5)? as u64,
                win_rate: row.get(6)?,
                max_drawdown: row.get(7)?,
                calmar_ratio: row.get(8)?,
                resume_offset: row.get::<_, i64>(9)? as u64,
                // The remaining fields are populated later via full-stat
                // recomputation; initialize them to neutral defaults here.
                label_hit_rate: 0.0,
                label_hits: 0,
                label_misses: 0,
                expectancy: 0.0,
                total_return: 0.0,
                profit_factor: 0.0,
                win_loss_ratio: 0.0,
                ulcer_index: 0.0,
                pain_ratio: 0.0,
                max_consecutive_wins: 0,
                max_consecutive_losses: 0,
                avg_winning_rr: 0.0,
                avg_win_streak: 0.0,
                avg_loss_streak: 0.0,
                median_rr: 0.0,
                avg_losing_rr: 0.0,
                p05_rr: 0.0,
                p95_rr: 0.0,
                largest_win: 0.0,
                largest_loss: 0.0,
                final_capital: 0.0,
                total_return_pct: 0.0,
                cagr_pct: 0.0,
                max_drawdown_pct_equity: 0.0,
                calmar_equity: 0.0,
                sharpe_equity: 0.0,
                sortino_equity: 0.0,
            });
        }
        Ok(out)
    }

    /// Load the set of already-evaluated combinations for this CSV/target/direction.
    /// Used by tests and verification; the pipeline handles reuse in memory.
    pub fn existing_combinations(&self) -> Result<HashSet<String>> {
        let mut existing = HashSet::new();
        if !has_parquet_files(&self.results_dir)? {
            return Ok(existing);
        }

        let sql = "\
            SELECT DISTINCT combination \
            FROM results \
            WHERE csv_hash = ? \
              AND direction = ? \
              AND target = ?";

        let mut stmt = self.duckdb_conn.prepare(sql)?;
        let mut rows = stmt.query(params![&self.csv_hash, &self.direction, &self.target])?;
        while let Some(row) = rows.next()? {
            let combination: String = row.get(0)?;
            existing.insert(combination);
        }

        Ok(existing)
    }

    pub fn refresh_view(&self) -> Result<()> {
        refresh_results_view(&self.duckdb_conn, &self.results_dir)
    }

    fn update_metadata(&mut self, delta: i64) -> Result<()> {
        self.duckdb_conn.execute(
            "INSERT INTO metadata (config_hash, csv_hash, processed, last_updated, min_sample_size, strict_min_pruning)
             VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
             ON CONFLICT(config_hash, csv_hash) DO UPDATE
                SET processed = metadata.processed + excluded.processed,
                 last_updated = excluded.last_updated,
                 min_sample_size = excluded.min_sample_size,
                 strict_min_pruning = excluded.strict_min_pruning",
            params![
                &self.run_identity_hash,
                &self.csv_hash,
                delta,
                self.current_min_sample_size() as i64,
                self.current_strict_min_pruning()
            ],
        )?;
        Ok(())
    }

    fn build_batch_frame(
        &self,
        combinations: &[String],
        stats: &[StatSummary],
        batch_start_offset: u64,
    ) -> Result<DataFrame> {
        let count = combinations.len();
        let combination_text = combinations.to_vec();

        let csv_col = vec![self.csv_hash.clone(); count];
        let dir_col = vec![self.direction.clone(); count];
        let target_col = vec![self.target.clone(); count];
        let processed_at = vec![Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(); count];
        let resume_offsets: Vec<u64> = (0..count)
            .map(|idx| batch_start_offset + idx as u64)
            .collect();

        let mut columns = vec![
            Column::new("csv_hash".into(), csv_col),
            Column::new("direction".into(), dir_col),
            Column::new("target".into(), target_col),
            Column::new("combination".into(), combination_text),
            Column::new("processed_at".into(), processed_at),
            Column::new("resume_offset".into(), resume_offsets),
        ];

        macro_rules! push_series {
            ($name:expr, $iter:expr) => {
                columns.push(Column::new($name.into(), $iter));
            };
        }

        push_series!(
            "depth",
            stats
                .iter()
                .map(|stat| stat.depth as u32)
                .collect::<Vec<_>>()
        );
        push_series!(
            "total_bars",
            stats
                .iter()
                .map(|stat| stat.total_bars as u64)
                .collect::<Vec<_>>()
        );
        push_series!(
            "profitable_bars",
            stats
                .iter()
                .map(|stat| stat.profitable_bars as u64)
                .collect::<Vec<_>>()
        );
        push_series!(
            "win_rate",
            stats.iter().map(|stat| stat.win_rate).collect::<Vec<_>>()
        );
        push_series!(
            "max_drawdown",
            stats
                .iter()
                .map(|stat| stat.max_drawdown)
                .collect::<Vec<_>>()
        );
        push_series!(
            "calmar_ratio",
            stats
                .iter()
                .map(|stat| stat.calmar_ratio)
                .collect::<Vec<_>>()
        );
        push_series!(
            "total_return",
            stats
                .iter()
                .map(|stat| stat.total_return)
                .collect::<Vec<_>>()
        );
        push_series!(
            "calmar_r",
            stats
                .iter()
                .map(|stat| {
                    if stat.max_drawdown > 0.0 {
                        stat.total_return / stat.max_drawdown
                    } else if stat.total_return > 0.0 {
                        f64::INFINITY
                    } else {
                        0.0
                    }
                })
                .collect::<Vec<_>>()
        );

        DataFrame::new_infer_height(columns).context("Failed to build batch DataFrame")
    }
}

fn existing_batch_count(results_dir: &Path) -> Result<usize> {
    let mut count = 0usize;
    if results_dir.exists() {
        for entry in fs::read_dir(results_dir)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with("part-") {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn has_parquet_files(results_dir: &Path) -> Result<bool> {
    if !results_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(results_dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with("part-") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_existing_run_state(results_dir: &Path, duckdb_path: &Path) -> Result<bool> {
    Ok(duckdb_path.exists() || has_parquet_files(results_dir)?)
}

fn query_resume_offset(conn: &Connection, run_identity_hash: &str, csv_hash: &str) -> Result<u64> {
    let sql = "\
        SELECT processed \
        FROM metadata \
        WHERE config_hash = ?
          AND csv_hash = ?";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(params![run_identity_hash, csv_hash])?;
    if let Some(row) = rows.next()? {
        let processed: i64 = row.get(0)?;
        Ok(processed.max(0) as u64)
    } else {
        Ok(0)
    }
}

fn enforce_csv_consistency(conn: &Connection, csv_hash: &str, force: bool) -> Result<()> {
    // If there is no metadata yet, there is nothing to enforce.
    let mut stmt = conn.prepare("SELECT DISTINCT csv_hash FROM metadata")?;
    let mut rows = stmt.query([])?;
    let mut seen: Vec<String> = Vec::new();
    while let Some(row) = rows.next()? {
        let existing: String = row.get(0)?;
        if !seen.contains(&existing) {
            seen.push(existing);
        }
    }

    // Old rows used bare hex fingerprints. Do not let those block reuse under
    // the current scheme-tagged fingerprint format.
    let relevant: Vec<&String> = if csv_hash.contains(':') {
        seen.iter().filter(|value| value.contains(':')).collect()
    } else {
        seen.iter().collect()
    };

    if relevant.is_empty() {
        return Ok(());
    }

    // If all existing hashes match the current one, we are fine.
    if relevant
        .iter()
        .all(|existing| existing.as_str() == csv_hash)
    {
        return Ok(());
    }

    if force {
        // Caller opted in to reusing this run folder despite a different CSV.
        return Ok(());
    }

    Err(anyhow!(
        "Existing cumulative metadata in this run folder was created from a different CSV. \
         Run with --force-recompute or choose a fresh --run-id for this dataset."
    ))
}

fn format_direction(direction: Direction) -> String {
    match direction {
        Direction::Long => "long",
        Direction::Short => "short",
        Direction::Both => "both",
    }
    .to_string()
}

fn csv_fingerprint(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("Unable to open {} for fingerprinting", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    // Prefix the hash so future readers can tell which fingerprinting scheme
    // produced it.
    let hex = hex::encode(hasher.finalize());
    Ok(format!("raw:{}", hex))
}
