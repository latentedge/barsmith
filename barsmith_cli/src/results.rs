use std::fs;
use std::path::Path;

use anyhow::Result;
use barsmith_rs::protocol::{
    FormulaExportManifest, FormulaExportManifestDraft, ResearchProtocol, load_json,
    parse_manifest_date, sha256_file, sha256_text as protocol_sha256_text,
    validate_protocol_binding, write_json_pretty,
};
use barsmith_rs::storage::{ResultQuery, ResultRankBy, ResultRow, query_result_store};
use sha2::{Digest, Sha256};

use crate::cli::ResultsArgs;

pub fn run(args: ResultsArgs) -> Result<()> {
    let export_formulas = args.export_formulas.clone();
    let export_formula_manifest = args.export_formula_manifest.clone();
    let protocol = args
        .research_protocol
        .as_deref()
        .map(load_json::<ResearchProtocol>)
        .transpose()?;
    let query = ResultQuery {
        output_dir: args.output_dir,
        direction: format!("{:?}", args.direction.to_direction()).to_ascii_lowercase(),
        target: normalize_target(&args.target),
        min_sample_size: args.min_samples,
        min_win_rate: args.min_win_rate,
        max_drawdown: args.max_drawdown,
        min_calmar: args.min_calmar,
        rank_by: args.rank_by.to_rank_by(),
        limit: args.limit,
    };

    let rows = query_result_store(&query)?;
    if rows.is_empty() {
        println!("No results matched the given filters.");
        return Ok(());
    }

    println!(
        "Top {} combinations for direction={}, target={}, rank_by={}",
        rows.len(),
        query.direction,
        query.target,
        rank_by_label(query.rank_by)
    );
    println!("{}", "=".repeat(80));
    for (idx, row) in rows.iter().enumerate() {
        print_result_row(idx + 1, row);
    }
    if let Some(path) = export_formulas {
        write_ranked_formulas(&path, &rows, &query)?;
        println!("Ranked formulas written: {}", path.display());
        let manifest_path = export_formula_manifest.unwrap_or_else(|| {
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .join("formula_export_manifest.json")
        });
        let manifest = build_formula_export_manifest(&path, &rows, &query, protocol.as_ref())?;
        write_json_pretty(&manifest_path, &manifest)?;
        println!(
            "Formula export manifest written: {}",
            manifest_path.display()
        );
        println!(
            "Research note: use this export only from a discovery/pre-only run before holdout evaluation."
        );
    }

    Ok(())
}

fn rank_by_label(rank_by: ResultRankBy) -> &'static str {
    match rank_by {
        ResultRankBy::CalmarRatio => "calmar-ratio",
        ResultRankBy::TotalReturn => "total-return",
    }
}

fn normalize_target(target: &str) -> String {
    if target == "atr_stop" {
        "2x_atr_tp_atr_stop".to_string()
    } else {
        target.to_string()
    }
}

fn print_result_row(rank: usize, row: &ResultRow) {
    println!();
    println!("Rank {rank}: {}", row.combination);
    println!(
        "  Depth: {} | Bars: {}/{} | Win rate: {}%",
        row.depth,
        row.profitable_bars,
        row.total_bars,
        f2(row.win_rate)
    );
    println!(
        "  Total R: {} | Max DD: {}R | Calmar: {} | Resume offset: {}",
        f2(row.total_return),
        f2(row.max_drawdown),
        f2(row.calmar_ratio),
        row.resume_offset
    );
}

fn write_ranked_formulas(path: &Path, rows: &[ResultRow], query: &ResultQuery) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut text = String::new();
    write_formula_export_header(&mut text, query, rows.len());
    for (idx, row) in rows.iter().enumerate() {
        text.push_str(&format!("Rank {}: {}\n", idx + 1, row.combination));
    }
    fs::write(path, text)?;
    Ok(())
}

fn write_formula_export_header(text: &mut String, query: &ResultQuery, exported_rows: usize) {
    text.push_str("# Barsmith ranked formula export\n");
    text.push_str(
        "# Research note: export from a discovery/pre-only run. If the source run includes the intended post or lockbox window, later evaluation is contaminated.\n",
    );
    text.push_str(&format!(
        "# source_output_dir_sha256: {}\n",
        sha256_text(&query.output_dir.display().to_string())
    ));
    text.push_str(&format!("# direction: {}\n", query.direction));
    text.push_str(&format!("# target: {}\n", query.target));
    text.push_str(&format!("# rank_by: {}\n", rank_by_label(query.rank_by)));
    text.push_str(&format!("# min_sample_size: {}\n", query.min_sample_size));
    text.push_str(&format!("# min_win_rate: {}\n", f2(query.min_win_rate)));
    text.push_str(&format!("# max_drawdown: {}\n", f2(query.max_drawdown)));
    match query.min_calmar {
        Some(min_calmar) => text.push_str(&format!("# min_calmar: {}\n", f2(min_calmar))),
        None => text.push_str("# min_calmar: none\n"),
    }
    text.push_str(&format!("# requested_limit: {}\n", query.limit));
    text.push_str(&format!("# exported_rows: {exported_rows}\n"));
}

fn build_formula_export_manifest(
    formulas_path: &Path,
    rows: &[ResultRow],
    query: &ResultQuery,
    protocol: Option<&ResearchProtocol>,
) -> Result<FormulaExportManifest> {
    if let Some(protocol) = protocol {
        validate_protocol_binding(protocol, &query.target, Some(&query.direction))?;
    }

    let source_manifest_path = query.output_dir.join("run_manifest.json");
    let source_manifest_sha = source_manifest_path
        .is_file()
        .then(|| sha256_file(&source_manifest_path))
        .transpose()?;
    let source_manifest_json = source_manifest_path
        .is_file()
        .then(|| std::fs::read_to_string(&source_manifest_path))
        .transpose()?
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
    let source_run_identity_hash = source_manifest_json
        .as_ref()
        .and_then(|json| json.get("run_identity_hash"))
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let source_date_start = source_manifest_json
        .as_ref()
        .and_then(|json| json.pointer("/identity/include_date_start"))
        .and_then(|value| parse_manifest_date(Some(value)));
    let source_date_end = source_manifest_json
        .as_ref()
        .and_then(|json| json.pointer("/identity/include_date_end"))
        .and_then(|value| parse_manifest_date(Some(value)));

    Ok(FormulaExportManifest::from_draft(
        FormulaExportManifestDraft {
            source_output_dir_sha256: protocol_sha256_text(&query.output_dir.display().to_string()),
            source_run_manifest_sha256: source_manifest_sha,
            source_run_identity_hash,
            source_date_start,
            source_date_end,
            target: query.target.clone(),
            direction: query.direction.clone(),
            rank_by: rank_by_label(query.rank_by).to_string(),
            min_sample_size: query.min_sample_size,
            min_win_rate: query.min_win_rate,
            max_drawdown: query.max_drawdown,
            min_calmar: query.min_calmar,
            requested_limit: query.limit,
            exported_rows: rows.len(),
            formulas_sha256: sha256_file(formulas_path)?,
            protocol_sha256: protocol.map(ResearchProtocol::hash).transpose()?,
        },
    ))
}

fn sha256_text(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    hex::encode(hasher.finalize())
}

fn f2(value: f64) -> String {
    format_float(value, 2)
}

fn format_float(value: f64, decimals: usize) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "Inf".to_string()
    } else if value.is_infinite() && value.is_sign_negative() {
        "-Inf".to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else {
        format!("{value:.decimals$}")
    }
}
