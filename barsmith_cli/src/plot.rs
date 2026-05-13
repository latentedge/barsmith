use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use barsmith_rs::formula_eval::EquityCurveRow;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use plotters::prelude::*;

use crate::cli::{EvalFormulasArgs, PlotMetricValue, PlotModeValue, PlotXValue};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CurveKey {
    rank_by: String,
    window: String,
    rank: usize,
    formula: String,
}

#[derive(Debug, Clone)]
struct CurvePoint {
    x: f64,
    y: f64,
}

pub fn render_plots(rows: &[EquityCurveRow], args: &EvalFormulasArgs) -> Result<Vec<PathBuf>> {
    if rows.is_empty() {
        return Err(anyhow!(
            "--plot requested but no equity curve rows are available"
        ));
    }

    let curves = curves_from_rows(rows, args.plot_x, args.plot_metric)?;
    let mut written = Vec::new();
    match args.plot_mode {
        PlotModeValue::Combined => {
            let path = args.plot_out.clone().unwrap_or_else(|| {
                args.prepared_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(format!("equity_curves_top{}.png", args.equity_curves_top_k))
            });
            render_combined(&curves, &path, args.plot_x, args.plot_metric)?;
            println!("Equity curve plot written: {}", path.display());
            written.push(path);
        }
        PlotModeValue::Individual => {
            let dir = args.plot_dir.clone().unwrap_or_else(|| {
                args.prepared_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(format!(
                        "equity_curves_top{}_plots",
                        args.equity_curves_top_k
                    ))
            });
            std::fs::create_dir_all(&dir)?;
            for (key, points) in &curves {
                let path = dir.join(format!(
                    "{}_{}_rank_{:02}.png",
                    sanitize(&key.rank_by),
                    sanitize(&key.window),
                    key.rank
                ));
                render_single(key, points, &path, args.plot_x, args.plot_metric)?;
                written.push(path);
            }
            println!("Equity curve plots written: {}", dir.display());
        }
    }

    Ok(written)
}

fn curves_from_rows(
    rows: &[EquityCurveRow],
    x_axis: PlotXValue,
    metric: PlotMetricValue,
) -> Result<BTreeMap<CurveKey, Vec<CurvePoint>>> {
    let mut curves: BTreeMap<CurveKey, Vec<CurvePoint>> = BTreeMap::new();
    for row in rows {
        let y = match metric {
            PlotMetricValue::Dollar => row.equity_dollar.ok_or_else(|| {
                anyhow!("--plot-metric dollar requires equity_dollar values; provide capital/risk settings")
            })?,
            PlotMetricValue::R => row.equity_r,
        };
        let x = match x_axis {
            PlotXValue::TradeIndex => row.trade_index as f64,
            PlotXValue::Timestamp => parse_timestamp_seconds(&row.timestamp).ok_or_else(|| {
                anyhow!(
                    "unable to parse timestamp '{}' for plotting; use --plot-x trade-index",
                    row.timestamp
                )
            })?,
        };
        curves
            .entry(CurveKey {
                rank_by: row.rank_by.clone(),
                window: row.window.clone(),
                rank: row.rank,
                formula: row.formula.clone(),
            })
            .or_default()
            .push(CurvePoint { x, y });
    }
    Ok(curves)
}

fn render_combined(
    curves: &BTreeMap<CurveKey, Vec<CurvePoint>>,
    path: &Path,
    x_axis: PlotXValue,
    metric: PlotMetricValue,
) -> Result<()> {
    let (x_min, x_max, y_min, y_max) = bounds(curves.values().flatten())?;
    ensure_parent(path)?;
    let root = BitMapBackend::new(path, (1400, 800)).into_drawing_area();
    root.fill(&WHITE)?;
    let mut chart = ChartBuilder::on(&root)
        .caption("Barsmith Equity Curves", ("sans-serif", 28))
        .margin(18)
        .x_label_area_size(42)
        .y_label_area_size(64)
        .build_cartesian_2d(x_min..x_max, y_min..y_max)?;

    chart
        .configure_mesh()
        .x_desc(x_label(x_axis))
        .y_desc(y_label(metric))
        .light_line_style(WHITE.mix(0.0))
        .draw()?;

    for (idx, (key, points)) in curves.iter().enumerate() {
        let color = Palette99::pick(idx).mix(0.9);
        chart
            .draw_series(LineSeries::new(points.iter().map(|p| (p.x, p.y)), color))?
            .label(format!("{} {} #{}", key.rank_by, key.window, key.rank))
            .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color));
    }

    chart
        .configure_series_labels()
        .border_style(BLACK)
        .background_style(WHITE.mix(0.85))
        .draw()?;
    root.present()?;
    Ok(())
}

fn render_single(
    key: &CurveKey,
    points: &[CurvePoint],
    path: &Path,
    x_axis: PlotXValue,
    metric: PlotMetricValue,
) -> Result<()> {
    let (x_min, x_max, y_min, y_max) = bounds(points.iter())?;
    ensure_parent(path)?;
    let root = BitMapBackend::new(path, (1400, 800)).into_drawing_area();
    root.fill(&WHITE)?;
    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!(
                "{} {} Rank {}\n{}",
                key.rank_by, key.window, key.rank, key.formula
            ),
            ("sans-serif", 22),
        )
        .margin(18)
        .x_label_area_size(42)
        .y_label_area_size(64)
        .build_cartesian_2d(x_min..x_max, y_min..y_max)?;

    chart
        .configure_mesh()
        .x_desc(x_label(x_axis))
        .y_desc(y_label(metric))
        .light_line_style(WHITE.mix(0.0))
        .draw()?;
    chart.draw_series(LineSeries::new(points.iter().map(|p| (p.x, p.y)), &BLUE))?;
    root.present()?;
    Ok(())
}

fn bounds<'a>(points: impl Iterator<Item = &'a CurvePoint>) -> Result<(f64, f64, f64, f64)> {
    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    let mut y_min = f64::INFINITY;
    let mut y_max = f64::NEG_INFINITY;
    let mut seen = false;
    for point in points {
        if !point.x.is_finite() || !point.y.is_finite() {
            continue;
        }
        seen = true;
        x_min = x_min.min(point.x);
        x_max = x_max.max(point.x);
        y_min = y_min.min(point.y);
        y_max = y_max.max(point.y);
    }
    if !seen {
        return Err(anyhow!("plot has no finite points"));
    }
    if x_min == x_max {
        x_min -= 1.0;
        x_max += 1.0;
    }
    if y_min == y_max {
        y_min -= 1.0;
        y_max += 1.0;
    }
    let y_pad = ((y_max - y_min) * 0.05).max(1.0);
    Ok((x_min, x_max, y_min - y_pad, y_max + y_pad))
}

fn x_label(value: PlotXValue) -> &'static str {
    match value {
        PlotXValue::Timestamp => "timestamp_unix_seconds",
        PlotXValue::TradeIndex => "trade_index",
    }
}

fn y_label(value: PlotMetricValue) -> &'static str {
    match value {
        PlotMetricValue::Dollar => "equity_dollar",
        PlotMetricValue::R => "equity_r",
    }
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn parse_timestamp_seconds(raw: &str) -> Option<f64> {
    let value = raw.trim().trim_matches('"');
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.timestamp() as f64)
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .map(|dt| dt.and_utc().timestamp() as f64)
                .ok()
        })
        .or_else(|| {
            NaiveDate::parse_from_str(value.get(..10)?, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|dt| dt.and_utc().timestamp() as f64)
        })
}
