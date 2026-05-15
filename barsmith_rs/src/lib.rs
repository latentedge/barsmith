//! Core Barsmith engine.
//!
//! This crate owns prepared-dataset loading, combination enumeration,
//! bitset-backed evaluation, resume identity, result storage, formula
//! evaluation, and selection support. The public API is still pre-1.0: prefer
//! the re-exported `Config`, `FeatureDescriptor`, `MaskCache`, and
//! `PermutationPipeline` entrypoints unless a module's documentation says it is
//! intended for direct use.
//!
//! Keep strategy-specific preparation outside this crate. First-party targets
//! live in `custom_rs`, and reusable indicator math lives in
//! `barsmith_indicators`.

pub mod asset;
pub mod backtest;
mod batch_tuning;
#[cfg(feature = "bench-api")]
pub mod benchmark;
mod bitset;
pub mod combinator;
pub mod config;
pub mod data;
pub mod feature;
pub mod formula;
pub mod formula_eval;
pub mod frs;
pub mod mask;
pub mod overfit;
pub mod pipeline;
pub mod progress;
pub mod protocol;
mod run_identity;
pub mod s3;
pub mod selection;
pub mod stats;
pub mod storage;
pub mod stress;
mod subset_pruning;

pub use config::{Config, Direction, ReportMetricsMode};
pub use feature::{FeatureCategory, FeatureDescriptor};
pub use mask::MaskCache;
pub use pipeline::PermutationPipeline;
