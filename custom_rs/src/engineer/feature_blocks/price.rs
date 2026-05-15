use anyhow::Result;
use polars::prelude::DataFrame;

use super::super::io::column_to_vec;

pub(in crate::engineer) struct PriceSeries {
    pub(in crate::engineer) open: Vec<f64>,
    pub(in crate::engineer) high: Vec<f64>,
    pub(in crate::engineer) low: Vec<f64>,
    pub(in crate::engineer) close: Vec<f64>,
}

impl PriceSeries {
    pub(in crate::engineer) fn from_frame(frame: &DataFrame) -> Result<Self> {
        Ok(Self {
            open: column_to_vec(frame, "open")?,
            high: column_to_vec(frame, "high")?,
            low: column_to_vec(frame, "low")?,
            close: column_to_vec(frame, "close")?,
        })
    }

    pub(in crate::engineer) fn len(&self) -> usize {
        self.close.len()
    }
}
