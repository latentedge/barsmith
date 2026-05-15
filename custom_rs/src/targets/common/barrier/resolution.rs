#[derive(Debug, Clone)]
pub(crate) struct TargetResolution {
    pub(crate) long: Vec<bool>,
    pub(crate) short: Vec<bool>,
    pub(crate) rr_long: Vec<f64>,
    pub(crate) rr_short: Vec<f64>,
    pub(crate) exit_i_long: Vec<Option<usize>>,
    pub(crate) exit_i_short: Vec<Option<usize>>,
    pub(super) risk_long: Option<Vec<f64>>,
    pub(super) risk_short: Option<Vec<f64>>,
}

pub(crate) type TargetTuple = (
    Vec<bool>,
    Vec<bool>,
    Vec<f64>,
    Vec<f64>,
    Vec<Option<usize>>,
    Vec<Option<usize>>,
);

impl TargetResolution {
    pub(super) fn new(len: usize, include_risk: bool) -> Self {
        Self {
            long: vec![false; len],
            short: vec![false; len],
            rr_long: vec![f64::NAN; len],
            rr_short: vec![f64::NAN; len],
            exit_i_long: vec![None; len],
            exit_i_short: vec![None; len],
            risk_long: include_risk.then(|| vec![f64::NAN; len]),
            risk_short: include_risk.then(|| vec![f64::NAN; len]),
        }
    }

    pub(crate) fn take_risk_columns(&mut self) -> Option<(Vec<f64>, Vec<f64>)> {
        Some((self.risk_long.take()?, self.risk_short.take()?))
    }

    #[cfg(test)]
    pub(crate) fn risk_long_values(&self) -> &[f64] {
        self.risk_long
            .as_deref()
            .expect("target resolution should include long risk")
    }

    #[cfg(test)]
    pub(crate) fn risk_short_values(&self) -> &[f64] {
        self.risk_short
            .as_deref()
            .expect("target resolution should include short risk")
    }

    pub(super) fn into_targets_and_rr(self) -> TargetTuple {
        (
            self.long,
            self.short,
            self.rr_long,
            self.rr_short,
            self.exit_i_long,
            self.exit_i_short,
        )
    }
}
