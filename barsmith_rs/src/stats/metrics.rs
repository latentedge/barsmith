use crate::config::{PositionSizingMode, StatsDetail};

use super::{SORTED_RETURNS_BUFFER, StatSummary};

#[allow(clippy::too_many_arguments)]
pub(super) fn compute_statistics(
    depth: usize,
    total_bars: usize,
    label_hits: usize,
    returns: Option<&[f64]>,
    risk_per_contract_dollar: Option<&[f64]>,
    _dataset_rows: usize,
    detail: StatsDetail,
    position_sizing: PositionSizingMode,
    dollars_per_r: Option<f64>,
    cost_per_trade_r: Option<f64>,
    capital_dollar: Option<f64>,
    risk_pct_per_trade: Option<f64>,
    equity_time_years: Option<f64>,
    min_contracts: usize,
    max_contracts: Option<usize>,
    margin_per_contract_dollar: Option<f64>,
) -> StatSummary {
    if let Some(filtered_rr) = returns {
        match detail {
            StatsDetail::Core => compute_core_statistics(
                depth,
                total_bars,
                filtered_rr,
                risk_per_contract_dollar,
                label_hits,
                position_sizing,
                capital_dollar,
                risk_pct_per_trade,
                equity_time_years,
                min_contracts,
                max_contracts,
                margin_per_contract_dollar,
            ),
            StatsDetail::Full => compute_full_statistics(
                depth,
                total_bars,
                filtered_rr,
                risk_per_contract_dollar,
                label_hits,
                dollars_per_r,
                cost_per_trade_r,
                capital_dollar,
                risk_pct_per_trade,
                equity_time_years,
                position_sizing,
                min_contracts,
                max_contracts,
                margin_per_contract_dollar,
            ),
        }
    } else {
        let wins = label_hits;
        let losses = total_bars.saturating_sub(wins);
        let expectancy_raw = if total_bars > 0 {
            let win_ratio = wins as f64 / total_bars as f64;
            (2.0 * win_ratio) - 1.0
        } else {
            0.0
        };
        let win_rate_raw = if total_bars > 0 {
            (wins as f64 / total_bars as f64) * 100.0
        } else {
            0.0
        };
        let profit_factor_raw = if win_rate_raw >= 100.0 {
            f64::INFINITY
        } else if win_rate_raw > 0.0 {
            win_rate_raw / (100.0 - win_rate_raw)
        } else {
            0.0
        };
        let expectancy = expectancy_raw;
        let win_rate = win_rate_raw;
        let label_hit_rate = win_rate_raw;
        let label_misses = total_bars.saturating_sub(label_hits);
        let profit_factor = profit_factor_raw;

        StatSummary {
            depth,
            mask_hits: total_bars,
            total_bars,
            profitable_bars: wins,
            unprofitable_bars: losses,
            win_rate,
            label_hit_rate,
            label_hits,
            label_misses,
            expectancy,
            profit_factor,
            avg_winning_rr: 0.0,
            calmar_ratio: 0.0,
            max_drawdown: 0.0,
            win_loss_ratio: 0.0,
            ulcer_index: 0.0,
            pain_ratio: 0.0,
            max_consecutive_wins: 0,
            max_consecutive_losses: 0,
            avg_win_streak: 0.0,
            avg_loss_streak: 0.0,
            median_rr: 0.0,
            avg_losing_rr: 0.0,
            p05_rr: 0.0,
            p95_rr: 0.0,
            largest_win: 0.0,
            largest_loss: 0.0,
            sample_quality: classify_sample(total_bars),
            total_return: 0.0,
            cost_per_trade_r: 0.0,
            dollars_per_r: 0.0,
            total_return_dollar: 0.0,
            max_drawdown_dollar: 0.0,
            expectancy_dollar: 0.0,
            final_capital: 0.0,
            total_return_pct: 0.0,
            cagr_pct: 0.0,
            max_drawdown_pct_equity: 0.0,
            calmar_equity: 0.0,
            sharpe_equity: 0.0,
            sortino_equity: 0.0,
        }
    }
}

/// Streaming accumulator for core statistics.
///
/// The hot path can update this directly during bitset scans instead of
/// building an intermediate RR vector.
pub(super) struct CoreStatsAccumulator {
    pub(super) total_bars: usize,
    total_return: f64,
    equity: f64,
    equity_peak: f64,
    max_drawdown: f64,
    profit_count: usize,
    loss_count: usize,
    simulate_equity: bool,
    min_contracts: usize,
    max_contracts: Option<usize>,
    margin_per_contract_dollar: Option<f64>,
    capital_0: f64,
    risk_factor: f64,
    capital: f64,
    peak_capital: f64,
    max_drawdown_pct_equity: f64,
}

impl CoreStatsAccumulator {
    pub(super) fn new(
        capital_dollar: Option<f64>,
        risk_pct_per_trade: Option<f64>,
        min_contracts: usize,
        max_contracts: Option<usize>,
        margin_per_contract_dollar: Option<f64>,
    ) -> Self {
        let capital_0 = capital_dollar.unwrap_or(0.0);
        let risk_pct = risk_pct_per_trade.unwrap_or(0.0);
        let simulate_equity = capital_0 > 0.0 && risk_pct > 0.0;
        let risk_factor = risk_pct * 0.01;
        Self {
            total_bars: 0,
            total_return: 0.0,
            equity: 0.0,
            equity_peak: 0.0,
            max_drawdown: 0.0,
            profit_count: 0,
            loss_count: 0,
            simulate_equity,
            min_contracts: min_contracts.max(1),
            max_contracts,
            margin_per_contract_dollar,
            capital_0,
            risk_factor,
            capital: capital_0,
            peak_capital: capital_0,
            max_drawdown_pct_equity: 0.0,
        }
    }

    #[inline(always)]
    pub(super) fn push_fractional(&mut self, rr: f64) {
        self.push_base(rr);

        if self.simulate_equity {
            self.apply_fractional_capital_pnl(rr);
        }
    }

    #[inline(always)]
    pub(super) fn push_fractional_with_equity(&mut self, rr: f64) {
        self.push_base(rr);
        self.apply_fractional_capital_pnl(rr);
    }

    #[inline(always)]
    pub(super) fn push_core_only(&mut self, rr: f64) {
        self.push_base(rr);
    }

    #[inline(always)]
    pub(super) fn push_contracts(&mut self, rr: f64, risk_per_contract_dollar: Option<f64>) {
        self.push_base(rr);

        if !self.simulate_equity {
            return;
        }

        let rpc = match risk_per_contract_dollar {
            Some(v) if v.is_finite() && v > 0.0 => v,
            _ => return,
        };
        let risk_budget = self.capital * self.risk_factor;
        let raw = (risk_budget / rpc).floor();
        let mut contracts = if raw.is_finite() && raw >= 0.0 {
            raw as usize
        } else {
            0
        };
        if contracts < self.min_contracts {
            contracts = self.min_contracts;
        }
        if let Some(max_contracts) = self.max_contracts {
            contracts = contracts.min(max_contracts);
        }
        if let Some(margin) = self.margin_per_contract_dollar {
            if margin.is_finite() && margin > 0.0 && self.capital.is_finite() && self.capital > 0.0
            {
                let cap = (self.capital / margin).floor();
                if cap.is_finite() && cap >= 0.0 {
                    contracts = contracts.min(cap as usize);
                }
            }
        }
        if contracts > 0 {
            self.apply_capital_pnl(rr * rpc * (contracts as f64));
        }
    }

    #[inline(always)]
    fn push_base(&mut self, rr: f64) {
        self.total_return += rr;
        if rr > 0.0 {
            self.profit_count += 1;
        } else if rr < 0.0 {
            self.loss_count += 1;
        }

        // R-space equity and drawdown.
        self.equity += rr;
        if self.equity > self.equity_peak {
            self.equity_peak = self.equity;
        }
        let dd = self.equity - self.equity_peak;
        if dd < self.max_drawdown {
            self.max_drawdown = dd;
        }
    }

    #[inline(always)]
    fn apply_fractional_capital_pnl(&mut self, rr: f64) {
        let risk_i = self.capital * self.risk_factor;
        self.apply_capital_pnl(rr * risk_i);
    }

    #[inline(always)]
    fn apply_capital_pnl(&mut self, pnl: f64) {
        self.capital += pnl;
        if self.capital > self.peak_capital {
            self.peak_capital = self.capital;
        }
        if self.peak_capital > 0.0 {
            let dd_pct = ((self.capital - self.peak_capital) / self.peak_capital) * 100.0;
            let dd_mag = -dd_pct;
            if dd_mag > self.max_drawdown_pct_equity {
                self.max_drawdown_pct_equity = dd_mag;
            }
        }
    }

    #[inline(always)]
    pub(super) fn simulates_equity(&self) -> bool {
        self.simulate_equity
    }

    pub(super) fn finalize(
        self,
        depth: usize,
        label_hits: usize,
        equity_time_years: Option<f64>,
    ) -> StatSummary {
        let total_bars = self.total_bars;

        // Equity-curve metrics driven by capital and risk%.
        let final_capital;
        let total_return_pct;
        let cagr_pct;
        let mut max_drawdown_pct_equity = self.max_drawdown_pct_equity;
        let calmar_equity;

        if self.simulate_equity && self.capital_0 > 0.0 && total_bars > 0 {
            let fc = self.capital;
            let tr_pct = ((fc / self.capital_0) - 1.0) * 100.0;

            let years = equity_time_years.unwrap_or(1.0).max(1e-9);
            let growth = if self.capital_0 > 0.0 {
                fc / self.capital_0
            } else {
                1.0
            };
            let cagr = if years > 0.0 && growth.is_finite() && growth > 0.0 {
                (growth.powf(1.0 / years) - 1.0) * 100.0
            } else {
                tr_pct
            };

            let calmar;
            if max_drawdown_pct_equity > 0.0 {
                calmar = cagr / max_drawdown_pct_equity;
            } else if cagr > 0.0 {
                calmar = f64::INFINITY;
            } else {
                calmar = 0.0;
            }

            final_capital = fc;
            total_return_pct = tr_pct;
            cagr_pct = cagr;
            calmar_equity = calmar;
        } else {
            // No capital/risk% context; keep equity metrics at zero.
            max_drawdown_pct_equity = 0.0;
            final_capital = 0.0;
            total_return_pct = 0.0;
            cagr_pct = 0.0;
            calmar_equity = 0.0;
        }

        let max_drawdown_abs = self.max_drawdown.abs();
        let win_rate_raw = if total_bars > 0 {
            (self.profit_count as f64 / total_bars as f64) * 100.0
        } else {
            0.0
        };
        let win_rate = win_rate_raw;

        let label_hits_count = label_hits;
        let label_misses = total_bars.saturating_sub(label_hits_count);
        let label_hit_rate_raw = if total_bars > 0 {
            (label_hits_count as f64 / total_bars as f64) * 100.0
        } else {
            0.0
        };
        let label_hit_rate = label_hit_rate_raw;

        StatSummary {
            depth,
            mask_hits: total_bars,
            total_bars,
            profitable_bars: self.profit_count,
            unprofitable_bars: self.loss_count,
            win_rate,
            label_hit_rate,
            label_hits: label_hits_count,
            label_misses,
            // Richer metrics are recomputed only for the reported top-K
            // combinations via the full-detail path.
            expectancy: 0.0,
            profit_factor: 0.0,
            avg_winning_rr: 0.0,
            calmar_ratio: calmar_equity,
            max_drawdown: max_drawdown_abs,
            win_loss_ratio: 0.0,
            ulcer_index: 0.0,
            pain_ratio: 0.0,
            max_consecutive_wins: 0,
            max_consecutive_losses: 0,
            avg_win_streak: 0.0,
            avg_loss_streak: 0.0,
            median_rr: 0.0,
            avg_losing_rr: 0.0,
            p05_rr: 0.0,
            p95_rr: 0.0,
            largest_win: 0.0,
            largest_loss: 0.0,
            sample_quality: classify_sample(total_bars),
            total_return: self.total_return,
            // No R-to-dollar approximations in core; these remain zero until the full
            // statistics path is invoked for reporting.
            cost_per_trade_r: 0.0,
            dollars_per_r: 0.0,
            total_return_dollar: 0.0,
            max_drawdown_dollar: 0.0,
            expectancy_dollar: 0.0,
            final_capital,
            total_return_pct,
            cagr_pct,
            max_drawdown_pct_equity,
            calmar_equity,
            // Equity Sharpe/Sortino are only computed in the full-detail path.
            sharpe_equity: 0.0,
            sortino_equity: 0.0,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_core_statistics(
    depth: usize,
    _total_bars: usize,
    filtered_rr: &[f64],
    risk_per_contract_dollar: Option<&[f64]>,
    label_hits: usize,
    position_sizing: PositionSizingMode,
    capital_dollar: Option<f64>,
    risk_pct_per_trade: Option<f64>,
    equity_time_years: Option<f64>,
    min_contracts: usize,
    max_contracts: Option<usize>,
    margin_per_contract_dollar: Option<f64>,
) -> StatSummary {
    let mut acc = CoreStatsAccumulator::new(
        capital_dollar,
        risk_pct_per_trade,
        min_contracts,
        max_contracts,
        margin_per_contract_dollar,
    );
    match position_sizing {
        PositionSizingMode::Fractional => {
            if acc.simulates_equity() {
                for &rr in filtered_rr {
                    acc.total_bars += 1;
                    acc.push_fractional_with_equity(rr);
                }
            } else {
                for &rr in filtered_rr {
                    acc.total_bars += 1;
                    acc.push_core_only(rr);
                }
            }
        }
        PositionSizingMode::Contracts => {
            for (idx, &rr) in filtered_rr.iter().enumerate() {
                acc.total_bars += 1;
                let rpc = risk_per_contract_dollar
                    .and_then(|values| values.get(idx).copied())
                    .filter(|v| v.is_finite());
                acc.push_contracts(rr, rpc);
            }
        }
    }
    acc.finalize(depth, label_hits, equity_time_years)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compute_full_statistics(
    depth: usize,
    total_bars: usize,
    filtered_rr: &[f64],
    risk_per_contract_dollar: Option<&[f64]>,
    label_hits: usize,
    dollars_per_r: Option<f64>,
    cost_per_trade_r: Option<f64>,
    capital_dollar: Option<f64>,
    risk_pct_per_trade: Option<f64>,
    equity_time_years: Option<f64>,
    position_sizing: PositionSizingMode,
    min_contracts: usize,
    max_contracts: Option<usize>,
    margin_per_contract_dollar: Option<f64>,
) -> StatSummary {
    let n = total_bars;
    let mut total_return = 0.0;
    let mut profit_sum = 0.0;
    let mut loss_sum = 0.0;
    let mut profit_count = 0usize;
    let mut loss_count = 0usize;

    // Drawdown-related accumulators.
    let mut equity = 0.0;
    let mut equity_peak = 0.0;
    let mut max_drawdown = 0.0; // most negative drawdown (in R)
    let mut dd_sum = 0.0;
    let mut dd_pct_sq_sum = 0.0;
    let mut dd_count = 0usize;
    let mut have_nonzero_peak = false;

    // Streak-related accumulators.
    let mut current_win_streak = 0usize;
    let mut current_loss_streak = 0usize;
    let mut max_consecutive_wins = 0usize;
    let mut max_consecutive_losses = 0usize;
    let mut total_win_streak_len = 0usize;
    let mut total_loss_streak_len = 0usize;
    let mut win_streak_count = 0usize;
    let mut loss_streak_count = 0usize;

    // Extremes.
    let mut largest_win = 0.0;
    let mut largest_loss = 0.0;

    for &rr in filtered_rr {
        total_return += rr;

        if rr > 0.0 {
            profit_sum += rr;
            profit_count += 1;
            if rr > largest_win {
                largest_win = rr;
            }
        } else if rr < 0.0 {
            let abs_rr = -rr;
            loss_sum += abs_rr;
            loss_count += 1;
            if abs_rr > largest_loss {
                largest_loss = abs_rr;
            }
        }

        // Equity curve and drawdowns.
        equity += rr;
        if equity > equity_peak {
            equity_peak = equity;
            if equity_peak.abs() >= f64::EPSILON {
                have_nonzero_peak = true;
            }
        }
        let dd = equity - equity_peak; // <= 0
        dd_sum += dd;
        dd_count += 1;
        if dd < max_drawdown {
            max_drawdown = dd;
        }
        if equity_peak.abs() >= f64::EPSILON {
            let pct = (dd / (equity_peak + f64::EPSILON)) * 100.0;
            dd_pct_sq_sum += pct * pct;
        }

        // Streak tracking: wins > 0, losses < 0, zero breaks both.
        if rr > 0.0 {
            if current_loss_streak > 0 {
                total_loss_streak_len += current_loss_streak;
                loss_streak_count += 1;
                if current_loss_streak > max_consecutive_losses {
                    max_consecutive_losses = current_loss_streak;
                }
                current_loss_streak = 0;
            }
            current_win_streak += 1;
        } else if rr < 0.0 {
            if current_win_streak > 0 {
                total_win_streak_len += current_win_streak;
                win_streak_count += 1;
                if current_win_streak > max_consecutive_wins {
                    max_consecutive_wins = current_win_streak;
                }
                current_win_streak = 0;
            }
            current_loss_streak += 1;
        } else {
            if current_win_streak > 0 {
                total_win_streak_len += current_win_streak;
                win_streak_count += 1;
                if current_win_streak > max_consecutive_wins {
                    max_consecutive_wins = current_win_streak;
                }
                current_win_streak = 0;
            }
            if current_loss_streak > 0 {
                total_loss_streak_len += current_loss_streak;
                loss_streak_count += 1;
                if current_loss_streak > max_consecutive_losses {
                    max_consecutive_losses = current_loss_streak;
                }
                current_loss_streak = 0;
            }
        }
    }

    // Flush any trailing streaks.
    if current_win_streak > 0 {
        total_win_streak_len += current_win_streak;
        win_streak_count += 1;
        if current_win_streak > max_consecutive_wins {
            max_consecutive_wins = current_win_streak;
        }
    }
    if current_loss_streak > 0 {
        total_loss_streak_len += current_loss_streak;
        loss_streak_count += 1;
        if current_loss_streak > max_consecutive_losses {
            max_consecutive_losses = current_loss_streak;
        }
    }

    let expectancy_raw = if n > 0 { total_return / n as f64 } else { 0.0 };
    let expectancy = expectancy_raw;

    // Profit factor and win/loss geometry.
    let profit_factor_raw = if loss_sum > 0.0 {
        profit_sum / loss_sum
    } else if profit_sum > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };

    let avg_winning_rr = if profit_count > 0 {
        profit_sum / profit_count as f64
    } else {
        0.0
    };
    let avg_loss_abs = if loss_count > 0 {
        loss_sum / loss_count as f64
    } else {
        0.0
    };
    let avg_losing_rr = if loss_count > 0 { -avg_loss_abs } else { 0.0 };
    let win_loss_ratio_raw = if avg_loss_abs > 0.0 {
        avg_winning_rr / avg_loss_abs
    } else if avg_winning_rr > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };

    // Drawdown-derived metrics.
    let max_drawdown_abs = max_drawdown.abs();
    let ulcer_index = if !have_nonzero_peak || dd_count == 0 {
        0.0
    } else {
        (dd_pct_sq_sum / dd_count as f64).sqrt()
    };
    let avg_drawdown = if dd_count > 0 {
        dd_sum / dd_count as f64
    } else {
        0.0
    };
    let pain_ratio = if avg_drawdown < 0.0 {
        total_return / avg_drawdown.abs()
    } else {
        0.0
    };
    let avg_win_streak = if win_streak_count > 0 {
        total_win_streak_len as f64 / win_streak_count as f64
    } else {
        0.0
    };
    let avg_loss_streak = if loss_streak_count > 0 {
        total_loss_streak_len as f64 / loss_streak_count as f64
    } else {
        0.0
    };

    // Distribution shape: median and simple 5th/95th percentiles.
    let (median_rr, p05_rr, p95_rr) = if n > 0 {
        percentile_triplet(filtered_rr)
    } else {
        (0.0, 0.0, 0.0)
    };

    let cost_r = cost_per_trade_r.unwrap_or(0.0);
    let dollars_per_r = dollars_per_r.unwrap_or(0.0);
    let mut total_return_dollar = if dollars_per_r > 0.0 {
        total_return * dollars_per_r
    } else {
        0.0
    };
    let mut max_drawdown_dollar = if dollars_per_r > 0.0 {
        max_drawdown_abs * dollars_per_r
    } else {
        0.0
    };
    let mut expectancy_dollar = if dollars_per_r > 0.0 {
        expectancy * dollars_per_r
    } else {
        0.0
    };

    // Net-R and label-based win rates.
    let win_rate_raw = if total_bars > 0 {
        (profit_count as f64 / total_bars as f64) * 100.0
    } else {
        0.0
    };
    let win_rate = win_rate_raw;

    let label_hits_count = label_hits;
    let label_misses = total_bars.saturating_sub(label_hits_count);
    let label_hit_rate_raw = if total_bars > 0 {
        (label_hits_count as f64 / total_bars as f64) * 100.0
    } else {
        0.0
    };
    let label_hit_rate = label_hit_rate_raw;

    // Equity-curve metrics driven by capital and risk%.
    let capital_0 = capital_dollar.unwrap_or(0.0);
    let risk_pct = risk_pct_per_trade.unwrap_or(0.0);
    let mut final_capital = 0.0;
    let mut total_return_pct = 0.0;
    let mut cagr_pct = 0.0;
    let mut max_drawdown_pct_equity = 0.0;
    let mut calmar_equity = 0.0;
    let mut sharpe_equity = 0.0;
    let mut sortino_equity = 0.0;

    if capital_0 > 0.0 && risk_pct > 0.0 && n > 0 {
        let mut capital = capital_0;
        let mut peak_capital = capital_0;
        let mut max_drawdown_dollar_sim = 0.0;
        let mut pnl_sum = 0.0;
        let mut eq_ret_sum = 0.0;
        let mut eq_ret_sq_sum = 0.0;
        let mut downside_sq_sum = 0.0;
        let mut downside_count = 0usize;

        for (idx, &rr) in filtered_rr.iter().enumerate() {
            let pnl = match position_sizing {
                PositionSizingMode::Fractional => {
                    let risk_i = capital * (risk_pct / 100.0);
                    rr * risk_i
                }
                PositionSizingMode::Contracts => {
                    let rpc = match risk_per_contract_dollar
                        .and_then(|values| values.get(idx).copied())
                    {
                        Some(v) if v.is_finite() && v > 0.0 => v,
                        _ => 0.0,
                    };
                    if rpc <= 0.0 {
                        0.0
                    } else {
                        let risk_budget = capital * (risk_pct / 100.0);
                        let raw = (risk_budget / rpc).floor();
                        let mut contracts = if raw.is_finite() && raw >= 0.0 {
                            raw as usize
                        } else {
                            0
                        };
                        let min_contracts = min_contracts.max(1);
                        if contracts < min_contracts {
                            contracts = min_contracts;
                        }
                        if let Some(max_contracts) = max_contracts {
                            contracts = contracts.min(max_contracts);
                        }
                        if let Some(margin) = margin_per_contract_dollar {
                            if margin.is_finite()
                                && margin > 0.0
                                && capital.is_finite()
                                && capital > 0.0
                            {
                                let cap = (capital / margin).floor();
                                if cap.is_finite() && cap >= 0.0 {
                                    contracts = contracts.min(cap as usize);
                                }
                            }
                        }
                        if contracts == 0 {
                            0.0
                        } else {
                            rr * rpc * (contracts as f64)
                        }
                    }
                }
            };
            let next_capital = capital + pnl;
            let ret = if capital > 0.0 {
                (next_capital / capital) - 1.0
            } else {
                0.0
            };

            pnl_sum += pnl;
            eq_ret_sum += ret;
            eq_ret_sq_sum += ret * ret;
            if ret < 0.0 {
                downside_sq_sum += ret * ret;
                downside_count += 1;
            }

            capital = next_capital;
            if capital > peak_capital {
                peak_capital = capital;
            }
            if peak_capital > 0.0 {
                let dd_pct = ((capital - peak_capital) / peak_capital) * 100.0;
                let dd_mag = -dd_pct;
                if dd_mag > max_drawdown_pct_equity {
                    max_drawdown_pct_equity = dd_mag;
                }
                let dd_dollar = capital - peak_capital;
                let dd_dollar_mag = -dd_dollar;
                if dd_dollar_mag > max_drawdown_dollar_sim {
                    max_drawdown_dollar_sim = dd_dollar_mag;
                }
            }
        }

        final_capital = capital;
        if capital_0 > 0.0 {
            total_return_pct = ((final_capital / capital_0) - 1.0) * 100.0;
        }

        total_return_dollar = pnl_sum;
        expectancy_dollar = pnl_sum / (n as f64);
        max_drawdown_dollar = max_drawdown_dollar_sim;

        let years = equity_time_years.unwrap_or(1.0).max(1e-9);
        let growth = if capital_0 > 0.0 {
            final_capital / capital_0
        } else {
            1.0
        };
        if years > 0.0 && growth.is_finite() && growth > 0.0 {
            cagr_pct = (growth.powf(1.0 / years) - 1.0) * 100.0;
        } else {
            cagr_pct = total_return_pct;
        }

        if max_drawdown_pct_equity > 0.0 {
            calmar_equity = cagr_pct / max_drawdown_pct_equity;
        } else if cagr_pct > 0.0 {
            calmar_equity = f64::INFINITY;
        } else {
            calmar_equity = 0.0;
        }

        let n_returns = n as f64;
        let mean = eq_ret_sum / n_returns;
        let var = (eq_ret_sq_sum / n_returns) - mean * mean;
        let std = var.max(0.0).sqrt();

        let downside_std = if downside_count > 0 {
            (downside_sq_sum / (downside_count as f64)).sqrt()
        } else {
            0.0
        };

        let trades_per_year = (n_returns / years).max(1e-9);
        let annual_scale = trades_per_year.sqrt();

        if std > 0.0 {
            sharpe_equity = (mean / std) * annual_scale;
        } else {
            sharpe_equity = 0.0;
        }

        sortino_equity = if downside_std > 0.0 {
            (mean / downside_std) * annual_scale
        } else if downside_std == 0.0 && mean > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
    }

    StatSummary {
        depth,
        mask_hits: total_bars,
        total_bars,
        profitable_bars: profit_count,
        unprofitable_bars: loss_count,
        win_rate,
        label_hit_rate,
        label_hits: label_hits_count,
        label_misses,
        expectancy,
        profit_factor: profit_factor_raw,
        avg_winning_rr,
        calmar_ratio: calmar_equity,
        max_drawdown: max_drawdown_abs,
        win_loss_ratio: win_loss_ratio_raw,
        ulcer_index,
        pain_ratio,
        max_consecutive_wins,
        max_consecutive_losses,
        avg_win_streak,
        avg_loss_streak,
        median_rr,
        avg_losing_rr,
        p05_rr,
        p95_rr,
        largest_win,
        largest_loss,
        sample_quality: classify_sample(total_bars),
        total_return,
        cost_per_trade_r: cost_r,
        dollars_per_r,
        total_return_dollar,
        max_drawdown_dollar,
        expectancy_dollar,
        final_capital,
        total_return_pct,
        cagr_pct,
        max_drawdown_pct_equity,
        calmar_equity,
        sharpe_equity,
        sortino_equity,
    }
}

pub(super) fn percentile_triplet(filtered_rr: &[f64]) -> (f64, f64, f64) {
    SORTED_RETURNS_BUFFER.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        buf.extend_from_slice(filtered_rr);
        let len = buf.len();
        if len == 0 {
            return (0.0, 0.0, 0.0);
        }

        let len_minus_one = len - 1;
        let idx_p05 = ((len_minus_one as f64) * 0.05).round() as usize;
        let idx_p95 = ((len_minus_one as f64) * 0.95).round() as usize;
        let idx_p05 = idx_p05.min(len - 1);
        let idx_p95 = idx_p95.min(len - 1);

        // Median via selection; even lengths need the lower neighbor from the
        // left partition.
        let mid = len / 2;
        buf.select_nth_unstable_by(mid, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });
        let median = if len % 2 == 0 {
            let mid_val = buf[mid];
            let mut max_left = buf[0];
            for &v in &buf[1..mid] {
                if v > max_left {
                    max_left = v;
                }
            }
            (max_left + mid_val) / 2.0
        } else {
            buf[mid]
        };

        // 5th and 95th percentiles via selection.
        buf.select_nth_unstable_by(idx_p05, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });
        let p05 = buf[idx_p05];

        buf.select_nth_unstable_by(idx_p95, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });
        let p95 = buf[idx_p95];

        (median, p05, p95)
    })
}

#[cfg(test)]
pub(super) fn round_to(value: f64, decimals: i32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let factor = 10f64.powi(decimals);
    let scaled = value * factor;
    let floor = scaled.floor();
    let diff = scaled - floor;
    let epsilon = 1e-9;
    let rounded = if (diff - 0.5).abs() < epsilon {
        let floor_even = ((floor as i64) & 1) == 0;
        if floor_even { floor } else { floor + 1.0 }
    } else {
        scaled.round()
    };
    rounded / factor
}

pub(super) fn classify_sample(total_bars: usize) -> &'static str {
    match total_bars {
        n if n >= 100 => "excellent",
        n if n >= 50 => "good",
        n if n >= 30 => "fair",
        _ => "poor",
    }
}
