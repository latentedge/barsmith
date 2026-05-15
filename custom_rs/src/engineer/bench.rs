use crate::targets::common::barrier::compute_2x_atr_tp_atr_stop_targets_and_rr;
use barsmith_rs::Direction;

pub fn benchmark_2x_atr_tp_atr_stop_checksum(rows: usize, repeats: usize) -> f64 {
    let rows = rows.max(2);
    let mut open = Vec::with_capacity(rows);
    let mut high = Vec::with_capacity(rows);
    let mut low = Vec::with_capacity(rows);
    let mut close = Vec::with_capacity(rows);
    let mut atr = Vec::with_capacity(rows);

    let mut last_close = 4_800.0;
    for idx in 0..rows {
        let drift = ((idx % 29) as f64 - 14.0) * 0.18;
        let impulse = if idx % 41 == 0 {
            3.75
        } else if idx % 37 == 0 {
            -3.25
        } else {
            0.0
        };
        let next_open = last_close + drift * 0.35;
        let next_close = next_open + drift + impulse;
        let wick = 1.25 + (idx % 11) as f64 * 0.22;

        open.push(next_open);
        high.push(next_open.max(next_close) + wick);
        low.push(next_open.min(next_close) - wick * 0.95);
        close.push(next_close);
        atr.push(3.5 + (idx % 17) as f64 * 0.08);

        last_close = next_close;
    }

    let mut checksum = 0.0;
    for repeat in 0..repeats.max(1) {
        let cutoff = if repeat % 2 == 0 {
            Some(rows.saturating_mul(3) / 4)
        } else {
            None
        };
        let (long, short, rr_long, rr_short, exit_long, exit_short) =
            compute_2x_atr_tp_atr_stop_targets_and_rr(
                &open,
                &high,
                &low,
                &close,
                &atr,
                Some(0.25),
                cutoff,
                Direction::Both,
            );

        for idx in (repeat % 17..rows).step_by(17) {
            if long[idx] {
                checksum += 1.0;
            }
            if short[idx] {
                checksum -= 1.0;
            }
            if rr_long[idx].is_finite() {
                checksum += rr_long[idx] * 0.125;
            }
            if rr_short[idx].is_finite() {
                checksum -= rr_short[idx] * 0.125;
            }
            checksum += exit_long[idx].unwrap_or(0) as f64 * 0.000_001;
            checksum -= exit_short[idx].unwrap_or(0) as f64 * 0.000_001;
        }
    }

    std::hint::black_box(checksum)
}
