use std::hint::black_box;

use crate::bitset::{BitsetMask, scan_bitsets_simd_dyn_gated};

/// Small benchmark hooks for the standalone Barsmith benchmark binary.
///
/// This module is behind the `bench-api` feature so normal library consumers do
/// not inherit benchmark-only API surface.
pub fn bitset_gated_scan_checksum(rows: usize, repeats: usize) -> usize {
    BenchmarkBitsetGatedScan::new(rows).scan_checksum(repeats)
}

pub struct BenchmarkBitsetGatedScan {
    rows: usize,
    first: BitsetMask,
    second: BitsetMask,
    third: BitsetMask,
    eligible: BitsetMask,
    finite: BitsetMask,
}

impl BenchmarkBitsetGatedScan {
    pub fn new(rows: usize) -> Self {
        Self {
            rows,
            first: deterministic_mask(rows, 3, 1),
            second: deterministic_mask(rows, 5, 2),
            third: deterministic_mask(rows, 7, 3),
            eligible: deterministic_mask(rows, 11, 0),
            finite: deterministic_mask(rows, 13, 4),
        }
    }

    pub fn scan_checksum(&self, repeats: usize) -> usize {
        let masks = [&self.first, &self.second, &self.third];
        let mut checksum = 0usize;
        for _ in 0..repeats {
            let mut hit_mix = 0usize;
            let scanned = scan_bitsets_simd_dyn_gated(
                &masks,
                self.rows,
                Some(&self.eligible),
                Some(&self.finite),
                &mut |idx| {
                    hit_mix = hit_mix.wrapping_add(idx.rotate_left(3));
                },
            );
            checksum = checksum.wrapping_add(scanned ^ hit_mix);
        }
        black_box(checksum)
    }
}

fn deterministic_mask(rows: usize, modulus: usize, offset: usize) -> BitsetMask {
    let values: Vec<bool> = (0..rows)
        .map(|idx| (idx.wrapping_add(offset)) % modulus != 0)
        .collect();
    BitsetMask::from_bools(&values)
}
