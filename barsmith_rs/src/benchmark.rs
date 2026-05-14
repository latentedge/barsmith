use std::hint::black_box;

use crate::bitset::{BitsetMask, scan_bitsets_simd_dyn_gated, sort_bitsets_by_support};
use crate::combinator::{SeekableIndexIterator, total_combinations};

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
    trade_gate: BitsetMask,
}

pub struct BenchmarkCombSearch {
    rows: usize,
    feature_count: usize,
    max_depth: usize,
    bitsets: Vec<BitsetMask>,
    trade_gate: BitsetMask,
    min_support: usize,
}

impl BenchmarkCombSearch {
    pub fn new(rows: usize, feature_count: usize, max_depth: usize) -> Self {
        let bitsets = (0..feature_count)
            .map(|idx| deterministic_mask(rows, 3 + (idx % 17), idx % 11))
            .collect();
        let eligible = deterministic_mask(rows, 19, 3);
        let finite = deterministic_mask(rows, 23, 5);
        let trade_gate = BitsetMask::from_eval_gates(rows, Some(&eligible), Some(&finite))
            .expect("benchmark gates should produce a trade gate");
        Self {
            rows,
            feature_count,
            max_depth,
            bitsets,
            trade_gate,
            min_support: rows / 64,
        }
    }

    pub fn scan_checksum(&self, combinations: usize) -> usize {
        self.scan_checksum_from(0, combinations)
    }

    pub fn scan_checksum_from(&self, start_offset: u128, combinations: usize) -> usize {
        let mut iter =
            SeekableIndexIterator::starting_at(self.feature_count, self.max_depth, start_offset);
        let mut checksum = 0usize;
        let mut observed = 0usize;

        while observed < combinations {
            let Some(indices) = iter.next() else {
                break;
            };
            let mut masks: smallvec::SmallVec<[&BitsetMask; 8]> =
                indices.iter().map(|&idx| &self.bitsets[idx]).collect();
            sort_bitsets_by_support(masks.as_mut_slice());

            let Some(first) = masks.first() else {
                continue;
            };
            if first.support < self.min_support {
                observed += 1;
                continue;
            }

            let mut hit_mix = indices.len();
            let scanned = scan_bitsets_simd_dyn_gated(
                &masks,
                self.rows,
                Some(&self.trade_gate),
                None,
                &mut |idx| {
                    hit_mix = hit_mix.wrapping_add(idx.rotate_left((indices.len() % 8) as u32));
                },
            );
            checksum = checksum.wrapping_add(scanned ^ hit_mix);
            observed += 1;
        }

        black_box(checksum ^ observed)
    }

    pub fn depth_start_offset(&self, depth: usize) -> u128 {
        if depth == 0 {
            return 0;
        }
        total_combinations(self.feature_count, depth.saturating_sub(1))
    }
}

impl BenchmarkBitsetGatedScan {
    pub fn new(rows: usize) -> Self {
        let eligible = deterministic_mask(rows, 11, 0);
        let finite = deterministic_mask(rows, 13, 4);
        let trade_gate = BitsetMask::from_eval_gates(rows, Some(&eligible), Some(&finite))
            .expect("benchmark gates should produce a trade gate");
        Self {
            rows,
            first: deterministic_mask(rows, 3, 1),
            second: deterministic_mask(rows, 5, 2),
            third: deterministic_mask(rows, 7, 3),
            trade_gate,
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
                Some(&self.trade_gate),
                None,
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
