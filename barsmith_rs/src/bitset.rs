use std::collections::HashMap;

#[cfg(all(target_arch = "aarch64", feature = "simd-eval"))]
use std::arch::aarch64::*;

/// Compact bitset representation for feature masks and evaluation gates.
///
/// `support` is cached because the evaluator uses it to order masks from most
/// selective to least selective before scanning.
#[derive(Debug, Clone)]
pub(crate) struct BitsetMask {
    pub(crate) words: Vec<u64>,
    pub(crate) len: usize,
    pub(crate) support: usize,
}

impl BitsetMask {
    pub(crate) fn from_bools(values: &[bool]) -> Self {
        let len = values.len();
        if len == 0 {
            return Self {
                words: Vec::new(),
                len: 0,
                support: 0,
            };
        }
        let words_len = len.div_ceil(64);
        let mut words = vec![0u64; words_len];
        for (idx, value) in values.iter().enumerate() {
            if *value {
                let word = idx / 64;
                let bit = idx % 64;
                words[word] |= 1u64 << bit;
            }
        }
        let support = words.iter().map(|w| w.count_ones() as usize).sum();
        Self {
            words,
            len,
            support,
        }
    }

    pub(crate) fn from_finite_f64(values: &[f64]) -> Self {
        let len = values.len();
        if len == 0 {
            return Self {
                words: Vec::new(),
                len: 0,
                support: 0,
            };
        }
        let words_len = len.div_ceil(64);
        let mut words = vec![0u64; words_len];
        for (idx, value) in values.iter().enumerate() {
            if value.is_finite() {
                let word = idx / 64;
                let bit = idx % 64;
                words[word] |= 1u64 << bit;
            }
        }
        let support = words.iter().map(|w| w.count_ones() as usize).sum();
        Self {
            words,
            len,
            support,
        }
    }

    /// Build the per-run trade gate used by evaluator scans.
    ///
    /// Eligibility masks default to "allowed" when they are absent or shorter
    /// than the scan window. Finite-reward masks are stricter: missing words
    /// are treated as not tradable, matching the old per-hit RR guard.
    pub(crate) fn from_eval_gates(
        len: usize,
        eligible: Option<&BitsetMask>,
        finite: Option<&BitsetMask>,
    ) -> Option<Self> {
        if eligible.is_none() && finite.is_none() {
            return None;
        }

        let words_len = len.div_ceil(64);
        let mut words = Vec::with_capacity(words_len);
        let rem = len % 64;
        let last_mask = if rem == 0 {
            u64::MAX
        } else {
            (1u64 << rem) - 1
        };

        for word_index in 0..words_len {
            let mut word = u64::MAX;
            if let Some(gate) = eligible {
                word &= gate_word_allow_out_of_bounds_true(gate, word_index);
            }
            if let Some(gate) = finite {
                if word_index < gate.words.len() {
                    word &= gate.words[word_index];
                } else {
                    word = 0;
                }
            }
            if word_index + 1 == words_len {
                word &= last_mask;
            }
            words.push(word);
        }

        let support = words.iter().map(|w| w.count_ones() as usize).sum();
        Some(Self {
            words,
            len,
            support,
        })
    }
}

/// In-memory catalog of bitset masks for all features in the current run.
///
/// Built once from boolean masks and then shared read-only across workers, so
/// combination scans only borrow plain mask references.
#[derive(Clone)]
pub struct BitsetCatalog {
    bitsets: Vec<BitsetMask>,
    name_to_index: HashMap<String, usize>,
}

impl BitsetCatalog {
    pub(crate) fn new(bitsets: Vec<BitsetMask>, name_to_index: HashMap<String, usize>) -> Self {
        Self {
            bitsets,
            name_to_index,
        }
    }

    pub(crate) fn get(&self, feature: &str) -> Option<&BitsetMask> {
        self.name_to_index
            .get(feature)
            .and_then(|&idx| self.bitsets.get(idx))
    }

    pub(crate) fn get_by_index(&self, index: usize) -> Option<&BitsetMask> {
        self.bitsets.get(index)
    }
}

#[allow(dead_code)]
pub(crate) fn scan_bitsets_scalar_dyn(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    if combo_bitsets.is_empty() || max_len == 0 {
        return 0;
    }

    let words_len = combo_bitsets[0].words.len();
    let mut total = 0usize;

    for word_index in 0..words_len {
        let mut combined = u64::MAX;
        for bitset in combo_bitsets {
            combined &= bitset.words[word_index];
        }
        let mut w = combined;
        while w != 0 {
            let tz = w.trailing_zeros() as usize;
            let idx = word_index * 64 + tz;
            if idx >= max_len {
                break;
            }
            w &= w - 1;

            total += 1;
            on_hit(idx);
        }
    }

    total
}

fn gate_word_allow_out_of_bounds_true(gate: &BitsetMask, word_index: usize) -> u64 {
    if word_index >= gate.words.len() {
        return u64::MAX;
    }
    let mut word = gate.words[word_index];
    if word_index + 1 == gate.words.len() {
        let rem = gate.len % 64;
        if rem != 0 {
            word |= !((1u64 << rem) - 1);
        }
    }
    word
}

pub(crate) fn scan_bitsets_scalar_dyn_gated(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    gate_eligible: Option<&BitsetMask>,
    gate_finite: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    if gate_finite.is_none() {
        return scan_bitsets_scalar_precombined_gated(
            combo_bitsets,
            max_len,
            gate_eligible,
            on_hit,
        );
    }
    let gate = BitsetMask::from_eval_gates(max_len, gate_eligible, gate_finite);
    scan_bitsets_scalar_precombined_gated(combo_bitsets, max_len, gate.as_ref(), on_hit)
}

pub(crate) fn scan_bitsets_scalar_precombined_gated(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    gate: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    if combo_bitsets.is_empty() || max_len == 0 {
        return 0;
    }

    match combo_bitsets {
        [one] => scan_bitsets_one_gated(one, max_len, gate, on_hit),
        [first, second] => scan_bitsets_two_gated(first, second, max_len, gate, on_hit),
        [first, second, third] => {
            scan_bitsets_three_gated(first, second, third, max_len, gate, on_hit)
        }
        _ => scan_bitsets_many_gated(combo_bitsets, max_len, gate, on_hit),
    }
}

fn scan_bitsets_one_gated(
    first: &BitsetMask,
    max_len: usize,
    gate: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    scan_words_gated(
        max_len,
        first.words.len(),
        gate,
        |word_index| first.words[word_index],
        on_hit,
    )
}

fn scan_bitsets_two_gated(
    first: &BitsetMask,
    second: &BitsetMask,
    max_len: usize,
    gate: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    let words_len = first.words.len().min(second.words.len());
    scan_words_gated(
        max_len,
        words_len,
        gate,
        |word_index| first.words[word_index] & second.words[word_index],
        on_hit,
    )
}

fn scan_bitsets_three_gated(
    first: &BitsetMask,
    second: &BitsetMask,
    third: &BitsetMask,
    max_len: usize,
    gate: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    let words_len = first
        .words
        .len()
        .min(second.words.len())
        .min(third.words.len());
    scan_words_gated(
        max_len,
        words_len,
        gate,
        |word_index| first.words[word_index] & second.words[word_index] & third.words[word_index],
        on_hit,
    )
}

fn scan_bitsets_many_gated(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    gate: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    let words_len = combo_bitsets
        .iter()
        .map(|bitset| bitset.words.len())
        .min()
        .unwrap_or(0);
    scan_words_gated(
        max_len,
        words_len,
        gate,
        |word_index| {
            let mut combined = u64::MAX;
            for bitset in combo_bitsets {
                combined &= bitset.words[word_index];
            }
            combined
        },
        on_hit,
    )
}

fn scan_words_gated<C>(
    max_len: usize,
    words_len: usize,
    gate: Option<&BitsetMask>,
    mut combine_word: C,
    on_hit: &mut dyn FnMut(usize),
) -> usize
where
    C: FnMut(usize) -> u64,
{
    if words_len == 0 {
        return 0;
    }

    let words_len = max_len.div_ceil(64).min(words_len);
    let mut scan_total = 0usize;
    let rem = max_len % 64;
    let last_mask = if rem == 0 {
        u64::MAX
    } else {
        (1u64 << rem) - 1
    };

    for word_index in 0..words_len {
        let mut combined = combine_word(word_index);
        if word_index + 1 == words_len {
            combined &= last_mask;
        }
        scan_total += combined.count_ones() as usize;
        if combined == 0 {
            continue;
        }

        let mut gated = combined;
        if let Some(gate) = gate {
            if word_index < gate.words.len() {
                gated &= gate.words[word_index];
            } else {
                gated = 0;
            }
        }

        let mut w = gated;
        while w != 0 {
            let tz = w.trailing_zeros() as usize;
            let idx = word_index * 64 + tz;
            w &= w - 1;
            on_hit(idx);
        }
    }

    scan_total
}

#[cfg(all(target_arch = "aarch64", feature = "simd-eval"))]
#[allow(dead_code)]
unsafe fn scan_bitsets_neon_dyn(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    if combo_bitsets.is_empty() || max_len == 0 {
        return 0;
    }

    let words_len = combo_bitsets[0].words.len();
    let mut total = 0usize;

    let mut word_index = 0usize;
    while word_index + 1 < words_len {
        // SAFETY: aarch64 NEON intrinsics are only compiled for aarch64 with
        // `simd-eval`; `word_index + 1 < words_len` guarantees two u64 lanes
        // are available for every bitset loaded in this loop.
        let mut combined = unsafe { vdupq_n_u64(u64::MAX) };

        for bitset in combo_bitsets {
            let ptr = unsafe { bitset.words.as_ptr().add(word_index) };
            let vec = unsafe { vld1q_u64(ptr) };
            combined = unsafe { vandq_u64(combined, vec) };
        }

        let lane0 = unsafe { vgetq_lane_u64(combined, 0) };
        let lane1 = unsafe { vgetq_lane_u64(combined, 1) };

        let mut w0 = lane0;
        while w0 != 0 {
            let tz = w0.trailing_zeros() as usize;
            let idx = word_index * 64 + tz;
            if idx >= max_len {
                break;
            }
            w0 &= w0 - 1;
            total += 1;
            on_hit(idx);
        }

        let mut w1 = lane1;
        while w1 != 0 {
            let tz = w1.trailing_zeros() as usize;
            let idx = (word_index + 1) * 64 + tz;
            if idx >= max_len {
                break;
            }
            w1 &= w1 - 1;
            total += 1;
            on_hit(idx);
        }

        word_index += 2;
    }

    while word_index < words_len {
        let mut combined = u64::MAX;
        for bitset in combo_bitsets {
            combined &= bitset.words[word_index];
        }
        let mut w = combined;
        while w != 0 {
            let tz = w.trailing_zeros() as usize;
            let idx = word_index * 64 + tz;
            if idx >= max_len {
                break;
            }
            w &= w - 1;

            total += 1;
            on_hit(idx);
        }
        word_index += 1;
    }

    total
}

#[cfg(all(target_arch = "aarch64", feature = "simd-eval"))]
unsafe fn scan_bitsets_neon_dyn_gated(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    gate_eligible: Option<&BitsetMask>,
    gate_finite: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    if combo_bitsets.is_empty() || max_len == 0 {
        return 0;
    }

    let words_len = max_len.div_ceil(64).min(combo_bitsets[0].words.len());
    let mut scan_total = 0usize;
    let rem = max_len % 64;
    let last_mask = if rem == 0 {
        u64::MAX
    } else {
        (1u64 << rem) - 1
    };

    let mut word_index = 0usize;
    while word_index + 1 < words_len {
        // SAFETY: aarch64 NEON intrinsics are only compiled for aarch64 with
        // `simd-eval`; `word_index + 1 < words_len` guarantees two u64 lanes
        // are available for every bitset loaded in this loop.
        let mut combined = unsafe { vdupq_n_u64(u64::MAX) };

        for bitset in combo_bitsets {
            let ptr = unsafe { bitset.words.as_ptr().add(word_index) };
            let vec = unsafe { vld1q_u64(ptr) };
            combined = unsafe { vandq_u64(combined, vec) };
        }

        let lane0 = unsafe { vgetq_lane_u64(combined, 0) };
        let mut lane1 = unsafe { vgetq_lane_u64(combined, 1) };
        if word_index + 1 + 1 == words_len {
            lane1 &= last_mask;
        }

        scan_total += lane0.count_ones() as usize;
        scan_total += lane1.count_ones() as usize;
        if (lane0 | lane1) == 0 {
            word_index += 2;
            continue;
        }

        let mut gated0 = lane0;
        let mut gated1 = lane1;
        if let Some(gate) = gate_eligible {
            gated0 &= gate_word_allow_out_of_bounds_true(gate, word_index);
            gated1 &= gate_word_allow_out_of_bounds_true(gate, word_index + 1);
        }
        if let Some(gate) = gate_finite {
            if word_index < gate.words.len() {
                gated0 &= gate.words[word_index];
            } else {
                gated0 = 0;
            }
            if word_index + 1 < gate.words.len() {
                gated1 &= gate.words[word_index + 1];
            } else {
                gated1 = 0;
            }
        }

        let mut w0 = gated0;
        while w0 != 0 {
            let tz = w0.trailing_zeros() as usize;
            let idx = word_index * 64 + tz;
            w0 &= w0 - 1;
            on_hit(idx);
        }

        let mut w1 = gated1;
        while w1 != 0 {
            let tz = w1.trailing_zeros() as usize;
            let idx = (word_index + 1) * 64 + tz;
            w1 &= w1 - 1;
            on_hit(idx);
        }

        word_index += 2;
    }

    while word_index < words_len {
        let mut combined = u64::MAX;
        for bitset in combo_bitsets {
            combined &= bitset.words[word_index];
        }
        if word_index + 1 == words_len {
            combined &= last_mask;
        }
        scan_total += combined.count_ones() as usize;
        if combined == 0 {
            word_index += 1;
            continue;
        }

        let mut gated = combined;
        if let Some(gate) = gate_eligible {
            gated &= gate_word_allow_out_of_bounds_true(gate, word_index);
        }
        if let Some(gate) = gate_finite {
            if word_index < gate.words.len() {
                gated &= gate.words[word_index];
            } else {
                gated = 0;
            }
        }

        let mut w = gated;
        while w != 0 {
            let tz = w.trailing_zeros() as usize;
            let idx = word_index * 64 + tz;
            w &= w - 1;
            on_hit(idx);
        }
        word_index += 1;
    }

    scan_total
}

#[cfg(all(target_arch = "aarch64", feature = "simd-eval"))]
#[allow(dead_code)]
pub(crate) fn scan_bitsets_simd_dyn(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    unsafe { scan_bitsets_neon_dyn(combo_bitsets, max_len, on_hit) }
}

#[cfg(all(not(target_arch = "aarch64"), feature = "simd-eval"))]
#[allow(dead_code)]
pub(crate) fn scan_bitsets_simd_dyn(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    scan_bitsets_scalar_dyn(combo_bitsets, max_len, on_hit)
}

#[cfg(not(feature = "simd-eval"))]
#[allow(dead_code)]
pub(crate) fn scan_bitsets_simd_dyn(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    scan_bitsets_scalar_dyn(combo_bitsets, max_len, on_hit)
}

#[cfg(all(target_arch = "aarch64", feature = "simd-eval"))]
pub(crate) fn scan_bitsets_simd_dyn_gated(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    gate_eligible: Option<&BitsetMask>,
    gate_finite: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    unsafe {
        scan_bitsets_neon_dyn_gated(combo_bitsets, max_len, gate_eligible, gate_finite, on_hit)
    }
}

#[cfg(all(not(target_arch = "aarch64"), feature = "simd-eval"))]
pub(crate) fn scan_bitsets_simd_dyn_gated(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    gate_eligible: Option<&BitsetMask>,
    gate_finite: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    scan_bitsets_scalar_dyn_gated(combo_bitsets, max_len, gate_eligible, gate_finite, on_hit)
}

#[cfg(not(feature = "simd-eval"))]
pub(crate) fn scan_bitsets_simd_dyn_gated(
    combo_bitsets: &[&BitsetMask],
    max_len: usize,
    gate_eligible: Option<&BitsetMask>,
    gate_finite: Option<&BitsetMask>,
    on_hit: &mut dyn FnMut(usize),
) -> usize {
    scan_bitsets_scalar_dyn_gated(combo_bitsets, max_len, gate_eligible, gate_finite, on_hit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitset_mask_from_bools_sets_bits_correctly() {
        let mask = BitsetMask::from_bools(&[true, false, true, false, false, true]);
        assert_eq!(mask.len, 6);
        assert_eq!(mask.words.len(), 1);
        let word = mask.words[0];
        assert_eq!(word & 1, 1, "bit 0 should be set");
        assert_eq!((word >> 2) & 1, 1, "bit 2 should be set");
        assert_eq!((word >> 5) & 1, 1, "bit 5 should be set");
    }

    #[test]
    fn gated_scan_counts_mask_hits_before_gates_but_calls_hits_after_gates() {
        let combo = BitsetMask::from_bools(&[true, true, true, false]);
        let eligible = BitsetMask::from_bools(&[true, false, true, true]);
        let finite = BitsetMask::from_bools(&[false, true, true, true]);
        let mut hits = Vec::new();
        let total = scan_bitsets_scalar_dyn_gated(
            &[&combo],
            4,
            Some(&eligible),
            Some(&finite),
            &mut |idx| hits.push(idx),
        );

        assert_eq!(total, 3, "scan total should count combo mask support");
        assert_eq!(hits, vec![2], "gates should restrict callback hits");
    }

    #[test]
    fn precomputed_eval_gate_matches_separate_gates() {
        let combo = BitsetMask::from_bools(&[true, true, true, false, true]);
        let eligible = BitsetMask::from_bools(&[true, false, true, true, true]);
        let finite = BitsetMask::from_bools(&[false, true, true, true, false]);
        let gate = BitsetMask::from_eval_gates(5, Some(&eligible), Some(&finite))
            .expect("expected a combined gate");

        let mut separate_hits = Vec::new();
        let separate_total = scan_bitsets_scalar_dyn_gated(
            &[&combo],
            5,
            Some(&eligible),
            Some(&finite),
            &mut |idx| separate_hits.push(idx),
        );

        let mut combined_hits = Vec::new();
        let combined_total =
            scan_bitsets_scalar_precombined_gated(&[&combo], 5, Some(&gate), &mut |idx| {
                combined_hits.push(idx)
            });

        assert_eq!(combined_total, separate_total);
        assert_eq!(combined_hits, separate_hits);
    }

    #[test]
    fn small_depth_scans_match_generic_path() {
        let first = BitsetMask::from_bools(&[true, true, false, true, true, false, true]);
        let second = BitsetMask::from_bools(&[true, false, true, true, false, true, true]);
        let third = BitsetMask::from_bools(&[false, true, true, true, true, false, true]);
        let gate = BitsetMask::from_bools(&[true, true, true, false, true, true, false]);
        let masks = [&first, &second, &third];

        for depth in 1..=3 {
            let selected = &masks[..depth];

            let mut specialized_hits = Vec::new();
            let specialized_total =
                scan_bitsets_scalar_precombined_gated(selected, 7, Some(&gate), &mut |idx| {
                    specialized_hits.push(idx)
                });

            let mut generic_hits = Vec::new();
            let generic_total = scan_bitsets_many_gated(selected, 7, Some(&gate), &mut |idx| {
                generic_hits.push(idx)
            });

            assert_eq!(specialized_total, generic_total, "depth {depth}");
            assert_eq!(specialized_hits, generic_hits, "depth {depth}");
        }
    }
}
