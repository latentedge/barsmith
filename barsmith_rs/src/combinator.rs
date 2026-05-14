use itertools::Itertools;
use smallvec::SmallVec;

use crate::feature::FeatureDescriptor;

/// Descriptor-based combination used in reporting, tests, and storage.
pub type Combination = Vec<FeatureDescriptor>;

/// Feature-catalog indices used in the evaluator's hot path.
pub type IndexCombination = SmallVec<[usize; 8]>;

/// Return C(n, k), or 0 when k is larger than n.
pub fn combinations_for_depth(feature_count: usize, depth: usize) -> u128 {
    if depth > feature_count {
        return 0;
    }
    if depth == 0 {
        return 1;
    }
    // Work on the smaller side of the coefficient to keep multiplication short.
    let k = depth.min(feature_count - depth);
    let mut numerator = 1u128;
    let mut denominator = 1u128;
    for i in 0..k {
        numerator *= (feature_count - i) as u128;
        denominator *= (i + 1) as u128;
    }
    numerator / denominator
}

/// Compute the total number of combinations across depths 1..=max_depth.
pub fn total_combinations(feature_count: usize, max_depth: usize) -> u128 {
    (1..=max_depth)
        .map(|depth| combinations_for_depth(feature_count, depth))
        .sum()
}

/// Resolve a global combination offset into its depth and local rank.
pub fn global_to_depth_and_local(
    global_index: u128,
    n: usize,
    max_depth: usize,
) -> Option<(usize, u128)> {
    let mut remaining = global_index;

    for depth in 1..=max_depth {
        let count_at_depth = combinations_for_depth(n, depth);
        if remaining < count_at_depth {
            return Some((depth, remaining));
        }
        remaining -= count_at_depth;
    }

    None
}

/// Resolve a 0-based rank within C(n, k) into lexicographic indices.
pub fn unrank_combination(rank: u128, n: usize, k: usize) -> Vec<usize> {
    if k == 0 || k > n || rank >= combinations_for_depth(n, k) {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(k);
    let mut remaining = rank;
    let mut start = 0usize;

    for i in 0..k {
        let elements_remaining = k - i;
        // Pick the next lexicographic element and account for the ranks skipped.
        let c = find_lex_element(remaining, n, start, elements_remaining);
        result.push(c);

        let combos_skipped = count_combos_before(c, n, start, elements_remaining);
        remaining -= combos_skipped;

        start = c + 1;
    }

    result
}

/// Find the next index for a ranked lexicographic combination.
fn find_lex_element(remaining: u128, n: usize, start: usize, elements_remaining: usize) -> usize {
    let max_valid = n - elements_remaining;

    if start > max_valid {
        return max_valid;
    }

    let mut lo = start;
    let mut hi = max_valid;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let combos_after = combinations_for_depth(n - mid - 1, elements_remaining - 1);
        let combos_before = count_combos_before(mid, n, start, elements_remaining);

        if remaining < combos_after + combos_before {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    lo
}

/// Count combinations that start with an index less than `c`.
fn count_combos_before(c: usize, n: usize, start: usize, elements_remaining: usize) -> u128 {
    if c <= start || elements_remaining == 0 {
        return 0;
    }

    // Hockey-stick identity:
    // sum_{idx=start}^{c-1} C(n - idx - 1, r - 1)
    // = C(n - start, r) - C(n - c, r)
    combinations_for_depth(n - start, elements_remaining)
        - combinations_for_depth(n - c, elements_remaining)
}

/// Compute the rank of a sorted combination (inverse of unrank).
/// Useful for testing roundtrip correctness.
pub fn rank_combination(combo: &[usize], n: usize) -> u128 {
    let k = combo.len();
    if k == 0 || k > n {
        return 0;
    }

    let mut rank = 0u128;
    let mut start = 0usize;

    for (i, &c) in combo.iter().enumerate() {
        let elements_remaining = k - i;
        rank += count_combos_before(c, n, start, elements_remaining);
        start = c + 1;
    }

    rank
}

/// Unrank a global index across all depths to feature indices.
/// Returns None if the index exceeds total combinations.
pub fn unrank_global(global_index: u128, n: usize, max_depth: usize) -> Option<Vec<usize>> {
    let (depth, local_index) = global_to_depth_and_local(global_index, n, max_depth)?;
    Some(unrank_combination(local_index, n, depth))
}

/// A combination iterator that can start from any global index in O(k) time.
///
/// Used by resume to jump to an offset instead of replaying every earlier
/// combination.
pub struct SeekableCombinationIterator<'a> {
    features: &'a [FeatureDescriptor],
    n: usize,
    max_depth: usize,
    current_depth: usize,
    current_indices: IndexCombination,
    exhausted: bool,
}

impl<'a> SeekableCombinationIterator<'a> {
    /// Create an iterator starting at global_index (0-based).
    ///
    /// This operation is O(k) where k is the depth of the starting combination,
    /// compared to O(n) for the naive skip approach.
    pub fn starting_at(
        features: &'a [FeatureDescriptor],
        max_depth: usize,
        start_index: u128,
    ) -> Self {
        let n = features.len();
        let max_depth = max_depth.min(n).max(1);

        if start_index == 0 {
            return Self {
                features,
                n,
                max_depth,
                current_depth: 1,
                current_indices: [0usize].into_iter().collect(),
                exhausted: n == 0,
            };
        }

        if let Some((depth, local_index)) = global_to_depth_and_local(start_index, n, max_depth) {
            let indices = unrank_combination(local_index, n, depth);
            if indices.is_empty() {
                Self {
                    features,
                    n,
                    max_depth,
                    current_depth: max_depth + 1,
                    current_indices: IndexCombination::new(),
                    exhausted: true,
                }
            } else {
                Self {
                    features,
                    n,
                    max_depth,
                    current_depth: depth,
                    current_indices: indices.into_iter().collect(),
                    exhausted: false,
                }
            }
        } else {
            Self {
                features,
                n,
                max_depth,
                current_depth: max_depth + 1,
                current_indices: IndexCombination::new(),
                exhausted: true,
            }
        }
    }

    /// Create an iterator starting from the beginning.
    pub fn new(features: &'a [FeatureDescriptor], max_depth: usize) -> Self {
        Self::starting_at(features, max_depth, 0)
    }

    /// Advance to the next combination in lexicographic order.
    fn advance(&mut self) {
        if self.exhausted {
            return;
        }

        let k = self.current_indices.len();

        // Move the rightmost index that can still advance, then rebuild the suffix.
        for i in (0..k).rev() {
            let max_val = self.n - (k - i);
            if self.current_indices[i] < max_val {
                self.current_indices[i] += 1;
                for j in (i + 1)..k {
                    self.current_indices[j] = self.current_indices[j - 1] + 1;
                }
                return;
            }
        }

        self.current_depth += 1;
        if self.current_depth > self.max_depth {
            self.exhausted = true;
            return;
        }

        self.current_indices = (0..self.current_depth).collect();
    }

    /// Get the current global index (for debugging/verification).
    pub fn current_global_index(&self) -> Option<u128> {
        if self.exhausted {
            return None;
        }

        let mut index = 0u128;
        for d in 1..self.current_depth {
            index += combinations_for_depth(self.n, d);
        }

        index += rank_combination(&self.current_indices, self.n);

        Some(index)
    }
}

impl<'a> Iterator for SeekableCombinationIterator<'a> {
    type Item = Combination;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        let combo: Combination = self
            .current_indices
            .iter()
            .map(|&i| self.features[i].clone())
            .collect();

        self.advance();
        Some(combo)
    }
}

/// Seekable iterator that yields index-based combinations only. This avoids
/// cloning feature descriptors in the hot evaluation path; indices are later
/// mapped back to names only for reporting/storage.
pub struct SeekableIndexIterator {
    n: usize,
    max_depth: usize,
    current_depth: usize,
    current_indices: IndexCombination,
    exhausted: bool,
}

impl SeekableIndexIterator {
    pub fn starting_at(n: usize, max_depth: usize, start_index: u128) -> Self {
        let max_depth = max_depth.min(n).max(1);

        if n == 0 {
            return Self {
                n,
                max_depth,
                current_depth: max_depth + 1,
                current_indices: IndexCombination::new(),
                exhausted: true,
            };
        }

        if let Some(indices) = unrank_global(start_index, n, max_depth) {
            let depth = indices.len();
            Self {
                n,
                max_depth,
                current_depth: depth,
                current_indices: indices.into_iter().collect(),
                exhausted: false,
            }
        } else {
            Self {
                n,
                max_depth,
                current_depth: max_depth + 1,
                current_indices: IndexCombination::new(),
                exhausted: true,
            }
        }
    }

    fn advance(&mut self) {
        if self.exhausted {
            return;
        }

        let k = self.current_indices.len();

        // Move the rightmost index that can still advance, then rebuild the suffix.
        for i in (0..k).rev() {
            let max_val = self.n - (k - i);
            if self.current_indices[i] < max_val {
                self.current_indices[i] += 1;
                for j in (i + 1)..k {
                    self.current_indices[j] = self.current_indices[j - 1] + 1;
                }
                return;
            }
        }

        self.current_depth += 1;
        if self.current_depth > self.max_depth {
            self.exhausted = true;
            return;
        }

        self.current_indices = (0..self.current_depth).collect();
    }
}

impl Iterator for SeekableIndexIterator {
    type Item = IndexCombination;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        let combo = self.current_indices.clone();
        self.advance();
        Some(combo)
    }
}

pub struct FeaturePools {
    features: Vec<FeatureDescriptor>,
}

impl FeaturePools {
    pub fn new(features: Vec<FeatureDescriptor>) -> Self {
        Self { features }
    }

    pub fn descriptors(&self) -> &[FeatureDescriptor] {
        &self.features
    }
}

/// Descriptor-yielding iterator kept for tests and older callers.
pub struct CombinationIterator<'a> {
    features: &'a [FeatureDescriptor],
    current_depth: usize,
    max_depth: usize,
    inner: Option<Box<dyn Iterator<Item = Combination> + 'a>>,
}

impl<'a> CombinationIterator<'a> {
    pub fn new(features: &'a [FeatureDescriptor], max_depth: usize) -> Self {
        Self {
            features,
            current_depth: 1,
            max_depth: max_depth.max(1),
            inner: None,
        }
    }
}

impl<'a> Iterator for CombinationIterator<'a> {
    type Item = Combination;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.inner.as_mut() {
                if let Some(combo) = iter.next() {
                    return Some(combo);
                }
            }

            if self.current_depth > self.max_depth {
                return None;
            }

            let depth = self.current_depth;
            self.current_depth += 1;
            self.inner = Some(Box::new(
                self.features
                    .iter()
                    .cloned()
                    .combinations(depth)
                    .map(|combo| combo.into_iter().collect()),
            ));
        }
    }
}

pub struct CombinationBatcher<'a> {
    iter: SeekableCombinationIterator<'a>,
}

impl<'a> CombinationBatcher<'a> {
    /// Create a descriptor batcher starting at the given global offset.
    pub fn new(features: &'a FeaturePools, max_depth: usize, start_offset: u64) -> Self {
        let iter = SeekableCombinationIterator::starting_at(
            features.descriptors(),
            max_depth,
            start_offset as u128,
        );
        Self { iter }
    }

    pub fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Combination>> {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
            match self.iter.next() {
                Some(combo) => batch.push(combo),
                None => break,
            }
        }
        if batch.is_empty() { None } else { Some(batch) }
    }
}

/// Index-based batcher used by the evaluation pipeline to avoid cloning
/// feature descriptors for every enumerated combination.
pub struct IndexCombinationBatcher {
    iter: SeekableIndexIterator,
}

impl IndexCombinationBatcher {
    pub fn new(features: &FeaturePools, max_depth: usize, start_offset: u64) -> Self {
        let n = features.descriptors().len();
        let iter = SeekableIndexIterator::starting_at(n, max_depth, start_offset as u128);
        Self { iter }
    }

    pub fn next_batch(&mut self, batch_size: usize) -> Option<Vec<IndexCombination>> {
        let mut batch = Vec::with_capacity(batch_size);
        if self.fill_batch(&mut batch, batch_size) {
            Some(batch)
        } else {
            None
        }
    }

    pub fn fill_batch(&mut self, batch: &mut Vec<IndexCombination>, batch_size: usize) -> bool {
        batch.clear();
        batch.reserve(batch_size);
        while batch.len() < batch_size {
            match self.iter.next() {
                Some(combo) => batch.push(combo),
                None => break,
            }
        }
        !batch.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feature::FeatureCategory;

    fn make_features(n: usize) -> Vec<FeatureDescriptor> {
        (0..n)
            .map(|i| FeatureDescriptor {
                name: format!("f{}", i),
                category: FeatureCategory::Boolean,
                note: "test".to_string(),
            })
            .collect()
    }

    #[test]
    fn combinations_for_depth_basic() {
        assert_eq!(combinations_for_depth(5, 1), 5);
        assert_eq!(combinations_for_depth(5, 2), 10);
        assert_eq!(combinations_for_depth(5, 3), 10);
        assert_eq!(combinations_for_depth(5, 4), 5);
        assert_eq!(combinations_for_depth(5, 5), 1);
    }

    #[test]
    fn combinations_for_depth_edge_cases() {
        assert_eq!(combinations_for_depth(5, 0), 1);
        assert_eq!(combinations_for_depth(0, 0), 1);
        assert_eq!(combinations_for_depth(5, 6), 0);
        assert_eq!(combinations_for_depth(0, 1), 0);
        assert_eq!(combinations_for_depth(1, 1), 1);
    }

    #[test]
    fn combinations_for_depth_large() {
        assert_eq!(combinations_for_depth(100, 2), 4950);
        assert_eq!(combinations_for_depth(100, 3), 161700);
        assert_eq!(combinations_for_depth(1000, 2), 499500);
    }

    #[test]
    fn combinations_for_depth_symmetry() {
        for n in 1..=20 {
            for k in 0..=n {
                assert_eq!(
                    combinations_for_depth(n, k),
                    combinations_for_depth(n, n - k),
                    "Symmetry failed for C({}, {})",
                    n,
                    k
                );
            }
        }
    }

    #[test]
    fn total_combinations_basic() {
        assert_eq!(total_combinations(5, 1), 5);
        assert_eq!(total_combinations(5, 2), 15);
        assert_eq!(total_combinations(5, 3), 25);
    }

    #[test]
    fn total_combinations_all_depths() {
        for n in 1..=10 {
            let total = total_combinations(n, n);
            let expected = (1u128 << n) - 1;
            assert_eq!(total, expected, "Failed for n={}", n);
        }
    }

    #[test]
    fn global_to_depth_basic() {
        assert_eq!(global_to_depth_and_local(0, 5, 3), Some((1, 0)));
        assert_eq!(global_to_depth_and_local(4, 5, 3), Some((1, 4)));
        assert_eq!(global_to_depth_and_local(5, 5, 3), Some((2, 0)));
        assert_eq!(global_to_depth_and_local(14, 5, 3), Some((2, 9)));
        assert_eq!(global_to_depth_and_local(15, 5, 3), Some((3, 0)));
        assert_eq!(global_to_depth_and_local(24, 5, 3), Some((3, 9)));
    }

    #[test]
    fn global_to_depth_beyond_total() {
        assert_eq!(global_to_depth_and_local(25, 5, 3), None);
        assert_eq!(global_to_depth_and_local(100, 5, 3), None);
    }

    #[test]
    fn global_to_depth_single_depth() {
        for i in 0..5 {
            assert_eq!(global_to_depth_and_local(i, 5, 1), Some((1, i)));
        }
        assert_eq!(global_to_depth_and_local(5, 5, 1), None);
    }

    #[test]
    fn unrank_depth_1() {
        assert_eq!(unrank_combination(0, 5, 1), vec![0]);
        assert_eq!(unrank_combination(1, 5, 1), vec![1]);
        assert_eq!(unrank_combination(2, 5, 1), vec![2]);
        assert_eq!(unrank_combination(3, 5, 1), vec![3]);
        assert_eq!(unrank_combination(4, 5, 1), vec![4]);
    }

    #[test]
    fn unrank_depth_2() {
        assert_eq!(unrank_combination(0, 5, 2), vec![0, 1]);
        assert_eq!(unrank_combination(1, 5, 2), vec![0, 2]);
        assert_eq!(unrank_combination(2, 5, 2), vec![0, 3]);
        assert_eq!(unrank_combination(3, 5, 2), vec![0, 4]);
        assert_eq!(unrank_combination(4, 5, 2), vec![1, 2]);
        assert_eq!(unrank_combination(5, 5, 2), vec![1, 3]);
        assert_eq!(unrank_combination(6, 5, 2), vec![1, 4]);
        assert_eq!(unrank_combination(7, 5, 2), vec![2, 3]);
        assert_eq!(unrank_combination(8, 5, 2), vec![2, 4]);
        assert_eq!(unrank_combination(9, 5, 2), vec![3, 4]);
    }

    #[test]
    fn unrank_depth_3() {
        assert_eq!(unrank_combination(0, 5, 3), vec![0, 1, 2]);
        assert_eq!(unrank_combination(1, 5, 3), vec![0, 1, 3]);
        assert_eq!(unrank_combination(2, 5, 3), vec![0, 1, 4]);
        assert_eq!(unrank_combination(3, 5, 3), vec![0, 2, 3]);
        assert_eq!(unrank_combination(4, 5, 3), vec![0, 2, 4]);
        assert_eq!(unrank_combination(5, 5, 3), vec![0, 3, 4]);
        assert_eq!(unrank_combination(6, 5, 3), vec![1, 2, 3]);
        assert_eq!(unrank_combination(7, 5, 3), vec![1, 2, 4]);
        assert_eq!(unrank_combination(8, 5, 3), vec![1, 3, 4]);
        assert_eq!(unrank_combination(9, 5, 3), vec![2, 3, 4]);
    }

    #[test]
    fn unrank_invalid_inputs() {
        let empty: Vec<usize> = vec![];
        assert_eq!(unrank_combination(0, 5, 0), empty);
        assert_eq!(unrank_combination(0, 5, 6), empty);
        assert_eq!(unrank_combination(10, 5, 2), empty);
        assert_eq!(unrank_combination(100, 5, 2), empty);
    }

    #[test]
    fn unrank_single_element() {
        let empty: Vec<usize> = vec![];
        assert_eq!(unrank_combination(0, 1, 1), vec![0]);
        assert_eq!(unrank_combination(1, 1, 1), empty);
    }

    #[test]
    fn unrank_all_elements() {
        let empty: Vec<usize> = vec![];
        assert_eq!(unrank_combination(0, 5, 5), vec![0, 1, 2, 3, 4]);
        assert_eq!(unrank_combination(1, 5, 5), empty);
    }

    #[test]
    fn rank_depth_1() {
        assert_eq!(rank_combination(&[0], 5), 0);
        assert_eq!(rank_combination(&[1], 5), 1);
        assert_eq!(rank_combination(&[4], 5), 4);
    }

    #[test]
    fn rank_depth_2() {
        assert_eq!(rank_combination(&[0, 1], 5), 0);
        assert_eq!(rank_combination(&[0, 4], 5), 3);
        assert_eq!(rank_combination(&[1, 2], 5), 4);
        assert_eq!(rank_combination(&[3, 4], 5), 9);
    }

    #[test]
    fn rank_depth_3() {
        assert_eq!(rank_combination(&[0, 1, 2], 5), 0);
        assert_eq!(rank_combination(&[2, 3, 4], 5), 9);
    }

    #[test]
    fn rank_unrank_roundtrip_small() {
        for n in 1..=8 {
            for k in 1..=n.min(4) {
                let count = combinations_for_depth(n, k);
                for rank in 0..count {
                    let combo = unrank_combination(rank, n, k);
                    let reranked = rank_combination(&combo, n);
                    assert_eq!(
                        rank, reranked,
                        "Roundtrip failed: n={}, k={}, rank={}, combo={:?}",
                        n, k, rank, combo
                    );
                }
            }
        }
    }

    #[test]
    fn rank_unrank_roundtrip_large() {
        let test_cases = [
            (100, 2, 0),
            (100, 2, 4949),
            (100, 3, 0),
            (100, 3, 161699),
            (1000, 2, 250000),
        ];

        for (n, k, rank) in test_cases {
            let combo = unrank_combination(rank, n, k);
            let reranked = rank_combination(&combo, n);
            assert_eq!(
                rank, reranked,
                "Roundtrip failed: n={}, k={}, rank={}",
                n, k, rank
            );
        }
    }

    #[test]
    fn unrank_rank_roundtrip_exhaustive_small() {
        for n in 2..=6 {
            for k in 1..=n.min(3) {
                let count = combinations_for_depth(n, k) as usize;
                for rank in 0..count {
                    let combo = unrank_combination(rank as u128, n, k);
                    assert_eq!(
                        combo.len(),
                        k,
                        "Wrong length for n={}, k={}, rank={}",
                        n,
                        k,
                        rank
                    );

                    for i in 0..k {
                        assert!(combo[i] < n, "Out of bounds: {:?}", combo);
                        if i > 0 {
                            assert!(combo[i] > combo[i - 1], "Not sorted: {:?}", combo);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn unrank_global_basic() {
        assert_eq!(unrank_global(0, 5, 2), Some(vec![0]));
        assert_eq!(unrank_global(4, 5, 2), Some(vec![4]));
        assert_eq!(unrank_global(5, 5, 2), Some(vec![0, 1]));
        assert_eq!(unrank_global(14, 5, 2), Some(vec![3, 4]));
    }

    #[test]
    fn unrank_global_beyond_total() {
        assert_eq!(unrank_global(15, 5, 2), None);
        assert_eq!(unrank_global(100, 5, 2), None);
    }

    #[test]
    fn seekable_from_start() {
        let features = make_features(5);
        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, 0);

        for i in 0..5 {
            let combo = iter.next().expect("Should have combo");
            assert_eq!(combo.len(), 1);
            assert_eq!(combo[0].name, format!("f{}", i));
        }

        let combo = iter.next().expect("Should have combo");
        assert_eq!(combo.len(), 2);
        assert_eq!(combo[0].name, "f0");
        assert_eq!(combo[1].name, "f1");
    }

    #[test]
    fn seekable_from_middle() {
        let features = make_features(5);

        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, 5);

        let combo = iter.next().expect("Should have combo");
        assert_eq!(combo.len(), 2);
        assert_eq!(combo[0].name, "f0");
        assert_eq!(combo[1].name, "f1");
    }

    #[test]
    fn seekable_from_end() {
        let features = make_features(5);

        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, 14);

        let combo = iter.next().expect("Should have combo");
        assert_eq!(combo.len(), 2);
        assert_eq!(combo[0].name, "f3");
        assert_eq!(combo[1].name, "f4");

        assert!(iter.next().is_none());
    }

    #[test]
    fn seekable_beyond_end() {
        let features = make_features(5);

        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, 100);
        assert!(iter.next().is_none());
    }

    #[test]
    fn seekable_matches_sequential() {
        let features = make_features(6);

        let sequential: Vec<_> = CombinationIterator::new(&features, 3).collect();

        for offset in [0u128, 1, 5, 6, 10, 15, 20, 25] {
            if offset as usize >= sequential.len() {
                continue;
            }
            let seekable: Vec<_> =
                SeekableCombinationIterator::starting_at(&features, 3, offset).collect();
            assert_eq!(
                seekable,
                sequential[offset as usize..].to_vec(),
                "Mismatch at offset {}",
                offset
            );
        }
    }

    #[test]
    fn seekable_all_offsets_match() {
        let features = make_features(5);
        let sequential: Vec<_> = CombinationIterator::new(&features, 2).collect();
        let total = sequential.len();

        for offset in 0..=total {
            let seekable: Vec<_> =
                SeekableCombinationIterator::starting_at(&features, 2, offset as u128).collect();

            if offset < total {
                assert_eq!(
                    seekable,
                    sequential[offset..].to_vec(),
                    "Mismatch at offset {}",
                    offset
                );
            } else {
                assert!(seekable.is_empty(), "Should be empty at offset {}", offset);
            }
        }
    }

    #[test]
    fn seekable_current_global_index() {
        let features = make_features(5);
        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, 0);

        for expected_index in 0..15u128 {
            assert_eq!(
                iter.current_global_index(),
                Some(expected_index),
                "Wrong index at step {}",
                expected_index
            );
            iter.next();
        }
        assert_eq!(iter.current_global_index(), None);
    }

    #[test]
    fn seekable_empty_features() {
        let features: Vec<FeatureDescriptor> = vec![];
        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, 0);
        assert!(iter.next().is_none());
    }

    #[test]
    fn seekable_single_feature() {
        let features = make_features(1);
        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, 0);

        let combo = iter.next().expect("Should have one combo");
        assert_eq!(combo.len(), 1);
        assert_eq!(combo[0].name, "f0");

        assert!(iter.next().is_none());
    }

    #[test]
    fn seekable_depth_transition() {
        let features = make_features(4);

        let mut iter = SeekableCombinationIterator::starting_at(&features, 3, 3);

        let combo = iter.next().expect("Should have combo");
        assert_eq!(combo.len(), 1);
        assert_eq!(combo[0].name, "f3");

        let combo = iter.next().expect("Should have combo");
        assert_eq!(combo.len(), 2);
        assert_eq!(combo[0].name, "f0");
        assert_eq!(combo[1].name, "f1");
    }

    #[test]
    fn batcher_from_start() {
        let features = make_features(5);
        let pools = FeaturePools::new(features);
        let mut batcher = CombinationBatcher::new(&pools, 2, 0);

        let batch = batcher.next_batch(3).expect("Should have batch");
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].len(), 1);
        assert_eq!(batch[0][0].name, "f0");
    }

    #[test]
    fn batcher_with_offset() {
        let features = make_features(5);
        let pools = FeaturePools::new(features);

        let mut batcher = CombinationBatcher::new(&pools, 2, 5);

        let batch = batcher.next_batch(2).expect("Should have batch");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].len(), 2);
        assert_eq!(batch[0][0].name, "f0");
        assert_eq!(batch[0][1].name, "f1");
    }

    #[test]
    fn batcher_exhaustion() {
        let features = make_features(3);
        let pools = FeaturePools::new(features);
        let mut batcher = CombinationBatcher::new(&pools, 1, 0);

        let batch = batcher.next_batch(10).expect("Should have batch");
        assert_eq!(batch.len(), 3);

        assert!(batcher.next_batch(10).is_none());
    }

    #[test]
    fn batcher_matches_sequential_collection() {
        let features = make_features(6);
        let pools = FeaturePools::new(features.clone());

        let mut batcher = CombinationBatcher::new(&pools, 2, 0);
        let mut batcher_combos = Vec::new();
        while let Some(batch) = batcher.next_batch(5) {
            batcher_combos.extend(batch);
        }

        let sequential: Vec<_> = CombinationIterator::new(&features, 2).collect();

        assert_eq!(batcher_combos, sequential);
    }

    #[test]
    fn batcher_with_large_offset() {
        let features = make_features(10);
        let pools = FeaturePools::new(features);

        let mut batcher = CombinationBatcher::new(&pools, 2, 50);

        let batch = batcher.next_batch(10).expect("Should have batch");
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn seekable_handles_larger_feature_set() {
        let features = make_features(100);

        let offset = 2575u128;
        let mut iter = SeekableCombinationIterator::starting_at(&features, 2, offset);

        let combo = iter.next().expect("Should have combo");
        assert_eq!(combo.len(), 2);

        assert!(combo[0].name.starts_with("f"));
        assert!(combo[1].name.starts_with("f"));
    }

    #[test]
    fn unrank_handles_depth_4() {
        let combo = unrank_combination(2422, 20, 4);
        assert_eq!(combo.len(), 4);

        let rank = rank_combination(&combo, 20);
        assert_eq!(rank, 2422);
    }

    #[test]
    fn unrank_handles_depth_5() {
        let combo = unrank_combination(71253, 30, 5);
        assert_eq!(combo.len(), 5);

        let rank = rank_combination(&combo, 30);
        assert_eq!(rank, 71253);
    }

    #[test]
    fn seekable_max_depth_exceeds_features() {
        let features = make_features(3);
        let combos: Vec<_> = SeekableCombinationIterator::starting_at(&features, 10, 0).collect();

        assert_eq!(combos.len(), 7);
    }

    #[test]
    fn seekable_max_depth_zero_treated_as_one() {
        let features = make_features(3);
        let combos: Vec<_> = SeekableCombinationIterator::new(&features, 0).collect();

        assert_eq!(combos.len(), 3);
    }

    #[test]
    fn combinations_boundary_values() {
        let large = combinations_for_depth(10000, 6);
        assert!(large > 0);
        assert!(large < u128::MAX / 2);
    }
}
