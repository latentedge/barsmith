/// Per-batch timing snapshot used by the batch tuner.
#[derive(Clone, Debug)]
pub(crate) struct BatchTimingSnapshot {
    pub(crate) enumeration_ms: u64,
    pub(crate) filter_ms: u64,
    pub(crate) eval_ms: u64,
    pub(crate) ingest_ms: u64,
    pub(crate) prune_subset_ms: u64,
    pub(crate) prune_struct_ms: u64,
}

/// Batch-size tuner that only changes chunking, never the search space.
#[derive(Clone, Debug)]
pub(crate) struct BatchTuner {
    min_batch: usize,
    max_batch: usize,
    shrink_factor: f32,
    grow_factor: f32,
    target_total_ms: f32,
    history_len: usize,
}

impl BatchTuner {
    pub(crate) fn new(initial_batch: usize) -> Self {
        let hard_max = initial_batch.saturating_mul(4).max(200_000usize);
        let min_batch = initial_batch.max(1);
        let max_batch = hard_max.max(initial_batch.max(1));
        Self {
            min_batch,
            max_batch,
            shrink_factor: 0.5,
            grow_factor: 2.0,
            target_total_ms: 15_000.0,
            history_len: 5,
        }
    }

    pub(crate) fn recommend(
        &self,
        current_batch: usize,
        snapshots: &[BatchTimingSnapshot],
    ) -> usize {
        if snapshots.is_empty() {
            return current_batch.max(1);
        }

        let len = snapshots.len();
        let start = len.saturating_sub(self.history_len);
        let window = &snapshots[start..];

        let mut sum_enum = 0u64;
        let mut sum_filter = 0u64;
        let mut sum_eval = 0u64;
        let mut sum_ingest = 0u64;
        let mut _sum_subset = 0u64;
        let mut _sum_struct = 0u64;
        for snap in window {
            sum_enum += snap.enumeration_ms;
            sum_filter += snap.filter_ms;
            sum_eval += snap.eval_ms;
            sum_ingest += snap.ingest_ms;
            _sum_subset += snap.prune_subset_ms;
            _sum_struct += snap.prune_struct_ms;
        }

        let count = window.len() as u64;
        if count == 0 {
            return current_batch.max(1);
        }

        let mean_total_ms: f32 =
            (sum_enum + sum_filter + sum_eval + sum_ingest) as f32 / count as f32;

        let mut proposed = current_batch.max(1);
        let hi = self.target_total_ms * 2.0;
        let lo = self.target_total_ms * 0.5;

        if mean_total_ms > hi {
            let shrunk = (proposed as f32 * self.shrink_factor).round() as usize;
            if shrunk < proposed {
                proposed = shrunk.max(1);
            }
        } else if mean_total_ms < lo {
            let grown = (proposed as f32 * self.grow_factor).round() as usize;
            if grown > proposed {
                proposed = grown;
            }
        }

        proposed.clamp(self.min_batch, self.max_batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(filter_ms: u64, eval_ms: u64) -> BatchTimingSnapshot {
        BatchTimingSnapshot {
            enumeration_ms: 10,
            filter_ms,
            eval_ms,
            ingest_ms: 0,
            prune_subset_ms: filter_ms,
            prune_struct_ms: 0,
        }
    }

    #[test]
    fn batch_tuner_keeps_batch_when_no_history() {
        let tuner = BatchTuner::new(50_000);
        let recommended = tuner.recommend(50_000, &[]);
        assert_eq!(
            recommended, 50_000,
            "without history, tuner should keep the current batch size"
        );
    }

    #[test]
    fn batch_tuner_grows_in_reuse_heavy_region() {
        let tuner = BatchTuner::new(50_000);
        let snaps = vec![snapshot(10, 0), snapshot(12, 0), snapshot(8, 0)];
        let recommended = tuner.recommend(50_000, &snaps);
        assert!(
            recommended > 50_000,
            "cheap reuse region should allow the batch size to grow"
        );
    }

    #[test]
    fn batch_tuner_shrinks_when_filter_dominates() {
        let tuner = BatchTuner::new(100_000);
        let snaps = vec![
            snapshot(25_000, 15_000),
            snapshot(28_000, 12_000),
            snapshot(26_000, 14_000),
        ];
        let recommended = tuner.recommend(200_000, &snaps);
        assert!(
            recommended < 200_000,
            "filter-bound batches should trigger a shrink recommendation"
        );
        assert!(
            recommended >= 100_000,
            "shrink recommendation should not go below the configured floor"
        );
    }

    #[test]
    fn batch_tuner_grows_when_both_filter_and_eval_are_cheap() {
        let tuner = BatchTuner::new(20_000);
        let snaps = vec![snapshot(100, 200), snapshot(80, 150), snapshot(90, 100)];
        let recommended = tuner.recommend(20_000, &snaps);
        assert!(
            recommended > 20_000,
            "balanced cheap region should allow the batch size to grow"
        );
    }

    #[test]
    fn batch_tuner_respects_min_and_max_bounds() {
        let tuner = BatchTuner::new(10_000);
        let snaps = vec![
            snapshot(5_000, 10),
            snapshot(4_800, 20),
            snapshot(4_900, 15),
        ];
        let recommended = tuner.recommend(10_000, &snaps);
        assert!(
            recommended >= tuner.min_batch,
            "recommended batch size should never fall below the configured min_batch"
        );

        let tuner_large = BatchTuner::new(300_000);
        let snaps_large = vec![snapshot(10, 0), snapshot(12, 0), snapshot(8, 0)];
        let recommended_large = tuner_large.recommend(300_000, &snaps_large);
        assert!(
            recommended_large <= tuner_large.max_batch,
            "recommended batch size should never exceed the configured max_batch"
        );
    }
}
