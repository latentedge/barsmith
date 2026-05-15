use crate::config::Config;

#[derive(Debug)]
pub struct ProgressTracker {
    processed: usize,
    limit: Option<usize>,
    start_offset: usize,
}

impl ProgressTracker {
    pub fn new(config: &Config) -> Self {
        let start_offset = config.resume_offset as usize;
        Self {
            processed: start_offset,
            limit: config.max_combos,
            start_offset,
        }
    }

    pub fn processed(&self) -> usize {
        self.processed
    }

    pub fn start_offset(&self) -> usize {
        self.start_offset
    }

    pub fn processed_since_start(&self) -> usize {
        self.processed.saturating_sub(self.start_offset)
    }

    pub fn next_batch_size(&self, requested: usize) -> usize {
        match self.limit {
            Some(limit) => requested.min(limit.saturating_sub(self.processed)),
            None => requested,
        }
    }

    pub fn record_batch(&mut self, batch_size: usize) -> bool {
        self.processed += batch_size;
        self.limit.is_none_or(|limit| self.processed < limit)
    }
}

#[cfg(test)]
mod tests {
    use super::ProgressTracker;

    fn tracker(processed: usize, limit: Option<usize>) -> ProgressTracker {
        ProgressTracker {
            processed,
            limit,
            start_offset: processed,
        }
    }

    #[test]
    fn next_batch_size_caps_to_remaining_limit() {
        let tracker = tracker(8, Some(10));

        assert_eq!(tracker.next_batch_size(16), 2);
    }

    #[test]
    fn next_batch_size_returns_zero_after_limit() {
        let tracker = tracker(10, Some(10));

        assert_eq!(tracker.next_batch_size(16), 0);
    }

    #[test]
    fn next_batch_size_preserves_requested_size_without_limit() {
        let tracker = tracker(8, None);

        assert_eq!(tracker.next_batch_size(16), 16);
    }
}
