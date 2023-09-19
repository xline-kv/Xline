#![allow(unused)] // TODO: remove

use std::collections::VecDeque;
use std::ops::AddAssign;
use std::sync::atomic::AtomicU64;

/// Track requests for client
pub(crate) struct RequestTracker {
    /// First incomplete cmd
    first_incomplete: u64,
    /// Inflight seq nums proposed by the client
    seq_nums: VecDeque<Option<u64>>,
    /// The last sent seq num
    last_sent: AtomicU64,
}

impl RequestTracker {
    /// Create a new request tracker
    pub(crate) fn new() -> Self {
        Self {
            first_incomplete: 0,
            seq_nums: VecDeque::new(),
            last_sent: AtomicU64::new(0),
        }
    }

    /// Create a seq num and record it
    pub(crate) fn new_seq_num(&mut self) -> u64 {
        let last_sent = self
            .last_sent
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.seq_nums.push_back(Some(last_sent));
        last_sent
    }

    /// Record a completed seq num
    pub(crate) fn record_complete(&mut self, seq_num: u64) {
        let slot = self
            .seq_nums
            .iter_mut()
            .find(|num| num.is_some_and(|inner| inner == seq_num));
        if let Some(slot) = slot {
            let _ig = slot.take();
        }
        while let Some(&None) = self.seq_nums.front() {
            let _ig = self.seq_nums.pop_front();
            self.first_incomplete.add_assign(1);
        }
    }

    /// Get the first incomplete
    pub(crate) fn first_incomplete(&self) -> u64 {
        self.first_incomplete
    }
}

#[cfg(test)]
mod test {
    use crate::request_tracker::RequestTracker;

    #[test]
    fn test_advance_first_incomplete() {
        let mut tracker = RequestTracker::new();
        tracker.new_seq_num();
        tracker.new_seq_num();
        tracker.record_complete(0);
        tracker.record_complete(1);
        assert_eq!(tracker.first_incomplete(), 2);
    }

    #[test]
    fn test_not_advance_first_incomplete_when_meet_gap() {
        let mut tracker = RequestTracker::new();
        tracker.new_seq_num();
        tracker.new_seq_num();
        tracker.new_seq_num();
        tracker.record_complete(0);
        tracker.record_complete(2);
        assert_eq!(tracker.first_incomplete(), 1);
    }

    #[test]
    fn test_advance_first_incomplete_when_gap_fill() {
        let mut tracker = RequestTracker::new();
        for _ in 0..100 {
            tracker.new_seq_num();
        }
        for i in (1..100).step_by(2) {
            tracker.record_complete(i);
        }
        assert_eq!(tracker.first_incomplete(), 0);
        for i in (0..100).step_by(2) {
            tracker.record_complete(i);
            assert_eq!(tracker.first_incomplete(), i + 2);
        }
    }
}
