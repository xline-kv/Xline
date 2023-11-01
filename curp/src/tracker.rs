#![allow(unused)] // TODO remove

use std::collections::VecDeque;
use std::ops::{AddAssign, Sub};

/// Bits of usize
const USIZE_BITS: usize = std::mem::size_of::<usize>() * 8;

/// A one-direction bit vector queue
/// It use a ring buffer `VecDeque<usize>` to store bits
///
/// Memory Layout:
///
/// `010000000101_110111011111_000001000000000`
///  ^        ^                ^    ^
///  |        |________________|____|
///  |________|         len    |____|
///     head                    tail
#[derive(Debug, Default)]
struct BitVecQueue {
    /// Bits store
    store: VecDeque<usize>,
    /// Head length indicator
    head: usize,
    /// Tail length indicator
    tail: usize,
}

#[allow(clippy::integer_arithmetic, clippy::unwrap_used)] // They are checked
impl BitVecQueue {
    /// New with a capacity (in bits)
    fn with_capacity(cap_bits: usize) -> Self {
        Self {
            store: VecDeque::with_capacity(cap_bits / USIZE_BITS),
            head: 0,
            tail: 0,
        }
    }

    /// Get the length
    fn len(&self) -> usize {
        if self.store.is_empty() {
            return 0;
        }
        (self.store.len() - 1) * USIZE_BITS + self.tail - self.head + 1
    }

    /// Get the bit value
    fn get(&self, idx: usize) -> Option<bool> {
        let idx = self.head + idx;
        let index = idx / USIZE_BITS;
        let slot = idx % USIZE_BITS;
        if index == self.store.len() - 1 && slot > self.tail {
            return None;
        }
        let bits = self.store.get(index)?;
        Some(bits & (1 << (USIZE_BITS - 1 - slot)) != 0)
    }

    /// Set the bit value
    fn set(&mut self, idx: usize, v: bool) {
        let idx = self.head + idx;
        let index = idx / USIZE_BITS;
        let slot = idx % USIZE_BITS;
        if index == self.store.len() - 1 && slot > self.tail {
            return;
        }
        let Some(bits) = self.store.get_mut(index) else {
            return;
        };
        if v {
            *bits |= 1 << (USIZE_BITS - 1 - slot);
        } else {
            *bits &= !(1 << (USIZE_BITS - 1 - slot));
        }
    }

    /// Push a value in the back
    fn push(&mut self, v: bool) {
        if self.tail == (USIZE_BITS - 1) || self.store.is_empty() {
            self.tail = 0;
            if v {
                self.store.push_back(1 << (USIZE_BITS - 1));
            } else {
                self.store.push_back(0);
            }
            return;
        }
        self.tail += 1;
        if v {
            let last = self.store.back_mut().unwrap();
            *last |= 1 << (USIZE_BITS - 1 - self.tail);
        }
    }

    /// Peek the front of the bit queue
    fn front(&self) -> Option<bool> {
        let front = self.store.front()?;
        Some(front & (1 << (USIZE_BITS - 1 - self.head)) != 0)
    }

    /// Pop a value from front
    fn pop(&mut self) {
        let len = self.store.len();
        if self.head == (USIZE_BITS - 1) || len == 0 {
            self.head = 0;
            let _ig = self.store.pop_front();
            return;
        }
        if len == 1 && self.head == self.tail {
            self.clear();
        }
        self.head += 1;
    }

    /// Clear the bit queue
    fn clear(&mut self) {
        self.head = 0;
        self.tail = 0;
        self.store.clear();
    }
}

/// Track sequence number for commands
#[derive(Debug, Default)]
pub(super) struct Tracker {
    /// First incomplete seq num, it will be advanced by client
    first_incomplete: u64,
    /// inflight seq nums proposed by the client, each bit
    /// represent the received status starting from `first_incomplete`
    inflight: BitVecQueue,
}

impl Tracker {
    /// Record a sequence number, return whether it is duplicated
    #[allow(clippy::as_conversions)]
    #[allow(clippy::cast_possible_truncation)] // TODO: support 32 bits computers?
    pub(crate) fn record(&mut self, seq_num: u64) -> bool {
        if seq_num < self.first_incomplete {
            return true;
        }
        let gap = seq_num.sub(self.first_incomplete) as usize;
        if gap == 0 {
            // received the next sequence number, advanced the first_incomplete
            // and pop the front of inflight
            self.first_incomplete.add_assign(1);
            self.inflight.pop();
        } else if gap < self.inflight.len() {
            // received the sequence number that is recorded in inflight
            // check its status to determine whether it is duplicated
            if self.inflight.get(gap) == Some(true) {
                return true;
            }
            // mark it is received
            self.inflight.set(gap, true);
        } else {
            // received the sequence number that exceed inflight, extend
            // the inflight and record the inflight[gap] as received
            for _ in 0..gap.sub(self.inflight.len()) {
                self.inflight.push(false);
            }
            self.inflight.push(true);
        }
        while self.inflight.front() == Some(true) {
            self.inflight.pop();
            self.first_incomplete.add_assign(1);
        }
        false
    }

    /// Advance first incomplete without a check
    pub(crate) fn must_advance_to(&mut self, first_incomplete: u64) {
        if self.first_incomplete >= first_incomplete {
            return;
        }
        for _ in 0..first_incomplete.sub(self.first_incomplete) {
            self.inflight.pop();
            self.first_incomplete.add_assign(1);
        }
        while self.inflight.front() == Some(true) {
            self.inflight.pop();
            self.first_incomplete.add_assign(1);
        }
    }

    /// Reset the tracker
    pub(crate) fn reset(&mut self) {
        self.first_incomplete = 0;
        self.inflight.clear();
    }

    /// Get the first incomplete sequence number
    pub(crate) fn first_incomplete(&self) -> u64 {
        self.first_incomplete
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_check_not_duplicate_ordered() {
        let mut tracker = Tracker::default();
        for i in 0..1024 {
            assert!(!tracker.record(i));
            assert_eq!(tracker.first_incomplete, i + 1);
            assert!(tracker.inflight.len() <= 1);
        }
    }

    #[test]
    fn test_check_duplicate_ordered() {
        let mut tracker = Tracker::default();
        for i in 0..512 {
            assert!(!tracker.record(i));
            assert_eq!(tracker.first_incomplete, i + 1);
            assert!(tracker.inflight.len() <= 1);
        }
        for i in 0..512 {
            assert!(tracker.record(i));
        }
    }

    #[test]
    fn test_check_duplicate_gap() {
        let mut tracker = Tracker::default();
        assert!(!tracker.record(0));
        assert!(!tracker.record(1));
        assert!(!tracker.record(1000));
        assert!(!tracker.record(1001));

        assert!(tracker.record(0));
        assert!(tracker.record(1));
        assert!(tracker.record(1000));
        assert!(tracker.record(1001));
        assert_eq!(tracker.first_incomplete, 2);
    }

    #[test]
    fn test_check_duplicate_clear_inflight() {
        let mut tracker = Tracker::default();
        for i in (1..256).step_by(2) {
            assert!(!tracker.record(i));
        }
        for i in (0..256).step_by(2) {
            assert!(!tracker.record(i));
        }
        assert_eq!(tracker.inflight.len(), 0);
    }

    #[test]
    fn test_must_advance_first_incomplete() {
        let mut tracker = Tracker::default();
        tracker.record(5);
        tracker.record(6);
        tracker.record(8);
        tracker.must_advance_to(5);
        assert_eq!(tracker.first_incomplete, 7);
    }
}
