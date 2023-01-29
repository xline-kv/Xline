use std::{cmp::Reverse, time::Instant};

use priority_queue::PriorityQueue;

/// Priority queue of lease
#[derive(Debug)]
pub(super) struct LeaseQueue {
    /// Inner queue of lease queue
    inner: PriorityQueue<i64, Reverse<Instant>>,
}

impl LeaseQueue {
    /// New `LeaseQueue`
    pub(super) fn new() -> Self {
        Self {
            inner: PriorityQueue::new(),
        }
    }

    /// Insert lease
    pub(super) fn insert(&mut self, lease_id: i64, expiry: Instant) -> Option<Instant> {
        self.inner.push(lease_id, Reverse(expiry)).map(|v| v.0)
    }

    /// Update lease
    pub(super) fn update(&mut self, lease_id: i64, expiry: Instant) -> Option<Instant> {
        self.inner
            .change_priority(&lease_id, Reverse(expiry))
            .map(|v| v.0)
    }

    /// Return the smallest expiry in the queue, or None if it is empty.
    pub(super) fn peek(&self) -> Option<&Instant> {
        self.inner.peek().map(|(_, v)| &v.0)
    }

    /// Remove the lease id with the smallest expiry time
    pub(super) fn pop(&mut self) -> Option<i64> {
        self.inner.pop().map(|(k, _)| k)
    }

    /// Clear the lease heap
    pub(super) fn clear(&mut self) {
        self.inner.clear();
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    #[test]
    fn test_lease_heap() {
        let mut hp = LeaseQueue::new();

        let expiry1 = Instant::now() + Duration::from_secs(8);
        assert!(hp.insert(1, expiry1).is_none());
        assert_eq!(hp.inner.len(), 1);
        assert_eq!(hp.peek(), Some(&expiry1));

        let expiry2 = Instant::now() + Duration::from_secs(10);
        assert!(hp.insert(2, expiry2).is_none());
        assert_eq!(hp.inner.len(), 2);
        assert_eq!(hp.peek(), Some(&expiry1));

        let expiry3 = Instant::now() + Duration::from_secs(1);
        assert_eq!(hp.update(1, expiry3), Some(expiry1));
        assert_eq!(hp.inner.len(), 2);
        assert_eq!(hp.peek(), Some(&expiry3));
    }
}
