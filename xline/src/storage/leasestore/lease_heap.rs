use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
};

use tokio::time::Instant;

/// Lease id and expiration time
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct LeaseWithTime {
    /// Lease id
    lease_id: i64,
    /// Expiration time
    expiry: Instant,
}

impl LeaseWithTime {
    /// New `LeaseWithTime`
    pub(crate) fn new(lease_id: i64, expiry: Instant) -> Self {
        Self { lease_id, expiry }
    }

    /// Check if lease is expired
    pub(crate) fn expired(&self) -> bool {
        self.expiry <= Instant::now()
    }

    /// Lease id
    pub(crate) fn id(&self) -> i64 {
        self.lease_id
    }
}

impl PartialOrd for LeaseWithTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LeaseWithTime {
    fn cmp(&self, other: &Self) -> Ordering {
        other.expiry.cmp(&self.expiry)
    }
}

/// Updatable binary heap of lease
#[derive(Debug)]
pub(crate) struct LeaseHeap {
    /// Backup of lease heap, used to update lease heap
    map: HashMap<i64, LeaseWithTime>,
    /// Lease heap
    heap: BinaryHeap<LeaseWithTime>,
}

impl LeaseHeap {
    /// New `LeaseHeap`
    pub(crate) fn new() -> Self {
        Self {
            map: HashMap::new(),
            heap: BinaryHeap::new(),
        }
    }

    /// Insert or update lease
    pub(crate) fn insert_or_update(&mut self, le: LeaseWithTime) {
        if self.map.insert(le.lease_id, le).is_some() {
            self.heap = self.map.values().copied().collect();
        } else {
            self.heap.push(le);
        }
    }

    /// Return the smallest item in the binary heap, or None if it is empty.
    pub(crate) fn peek(&self) -> Option<&LeaseWithTime> {
        self.heap.peek()
    }

    /// Remove the lease with the smallest expiry time
    pub(crate) fn pop(&mut self) -> Option<LeaseWithTime> {
        let item = self.heap.pop()?;
        let _igonore = self.map.remove(&item.lease_id);
        Some(item)
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    #[test]
    fn test_lease_heap() {
        let mut hp = LeaseHeap {
            map: HashMap::new(),
            heap: BinaryHeap::new(),
        };

        let l1 = LeaseWithTime {
            lease_id: 1,
            expiry: Instant::now() + Duration::from_secs(8),
        };
        hp.insert_or_update(l1);
        assert_eq!(hp.heap.len(), 1);
        assert_eq!(hp.peek(), Some(&l1));

        let l2 = LeaseWithTime {
            lease_id: 2,
            expiry: Instant::now() + Duration::from_secs(10),
        };
        hp.insert_or_update(l2);
        assert_eq!(hp.heap.len(), 2);
        assert_eq!(hp.peek(), Some(&l1));

        let l3 = LeaseWithTime {
            lease_id: 1,
            expiry: Instant::now() + Duration::from_secs(1),
        };
        hp.insert_or_update(l3);
        assert_eq!(hp.heap.len(), 2);
        assert_eq!(hp.peek(), Some(&l3));
    }
}
