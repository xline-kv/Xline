use std::{cmp::Reverse, ops::Add, sync::Arc, time::Duration};

use parking_lot::RwLock;
use priority_queue::PriorityQueue;
use tokio::time::Instant;

/// Ref to lease manager
pub(crate) type LeaseManagerRef = Arc<RwLock<LeaseManager>>;

/// Default lease ttl
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(8);

/// Lease manager
pub(crate) struct LeaseManager {
    /// client_id => expired_at
    /// expiry queue to check the smallest expired_at
    pub(super) expiry_queue: PriorityQueue<u64, Reverse<Instant>>,
}

impl LeaseManager {
    /// Create a new lease manager
    pub(crate) fn new() -> Self {
        Self {
            expiry_queue: PriorityQueue::new(),
        }
    }

    /// Check if the client is alive
    pub(crate) fn check_alive(&self, client_id: u64) -> bool {
        if let Some(expired_at) = self.expiry_queue.get(&client_id).map(|(_, v)| v.0) {
            expired_at > Instant::now()
        } else {
            false
        }
    }

    /// Generate a new client id and grant a lease
    pub(crate) fn grant(&mut self) -> u64 {
        let mut client_id: u64 = rand::random();
        while self.expiry_queue.get(&client_id).is_some() {
            client_id = rand::random();
        }
        let expiry = Instant::now().add(DEFAULT_LEASE_TTL);
        let _ig = self.expiry_queue.push(client_id, Reverse(expiry));
        // gc all expired client id while granting a new client id
        self.gc_expired();
        client_id
    }

    /// GC the expired client ids
    pub(crate) fn gc_expired(&mut self) {
        while let Some(expiry) = self.expiry_queue.peek().map(|(_, v)| v.0) {
            if expiry > Instant::now() {
                return;
            }
            let _ig = self.expiry_queue.pop();
        }
    }

    /// Renew a client id
    pub(crate) fn renew(&mut self, client_id: u64) {
        let expiry = Instant::now().add(DEFAULT_LEASE_TTL);
        let _ig = self
            .expiry_queue
            .change_priority(&client_id, Reverse(expiry));
    }

    /// Clear, called when leader retires
    pub(crate) fn clear(&mut self) {
        self.expiry_queue.clear();
    }

    /// Revoke a lease
    pub(crate) fn revoke(&mut self, client_id: u64) {
        let _ig = self.expiry_queue.remove(&client_id);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic_lease_manager() {
        let mut lm = LeaseManager::new();

        let client_id = lm.grant();
        assert!(lm.check_alive(client_id));
        lm.revoke(client_id);
        assert!(!lm.check_alive(client_id));
    }

    #[tokio::test]
    async fn test_lease_expire() {
        let mut lm = LeaseManager::new();

        let client_id = lm.grant();
        assert!(lm.check_alive(client_id));
        tokio::time::sleep(DEFAULT_LEASE_TTL).await;
        assert!(!lm.check_alive(client_id));
    }

    #[tokio::test]
    async fn test_renew_lease() {
        let mut lm = LeaseManager::new();

        let client_id = lm.grant();
        assert!(lm.check_alive(client_id));
        tokio::time::sleep(DEFAULT_LEASE_TTL / 2).await;
        lm.renew(client_id);
        tokio::time::sleep(DEFAULT_LEASE_TTL / 2).await;
        assert!(lm.check_alive(client_id));
    }
}
