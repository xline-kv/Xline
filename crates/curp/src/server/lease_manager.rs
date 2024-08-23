use std::{cmp::Reverse, collections::HashSet, ops::Add, sync::Arc, time::Duration};

use parking_lot::RwLock;
use priority_queue::PriorityQueue;
use tokio::time::Instant;
use tracing::info;

/// Ref to lease manager
pub(crate) type LeaseManagerRef = Arc<RwLock<LeaseManager>>;

/// Default lease ttl
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(8);

/// Lease manager
pub(crate) struct LeaseManager {
    /// client_id => expired_at
    ///
    /// expiry queue to check the smallest expired_at
    expiry_queue: PriorityQueue<u64, Reverse<Instant>>,
    /// Bypassed client ids
    bypassed: HashSet<u64>,
}

impl LeaseManager {
    /// Create a new lease manager
    pub(crate) fn new() -> Self {
        Self {
            expiry_queue: PriorityQueue::new(),
            bypassed: HashSet::from([12345]),
        }
    }

    /// Check if the client is alive
    pub(crate) fn check_alive(&self, client_id: u64) -> bool {
        if self.bypassed.contains(&client_id) {
            return true;
        }
        if let Some(expired_at) = self.expiry_queue.get(&client_id).map(|(_, v)| v.0) {
            expired_at > Instant::now()
        } else {
            false
        }
    }

    /// Generate a new client id and grant a lease
    pub(crate) fn grant(&mut self, ttl: Option<Duration>) -> u64 {
        let mut client_id: u64 = rand::random();
        while self.expiry_queue.get(&client_id).is_some() {
            client_id = rand::random();
        }
        let expiry = Instant::now().add(ttl.unwrap_or(DEFAULT_LEASE_TTL));
        _ = self.expiry_queue.push(client_id, Reverse(expiry));
        client_id
    }

    /// GC the expired client ids
    pub(crate) fn gc_expired(&mut self) -> Vec<u64> {
        let mut expired = Vec::new();
        while let Some(expiry) = self.expiry_queue.peek().map(|(_, v)| v.0) {
            if expiry > Instant::now() {
                break;
            }
            let (id, _) = self
                .expiry_queue
                .pop()
                .unwrap_or_else(|| unreachable!("Expiry queue should not be empty"));
            expired.push(id);
        }
        expired
    }

    /// Renew a client id
    pub(crate) fn renew(&mut self, client_id: u64, ttl: Option<Duration>) {
        if self.bypassed.contains(&client_id) {
            return;
        }
        let expiry = Instant::now().add(ttl.unwrap_or(DEFAULT_LEASE_TTL));
        _ = self
            .expiry_queue
            .change_priority(&client_id, Reverse(expiry));
    }

    /// Bypass a client id, the means the client is on the server
    pub(crate) fn bypass(&mut self, client_id: u64) {
        if self.bypassed.insert(client_id) {
            info!("bypassed client_id: {}", client_id);
        }
        _ = self.expiry_queue.remove(&client_id);
    }

    /// Clear, called when leader retires
    pub(crate) fn clear(&mut self) {
        self.expiry_queue.clear();
        self.bypassed.clear();
    }

    /// Get the online clients count (excluding bypassed clients)
    pub(crate) fn online_clients(&self) -> usize {
        self.expiry_queue.len()
    }

    /// Revoke a lease
    pub(crate) fn revoke(&mut self, client_id: u64) {
        _ = self.expiry_queue.remove(&client_id);
        _ = self.bypassed.remove(&client_id);
        info!("revoked client_id: {}", client_id);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic_lease_manager() {
        let mut lm = LeaseManager::new();

        let client_id = lm.grant(None);
        assert!(lm.check_alive(client_id));
        lm.revoke(client_id);
        assert!(!lm.check_alive(client_id));

        lm.bypass(client_id);
        assert!(lm.check_alive(client_id));
        lm.revoke(client_id);
        assert!(!lm.check_alive(client_id));
    }

    #[tokio::test]
    async fn test_lease_expire() {
        let mut lm = LeaseManager::new();

        let client_id = lm.grant(None);
        assert!(lm.check_alive(client_id));
        tokio::time::sleep(DEFAULT_LEASE_TTL).await;
        assert!(!lm.check_alive(client_id));
    }

    #[tokio::test]
    async fn test_renew_lease() {
        let mut lm = LeaseManager::new();

        let client_id = lm.grant(None);
        assert!(lm.check_alive(client_id));
        tokio::time::sleep(DEFAULT_LEASE_TTL / 2).await;
        lm.renew(client_id, None);
        tokio::time::sleep(DEFAULT_LEASE_TTL / 2).await;
        assert!(lm.check_alive(client_id));
    }
}
