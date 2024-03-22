use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use clippy_utilities::NumericCast;
use itertools::Itertools;
use parking_lot::RwLock;
use utils::parking_lot_lock::RwLockMap;
use xlineapi::execute_error::ExecuteError;

use super::{lease_queue::LeaseQueue, Lease};
use crate::rpc::PbLease;

/// Collection of lease related data
#[derive(Debug)]
pub(crate) struct LeaseCollection {
    /// Inner data of `LeaseCollection`
    inner: RwLock<LeaseCollectionInner>,
    /// Min lease ttl
    min_ttl: i64,
}

#[derive(Debug)]
/// Inner data of `LeaseCollection`
struct LeaseCollectionInner {
    /// lease id to lease
    lease_map: HashMap<i64, Lease>,
    /// key to lease id
    item_map: HashMap<Vec<u8>, i64>,
    /// lease queue
    expired_queue: LeaseQueue,
}

impl LeaseCollection {
    /// New `LeaseCollection`
    pub(crate) fn new(min_ttl: i64) -> Self {
        Self {
            inner: RwLock::new(LeaseCollectionInner {
                lease_map: HashMap::new(),
                item_map: HashMap::new(),
                expired_queue: LeaseQueue::new(),
            }),
            min_ttl,
        }
    }

    /// Find expired leases
    pub(crate) fn find_expired_leases(&self) -> Vec<i64> {
        let mut expired_leases = vec![];
        let mut inner = self.inner.write();
        while let Some(expiry) = inner.expired_queue.peek() {
            if *expiry <= Instant::now() {
                #[allow(clippy::unwrap_used)] // queue.peek() returns Some
                let id = inner.expired_queue.pop().unwrap();
                if inner.lease_map.contains_key(&id) {
                    expired_leases.push(id);
                }
            } else {
                break;
            }
        }
        expired_leases
    }

    /// Renew lease
    pub(crate) fn renew(&self, lease_id: i64) -> Result<i64, ExecuteError> {
        let mut inner = self.inner.write();
        let (expiry, ttl) = {
            let Some(lease) = inner.lease_map.get_mut(&lease_id) else {
                return Err(ExecuteError::LeaseNotFound(lease_id));
            };
            if lease.expired() {
                return Err(ExecuteError::LeaseExpired(lease_id));
            }
            let expiry = lease.refresh(Duration::default());
            let ttl = lease.ttl().as_secs().numeric_cast();
            (expiry, ttl)
        };
        let _ignore = inner.expired_queue.update(lease_id, expiry);
        Ok(ttl)
    }

    /// Attach key to lease
    pub(crate) fn attach(&self, lease_id: i64, key: Vec<u8>) -> Result<(), ExecuteError> {
        let mut inner = self.inner.write();
        let Some(lease) = inner.lease_map.get_mut(&lease_id) else {
            return  Err(ExecuteError::LeaseNotFound(lease_id));
        };
        lease.insert_key(key.clone());
        let _ignore = inner.item_map.insert(key, lease_id);
        Ok(())
    }

    /// Detach key from lease
    pub(crate) fn detach(&self, lease_id: i64, key: &[u8]) -> Result<(), ExecuteError> {
        let mut inner = self.inner.write();
        let Some(lease) = inner.lease_map.get_mut(&lease_id) else {
            return  Err(ExecuteError::LeaseNotFound(lease_id));
        };
        lease.remove_key(key);
        let _ignore = inner.item_map.remove(key);
        Ok(())
    }

    /// Get lease id by given key
    pub(crate) fn get_lease(&self, key: &[u8]) -> i64 {
        self.inner.read().item_map.get(key).copied().unwrap_or(0)
    }

    /// Get Lease by lease id
    pub(crate) fn look_up(&self, lease_id: i64) -> Option<Lease> {
        self.inner.read().lease_map.get(&lease_id).cloned()
    }

    /// Get all leases
    pub(crate) fn leases(&self) -> Vec<Lease> {
        let mut leases = self
            .inner
            .read()
            .lease_map
            .values()
            .cloned()
            .collect::<Vec<_>>();
        leases.sort_by_key(Lease::remaining);
        leases
    }

    /// Check if a lease exists
    pub(crate) fn contains_lease(&self, lease_id: i64) -> bool {
        self.inner.read().lease_map.contains_key(&lease_id)
    }

    /// Grant a lease
    pub(crate) fn grant(&self, lease_id: i64, ttl: i64, is_leader: bool) -> PbLease {
        let mut lease = Lease::new(lease_id, ttl.max(self.min_ttl).numeric_cast());
        self.inner.map_write(|mut inner| {
            if is_leader {
                let expiry = lease.refresh(Duration::ZERO);
                let _ignore = inner.expired_queue.insert(lease_id, expiry);
            } else {
                lease.forever();
            }
            let _ignore = inner.lease_map.insert(lease_id, lease.clone());
        });
        PbLease {
            id: lease.id(),
            ttl: lease.ttl().as_secs().numeric_cast(),
            remaining_ttl: lease.remaining_ttl().as_secs().numeric_cast(),
        }
    }

    /// Revokes a lease
    pub(crate) fn revoke(&self, lease_id: i64) -> Option<Lease> {
        self.inner.write().lease_map.remove(&lease_id)
    }

    /// Demote current node
    pub(crate) fn demote(&self) {
        let mut inner = self.inner.write();
        inner.lease_map.values_mut().for_each(Lease::forever);
        inner.expired_queue.clear();
    }

    /// Promote current node
    pub(crate) fn promote(&self, extend: Duration) {
        let mut inner = self.inner.write();
        let pairs = inner
            .lease_map
            .values_mut()
            .map(|l| (l.id(), l.refresh(extend)))
            .collect_vec();
        for (lease_id, expiry) in pairs {
            let _ignore = inner.expired_queue.insert(lease_id, expiry);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_grant_less_than_min_ttl() {
        let c = LeaseCollection::new(3);
        c.grant(1, 2, false);
        let l = c.look_up(1);
        assert!(l.is_some());
        assert_eq!(l.unwrap().ttl(), Duration::from_secs(3));
    }
}
