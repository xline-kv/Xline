use std::{
    collections::HashSet,
    ops::Add,
    time::{Duration, Instant},
};

/// Lease
#[derive(Debug, Clone)]
pub(crate) struct Lease {
    /// Lease id
    id: i64,
    /// Lease ttl
    ttl: Duration,
    /// Remaining time of lease
    remaining_ttl: Duration,
    /// Keys attached to this lease
    keys_set: HashSet<Vec<u8>>,
    /// Expiration time
    expiry: Option<Instant>,
}

impl Lease {
    /// New `Lease`
    pub(crate) fn new(id: i64, ttl: u64) -> Self {
        Self {
            id,
            ttl: Duration::from_secs(ttl),
            remaining_ttl: Duration::from_secs(0),
            keys_set: HashSet::new(),
            expiry: None,
        }
    }

    /// Return keys of lease
    pub(crate) fn keys(&self) -> Vec<Vec<u8>> {
        self.keys_set.iter().cloned().collect()
    }

    /// Lease id
    pub(crate) fn id(&self) -> i64 {
        self.id
    }

    /// Lease ttl
    pub(crate) fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Lease remaining
    pub(crate) fn remaining(&self) -> Duration {
        if let Some(exp) = self.expiry {
            exp.saturating_duration_since(Instant::now())
        } else {
            Duration::from_secs(u64::MAX)
        }
    }

    /// Check if the lease is expired
    pub(crate) fn expired(&self) -> bool {
        self.remaining() <= Duration::from_secs(0)
    }

    /// Lease remaining ttl
    pub(crate) fn remaining_ttl(&self) -> Duration {
        if self.remaining_ttl > Duration::from_secs(0) {
            self.remaining_ttl
        } else {
            self.ttl
        }
    }

    /// Refresh expiry and return new expiry
    pub(crate) fn refresh(&mut self, extend: Duration) -> Instant {
        let new_expiry = Instant::now().add(extend).add(self.remaining_ttl());
        self.expiry = Some(new_expiry);
        new_expiry
    }

    /// Set expiry to `None`
    pub(crate) fn forever(&mut self) {
        self.expiry = None;
    }

    /// Insert a key to lease
    pub(crate) fn insert_key(&mut self, key: Vec<u8>) {
        let _ignore = self.keys_set.insert(key);
    }

    /// Remove a key from lease
    pub(crate) fn remove_key(&mut self, key: &[u8]) {
        let _ignore = self.keys_set.remove(key);
    }
}
