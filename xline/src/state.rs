use std::{sync::Arc, time::Duration};

use curp::role_change::RoleChange;

use crate::storage::{storage_api::StorageApi, LeaseStore};

/// State of current node
#[derive(Debug)]
pub(crate) struct State<DB: StorageApi> {
    /// lease storage
    lease_storage: Arc<LeaseStore<DB>>,
}

impl<DB: StorageApi> RoleChange for State<DB> {
    fn on_election_win(&self) {
        self.lease_storage.promote(Duration::from_secs(1)); // TODO: extend should be election timeout
    }

    fn on_calibrate(&self) {
        self.lease_storage.demote();
    }
}

impl<DB: StorageApi> State<DB> {
    /// Create a new State
    pub(super) fn new(lease_storage: Arc<LeaseStore<DB>>) -> Self {
        Self { lease_storage }
    }
}
