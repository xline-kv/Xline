use std::{sync::Arc, time::Duration};

use curp::role_change::RoleChange;

use crate::storage::{
    compact::{Compactable, Compactor},
    storage_api::StorageApi,
    LeaseStore,
};

/// State of current node
pub(crate) struct State<DB: StorageApi, C: Compactable> {
    /// lease storage
    lease_storage: Arc<LeaseStore<DB>>,
    /// auto compactor
    auto_compactor: Option<Arc<dyn Compactor<C>>>,
}

impl<DB: StorageApi, C: Compactable> Clone for State<DB, C> {
    fn clone(&self) -> Self {
        Self {
            lease_storage: Arc::clone(&self.lease_storage),
            auto_compactor: self.auto_compactor.clone(),
        }
    }
}

impl<DB: StorageApi, C: Compactable> RoleChange for State<DB, C> {
    fn on_election_win(&self) {
        self.lease_storage.promote(Duration::from_secs(1)); // TODO: extend should be election timeout
        if let Some(auto_compactor) = self.auto_compactor.as_ref() {
            auto_compactor.resume();
        }
    }

    fn on_calibrate(&self) {
        self.lease_storage.demote();
        if let Some(auto_compactor) = self.auto_compactor.as_ref() {
            auto_compactor.pause();
        }
    }
}

impl<DB: StorageApi, C: Compactable> State<DB, C> {
    /// Create a new State
    pub(super) fn new(
        lease_storage: Arc<LeaseStore<DB>>,
        auto_compactor: Option<Arc<dyn Compactor<C>>>,
    ) -> Self {
        Self {
            lease_storage,
            auto_compactor,
        }
    }
}
