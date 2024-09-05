use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use event_listener::Event;
use futures::Future;
use parking_lot::RwLock;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{debug, info, warn};

use super::cluster_state::ClusterState;
use crate::rpc::{connect::ConnectApi, CurpError, Redirect};

/// Keep alive
#[derive(Clone, Debug)]
pub(crate) struct KeepAlive {
    /// Heartbeat interval
    heartbeat_interval: Duration,
}

/// Handle of the keep alive task
#[derive(Debug)]
pub(crate) struct KeepAliveHandle {
    /// Client id
    client_id: Arc<AtomicU64>,
    /// Update event of client id
    update_event: Arc<Event>,
    /// Task join handle
    handle: JoinHandle<()>,
}

impl KeepAliveHandle {
    /// Wait for the client id
    pub(crate) async fn wait_id_update(&self, current_id: u64) -> u64 {
        loop {
            let id = self.client_id.load(Ordering::Relaxed);
            if current_id != id {
                return id;
            }
            self.update_event.listen().await;
        }
    }
}

impl KeepAlive {
    /// Creates a new `KeepAlive`
    pub(crate) fn new(heartbeat_interval: Duration) -> Self {
        Self { heartbeat_interval }
    }

    /// Streaming keep alive
    pub(crate) fn spawn_keep_alive(
        self,
        cluster_state: Arc<RwLock<ClusterState>>,
    ) -> KeepAliveHandle {
        /// Sleep duration when keep alive failed
        const FAIL_SLEEP_DURATION: Duration = Duration::from_secs(1);
        let client_id = Arc::new(AtomicU64::new(0));
        let client_id_c = Arc::clone(&client_id);
        let update_event = Arc::new(Event::new());
        let update_event_c = Arc::clone(&update_event);
        let handle = tokio::spawn(async move {
            loop {
                let current_state = cluster_state.read().clone();
                let current_id = client_id.load(Ordering::Relaxed);
                match self.keep_alive_with(current_id, current_state).await {
                    Ok(new_id) => {
                        client_id.store(new_id, Ordering::Relaxed);
                        let _ignore = update_event.notify(usize::MAX);
                    }
                    Err(e) => {
                        warn!("keep alive failed: {e:?}");
                        // Sleep for some time, the cluster state should be updated in a while
                        tokio::time::sleep(FAIL_SLEEP_DURATION).await;
                    }
                }
            }
        });

        KeepAliveHandle {
            client_id: client_id_c,
            update_event: update_event_c,
            handle,
        }
    }

    /// Keep alive with the given state and config
    pub(crate) async fn keep_alive_with(
        &self,
        client_id: u64,
        cluster_state: ClusterState,
    ) -> Result<u64, CurpError> {
        cluster_state
            .map_leader(|conn| async move {
                conn.lease_keep_alive(client_id, self.heartbeat_interval)
                    .await
            })
            .await
    }
}
