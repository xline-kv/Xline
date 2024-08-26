use std::{sync::Arc, time::Duration};

use futures::Future;
use tracing::{debug, info, warn};

use super::state::State;
use crate::rpc::{connect::ConnectApi, CurpError, Redirect};

/// Stream client config
#[derive(Debug)]
pub(super) struct StreamingConfig {
    /// Heartbeat interval
    heartbeat_interval: Duration,
}

impl StreamingConfig {
    /// Create a stream client config
    pub(super) fn new(heartbeat_interval: Duration) -> Self {
        Self { heartbeat_interval }
    }
}

/// Stream client
#[derive(Debug)]
pub(super) struct Streaming {
    /// Shared client state
    pub(super) state: Arc<State>,
    /// Stream client config
    config: StreamingConfig,
}

/// Prevent lock contention when leader crashed or some unknown errors
const RETRY_DELAY: Duration = Duration::from_millis(100);

impl Streaming {
    /// Create a stream client
    pub(super) fn new(state: Arc<State>, config: StreamingConfig) -> Self {
        Self { state, config }
    }

    /// Take an async function and map to the remote leader, hang up when no leader found or
    /// the leader is itself.
    async fn map_remote_leader<R, F: Future<Output = Result<R, CurpError>>>(
        &self,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> Result<R, CurpError> {
        loop {
            let Some(leader_id) = self.state.leader_id().await else {
                warn!("cannot find leader_id, refreshing state...");
                let _ig = self.state.try_refresh_state().await;
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            };
            if let Some(local_id) = self.state.local_server_id() {
                if leader_id == local_id {
                    self.state.check_gen_local_client_id().await;
                    debug!("skip keep heartbeat for local connection, wait for leadership update");
                    self.state.leader_notifier().listen().await;
                    continue;
                }
            }
            return self.state.map_server(leader_id, f).await;
        }
    }

    /// Keep heartbeat
    pub(super) async fn keep_heartbeat(&self) {
        #[allow(clippy::ignored_unit_patterns)] // tokio select internal triggered
        loop {
            let heartbeat = self.map_remote_leader::<(), _>(|conn| async move {
                loop {
                    let err = conn
                        .lease_keep_alive(
                            self.state.clone_client_id(),
                            self.config.heartbeat_interval,
                        )
                        .await;
                    #[allow(clippy::wildcard_enum_match_arm)]
                    match err {
                        CurpError::Redirect(Redirect { leader_id, term }) => {
                            let _ig = self
                                .state
                                .check_and_update_leader(leader_id.map(Into::into), term)
                                .await;
                        }
                        CurpError::WrongClusterVersion(()) => {
                            warn!(
                                "cannot find the leader in connects, wait for  leadership update"
                            );
                            self.state.leader_notifier().listen().await;
                        }
                        CurpError::RpcTransport(()) => {
                            warn!(
                                "got rpc transport error when keep heartbeat, refreshing state..."
                            );
                            let _ig = self.state.try_refresh_state().await;
                            tokio::time::sleep(RETRY_DELAY).await;
                        }
                        CurpError::ShuttingDown(()) => {
                            info!("cluster is shutting down, exiting heartbeat task");
                            return Ok(());
                        }
                        _ => {
                            warn!("got unexpected error {err:?} when keep heartbeat, retrying...");
                            tokio::time::sleep(RETRY_DELAY).await;
                        }
                    }
                }
            });

            tokio::select! {
                _ = self.state.leader_notifier().listen() => {
                    debug!("interrupt keep heartbeat because leadership changed");
                }
                _ = heartbeat => {
                    break;
                }
            }
        }
    }
}
