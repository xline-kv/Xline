use std::{collections::BTreeMap, sync::Arc, time::Duration};

use curp_external_api::{
    cmd::{Command, CommandExecutor},
    role_change::RoleChange,
};
use futures::FutureExt;
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinHandle, time::MissedTickBehavior};
use tonic::Response;
use tracing::{debug, error, info, warn};
use utils::config::CurpConfig;

use crate::{
    rpc::{connect::InnerConnectApiWrapper, AppendEntriesResponse, InstallSnapshotResponse},
    server::{
        metrics,
        raw_curp::{
            node_state::NodeState, replication::Action, AppendEntries, Heartbeat, SyncAction,
        },
        RawCurp,
    },
    snapshot::Snapshot,
};

use super::CurpNode;

// TODO: replace `lazy_static` with `LazyLock` after Rust version 1.80.0
lazy_static::lazy_static! {
    /// Replication handles
    static ref HANDLES: Mutex<Vec<JoinHandle<()>>> = Mutex::new(Vec::new());
}

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    #[allow(clippy::arithmetic_side_effects)] // a log index(u64) should never overflow
    /// Respawn replication tasks base on current node states
    ///
    /// The following assumption holds:
    /// * This method can only be called by the leader
    /// This method must be called under the following conditions:
    /// * When a new leader is elected
    /// * When membership changes
    pub(super) fn respawn_replication(curp: Arc<RawCurp<C, RC>>) {
        /// The size of the action channel
        const ACTION_CHANNEL_SIZE: usize = 0x1000;

        let self_id = curp.id();
        let cfg = curp.cfg().clone();
        let self_term = curp.term();
        let mut node_states = curp.all_node_states();
        // we don't needs to sync to self
        let _ignore = node_states.remove(&self_id);
        let connects: BTreeMap<_, _> = node_states
            .keys()
            .copied()
            .zip(node_states.values().map(NodeState::connect).cloned())
            .collect();
        let (action_tx, action_rx) = flume::bounded(ACTION_CHANNEL_SIZE);
        Self::abort_replication();

        let state_handle = tokio::spawn(Self::state_machine_worker(curp, action_rx, self_term));
        let heartbeat_handle = tokio::spawn(
            Self::heartbeat_worker(action_tx.clone(), connects, cfg.clone(), self_id, self_term)
                .map(|result| info!("heartbeat worker exit, result: {result:?}")),
        );
        let replication_handles = node_states.into_iter().map(|(id, state)| {
            let cfg = cfg.clone();
            info!("spawning replication task for {id}");
            tokio::spawn(Self::replication_worker(
                id,
                state,
                action_tx.clone(),
                self_id,
                self_term,
                cfg,
            ))
        });
        *HANDLES.lock() = replication_handles
            .chain([state_handle])
            .chain([heartbeat_handle])
            .collect();
    }

    /// Aborts all ongoing replication tasks
    pub(super) fn abort_replication() {
        HANDLES.lock().iter().for_each(JoinHandle::abort);
    }

    /// A worker responsible for synchronizing data with the curp state machine
    async fn state_machine_worker(
        curp: Arc<RawCurp<C, RC>>,
        action_rx: flume::Receiver<Action<C>>,
        // NOTE: `self_term` might differ from `curp.term()` due to external updates to curp
        self_term: u64,
    ) {
        // As we spawn the workers on every leader update, the term remains consistent
        while let Ok(action) = action_rx.recv_async().await {
            let exit = matches!(action, Action::StepDown(_));
            curp.sync_state_machine(self_term, action);
            if exit {
                break;
            }
        }
        // tx dropped, exit
        debug!("state update task exit");
    }

    /// A worker responsible for sending heartbeat to the cluster
    async fn heartbeat_worker(
        action_tx: flume::Sender<Action<C>>,
        connects: BTreeMap<u64, InnerConnectApiWrapper>,
        cfg: CurpConfig,
        self_id: u64,
        self_term: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let timeout = cfg.rpc_timeout;
        let mut ticker = tokio::time::interval(cfg.heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let _inst = ticker.tick().await;
            let (tx, rx) = oneshot::channel();
            action_tx.send(Action::GetCommitIndex(tx))?;
            let commit_index = rx.await?;
            let heartbeat = Heartbeat::new(self_term, self_id, commit_index);

            for (id, connect) in &connects {
                if let Some(action) =
                    Self::send_heartbeat(*id, connect, heartbeat, self_term, timeout).await
                {
                    debug_assert!(
                        matches!(action, Action::StepDown(_)),
                        "action not Action::StepDown"
                    );
                    // step down
                    let _ignore = action_tx.send(action);
                    return Ok(());
                }
            }
        }
    }

    /// Send the heartbeat to the give node, returns the term of that node
    async fn send_heartbeat(
        id: u64,
        connect: &InnerConnectApiWrapper,
        heartbeat: Heartbeat,
        self_term: u64,
        timeout: Duration,
    ) -> Option<Action<C>> {
        debug!("sending heartbeat to: {id}");
        connect
            .append_entries(heartbeat.into(), timeout)
            .await
            .map(Response::into_inner)
            .map(|resp| RawCurp::<C, RC>::heartbeat_action(resp.term, self_term))
            .map_err(|err| {
                warn!("heartbeat to {id} failed, {err:?}");
                metrics::get().heartbeat_send_failures.add(1, &[]);
            })
            .ok()
            .flatten()
    }

    #[allow(clippy::arithmetic_side_effects)] // a log index(u64) should never overflow
    /// A worker responsible for appending log entries to other nodes in the cluster
    async fn replication_worker(
        node_id: u64,
        node_state: NodeState,
        action_tx: flume::Sender<Action<C>>,
        self_id: u64,
        self_term: u64,
        cfg: CurpConfig,
    ) {
        let rpc_timeout = cfg.rpc_timeout;
        let batch_timeout = cfg.batch_timeout;
        let connect = node_state.connect();
        let sync_event = node_state.sync_event();
        let mut next_index = node_state.next_index();

        loop {
            let _ignore = tokio::time::timeout(batch_timeout, sync_event.listen()).await;
            let (tx, rx) = oneshot::channel();
            if action_tx
                .send(Action::GetLogFrom((next_index, tx)))
                .is_err()
            {
                debug!(
                    "action_rx closed because the leader stepped down, exiting replication worker"
                );
                break;
            }

            let action = match rx.await {
                Ok(SyncAction::AppendEntries(ae)) => {
                    Self::handle_append_entries(
                        &ae,
                        node_id,
                        connect,
                        rpc_timeout,
                        self_id,
                        self_term,
                    )
                    .await
                }
                Ok(SyncAction::Snapshot(rx)) => {
                    Self::handle_snapshot(rx, node_id, connect, self_id, self_term).await
                }
                Err(err) => {
                    error!("channel unexpectedly closed: {err}");
                    return;
                }
            };

            if let Some(action) = action {
                match action {
                    Action::UpdateMatchIndex((_, index)) => next_index = index + 1,
                    Action::UpdateNextIndex((_, index)) => next_index = index,
                    Action::GetLogFrom(_) | Action::StepDown(_) | Action::GetCommitIndex(_) => {}
                }
                let __ignore = action_tx.send(action);
            }
        }
    }

    /// Handle append entries
    async fn handle_append_entries(
        ae: &AppendEntries<C>,
        node_id: u64,
        connect: &InnerConnectApiWrapper,
        rpc_timeout: Duration,
        self_id: u64,
        self_term: u64,
    ) -> Option<Action<C>> {
        // no new entries to append
        if ae.entries.is_empty() {
            return None;
        }
        Self::send_append_entries(node_id, connect, ae, rpc_timeout, self_id)
            .await
            .map(|resp| {
                RawCurp::<C, RC>::append_entries_action(
                    resp.term,
                    resp.success,
                    resp.hint_index,
                    ae,
                    node_id,
                    self_term,
                )
            })
    }

    /// Send `append_entries` request
    async fn send_append_entries(
        node_id: u64,
        connect: &InnerConnectApiWrapper,
        ae: &AppendEntries<C>,
        timeout: Duration,
        self_id: u64,
    ) -> Option<AppendEntriesResponse> {
        debug!("{self_id} send append_entries to {node_id}");

        connect
            .append_entries(ae.into(), timeout)
            .await
            .map(Response::into_inner)
            .map_err(|err| warn!("ae to {node_id} failed, {err:?}"))
            .ok()
    }

    /// Handle snapshot
    async fn handle_snapshot(
        rx: oneshot::Receiver<Snapshot>,
        node_id: u64,
        connect: &InnerConnectApiWrapper,
        self_id: u64,
        self_term: u64,
    ) -> Option<Action<C>> {
        let snapshot = rx
            .await
            .map_err(|err| warn!("failed to receive snapshot result, {err}"))
            .ok()?;
        let last_include_index = snapshot.meta.last_included_index;
        Self::send_snapshot(node_id, connect, snapshot, self_id, self_term)
            .await
            .map(|resp| {
                RawCurp::<C, RC>::snapshot_action(resp.term, node_id, self_term, last_include_index)
            })
    }

    /// Send snapshot
    async fn send_snapshot(
        node_id: u64,
        connect: &InnerConnectApiWrapper,
        snapshot: Snapshot,
        self_id: u64,
        self_term: u64,
    ) -> Option<InstallSnapshotResponse> {
        connect
            .install_snapshot(self_term, self_id, snapshot)
            .await
            .map(Response::into_inner)
            .map_err(|err| warn!("snapshot to {node_id} failed, {err:?}"))
            .ok()
    }
}