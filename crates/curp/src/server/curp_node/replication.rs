use std::{sync::Arc, time::Duration};

use curp_external_api::{
    cmd::{Command, CommandExecutor},
    role_change::RoleChange,
    LogIndex,
};
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinHandle, time::MissedTickBehavior};
use tonic::Response;
use tracing::{debug, error, info, warn};
use utils::config::CurpConfig;

use crate::{
    rpc::{
        connect::InnerConnectApiWrapper, AppendEntriesResponse, CurpError, InstallSnapshotResponse,
    },
    server::{
        metrics,
        raw_curp::{node_state::NodeState, AppendEntries, Heartbeat, SyncAction},
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

/// Represents various actions that can be performed on the `RawCurp` state machine
enum Action<C> {
    /// Update the match index for a given node.
    /// Contains (node_id, match_index)
    UpdateMatchIndex((u64, LogIndex)),

    /// Update the next index for a given node.
    /// Contains (node_id, next_index)
    UpdateNextIndex((u64, LogIndex)),

    /// Request to get the log starting from a specific index.
    /// Contains a tuple with the starting log index and a sender to send the sync action.
    GetLogFrom((LogIndex, oneshot::Sender<SyncAction<C>>)),

    /// Step down the current node.
    /// Contains the latest term.
    StepDown(u64),
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
        let connects = node_states
            .values()
            .map(NodeState::connect)
            .cloned()
            .collect();
        let self_next_index = curp.last_log_index() + 1;
        let (action_tx, action_rx) = flume::bounded(ACTION_CHANNEL_SIZE);
        HANDLES.lock().iter().for_each(JoinHandle::abort);

        let state_handle = tokio::spawn(Self::state_machine_worker(curp, action_rx));
        let heartbeat_handle = tokio::spawn(Self::heartbeat_worker(
            connects,
            cfg.clone(),
            self_id,
            self_term,
        ));
        let replication_handles = node_states.into_iter().map(|(id, state)| {
            let cfg = cfg.clone();
            info!("spawning replication task for {id}");
            tokio::spawn(Self::replication_worker(
                state,
                action_tx.clone(),
                self_id,
                self_term,
                cfg,
                self_next_index,
            ))
        });
        *HANDLES.lock() = replication_handles
            .chain([state_handle])
            .chain([heartbeat_handle])
            .collect();
    }

    /// A worker responsible for synchronizing data with the curp state machine
    async fn state_machine_worker(
        curp: Arc<RawCurp<C, RC>>,
        action_rx: flume::Receiver<Action<C>>,
    ) {
        // As we spawn the workers on every leader update, the term remains consistent
        let self_term = curp.term();
        while let Ok(update) = action_rx.recv_async().await {
            match update {
                Action::UpdateMatchIndex((node_id, index)) => {
                    debug!("updating {node_id}'s match index to {index}");
                    curp.update_match_index(node_id, index);
                    curp.try_update_commit_index(index, self_term);
                }
                Action::UpdateNextIndex((node_id, index)) => {
                    debug!("updating {node_id}'s next index to {index}");
                    curp.update_next_index(node_id, index);
                }
                Action::GetLogFrom((next, tx)) => {
                    debug!("getting log from index {next}");
                    let sync = curp.sync_from(next);
                    if tx.send(sync).is_err() {
                        error!("send append entries failed");
                    }
                }
                Action::StepDown(node_term) => {
                    debug_assert!(node_term > self_term, "node_term no greater than self_term");
                    info!("received greater term: {node_term}, stepping down.");
                    curp.step_down(node_term);
                    break;
                }
            }
        }
        // tx dropped, exit
        debug!("state update task exit");
    }

    /// A worker responsible for sending heartbeat to the cluster
    async fn heartbeat_worker(
        connects: Vec<InnerConnectApiWrapper>,
        cfg: CurpConfig,
        self_id: u64,
        self_term: u64,
    ) {
        let timeout = cfg.rpc_timeout;
        let mut ticker = tokio::time::interval(cfg.heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let heartbeat = Heartbeat::new(self_term, self_id);
        loop {
            let _inst = ticker.tick().await;
            for connect in &connects {
                let result = Self::send_heartbeat(connect, heartbeat, timeout).await;
                match result {
                    Ok(other_term) if self_term < other_term => {
                        info!("heartbeat worker exiting");
                        return;
                    }
                    Err(err) => {
                        warn!("heartbeat to {} failed, {err:?}", connect.id());
                        metrics::get().heartbeat_send_failures.add(1, &[]);
                    }
                    Ok(_) => {}
                }
            }
        }
    }

    /// Send the heartbeat to the give node, returns the term of that node
    async fn send_heartbeat(
        connect: &InnerConnectApiWrapper,
        heartbeat: Heartbeat,
        timeout: Duration,
    ) -> Result<u64, CurpError> {
        debug!("sending heartbeat to: {}", connect.id());
        connect
            .append_entries(heartbeat.into(), timeout)
            .await
            .map(Response::into_inner)
            .map(|resp| resp.term)
            .map_err(Into::into)
    }

    /// A worker responsible for appending log entries to other nodes in the cluster
    async fn replication_worker(
        node_state: NodeState,
        action_tx: flume::Sender<Action<C>>,
        self_id: u64,
        self_term: u64,
        cfg: CurpConfig,
        self_next_index: LogIndex,
    ) {
        let rpc_timeout = cfg.rpc_timeout;
        let batch_timeout = cfg.batch_timeout;
        let connect = node_state.connect();
        let sync_event = node_state.sync_event();
        let mut next_index = node_state.next_index();
        // The next_index could be zero if a new leader is elected and it does not have the
        // infomations of other nodes. We set the initial index to the next index of the
        // current node.
        if next_index == 0 {
            next_index = self_next_index;
        }

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
                    Self::handle_append_entries(&ae, connect, rpc_timeout, self_id, self_term).await
                }
                Ok(SyncAction::Snapshot(rx)) => {
                    Self::handle_snapshot(rx, connect, self_id, self_term).await
                }
                Err(err) => {
                    error!("channel unexpectedly closed: {err}");
                    return;
                }
            };

            if let Some(action) = action {
                if let Action::UpdateNextIndex((_, index)) = action {
                    next_index = index;
                }
                let __ignore = action_tx.send(action);
            }
        }
    }

    /// Handle append entries
    async fn handle_append_entries(
        ae: &AppendEntries<C>,
        connect: &InnerConnectApiWrapper,
        rpc_timeout: Duration,
        self_id: u64,
        self_term: u64,
    ) -> Option<Action<C>> {
        // no new entries to append
        if ae.entries.is_empty() {
            return None;
        }
        Self::send_append_entries(connect, ae, rpc_timeout, self_id)
            .await
            .map(|resp| Self::append_entries_action(resp, ae, connect.id(), self_term))
            .map_err(|err| warn!("ae to {} failed, {err:?}", connect.id()))
            .ok()
    }

    /// Send `append_entries` request
    async fn send_append_entries(
        connect: &InnerConnectApiWrapper,
        ae: &AppendEntries<C>,
        timeout: Duration,
        self_id: u64,
    ) -> Result<AppendEntriesResponse, CurpError> {
        debug!("{self_id} send append_entries to {}", connect.id());

        connect
            .append_entries(ae.into(), timeout)
            .await
            .map(Response::into_inner)
            .map_err(Into::into)
    }

    #[allow(clippy::as_conversions, clippy::arithmetic_side_effects)] // converting usize to u64 is safe
    /// Generate `Action` from append entries response
    fn append_entries_action(
        resp: AppendEntriesResponse,
        ae: &AppendEntries<C>,
        node_id: u64,
        self_term: u64,
    ) -> Action<C> {
        let other_term = resp.term;
        let success = resp.success;
        let hint_index = resp.hint_index;

        if self_term < other_term {
            return Action::StepDown(other_term);
        }

        if !success {
            return Action::UpdateNextIndex((node_id, hint_index));
        }

        let last_sent_index = ae.prev_log_index + ae.entries.len() as u64;
        Action::UpdateMatchIndex((node_id, last_sent_index))
    }

    /// Handle snapshot
    async fn handle_snapshot(
        rx: oneshot::Receiver<Snapshot>,
        connect: &InnerConnectApiWrapper,
        self_id: u64,
        self_term: u64,
    ) -> Option<Action<C>> {
        let snapshot = rx
            .await
            .map_err(|err| warn!("failed to receive snapshot result, {err}"))
            .ok()?;
        let last_include_index = snapshot.meta.last_included_index;
        Self::send_snapshot(connect, snapshot, self_id, self_term)
            .await
            .map(|resp| Self::snapshot_action(resp, connect.id(), self_term, last_include_index))
            .map_err(|err| warn!("snapshot to {} failed, {err:?}", connect.id()))
            .ok()
    }

    /// Send snapshot
    async fn send_snapshot(
        connect: &InnerConnectApiWrapper,
        snapshot: Snapshot,
        self_id: u64,
        self_term: u64,
    ) -> Result<InstallSnapshotResponse, CurpError> {
        connect
            .install_snapshot(self_term, self_id, snapshot)
            .await
            .map(Response::into_inner)
            .map_err(Into::into)
    }

    /// Generate `Action` from snapshot response
    fn snapshot_action(
        resp: InstallSnapshotResponse,
        node_id: u64,
        self_term: u64,
        last_include_index: LogIndex,
    ) -> Action<C> {
        let other_term = resp.term;
        if self_term < other_term {
            return Action::StepDown(other_term);
        }
        Action::UpdateMatchIndex((node_id, last_include_index))
    }
}
