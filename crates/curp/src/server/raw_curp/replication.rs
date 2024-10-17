use curp_external_api::{cmd::Command, role_change::RoleChange, LogIndex};
use tokio::sync::oneshot;
use tracing::{debug, error, info};

use crate::rpc::{AppendEntriesResponse, InstallSnapshotResponse};

use super::{AppendEntries, RawCurp, SyncAction};

/// Represents various actions that can be performed on the `RawCurp` state machine
pub(crate) enum Action<C> {
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

impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Synchronizes a action
    pub(crate) fn sync_state_machine(&self, action: Action<C>) {
        let self_term = self.term();
        match action {
            Action::UpdateMatchIndex((node_id, index)) => {
                debug!("updating {node_id}'s match index to {index}");
                self.update_match_index(node_id, index);
                self.try_update_commit_index(index, self_term);
            }
            Action::UpdateNextIndex((node_id, index)) => {
                debug!("updating {node_id}'s next index to {index}");
                self.update_next_index(node_id, index);
            }
            Action::GetLogFrom((next, tx)) => {
                debug!("getting log from index {next}");
                let sync = self.sync_from(next);
                if tx.send(sync).is_err() {
                    error!("send append entries failed");
                }
            }
            Action::StepDown(node_term) => {
                debug_assert!(node_term > self_term, "node_term no greater than self_term");
                info!("received greater term: {node_term}, stepping down.");
                self.step_down(node_term);
            }
        }
    }

    /// Generate `Action` from heartbeat response
    pub(crate) fn heartbeat_action(other_term: u64, self_term: u64) -> Option<Action<C>> {
        (self_term < other_term).then_some(Action::StepDown(other_term))
    }

    #[allow(clippy::as_conversions, clippy::arithmetic_side_effects)] // converting usize to u64 is safe
    /// Generate `Action` from append entries response
    pub(crate) fn append_entries_action(
        other_term: u64,
        success: bool,
        hint_index: LogIndex,
        ae: &AppendEntries<C>,
        node_id: u64,
        self_term: u64,
    ) -> Action<C> {
        if self_term < other_term {
            return Action::StepDown(other_term);
        }

        if !success {
            return Action::UpdateNextIndex((node_id, hint_index));
        }

        let last_sent_index = ae.prev_log_index + ae.entries.len() as u64;
        Action::UpdateMatchIndex((node_id, last_sent_index))
    }

    /// Generate `Action` from snapshot response
    pub(crate) fn snapshot_action(
        other_term: u64,
        node_id: u64,
        self_term: u64,
        last_include_index: LogIndex,
    ) -> Action<C> {
        if self_term < other_term {
            return Action::StepDown(other_term);
        }
        Action::UpdateMatchIndex((node_id, last_include_index))
    }
}
