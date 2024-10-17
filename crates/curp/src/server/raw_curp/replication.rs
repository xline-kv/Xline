use curp_external_api::{cmd::Command, role_change::RoleChange, LogIndex};
use tokio::sync::oneshot;
use tracing::{debug, error, info};

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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use curp_test_utils::{mock_role_change, test_cmd::TestCommand, TestRoleChange};
    use tracing_test::traced_test;
    use utils::task_manager::TaskManager;

    use crate::server::raw_curp::Role;

    use super::*;

    type TestRawCurp = RawCurp<TestCommand, TestRoleChange>;

    #[traced_test]
    #[test]
    fn replication_entries_will_calibrate_term() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));
        let ae = AppendEntries::<TestCommand> {
            term: 1,
            leader_id: 1,
            prev_log_index: 2,
            prev_log_term: 1,
            leader_commit: 1,
            entries: vec![],
        };
        let action = TestRawCurp::append_entries_action(2, false, 1, &ae, 2, 1);
        curp.sync_state_machine(action);

        let st_r = curp.st.read();
        assert_eq!(st_r.term, 2);
        assert_eq!(st_r.role, Role::Follower);
    }

    #[traced_test]
    #[test]
    fn heartbeat_will_calibrate_term() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));
        let action = TestRawCurp::heartbeat_action(2, 1).unwrap();
        curp.sync_state_machine(action);

        let st_r = curp.st.read();
        assert_eq!(st_r.term, 2);
        assert_eq!(st_r.role, Role::Follower);
    }

    #[traced_test]
    #[test]
    fn snapshot_will_calibrate_term() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));
        let action = TestRawCurp::snapshot_action(2, 1, 1, 1);
        curp.sync_state_machine(action);

        let st_r = curp.st.read();
        assert_eq!(st_r.term, 2);
        assert_eq!(st_r.role, Role::Follower);
    }

    #[traced_test]
    #[test]
    fn snapshot_will_calibrate_index() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));

        let s1_id = curp.get_id_by_name("S1").unwrap();
        assert_eq!(curp.ctx.node_states.get_match_index(s1_id), Some(0));

        let action = TestRawCurp::snapshot_action(1, s1_id, 1, 1);
        curp.sync_state_machine(action);

        let st_r = curp.st.read();
        assert_eq!(st_r.term, 1);
        assert_eq!(curp.ctx.node_states.get_match_index(s1_id), Some(1));
    }

    #[traced_test]
    #[test]
    fn replication_entries_will_calibrate_next_index() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));

        let s1_id = curp.get_id_by_name("S1").unwrap();
        assert_eq!(curp.ctx.node_states.get_next_index(s1_id), Some(1));

        let ae = AppendEntries::<TestCommand> {
            term: 1,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            leader_commit: 1,
            entries: vec![],
        };
        let action = TestRawCurp::append_entries_action(1, false, 2, &ae, s1_id, 1);
        curp.sync_state_machine(action);

        let st_r = curp.st.read();
        assert_eq!(st_r.term, 1);
        assert_eq!(curp.ctx.node_states.get_next_index(s1_id), Some(2));
    }

    #[traced_test]
    #[test]
    fn replication_entries_will_calibrate_match_index() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));

        let s1_id = curp.get_id_by_name("S1").unwrap();
        assert_eq!(curp.ctx.node_states.get_match_index(s1_id), Some(0));

        let ae = AppendEntries::<TestCommand> {
            term: 1,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            leader_commit: 1,
            entries: vec![],
        };
        let action = TestRawCurp::append_entries_action(1, true, 2, &ae, s1_id, 1);
        curp.sync_state_machine(action);

        let st_r = curp.st.read();
        assert_eq!(st_r.term, 1);
        assert_eq!(curp.ctx.node_states.get_match_index(s1_id), Some(1));
    }

    #[traced_test]
    #[test]
    fn handle_ae_will_calibrate_term() {
        let task_manager = Arc::new(TaskManager::new());
        let curp = { Arc::new(RawCurp::new_test(3, mock_role_change(), task_manager)) };
        curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);
        let s2_id = curp.get_id_by_name("S2").unwrap();

        let result = curp.handle_append_entries(2, s2_id, 0, 0, vec![], 0);
        assert!(result.is_ok());

        let st_r = curp.st.read();
        assert_eq!(st_r.term, 2);
        assert_eq!(st_r.role, Role::Follower);
        assert_eq!(st_r.leader_id, Some(s2_id));
    }
}
