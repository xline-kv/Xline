#![allow(
    clippy::unused_self,
    clippy::unimplemented,
    clippy::needless_pass_by_value
)] // TODO: remove this after implemented

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use curp_external_api::cmd::Command;
use curp_external_api::cmd::CommandExecutor;
use curp_external_api::role_change::RoleChange;
use curp_external_api::LogIndex;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::debug;
use utils::task_manager::tasks::TaskName;

use super::CurpNode;
use crate::log_entry::EntryData;
use crate::log_entry::LogEntry;
use crate::member::Membership;
use crate::rpc;
use crate::rpc::connect::InnerConnectApiWrapper;
use crate::rpc::inner_connects;
use crate::rpc::Change;
use crate::rpc::ChangeMembershipRequest;
use crate::rpc::CurpError;
use crate::rpc::MembershipChange;
use crate::rpc::MembershipResponse;
use crate::rpc::ProposeId;
use crate::rpc::Redirect;
use crate::rpc::WaitLearnerRequest;
use crate::rpc::WaitLearnerResponse;
use crate::server::raw_curp::node_state::NodeState;

// Leader methods
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Performs a membership change to the cluster
    pub(crate) async fn change_membership(
        &self,
        request: ChangeMembershipRequest,
    ) -> Result<MembershipResponse, CurpError> {
        let changes = request
            .changes
            .into_iter()
            .map(MembershipChange::into_inner);

        self.change_membership_inner(changes).await
    }

    /// Handle `ProposeStream` requests
    pub(crate) fn wait_learner(
        &self,
        req: WaitLearnerRequest,
        tx: flume::Sender<Result<WaitLearnerResponse, tonic::Status>>,
    ) {
        let rxs = self.curp.register_monitoring(req.node_ids);
        let _handle = tokio::spawn(async move {
            let mut fused = futures::stream::select_all(rxs.into_iter().map(|(id, rx)| {
                let stream = BroadcastStream::new(rx);
                stream.map(move |res| res.map(|x| (id, x)))
            }))
            // ignores entry that has been removed
            .filter_map(Result::ok);

            while let Some(res) = fused.next().await {
                let (node_id, (current_idx, latest_idx)) = res;
                if tx
                    .send(Ok(WaitLearnerResponse {
                        node_id,
                        current_idx,
                        latest_idx,
                    }))
                    .is_err()
                {
                    debug!("wait learner stream unexpectedly closed");
                    break;
                }
            }
        });
    }

    /// Performs a membership change to the cluster
    pub(crate) async fn change_membership_inner(
        &self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<MembershipResponse, CurpError> {
        self.ensure_leader()?;
        let changes = Self::ensure_non_overlapping(changes)?;
        let configs = self.curp.generate_membership(changes);
        if configs.is_empty() {
            return Err(CurpError::invalid_member_change());
        }
        for config in configs {
            let propose_id = ProposeId(rand::random(), 0);
            let index = self.curp.push_log_entry(propose_id, config.clone()).index;
            self.update_states_with_membership(&config);
            self.curp
                .update_membership_state(None, Some((index, config)), None);
            self.curp.persistent_membership_state()?;
            // Leader also needs to update transferee
            self.curp.update_transferee();
            self.wait_commit(Some(propose_id)).await;
        }

        self.build_membership_response()
    }

    /// Builds a `ChangeMembershipResponse` from the given membership.
    pub(crate) fn build_membership_response(&self) -> Result<MembershipResponse, CurpError> {
        let (leader_id, term, _) = self.curp.leader();
        let Membership { members, nodes } = self.curp.effective_membership();
        let members = members
            .into_iter()
            .map(|s| rpc::QuorumSet {
                set: s.into_iter().collect(),
            })
            .collect();
        let nodes = nodes
            .into_iter()
            .map(|(node_id, meta)| rpc::Node {
                node_id,
                meta: Some(meta),
            })
            .collect();

        let leader_id =
            leader_id.ok_or(CurpError::LeaderTransfer("no current leader".to_owned()))?;

        Ok(MembershipResponse {
            members,
            nodes,
            term,
            leader_id,
        })
    }

    /// Wait the command with the propose id to be committed
    async fn wait_commit<Ids: IntoIterator<Item = ProposeId>>(&self, propose_ids: Ids) {
        self.curp.wait_propose_ids(propose_ids).await;
    }

    /// Ensures there are no overlapping ids
    fn ensure_non_overlapping<Changes>(changes: Changes) -> Result<Vec<Change>, CurpError>
    where
        Changes: IntoIterator<Item = Change>,
    {
        let changes: Vec<_> = changes.into_iter().collect();
        let mut ids = changes.iter().map(|c| match *c {
            Change::Add(ref node) => node.node_id,
            Change::Remove(id) | Change::Promote(id) | Change::Demote(id) => id,
        });

        let mut set = HashSet::new();
        if ids.all(|id| set.insert(id)) {
            return Ok(changes);
        }

        Err(CurpError::InvalidMemberChange(()))
    }

    /// Ensures that the current node is the leader
    fn ensure_leader(&self) -> Result<(), CurpError> {
        let (leader_id, term, is_leader) = self.curp.leader();
        if is_leader {
            return Ok(());
        }
        Err(CurpError::Redirect(Redirect {
            leader_id: leader_id.map(Into::into),
            term,
        }))
    }
}

// Common methods shared by both leader and followers
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Updates the membership config
    pub(crate) fn update_states_with_membership(&self, membership: &Membership) {
        let connects = self.connect_other_nodes(membership);
        let new_states = self.curp.update_node_states(connects);
        self.spawn_sync_follower_tasks(new_states.into_values());
        self.curp.update_role();
    }

    /// Filter out membership log entries
    pub(crate) fn filter_membership_entries<E, I>(
        entries: I,
    ) -> impl Iterator<Item = (LogIndex, Membership)>
    where
        E: AsRef<LogEntry<C>>,
        I: IntoIterator<Item = E>,
    {
        entries.into_iter().filter_map(|entry| {
            let entry = entry.as_ref();
            if let EntryData::Member(ref m) = entry.entry_data {
                Some((entry.index, m.clone()))
            } else {
                None
            }
        })
    }

    /// Establishes connections to all nodes specified in the membership configuration,
    /// excluding the current node.
    pub(crate) fn connect_other_nodes(
        &self,
        config: &Membership,
    ) -> BTreeMap<u64, InnerConnectApiWrapper> {
        let self_id = self.curp.id();
        let nodes = config
            .nodes
            .iter()
            .filter_map(|(id, meta)| (*id != self_id).then_some((*id, meta.peer_urls().to_vec())))
            .collect();

        inner_connects(nodes, self.curp.client_tls_config()).collect()
    }

    /// Spawns background follower sync tasks
    pub(super) fn spawn_sync_follower_tasks(&self, new_nodes: impl IntoIterator<Item = NodeState>) {
        let task_manager = self.curp.task_manager();
        for (connect, sync_event, remove_event) in new_nodes.into_iter().map(NodeState::into_parts)
        {
            task_manager.spawn(TaskName::SyncFollower, |n| {
                Self::sync_follower_task(
                    Arc::clone(&self.curp),
                    connect,
                    sync_event,
                    remove_event,
                    n,
                )
            });
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use curp_test_utils::{
        mock_role_change,
        test_cmd::{TestCE, TestCommand},
        TestRoleChange,
    };
    use engine::MemorySnapshotAllocator;
    use parking_lot::{Mutex, RwLock};
    use tokio::sync::mpsc;
    use tracing_test::traced_test;
    use utils::{config::EngineConfig, task_manager::TaskManager};

    use crate::{
        rpc::NodeMetadata,
        server::{cmd_board::CommandBoard, RawCurp, StorageApi, DB},
    };

    use super::*;

    fn build_curp_node() -> CurpNode<TestCommand, TestCE, TestRoleChange> {
        let curp = Arc::new(RawCurp::new_test(
            3,
            mock_role_change(),
            Arc::new(TaskManager::new()),
        ));
        let db_dir = tempfile::tempdir().unwrap().into_path();
        let storage_cfg = EngineConfig::RocksDB(db_dir.clone());
        let db = DB::<TestCommand>::open(&storage_cfg).unwrap();
        let (exe_tx, _exe_rx) = mpsc::unbounded_channel();
        let (tas_tx, _tas_rx) = mpsc::unbounded_channel();
        let (as_tx, _as_rx) = flume::unbounded();
        let (propose_tx, _propose_rx) = flume::unbounded();
        let ce = TestCE::new("testce".to_owned(), exe_tx, tas_tx, storage_cfg);
        let _ignore = db.recover().unwrap();

        CurpNode {
            curp: Arc::clone(&curp),
            cmd_board: Arc::new(RwLock::new(CommandBoard::new())),
            storage: Arc::new(db),
            snapshot_allocator: Box::new(MemorySnapshotAllocator::default()),
            cmd_executor: Arc::new(ce),
            as_tx,
            propose_tx,
            replication_handles: Mutex::default(),
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_handle_append_entries_will_update_membership() {
        let curp_node = build_curp_node();
        let curp = Arc::clone(&curp_node.curp);
        let init_membership = Membership::new(
            vec![(0..3).collect()],
            (0..3)
                .map(|i| (i, NodeMetadata::new(format!("S{i}"), ["addr"], ["addr"])))
                .collect(),
        );

        let membership = Membership::new(
            vec![(0..4).collect()],
            (0..4)
                .map(|i| (i, NodeMetadata::new(format!("S{i}"), ["addr"], ["addr"])))
                .collect(),
        );
        let entry_data = EntryData::Member(membership.clone());
        let entry = LogEntry::new(1, 0, ProposeId::default(), entry_data);

        let resp = curp_node
            .append_entries_inner(vec![entry.clone()], 0, 1, 0, 0, 0)
            .unwrap();
        assert!(resp.success);

        // append entries should update effective membership
        assert_eq!(curp.effective_membership(), membership);
        // append entries should update node states
        assert!(curp.node_states().contains_key(&3));
        // append entries should spawn new sync task
        assert_eq!(
            curp.task_manager()
                .num_handles(TaskName::SyncFollower)
                .unwrap(),
            1
        );
        // append entries should update the in-memory log structure
        assert_eq!(*curp.get_log_from(1)[0].as_ref(), entry);
        // append entries should persistent the membership
        let (id, ms) = curp.persisted_membership().unwrap();
        assert_eq!(id, 0);
        assert_eq!(*ms.effective(), membership);
        assert_eq!(*ms.committed(), init_membership);
    }

    fn commit_memberhip(curp: &RawCurp<TestCommand, TestRoleChange>, index: u64) {
        // for follower [1, 2]
        for id in 1..3 {
            assert!(curp
                .handle_append_entries_resp(id, Some(index), 1, true, index + 1)
                .unwrap());
        }
        curp.trigger_all();
    }

    async fn change_membership(
        curp_node: Arc<CurpNode<TestCommand, TestCE, TestRoleChange>>,
        change: Change,
    ) -> u64 {
        let curp = Arc::clone(&curp_node.curp);
        let mut commit_index = curp.last_log_index();
        let mut handle =
            tokio::spawn(async move { curp_node.change_membership_inner([change]).await });
        // change membership should wait before commit
        while {
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err()
        } {
            commit_index += 1;
            commit_memberhip(&curp, commit_index);
        }

        commit_index
    }

    //#[traced_test]
    #[tokio::test]
    async fn test_change_membership_will_update_membership() {
        let curp_node = Arc::new(build_curp_node());
        let curp = Arc::clone(&curp_node.curp);
        let init_membership = Membership::new(
            vec![(0..3).collect()],
            (0..3)
                .map(|i| (i, NodeMetadata::new(format!("S{i}"), ["addr"], ["addr"])))
                .collect(),
        );
        let change1 = Change::Add(rpc::Node::new(
            3,
            NodeMetadata::new("S3".to_owned(), ["addr"], ["addr"]),
        ));
        let membership1 = Membership::new(
            vec![(0..3).collect()],
            (0..4)
                .map(|i| (i, NodeMetadata::new(format!("S{i}"), ["addr"], ["addr"])))
                .collect(),
        );
        let last_index = change_membership(Arc::clone(&curp_node), change1).await;
        // committed one membership log entry
        assert_eq!(last_index, 1);

        // append entries should update effective membership
        assert_eq!(curp.effective_membership(), membership1.clone());
        // append entries should update node states
        assert!(curp.node_states().contains_key(&3));
        // append entries should spawn new sync task
        assert_eq!(
            curp.task_manager()
                .num_handles(TaskName::SyncFollower)
                .unwrap(),
            1
        );
        // append entries should update the in-memory log structure
        let EntryData::Member(entry) = curp.get_log_from(1)[0].as_ref().entry_data.clone() else {
            unreachable!()
        };
        assert_eq!(entry, membership1);
        // append entries should persistent the membership
        let (id, ms) = curp.persisted_membership().unwrap();
        assert_eq!(id, 0);
        assert_eq!(*ms.effective(), membership1);
        assert_eq!(*ms.committed(), init_membership);

        // promote the learner added previously
        let change2 = Change::Promote(3);
        let membership2 = Membership::new(
            vec![(0..4).collect()],
            (0..4)
                .map(|i| (i, NodeMetadata::new(format!("S{i}"), ["addr"], ["addr"])))
                .collect(),
        );
        let last_index = change_membership(curp_node, change2).await;
        // committed two membership(from 2 ot 3) log entry
        assert_eq!(last_index, 3);
        assert_eq!(curp.effective_membership(), membership2.clone());
    }

    #[traced_test]
    #[tokio::test]
    async fn test_change_membership_will_reject_duplicate_ids() {
        let curp_node = build_curp_node();
        let change1 = Change::Add(rpc::Node::new(
            3,
            NodeMetadata::new("S3".to_owned(), ["addr"], ["addr"]),
        ));
        let change2 = Change::Add(rpc::Node::new(
            3,
            NodeMetadata::new("S3".to_owned(), ["addr1"], ["addr1"]),
        ));
        assert_eq!(
            curp_node
                .change_membership_inner([change1, change2])
                .await
                .unwrap_err(),
            CurpError::InvalidMemberChange(())
        );
    }
}
