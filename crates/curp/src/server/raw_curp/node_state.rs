use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use curp_external_api::LogIndex;
use event_listener::Event;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::rpc::connect::InnerConnectApiWrapper;

use super::state::NodeStatus;

/// States of all nodes
#[derive(Debug)]
pub(crate) struct NodeStates {
    /// The states
    states: RwLock<BTreeMap<u64, NodeState>>,
}

impl NodeStates {
    /// Creates a new `NodeStates`
    pub(super) fn new_from_connects<Connects>(connects: Connects) -> Self
    where
        Connects: IntoIterator<Item = (u64, InnerConnectApiWrapper)>,
    {
        let states = connects
            .into_iter()
            .map(|(id, conn)| (id, NodeState::new(conn)))
            .collect();

        Self {
            states: RwLock::new(states),
        }
    }

    /// Updates the node states based on the provided set of ids.
    ///
    /// Returns the newly added node states.
    pub(super) fn update_with(
        &self,
        connects: BTreeMap<u64, InnerConnectApiWrapper>,
    ) -> BTreeMap<u64, NodeState> {
        let mut states_w = self.states.write();
        let ids: BTreeSet<_> = connects.keys().copied().collect();
        let old_ids: BTreeSet<_> = states_w.keys().copied().collect();
        let added: BTreeSet<_> = ids.difference(&old_ids).copied().collect();
        let removed: BTreeSet<_> = old_ids.difference(&ids).copied().collect();
        removed
            .iter()
            .filter_map(|id| states_w.remove(id))
            .for_each(|s| s.notify_remove());
        states_w.retain(|id, _| !removed.contains(id));
        let new_states: BTreeMap<_, _> = connects
            .into_iter()
            .filter_map(|(id, conn)| added.contains(&id).then_some((id, NodeState::new(conn))))
            .collect();
        states_w.append(&mut new_states.clone());

        info!("added nodes: {added:?}, removed nodes: {removed:?}");

        new_states
    }

    /// Update `next_index` for server
    pub(super) fn update_next_index(&self, id: u64, index: LogIndex) {
        let mut states_w = self.states.write();
        let opt = states_w
            .get_mut(&id)
            .map(|state| state.status_mut().next_index = index);
        if opt.is_none() {
            warn!("follower {} is not found, it maybe has been removed", id);
        }
    }

    /// Update `match_index` for server, will update `next_index` if possible
    pub(super) fn update_match_index(&self, id: u64, index: LogIndex) {
        let mut states_w = self.states.write();
        let opt = states_w.get_mut(&id).map(|state| {
            let status = state.status_mut();
            if status.match_index >= index {
                return;
            }
            status.match_index = index;
            status.next_index = index + 1;
            debug!("follower {id}'s match_index updated to {index}");
        });
        if opt.is_none() {
            warn!("follower {} is not found, it maybe has been removed", id);
        };
    }
    /// Get `next_index` for server
    pub(super) fn get_next_index(&self, id: u64) -> Option<LogIndex> {
        let states_r = self.states.read();
        states_r.get(&id).map(|state| state.status().next_index)
    }

    /// Get `match_index` for server
    pub(super) fn get_match_index(&self, id: u64) -> Option<LogIndex> {
        let states_r = self.states.read();
        states_r.get(&id).map(|state| state.status().match_index)
    }

    /// Create a `Iterator` for all statuses
    pub(super) fn map_status<F, R>(&self, f: F) -> impl Iterator<Item = R>
    where
        F: FnMut((&u64, &NodeStatus)) -> R,
    {
        let states_r = self.states.read();
        states_r
            .keys()
            .zip(states_r.values().map(NodeState::status))
            .map(f)
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Clone the references of the events
    pub(super) fn clone_events<I: IntoIterator<Item = u64>>(
        &self,
        ids: I,
    ) -> Vec<(Arc<Event>, Arc<Event>)> {
        let states_r = self.states.read();
        ids.into_iter()
            .filter_map(|id| states_r.get(&id).map(NodeState::close_events))
            .collect()
    }

    /// Notify sync events
    pub(super) fn notify_sync_events<F>(&self, filter: F)
    where
        F: Fn(LogIndex) -> bool,
    {
        let states_r = self.states.read();
        states_r
            .values()
            .filter(|state| filter(state.status().next_index))
            .for_each(|state| {
                let _ignore = state.sync_event().notify(1);
            });
    }

    /// Get rpc connect connects by ids
    pub(super) fn connects<'a, Ids: IntoIterator<Item = &'a u64>>(
        &self,
        ids: Ids,
    ) -> impl Iterator<Item = InnerConnectApiWrapper> {
        let states_r = self.states.read();
        ids.into_iter()
            .filter_map(|id| states_r.get(id).map(NodeState::connect).cloned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Get all rpc connects
    pub(super) fn all_connects(&self) -> BTreeMap<u64, InnerConnectApiWrapper> {
        let states_r = self.states.read();
        states_r
            .keys()
            .copied()
            .zip(states_r.values().map(NodeState::connect).cloned())
            .collect()
    }

    /// Get all node states
    #[cfg(test)]
    pub(super) fn all_states(&self) -> BTreeMap<u64, NodeState> {
        self.states.read().clone()
    }
}

/// The state of a node
#[derive(Clone, Debug)]
pub(crate) struct NodeState {
    /// The status of current node
    status: NodeStatus,
    /// The connect to the node
    connect: InnerConnectApiWrapper,
    /// Sync event trigger for a follower
    sync_event: Arc<Event>,
    /// Remove event trigger for a node
    remove_event: Arc<Event>,
}

impl NodeState {
    /// Creates a new `NodeState`
    fn new(connect: InnerConnectApiWrapper) -> Self {
        Self {
            connect,
            status: NodeStatus::default(),
            sync_event: Arc::default(),
            remove_event: Arc::default(),
        }
    }

    /// Get the status of the current node
    pub(super) fn status(&self) -> &NodeStatus {
        &self.status
    }

    /// Get the connection to the node
    pub(super) fn connect(&self) -> &InnerConnectApiWrapper {
        &self.connect
    }

    /// Clone the references of the events
    fn close_events(&self) -> (Arc<Event>, Arc<Event>) {
        (Arc::clone(&self.sync_event), Arc::clone(&self.remove_event))
    }

    /// Get the sync event trigger for a follower
    pub(super) fn sync_event(&self) -> &Event {
        &self.sync_event
    }

    /// Notify the remove event
    pub(super) fn notify_remove(&self) {
        let _ignore = self.remove_event.notify(1);
    }

    /// Get a mutable reference to the status of the current node
    pub(super) fn status_mut(&mut self) -> &mut NodeStatus {
        &mut self.status
    }

    /// Decomposes the `NodeState` into its constituent parts.
    pub(crate) fn into_parts(self) -> (InnerConnectApiWrapper, Arc<Event>, Arc<Event>) {
        let NodeState {
            connect,
            sync_event,
            remove_event,
            ..
        } = self;

        (connect, sync_event, remove_event)
    }
}
#[cfg(test)]
mod tests {
    use utils::parking_lot_lock::RwLockMap;

    use super::*;
    use crate::rpc::connect::{InnerConnectApiWrapper, MockInnerConnectApi};
    use std::sync::Arc;

    fn build_new_connect(id: u64) -> InnerConnectApiWrapper {
        let mut connect = MockInnerConnectApi::new();
        connect.expect_id().returning(move || id);
        InnerConnectApiWrapper::new_from_arc(Arc::new(connect))
    }

    fn build_initial_node_states() -> NodeStates {
        let init = (0..3).map(|id| (id, build_new_connect(id)));
        let node_states = NodeStates::new_from_connects(init);
        let ids: Vec<_> = node_states.states.map_read(|s| s.keys().copied().collect());
        assert_eq!(ids, [0, 1, 2]);
        node_states
    }

    #[test]
    fn test_node_state_update_case0() {
        let node_states = build_initial_node_states();
        node_states.update_match_index(2, 1);
        node_states.update_next_index(2, 2);

        // adds some nodes
        let new_connects = (0..5).map(|id| (id, build_new_connect(id))).collect();
        let new_states = node_states.update_with(new_connects);
        assert_eq!(new_states.keys().copied().collect::<Vec<_>>(), [3, 4]);

        let ids: Vec<_> = node_states.states.map_read(|s| s.keys().copied().collect());
        assert_eq!(ids, [0, 1, 2, 3, 4]);
        // makes sure that index won't be override
        assert_eq!(node_states.get_match_index(2), Some(1));
        assert_eq!(node_states.get_next_index(2), Some(2));
    }

    #[test]
    fn test_node_state_update_case1() {
        let node_states = build_initial_node_states();

        // remove some nodes
        let new_connects = (0..2).map(|id| (id, build_new_connect(id))).collect();
        let new_states = node_states.update_with(new_connects);
        assert_eq!(new_states.keys().count(), 0);

        let ids: Vec<_> = node_states.states.map_read(|s| s.keys().copied().collect());
        assert_eq!(ids, [0, 1]);
    }

    #[test]
    fn test_update_and_get_indices() {
        let node_states = build_initial_node_states();
        node_states.update_match_index(0, 1);
        node_states.update_match_index(1, 2);
        node_states.update_match_index(2, 3);

        node_states.update_next_index(0, 1);
        node_states.update_next_index(1, 2);
        node_states.update_next_index(2, 3);

        assert_eq!(node_states.get_match_index(0), Some(1));
        assert_eq!(node_states.get_match_index(1), Some(2));
        assert_eq!(node_states.get_match_index(2), Some(3));

        assert_eq!(node_states.get_next_index(0), Some(1));
        assert_eq!(node_states.get_next_index(1), Some(2));
        assert_eq!(node_states.get_next_index(2), Some(3));
    }

    #[test]
    fn test_map_status() {
        let node_states = build_initial_node_states();
        let ids: Vec<_> = node_states.map_status(|(id, _status)| *id).collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn test_get_connects() {
        let node_states = build_initial_node_states();
        let ids: Vec<_> = node_states.connects(&[1, 2]).map(|c| c.id()).collect();
        assert_eq!(ids, vec![1, 2]);
    }
}
