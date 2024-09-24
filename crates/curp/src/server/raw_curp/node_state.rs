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
    pub(super) fn update_with<ConnectTo, Connects>(
        &self,
        ids: &BTreeSet<u64>,
        mut connect_to: ConnectTo,
    ) -> BTreeMap<u64, NodeState>
    where
        ConnectTo: FnMut(&BTreeSet<u64>) -> Connects,
        Connects: IntoIterator<Item = InnerConnectApiWrapper>,
    {
        let mut states_w = self.states.write();
        let old_ids: BTreeSet<_> = states_w.keys().copied().collect();
        let added: BTreeSet<_> = ids.difference(&old_ids).copied().collect();
        let removed: BTreeSet<_> = old_ids.difference(ids).copied().collect();
        removed
            .iter()
            .filter_map(|id| states_w.remove(id))
            .for_each(|s| s.notify_remove());
        states_w.retain(|id, _| !removed.contains(id));
        let new_connects = connect_to(&added);
        let new_states: BTreeMap<_, _> = added
            .clone()
            .into_iter()
            .zip(new_connects.into_iter().map(NodeState::new))
            .collect();
        states_w.extend(new_states.clone());

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

    /// Clone parts of self
    pub(super) fn clone_parts(
        &self,
    ) -> (NodeStatus, InnerConnectApiWrapper, Arc<Event>, Arc<Event>) {
        let NodeState {
            ref status,
            ref connect,
            ref sync_event,
            ref remove_event,
        } = *self;
        (
            *status,
            connect.clone(),
            Arc::clone(sync_event),
            Arc::clone(remove_event),
        )
    }
}
