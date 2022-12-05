use std::collections::HashMap;

use event_listener::{Event, EventListener};
use parking_lot::RwLock;

/// State of current node
#[derive(Debug, Default)]
pub(crate) struct State {
    /// Server id
    id: String,
    /// Leader id
    leader_id: RwLock<Option<String>>,
    /// Address of all members
    members: HashMap<String, String>,
    /// leader change event, notify when get new leader_id
    event: Event,
}

impl State {
    /// New `State`
    pub(crate) fn new(
        id: String,
        leader_id: Option<String>,
        members: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            leader_id: RwLock::new(leader_id),
            members,
            event: Event::new(),
        }
    }

    /// Get server id
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// Get leader address
    pub(crate) fn leader_address(&self) -> Option<&str> {
        self.leader_id
            .read()
            .as_ref()
            .and_then(|id| self.members.get(id).map(String::as_str))
    }

    /// listener of leader change
    pub(crate) fn leader_listener(&self) -> EventListener {
        self.event.listen()
    }

    /// Set leader id
    pub(crate) fn set_leader_id(&self, leader_id: Option<String>) -> bool {
        let mut leader_id_w = self.leader_id.write();
        let is_leader_before = leader_id_w.as_ref().map_or(false, |id| self.id == *id);
        *leader_id_w = leader_id;
        let is_leader_after = leader_id_w.as_ref().map_or(false, |id| self.id == *id);
        let leader_state_changed = is_leader_before ^ is_leader_after;
        if leader_id_w.is_some() {
            self.event.notify(usize::MAX);
        }
        leader_state_changed
    }

    /// Check if current node is leader
    pub(crate) fn is_leader(&self) -> bool {
        self.leader_id
            .read()
            .as_ref()
            .map_or(false, |id| self.id == *id)
    }

    /// Get address of other members
    pub(crate) fn others(&self) -> HashMap<String, String> {
        let mut members = self.members.clone();
        let _ignore = members.remove(&self.id);
        members
    }

    /// Wait leader until current node has a leader
    pub(crate) async fn wait_leader(&self) -> Result<String, tonic::Status> {
        let listener = {
            if let Some(leader_addr) = self.leader_address() {
                return Ok(leader_addr.to_owned());
            }
            self.leader_listener()
        };

        listener.await;
        self.leader_address()
            .map(str::to_owned)
            .ok_or_else(|| tonic::Status::internal("Get leader address error"))
    }
}
