use std::collections::HashMap;

use event_listener::{Event, EventListener};

/// State of current node
#[derive(Debug, Default)]
pub(crate) struct State {
    /// Server id
    id: String,
    /// Leader id
    leader_id: Option<String>,
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
            leader_id,
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
            .as_ref()
            .and_then(|id| self.members.get(id).map(String::as_str))
    }

    /// listener of leader change
    pub(crate) fn leader_listener(&self) -> EventListener {
        self.event.listen()
    }

    /// Set leader id
    pub(crate) fn set_leader_id(&mut self, leader_id: Option<String>) {
        self.leader_id = leader_id;
        if self.leader_id.is_some() {
            self.event.notify(usize::MAX);
        }
    }

    /// Check if current node is leader
    pub(crate) fn is_leader(&self) -> bool {
        if let Some(ref leader_id) = self.leader_id {
            self.id == *leader_id
        } else {
            false
        }
    }

    /// Get address of other members
    pub(crate) fn others(&self) -> HashMap<String, String> {
        let mut members = self.members.clone();
        let _ignore = members.remove(&self.id);
        members
    }
}
