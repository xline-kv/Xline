use std::collections::HashMap;

/// State of current node
#[derive(Debug)]
pub(crate) struct State {
    /// Server id
    id: String,
    /// Leader id
    leader_id: Option<String>,
    /// Address of all members
    members: HashMap<String, String>,
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

    /// Set leader id
    pub(crate) fn set_leader_id(&mut self, leader_id: Option<String>) {
        self.leader_id = leader_id;
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
