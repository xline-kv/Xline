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
