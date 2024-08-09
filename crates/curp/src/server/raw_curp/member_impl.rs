use curp_external_api::cmd::Command;
use curp_external_api::role_change::RoleChange;
use curp_external_api::LogIndex;
use rand::Rng;

use crate::log_entry::EntryData;
use crate::log_entry::LogEntry;
use crate::member::Change;
use crate::member::Membership;
use crate::member::MembershipState;
use crate::rpc::ProposeId;

use super::RawCurp;
use super::Role;

impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Generates new node ids
    /// TODO: makes sure that the ids are unique
    #[allow(clippy::unused_self)] // it should be used after the previous TODO
    pub(crate) fn new_node_ids(&self, n: usize) -> Vec<u64> {
        let mut rng = rand::thread_rng();
        (0..n).map(|_| rng.gen()).collect()
    }

    /// Generate memberships based on the provided change
    pub(crate) fn generate_membership(&self, change: Change) -> Vec<Membership> {
        let ms_r = self.ms.read();
        ms_r.committed().change(change)
    }

    /// Updates the membership config
    pub(crate) fn update_membership(&self, config: Membership) -> ProposeId {
        // FIXME: define the lock order of log and ms
        let mut log_w = self.log.write();
        let mut ms_w = self.ms.write();
        ms_w.update_effective(config.clone());
        let st_r = self.st.read();
        let propose_id = ProposeId(rand::random(), 0);
        let _entry = log_w.push(st_r.term, propose_id, config);
        propose_id
    }

    /// Append membership entries
    pub(crate) fn append_membership<E, I>(
        &self,
        entries: I,
        truncate_at: LogIndex,
        commit_index: LogIndex,
    ) where
        E: AsRef<LogEntry<C>>,
        I: IntoIterator<Item = E>,
    {
        let mut ms_w = self.ms.write();
        ms_w.truncate(truncate_at);
        let configs = entries.into_iter().filter_map(|entry| {
            let entry = entry.as_ref();
            if let EntryData::Member(ref m) = entry.entry_data {
                Some((entry.index, m.clone()))
            } else {
                None
            }
        });
        for (index, config) in configs {
            ms_w.append(index, config);
            ms_w.commit(commit_index.min(index));
        }

        self.update_role(&ms_w);
    }

    /// Updates the commit index
    pub(crate) fn membership_commit_to(&self, index: LogIndex) {
        let mut ms_w = self.ms.write();
        ms_w.commit(index);
    }

    /// Updates the role of the node based on the current membership state
    fn update_role(&self, current: &MembershipState) {
        // FIXME: implement node id
        let id = 0;
        let mut st_w = self.st.write();
        if current.effective().contains(id) {
            if matches!(st_w.role, Role::Learner) {
                st_w.role = Role::Follower;
            }
        } else {
            st_w.role = Role::Learner;
        }
    }
}
