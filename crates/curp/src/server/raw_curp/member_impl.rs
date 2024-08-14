use std::collections::BTreeMap;
use std::sync::Arc;

use curp_external_api::cmd::Command;
use curp_external_api::role_change::RoleChange;
use curp_external_api::LogIndex;
use event_listener::Event;
use rand::Rng;

use crate::log_entry::EntryData;
use crate::log_entry::LogEntry;
use crate::member::Change;
use crate::member::Membership;
use crate::member::NodeMembershipState;
use crate::rpc::connect::InnerConnectApiWrapper;
use crate::rpc::inner_connects;
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
        ms_r.cluster().committed().change(change)
    }

    /// Updates the membership config
    pub(crate) fn update_membership<F>(&self, config: Membership, spawn_sync: F) -> ProposeId
    where
        F: Fn(Arc<Event>, Arc<Event>, InnerConnectApiWrapper),
    {
        // FIXME: define the lock order of log and ms
        let mut log_w = self.log.write();
        let mut ms_w = self.ms.write();
        let st_r = self.st.read();
        let propose_id = ProposeId(rand::random(), 0);
        let entry = log_w.push(st_r.term, propose_id, config.clone());
        let new_connects = self.build_connects(&config);
        ms_w.cluster_mut().append(entry.index, config);
        let (removed, added) = ms_w.update_connects(&new_connects);
        self.update_node_sync(removed, added, spawn_sync);

        propose_id
    }

    /// Append membership entries
    pub(crate) fn append_membership<E, I, F>(
        &self,
        entries: I,
        truncate_at: LogIndex,
        commit_index: LogIndex,
        spawn_sync: F,
    ) where
        E: AsRef<LogEntry<C>>,
        I: IntoIterator<Item = E>,
        F: Fn(Arc<Event>, Arc<Event>, InnerConnectApiWrapper),
    {
        let mut ms_w = self.ms.write();
        ms_w.cluster_mut().truncate(truncate_at);
        let configs = entries.into_iter().filter_map(|entry| {
            let entry = entry.as_ref();
            if let EntryData::Member(ref m) = entry.entry_data {
                Some((entry.index, m.clone()))
            } else {
                None
            }
        });
        for (index, config) in configs {
            let new_connects = self.build_connects(&config);
            let (removed, added) = ms_w.update_connects(&new_connects);
            self.update_node_sync(removed, added, &spawn_sync);
            ms_w.cluster_mut().append(index, config);
            ms_w.cluster_mut().commit(commit_index.min(index));
        }

        self.update_role(&ms_w);
    }

    /// Updates the commit index
    pub(crate) fn membership_commit_to(&self, index: LogIndex) {
        let mut ms_w = self.ms.write();
        ms_w.cluster_mut().commit(index);
    }

    /// Updates the role of the node based on the current membership state
    fn update_role(&self, current: &NodeMembershipState) {
        let mut st_w = self.st.write();
        if current.is_member() {
            if matches!(st_w.role, Role::Learner) {
                st_w.role = Role::Follower;
            }
        } else {
            st_w.role = Role::Learner;
        }
    }

    /// Creates connections for new membership configuration.
    ///
    /// Returns a closure can be used to update the existing connections
    fn build_connects(&self, config: &Membership) -> BTreeMap<u64, InnerConnectApiWrapper> {
        let nodes = config
            .nodes
            .iter()
            .map(|(id, addr)| (*id, vec![addr.clone()]))
            .collect();

        inner_connects(nodes, self.client_tls_config()).collect()
    }

    /// Updates the background task of node sync
    /// TODO: member persistent
    fn update_node_sync<F>(
        &self,
        removed: BTreeMap<u64, InnerConnectApiWrapper>,
        added: BTreeMap<u64, InnerConnectApiWrapper>,
        spawn_sync: F,
    ) where
        F: Fn(Arc<Event>, Arc<Event>, InnerConnectApiWrapper),
    {
        let mut remove_events_l = self.ctx.remove_events.lock();
        for (id, connect) in added {
            let sync_event = Arc::new(Event::new());
            let remove_event = Arc::new(Event::new());
            _ = self.ctx.sync_events.insert(id, Arc::new(Event::new()));
            let _ignore = remove_events_l.insert(id, Arc::new(Event::new()));
            spawn_sync(sync_event, remove_event, connect);
        }
        for (id, _connect) in removed {
            _ = self.ctx.sync_events.remove(&id);
            assert!(
                remove_events_l.remove(&id).map(|e| e.notify(1)).is_some(),
                "id doesn't exist"
            );
            // TODO: update persistent membership
        }
    }
}
