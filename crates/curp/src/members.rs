use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::{mapref::one::Ref, DashMap};
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::{debug, info};
#[cfg(madsim)]
use utils::ClientTlsConfig;

use crate::rpc::{self, FetchClusterRequest, FetchClusterResponse, Member};

/// Server Id
pub type ServerId = u64;

/// Cluster member
impl Member {
    /// Create a new `Member`
    #[inline]
    pub fn new(
        id: ServerId,
        name: impl Into<String>,
        peer_urls: impl Into<Vec<String>>,
        client_urls: impl Into<Vec<String>>,
        is_learner: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            peer_urls: peer_urls.into(),
            client_urls: client_urls.into(),
            is_learner,
        }
    }

    /// Get member id
    #[must_use]
    #[inline]
    pub fn id(&self) -> ServerId {
        self.id
    }

    /// Get member name
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get member addresses
    #[must_use]
    #[inline]
    pub fn peer_urls(&self) -> &[String] {
        self.peer_urls.as_slice()
    }

    /// Is learner or not
    #[must_use]
    #[inline]
    pub fn is_learner(&self) -> bool {
        self.is_learner
    }
}

/// cluster members information
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    /// cluster id
    cluster_id: u64,
    /// current server id
    member_id: ServerId,
    /// all members information
    members: DashMap<ServerId, Member>,
    /// cluster version
    cluster_version: Arc<AtomicU64>,
}

impl ClusterInfo {
    /// Construct a new `ClusterInfo`
    #[inline]
    #[must_use]
    pub fn new(cluster_id: u64, member_id: u64, members: Vec<Member>) -> Self {
        Self {
            cluster_id,
            member_id,
            members: members.into_iter().map(|m| (m.id, m)).collect(),
            cluster_version: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Construct a new `ClusterInfo` from members map
    /// # Panics
    /// panic if `all_members` is empty
    #[inline]
    #[must_use]
    pub fn from_members_map(
        all_members_peer_urls: HashMap<String, Vec<String>>,
        self_client_urls: impl Into<Vec<String>>,
        self_name: &str,
    ) -> Self {
        let mut member_id = 0;
        let self_client_urls = self_client_urls.into();
        let members = DashMap::new();
        for (name, peer_urls) in all_members_peer_urls {
            let id = Self::calculate_member_id(peer_urls.clone(), "", None);
            let mut member = Member::new(id, name.clone(), peer_urls, [], false);
            if name == self_name {
                member_id = id;
                member.client_urls = self_client_urls.clone();
            }
            let _ig = members.insert(id, member);
        }
        debug_assert!(member_id != 0, "self_id should not be 0");
        let mut cluster_info = Self {
            cluster_id: 0,
            member_id,
            members,
            cluster_version: Arc::new(AtomicU64::new(0)),
        };
        cluster_info.gen_cluster_id();
        cluster_info
    }

    /// Construct a new `ClusterInfo` from `FetchClusterResponse`
    /// # Panics
    /// panic if `cluster.members` doesn't contain `self_addr`
    #[inline]
    #[must_use]
    pub fn from_cluster(
        cluster: FetchClusterResponse,
        self_peer_urls: &[String],
        self_name: &str,
    ) -> Self {
        let mut member_id = 0;
        let members = cluster
            .members
            .into_iter()
            .map(|mut member| {
                if member.peer_urls() == self_peer_urls {
                    member_id = member.id;
                    member.name = self_name.to_owned();
                }
                (member.id, member)
            })
            .collect();
        assert!(member_id != 0, "self_id should not be 0");
        Self {
            cluster_id: cluster.cluster_id,
            member_id,
            members,
            cluster_version: Arc::new(AtomicU64::new(cluster.cluster_version)),
        }
    }

    /// Get all members
    #[must_use]
    #[inline]
    pub fn all_members(&self) -> HashMap<ServerId, Member> {
        self.members
            .iter()
            .map(|t| (t.id, t.value().clone()))
            .collect()
    }

    /// Get all members vec
    #[must_use]
    #[inline]
    pub fn all_members_vec(&self) -> Vec<Member> {
        self.members.iter().map(|t| t.value().clone()).collect()
    }

    /// Insert a member
    #[inline]
    #[must_use]
    pub fn insert(&self, member: Member) -> Option<Member> {
        self.members.insert(member.id, member)
    }

    /// Remove a member
    #[inline]
    #[must_use]
    pub fn remove(&self, id: &ServerId) -> Option<Member> {
        self.members.remove(id).map(|(_id, m)| m)
    }

    /// Get a member
    #[inline]
    #[must_use]
    pub fn get(&self, id: &ServerId) -> Option<Ref<'_, u64, Member>> {
        self.members.get(id)
    }

    /// Update a member and return old addrs
    #[inline]
    pub fn update(&self, id: &ServerId, addrs: impl Into<Vec<String>>) -> Vec<String> {
        let mut addrs = addrs.into();
        let mut member = self
            .members
            .get_mut(id)
            .unwrap_or_else(|| unreachable!("member {} not found", id));
        std::mem::swap(&mut addrs, &mut member.peer_urls);
        addrs
    }

    /// Get server peer urls via server id
    #[must_use]
    #[inline]
    pub fn peer_urls(&self, id: ServerId) -> Option<Vec<String>> {
        self.members.get(&id).map(|t| t.peer_urls.clone())
    }

    /// Get server client urls via server id
    #[must_use]
    #[inline]
    pub fn client_urls(&self, id: ServerId) -> Option<Vec<String>> {
        self.members.get(&id).map(|t| t.client_urls.clone())
    }

    /// Get the current member
    /// # Panics
    /// panic if self member id is not in members
    #[allow(clippy::unwrap_used)] // self member id must be in members
    #[must_use]
    #[inline]
    pub fn self_member(&self) -> Ref<'_, u64, Member> {
        self.members.get(&self.member_id).unwrap()
    }

    /// Get the current server peer urls
    #[must_use]
    #[inline]
    pub fn self_peer_urls(&self) -> Vec<String> {
        self.self_member().peer_urls.clone()
    }

    /// Get the current server client addrs
    #[must_use]
    #[inline]
    pub fn self_client_urls(&self) -> Vec<String> {
        self.self_member().client_urls.clone()
    }

    /// Get the current server id
    #[must_use]
    #[inline]
    pub fn self_name(&self) -> String {
        self.self_member().name.clone()
    }

    /// Get peers ids
    #[must_use]
    #[inline]
    pub fn peers_ids(&self) -> Vec<ServerId> {
        self.members
            .iter()
            .filter(|t| t.id != self.member_id)
            .map(|t| t.id)
            .collect()
    }

    /// Get all ids
    #[must_use]
    #[inline]
    pub fn all_ids(&self) -> Vec<ServerId> {
        self.members.iter().map(|t| t.id).collect()
    }

    /// Calculate the member id
    #[inline]
    #[must_use]
    pub fn calculate_member_id(
        mut addrs: Vec<String>,
        cluster_name: &str,
        timestamp: Option<u64>,
    ) -> ServerId {
        let mut hasher = DefaultHasher::new();
        // to make sure same addrs but different order will get same id
        addrs.sort();
        for addr in addrs {
            hasher.write(addr.as_bytes());
        }
        hasher.write(cluster_name.as_bytes());
        if let Some(ts) = timestamp {
            hasher.write_u64(ts);
        }
        hasher.finish()
    }

    /// Calculate the cluster id
    fn gen_cluster_id(&mut self) {
        let mut hasher = DefaultHasher::new();
        for id in self.members.iter().map(|t| t.id).sorted() {
            hasher.write_u64(id);
        }
        self.cluster_id = hasher.finish();
    }

    /// Get member id
    #[must_use]
    #[inline]
    pub fn self_id(&self) -> ServerId {
        self.member_id
    }

    /// Get cluster id
    #[must_use]
    #[inline]
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Get cluster version
    #[must_use]
    #[inline]
    pub fn cluster_version(&self) -> u64 {
        self.cluster_version.load(Ordering::Relaxed)
    }

    /// cluster version decrease
    pub(crate) fn cluster_version_update(&self) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.all_members_peer_urls()
            .into_iter()
            .sorted()
            .for_each(|(id, mut addrs)| {
                id.hash(&mut hasher);
                addrs.sort();
                addrs.hash(&mut hasher);
            });
        let ver = hasher.finish();
        info!("cluster version updates to {ver}");
        self.cluster_version.store(ver, Ordering::Relaxed);
    }

    /// Get peers
    #[must_use]
    #[inline]
    pub fn peers_addrs(&self) -> HashMap<ServerId, Vec<String>> {
        self.members
            .iter()
            .filter(|t| t.id != self.member_id)
            .map(|t| (t.id, t.peer_urls.clone()))
            .collect()
    }

    /// Get all members
    #[must_use]
    #[inline]
    pub fn all_members_peer_urls(&self) -> HashMap<ServerId, Vec<String>> {
        self.members
            .iter()
            .map(|t| (t.id, t.peer_urls.clone()))
            .collect()
    }

    /// Get length of peers
    #[must_use]
    #[inline]
    pub fn voters_len(&self) -> usize {
        self.members.iter().filter(|t| !t.is_learner).count()
    }

    /// Get id by name
    #[must_use]
    #[inline]
    #[cfg(test)]
    pub fn get_id_by_name(&self, name: &str) -> Option<ServerId> {
        self.members
            .iter()
            .find_map(|m| (m.name == name).then_some(m.id))
    }

    /// Promote a learner to voter
    pub(crate) fn promote(&self, node_id: ServerId) -> bool {
        if let Some(mut s) = self.members.get_mut(&node_id) {
            s.is_learner = false;
            return true;
        }
        false
    }

    /// Demote a voter to learner
    pub(crate) fn demote(&self, node_id: ServerId) {
        if let Some(mut s) = self.members.get_mut(&node_id) {
            s.is_learner = true;
        }
    }

    /// Check if cluster contains a node
    pub(crate) fn contains(&self, node_id: ServerId) -> bool {
        self.members.contains_key(&node_id)
    }

    /// Set state for a node
    pub(crate) fn set_node_state(&self, node_id: ServerId, name: String, client_urls: Vec<String>) {
        if let Some(mut s) = self.members.get_mut(&node_id) {
            debug!(
                "set name and client_urls for node {} to {},{:?}",
                node_id, name, client_urls
            );
            s.name = name;
            s.client_urls = client_urls;
        }
    }
}

/// Get cluster info from remote servers
#[inline]
pub async fn get_cluster_info_from_remote(
    init_cluster_info: &ClusterInfo,
    self_peer_urls: &[String],
    self_name: &str,
    timeout: Duration,
    tls_config: Option<&ClientTlsConfig>,
) -> Option<ClusterInfo> {
    let peers = init_cluster_info.peers_addrs();
    let connects = rpc::connects(peers, tls_config)
        .await
        .ok()?
        .map(|pair| pair.1)
        .collect_vec();
    let mut futs = connects
        .iter()
        .map(|c| {
            c.fetch_cluster(
                FetchClusterRequest {
                    linearizable: false,
                },
                timeout,
            )
        })
        .collect::<FuturesUnordered<_>>();
    while let Some(result) = futs.next().await {
        if let Ok(cluster_res) = result {
            debug!("get cluster info from remote success: {:?}", cluster_res);
            return Some(ClusterInfo::from_cluster(
                cluster_res.into_inner(),
                self_peer_urls,
                self_name,
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_id() {
        let all_members = HashMap::from([
            ("S1".to_owned(), vec!["S1".to_owned()]),
            ("S2".to_owned(), vec!["S2".to_owned()]),
            ("S3".to_owned(), vec!["S3".to_owned()]),
        ]);

        let node1 = ClusterInfo::from_members_map(all_members.clone(), [], "S1");
        let node2 = ClusterInfo::from_members_map(all_members.clone(), [], "S2");
        let node3 = ClusterInfo::from_members_map(all_members, [], "S3");

        assert_ne!(node1.self_id(), node2.self_id());
        assert_ne!(node1.self_id(), node3.self_id());
        assert_ne!(node3.self_id(), node2.self_id());

        assert_eq!(node1.cluster_id(), node2.cluster_id());
        assert_eq!(node3.cluster_id(), node2.cluster_id());
    }

    #[test]
    fn test_get_peers() {
        let all_members = HashMap::from([
            ("S1".to_owned(), vec!["S1".to_owned()]),
            ("S2".to_owned(), vec!["S2".to_owned()]),
            ("S3".to_owned(), vec!["S3".to_owned()]),
        ]);

        let node1 = ClusterInfo::from_members_map(all_members, [], "S1");
        let peers = node1.peers_addrs();
        let node1_id = node1.self_id();
        let node1_url = node1.self_peer_urls();
        assert!(!peers.contains_key(&node1_id));
        assert_eq!(peers.len(), 2);
        assert_eq!(node1.voters_len(), peers.len() + 1);

        let peer_urls = peers.values().collect::<Vec<_>>();

        let peer_ids = node1.peers_ids();

        assert_eq!(peer_ids.len(), peer_urls.len());

        assert!(peer_urls.iter().find(|url| ***url == node1_url).is_none());
        assert!(peer_ids.iter().find(|id| **id == node1_id).is_none());
    }
}
