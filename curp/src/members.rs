use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::Hasher,
    time::SystemTime,
};

use itertools::Itertools;

use crate::ServerId;

/// cluster members information
#[derive(Debug)]
pub struct ClusterMember {
    /// current server id
    self_id: ServerId,
    /// current server url
    self_address: String,
    /// other peers information
    peers: HashMap<ServerId, String>,
}

impl ClusterMember {
    /// Construct a new `ClusterMember`
    ///
    /// # Panics
    ///
    /// panic if `all_members` is empty
    #[inline]
    #[must_use]
    pub fn new(mut all_members: HashMap<ServerId, String>, id: String) -> Self {
        let address = all_members.remove(id.as_str()).unwrap_or_else(|| {
            unreachable!(
                "The address of {} not found in all_members {:?}",
                id, all_members
            )
        });
        Self {
            self_id: id,
            self_address: address,
            peers: all_members,
        }
    }

    /// get server address via server id
    #[must_use]
    #[inline]
    pub fn address(&self, id: &str) -> Option<&str> {
        if id == self.self_id {
            Some(self.self_address())
        } else {
            self.peers.get(id).map(String::as_str)
        }
    }

    /// get the current server address
    #[must_use]
    #[inline]
    pub fn self_address(&self) -> &str {
        self.self_address.as_str()
    }

    /// get the current server id
    #[must_use]
    #[inline]
    pub fn self_id(&self) -> &ServerId {
        &self.self_id
    }

    /// get peers id
    #[must_use]
    #[inline]
    pub fn peers_id(&self) -> Vec<ServerId> {
        self.peers.keys().cloned().collect()
    }

    /// calculate the member id
    #[must_use]
    #[inline]
    pub fn gen_member_id(&self, cluster_name: &str) -> u64 {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|e| unreachable!("SystemTime before UNIX EPOCH! {e}"))
            .as_secs();
        let mut hasher = DefaultHasher::new();
        hasher.write(self.self_address().as_bytes());
        hasher.write(cluster_name.as_bytes());
        hasher.write_u64(ts);
        hasher.finish()
    }

    /// calculate the cluster id
    #[must_use]
    #[inline]
    pub fn gen_cluster_id(&self, cluster_name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        let cluster_members = self.all_members();

        let member_urls = cluster_members
            .values()
            .sorted()
            .map(String::as_str)
            .collect::<Vec<_>>();

        for url in member_urls {
            hasher.write(url.as_bytes());
        }
        hasher.write(cluster_name.as_bytes());
        hasher.finish()
    }

    /// get peers
    #[must_use]
    #[inline]
    pub fn peers(&self) -> HashMap<ServerId, String> {
        self.peers.clone()
    }

    /// get all members
    #[must_use]
    #[inline]
    pub fn all_members(&self) -> HashMap<ServerId, String> {
        let mut cluster_members = self.peers.clone();
        let _ignore = cluster_members.insert(self.self_id.clone(), self.self_address.clone());
        cluster_members
    }

    /// peers count
    #[must_use]
    #[inline]
    pub fn peers_len(&self) -> usize {
        self.peers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_id() {
        let all_members: HashMap<ServerId, String> = vec![
            ("S1".to_owned(), "S1".to_owned()),
            ("S2".to_owned(), "S2".to_owned()),
            ("S3".to_owned(), "S3".to_owned()),
        ]
        .into_iter()
        .collect();

        let node1 = ClusterMember::new(all_members.clone(), "S1".to_owned());
        let node2 = ClusterMember::new(all_members.clone(), "S2".to_owned());
        let node3 = ClusterMember::new(all_members, "S3".to_owned());

        assert_ne!(node1.gen_member_id(""), node2.gen_member_id(""));
        assert_ne!(node1.gen_member_id(""), node3.gen_member_id(""));
        assert_ne!(node3.gen_member_id(""), node2.gen_member_id(""));

        assert_eq!(node1.gen_cluster_id(""), node2.gen_cluster_id(""));
        assert_eq!(node3.gen_cluster_id(""), node2.gen_cluster_id(""));
    }

    #[test]
    fn test_get_peers() {
        let all_members: HashMap<ServerId, String> = vec![
            ("S1".to_owned(), "S1".to_owned()),
            ("S2".to_owned(), "S2".to_owned()),
            ("S3".to_owned(), "S3".to_owned()),
        ]
        .into_iter()
        .collect();

        let node1 = ClusterMember::new(all_members, "S1".to_owned());
        let peers = node1.peers();
        let node1_id = node1.self_id();
        let node1_url = node1.self_address();
        assert!(!peers.contains_key(node1.self_id()));
        assert_eq!(peers.len(), 2);
        assert_eq!(node1.peers_len(), peers.len());

        let peer_urls = node1.peers.values().collect::<Vec<_>>();

        let peer_ids = node1.peers_id();

        assert_eq!(peer_ids.len(), peer_urls.len());

        assert!(peer_urls
            .iter()
            .find(|url| url.as_str() == node1_url)
            .is_none());
        assert!(peer_ids
            .iter()
            .find(|id| id.as_str() == node1_id.as_str())
            .is_none());
    }
}
