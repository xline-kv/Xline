use std::{
    collections::HashMap, error::Error, fmt::Display, iter, path::PathBuf, sync::Arc, thread,
    time::Duration,
};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
use curp::{
    client::Client,
    error::ServerError,
    members::{ClusterInfo, ServerId},
    server::Rpc,
    LogIndex, SnapshotAllocator,
};
use curp_external_api::cmd::ProposeId;
use curp_test_utils::{
    sleep_secs,
    test_cmd::{next_id, TestCE, TestCommand, TestCommandResult},
    TestRoleChange, TestRoleChangeInner,
};
use engine::{Engine, EngineType, Snapshot};
use futures::future::join_all;
use itertools::Itertools;
use tokio::{
    net::TcpListener,
    runtime::Runtime,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::debug;
use utils::{
    config::{ClientTimeout, CurpConfigBuilder, StorageConfig},
    shutdown::{self, Trigger},
};

pub mod proto {
    tonic::include_proto!("messagepb");
}
pub mod commandpb {
    tonic::include_proto!("commandpb");
}
pub mod errorpb {
    tonic::include_proto!("errorpb");
}
pub use commandpb::{ProposeRequest, ProposeResponse};
pub use proto::protocol_client::ProtocolClient;

use self::proto::{FetchLeaderRequest, FetchLeaderResponse};

struct MemorySnapshotAllocator;

#[async_trait]
impl SnapshotAllocator for MemorySnapshotAllocator {
    async fn allocate_new_snapshot(&self) -> Result<Snapshot, Box<dyn Error>> {
        Ok(Snapshot::new_for_receiving(EngineType::Memory)?)
    }
}

pub struct CurpNode {
    pub id: ServerId,
    pub addr: String,
    pub exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    pub as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    pub store: Arc<Engine>,
    pub storage_path: PathBuf,
    pub role_change_arc: Arc<TestRoleChangeInner>,
    pub handle: JoinHandle<Result<(), ServerError>>,
    pub trigger: shutdown::Trigger,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
    pub all: HashMap<ServerId, String>,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        assert!(n_nodes >= 3, "the number of nodes must >= 3");
        let listeners = join_all(
            iter::repeat_with(|| async { TcpListener::bind("0.0.0.0:0").await.unwrap() })
                .take(n_nodes),
        )
        .await;
        let all_members: HashMap<String, String> = listeners
            .iter()
            .enumerate()
            .map(|(i, listener)| (format!("S{i}"), listener.local_addr().unwrap().to_string()))
            .collect();
        let mut all = HashMap::new();
        let nodes = listeners
            .into_iter()
            .enumerate()
            .map(|(i, listener)| {
                let (trigger, l) = shutdown::channel();
                let name = format!("S{i}");
                let addr = listener.local_addr().unwrap().to_string();
                let storage_path = tempfile::tempdir().unwrap().into_path();

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let ce = TestCE::new(name.clone(), exe_tx, as_tx);
                let store = Arc::clone(&ce.store);

                let cluster_info = Arc::new(ClusterInfo::new(all_members.clone(), &name));
                all = cluster_info.all_members();
                let id = cluster_info.self_id();

                let storage_cfg = StorageConfig::RocksDB(storage_path.clone());
                let role_change_cb = TestRoleChange::default();
                let role_change_arc = role_change_cb.get_inner_arc();

                let handle = tokio::spawn(Rpc::run_from_listener(
                    cluster_info,
                    i == 0,
                    listener,
                    ce,
                    Box::new(MemorySnapshotAllocator),
                    role_change_cb,
                    Arc::new(
                        CurpConfigBuilder::default()
                            .storage_cfg(storage_cfg)
                            .log_entries_cap(10)
                            .build()
                            .unwrap(),
                    ),
                    trigger.clone(),
                ));

                (
                    id,
                    CurpNode {
                        id,
                        addr,
                        exe_rx,
                        as_rx,
                        store,
                        storage_path,
                        role_change_arc,
                        handle,
                        trigger,
                    },
                )
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(300)).await;
        debug!("successfully start group");
        Self { nodes, all }
    }

    pub fn get_node(&self, id: &ServerId) -> &CurpNode {
        &self.nodes[id]
    }

    pub async fn new_client(&self, timeout: ClientTimeout) -> Client<TestCommand> {
        Client::builder()
            .timeout(timeout)
            .build_from_addrs(self.all.values().cloned().collect_vec())
            .await
            .unwrap()
    }

    pub fn exe_rxs(
        &mut self,
    ) -> impl Iterator<Item = &mut mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>> {
        self.nodes.values_mut().map(|node| &mut node.exe_rx)
    }

    pub fn as_rxs(
        &mut self,
    ) -> impl Iterator<Item = &mut mpsc::UnboundedReceiver<(TestCommand, LogIndex)>> {
        self.nodes.values_mut().map(|node| &mut node.as_rx)
    }

    pub fn is_finished(&self) -> bool {
        self.nodes.values().all(|node| node.handle.is_finished())
    }

    pub async fn stop(mut self) {
        let paths = self
            .nodes
            .values()
            .map(|node| node.storage_path.clone())
            .collect_vec();
        debug!("curp group stopping");

        let futs = self
            .nodes
            .values_mut()
            .map(|n| n.trigger.shutdown_and_wait());
        futures::future::join_all(futs).await;

        self.nodes.clear();
        debug!("curp group stopped");
        for path in paths {
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    pub async fn try_get_leader(&self) -> Option<(ServerId, u64)> {
        let mut leader = None;
        let mut max_term = 0;
        for addr in self.all.values() {
            let addr = format!("http://{}", addr);
            let mut client = if let Ok(client) = ProtocolClient::connect(addr.clone()).await {
                client
            } else {
                continue;
            };

            let FetchLeaderResponse { leader_id, term } =
                if let Ok(resp) = client.fetch_leader(FetchLeaderRequest {}).await {
                    resp.into_inner()
                } else {
                    continue;
                };
            if term > max_term {
                max_term = term;
                leader = leader_id;
            } else if term == max_term && leader.is_none() {
                leader = leader_id;
            }
        }
        leader.map(|l| (l, max_term))
    }

    pub async fn get_leader(&self) -> (ServerId, u64) {
        for _ in 0..5 {
            if let Some(leader) = self.try_get_leader().await {
                return leader;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        panic!("can't get leader");
    }

    // get latest term and ensure every working node has the same term
    pub async fn get_term_checked(&self) -> u64 {
        let mut max_term = None;
        for addr in self.all.values() {
            let addr = format!("http://{}", addr);
            let mut client = if let Ok(client) = ProtocolClient::connect(addr.clone()).await {
                client
            } else {
                continue;
            };

            let FetchLeaderResponse { leader_id, term } =
                if let Ok(resp) = client.fetch_leader(FetchLeaderRequest {}).await {
                    resp.into_inner()
                } else {
                    continue;
                };

            if let Some(max_term) = max_term {
                assert_eq!(max_term, term);
            } else {
                max_term = Some(term);
            }
        }
        max_term.unwrap()
    }

    pub async fn get_connect(&self, id: &ServerId) -> ProtocolClient<tonic::transport::Channel> {
        let addr = self
            .all
            .iter()
            .find_map(|(node_id, addr)| (node_id == id).then_some(addr))
            .unwrap();
        let addr = format!("http://{}", addr);
        ProtocolClient::connect(addr.clone()).await.unwrap()
    }
}
