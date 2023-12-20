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
    rpc::Member,
    server::Rpc,
    LogIndex,
};
use curp_test_utils::{
    sleep_secs,
    test_cmd::{TestCE, TestCommand, TestCommandResult},
    TestRoleChange, TestRoleChangeInner,
};
use engine::{
    Engine, EngineType, MemorySnapshotAllocator, RocksSnapshotAllocator, Snapshot,
    SnapshotAllocator,
};
use futures::future::join_all;
use itertools::Itertools;
use tokio::{
    net::TcpListener,
    runtime::{Handle, Runtime},
    sync::{mpsc, watch},
    task::{block_in_place, JoinHandle},
};
use tracing::debug;
use utils::{
    config::{ClientConfig, CurpConfigBuilder, EngineConfig, StorageConfig},
    shutdown::{self, Trigger},
};
pub mod commandpb {
    tonic::include_proto!("commandpb");
}

pub use commandpb::{
    protocol_client::ProtocolClient, FetchClusterRequest, FetchClusterResponse, ProposeRequest,
    ProposeResponse,
};

pub struct CurpNode {
    pub id: ServerId,
    pub addr: String,
    pub exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    pub as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    pub role_change_arc: Arc<TestRoleChangeInner>,
    pub handle: JoinHandle<Result<(), ServerError>>,
    pub trigger: Trigger,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
    pub storage_path: Option<PathBuf>,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        Self::inner_new(n_nodes, None).await
    }

    pub async fn new_rocks(n_nodes: usize, path: PathBuf) -> Self {
        Self::inner_new(n_nodes, Some(path)).await
    }

    async fn inner_new(n_nodes: usize, storage_path: Option<PathBuf>) -> Self {
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
                let (curp_storage_config, xline_storage_config, snapshot_allocator) =
                    match storage_path {
                        Some(ref path) => {
                            let storage_path = path.join(&name);
                            (
                                EngineConfig::RocksDB(storage_path.join("curp")),
                                EngineConfig::RocksDB(storage_path.join("xline")),
                                Box::<RocksSnapshotAllocator>::default()
                                    as Box<dyn SnapshotAllocator>,
                            )
                        }
                        None => (
                            EngineConfig::Memory,
                            EngineConfig::Memory,
                            Box::<MemorySnapshotAllocator>::default() as Box<dyn SnapshotAllocator>,
                        ),
                    };

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let ce = TestCE::new(name.clone(), exe_tx, as_tx, xline_storage_config);

                let cluster_info = Arc::new(ClusterInfo::new(
                    all_members
                        .clone()
                        .into_iter()
                        .map(|(k, v)| (k, vec![v]))
                        .collect(),
                    &name,
                ));
                all = cluster_info
                    .all_members_addrs()
                    .into_iter()
                    .map(|(k, mut v)| (k, v.pop().unwrap()))
                    .collect();
                let id = cluster_info.self_id();

                let role_change_cb = TestRoleChange::default();
                let role_change_arc = role_change_cb.get_inner_arc();
                let handle = tokio::spawn(Rpc::run_from_listener(
                    cluster_info,
                    i == 0,
                    listener,
                    ce,
                    snapshot_allocator,
                    role_change_cb,
                    Arc::new(
                        CurpConfigBuilder::default()
                            .engine_cfg(curp_storage_config)
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
                        role_change_arc,
                        handle,
                        trigger,
                    },
                )
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(300)).await;
        debug!("successfully start group");
        Self {
            nodes,
            storage_path,
        }
    }

    pub async fn run_node(
        &mut self,
        listener: TcpListener,
        name: String,
        cluster_info: Arc<ClusterInfo>,
    ) {
        let (trigger, l) = shutdown::channel();
        let addr = listener.local_addr().unwrap().to_string();
        let (curp_storage_config, xline_storage_config, snapshot_allocator) =
            match self.storage_path {
                Some(ref path) => {
                    let storage_path = path.join(&name);
                    (
                        EngineConfig::RocksDB(storage_path.join("curp")),
                        EngineConfig::RocksDB(storage_path.join("xline")),
                        Box::<RocksSnapshotAllocator>::default() as Box<dyn SnapshotAllocator>,
                    )
                }
                None => (
                    EngineConfig::Memory,
                    EngineConfig::Memory,
                    Box::<MemorySnapshotAllocator>::default() as Box<dyn SnapshotAllocator>,
                ),
            };

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        let (as_tx, as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new(name.clone(), exe_tx, as_tx, xline_storage_config);

        let id = cluster_info.self_id();
        let role_change_cb = TestRoleChange::default();
        let role_change_arc = role_change_cb.get_inner_arc();
        let handle = tokio::spawn(Rpc::run_from_listener(
            cluster_info,
            false,
            listener,
            ce,
            snapshot_allocator,
            role_change_cb,
            Arc::new(
                CurpConfigBuilder::default()
                    .engine_cfg(curp_storage_config)
                    .log_entries_cap(10)
                    .build()
                    .unwrap(),
            ),
            trigger.clone(),
        ));
        self.nodes.insert(
            id,
            CurpNode {
                id,
                addr,
                exe_rx,
                as_rx,
                role_change_arc,
                handle,
                trigger,
            },
        );
        let client = self.new_client().await;
        client.publish(id, name).await;
    }

    pub fn all_addrs(&self) -> impl Iterator<Item = &String> {
        self.nodes.values().map(|n| &n.addr)
    }

    pub fn all_addrs_map(&self) -> HashMap<ServerId, Vec<String>> {
        self.nodes
            .iter()
            .map(|(id, n)| (*id, vec![n.addr.clone()]))
            .collect()
    }

    pub fn get_node(&self, id: &ServerId) -> &CurpNode {
        &self.nodes[id]
    }

    pub async fn new_client(&self) -> Client<TestCommand> {
        let addrs = self.all_addrs().cloned().collect();
        Client::builder()
            .config(ClientConfig::default())
            .build_from_addrs(addrs)
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

    async fn stop(&mut self) {
        debug!("curp group stopping");

        let futs = self
            .nodes
            .values_mut()
            .map(|n| n.trigger.self_shutdown_and_wait());
        futures::future::join_all(futs).await;

        self.nodes.clear();
        debug!("curp group stopped");

        if let Some(ref path) = self.storage_path {
            _ = std::fs::remove_dir_all(path);
        }
    }

    pub async fn try_get_leader(&self) -> Option<(ServerId, u64)> {
        let mut leader = None;
        let mut max_term = 0;
        for addr in self.all_addrs() {
            let addr = format!("http://{}", addr);
            let mut client = if let Ok(client) = ProtocolClient::connect(addr.clone()).await {
                client
            } else {
                continue;
            };

            let FetchClusterResponse {
                leader_id, term, ..
            } = if let Ok(resp) = client.fetch_cluster(FetchClusterRequest::default()).await {
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
        for addr in self.all_addrs() {
            let addr = format!("http://{}", addr);
            let mut client = if let Ok(client) = ProtocolClient::connect(addr.clone()).await {
                client
            } else {
                continue;
            };

            let FetchClusterResponse {
                leader_id, term, ..
            } = if let Ok(resp) = client.fetch_cluster(FetchClusterRequest::default()).await {
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
        let addr = format!("http://{}", self.nodes[id].addr);
        ProtocolClient::connect(addr.clone()).await.unwrap()
    }

    pub async fn fetch_cluster_info(&self, addrs: &[String], name: &str) -> ClusterInfo {
        let leader_id = self.get_leader().await.0;
        let mut connect = self.get_connect(&leader_id).await;
        let cluster_res_base = connect
            .fetch_cluster(tonic::Request::new(FetchClusterRequest {
                linearizable: false,
            }))
            .await
            .unwrap()
            .into_inner();
        let members = cluster_res_base
            .members
            .into_iter()
            .map(|m| Member::new(m.id, m.name, m.addrs, m.is_learner))
            .collect();
        let cluster_res = curp::rpc::FetchClusterResponse {
            leader_id: cluster_res_base.leader_id,
            term: cluster_res_base.term,
            cluster_id: cluster_res_base.cluster_id,
            members,
            cluster_version: cluster_res_base.cluster_version,
        };
        ClusterInfo::from_cluster(cluster_res, addrs, name)
    }
}

impl Drop for CurpGroup {
    fn drop(&mut self) {
        block_in_place(move || {
            Handle::current().block_on(async move {
                self.stop().await;
            });
        });
    }
}
