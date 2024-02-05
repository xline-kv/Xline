use std::{
    collections::HashMap, error::Error, fmt::Display, iter, path::PathBuf, sync::Arc, thread,
    time::Duration,
};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
use curp::{
    client::{ClientApi, ClientBuilder},
    error::ServerError,
    members::{ClusterInfo, ServerId},
    rpc::{InnerProtocolServer, Member, ProtocolServer},
    server::{Rpc, DB},
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
use futures::{future::join_all, Future};
use itertools::Itertools;
use tokio::{
    net::TcpListener,
    runtime::{Handle, Runtime},
    sync::{mpsc, watch},
    task::{block_in_place, JoinHandle},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, ServerTlsConfig};
use tracing::debug;
use utils::{
    build_endpoint,
    config::{
        default_quota, ClientConfig, CurpConfig, CurpConfigBuilder, EngineConfig, StorageConfig,
    },
    task_manager::{tasks::TaskName, Listener, TaskManager},
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
    pub task_manager: Arc<TaskManager>,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
    pub storage_path: Option<PathBuf>,
    pub client_tls_config: Option<ClientTlsConfig>,
    pub server_tls_config: Option<ServerTlsConfig>,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        let configs = (0..n_nodes)
            .map(|i| (format!("S{i}"), Default::default()))
            .collect();
        Self::new_with_configs(configs, "S0".to_owned()).await
    }

    pub async fn new_rocks(n_nodes: usize, path: PathBuf) -> Self {
        let configs = (0..n_nodes)
            .map(|i| {
                let name = format!("S{i}");
                let mut config = CurpConfig::default();
                let dir = path.join(&name);
                config.engine_cfg = EngineConfig::RocksDB(dir.join("curp"));
                let xline_storage_config = EngineConfig::RocksDB(dir.join("xline"));
                (name, (Arc::new(config), xline_storage_config))
            })
            .collect();
        let mut inner = Self::new_with_configs(configs, "S0".to_owned()).await;
        inner.storage_path = Some(path);
        inner
    }

    async fn new_with_configs(
        configs: HashMap<String, (Arc<CurpConfig>, EngineConfig)>,
        leader_name: String,
    ) -> Self {
        let n_nodes = configs.len();
        assert!(n_nodes >= 3, "the number of nodes must >= 3");
        let mut listeners = Self::gen_listeners(configs.keys()).await;
        let all_members_addrs = Self::listeners_to_all_members_addrs(&listeners);

        let mut nodes = HashMap::new();
        let client_tls_config = None;
        let server_tls_config = None;
        for (name, (config, xline_storage_config)) in configs.into_iter() {
            let task_manager = Arc::new(TaskManager::new());
            let snapshot_allocator = Self::get_snapshot_allocator_from_cfg(&config);
            let cluster_info = Arc::new(ClusterInfo::from_members_map(
                all_members_addrs.clone(),
                [],
                &name,
            ));
            let listener = listeners.remove(&name).unwrap();
            let id = cluster_info.self_id();
            let addr = cluster_info.self_peer_urls().pop().unwrap();

            let (exe_tx, exe_rx) = mpsc::unbounded_channel();
            let (as_tx, as_rx) = mpsc::unbounded_channel();
            let ce = Arc::new(TestCE::new(
                name.clone(),
                exe_tx,
                as_tx,
                xline_storage_config,
            ));

            let role_change_cb = TestRoleChange::default();
            let role_change_arc = role_change_cb.get_inner_arc();
            let curp_storage = Arc::new(DB::open(&config.engine_cfg).unwrap());
            let server = Arc::new(
                Rpc::new(
                    cluster_info,
                    name == leader_name,
                    ce,
                    snapshot_allocator,
                    role_change_cb,
                    config,
                    curp_storage,
                    Arc::clone(&task_manager),
                    client_tls_config.clone(),
                )
                .await,
            );
            task_manager.spawn(TaskName::TonicServer, |n| async move {
                let ig = Self::run(server, listener, n).await;
            });
            let curp_node = CurpNode {
                id,
                addr,
                exe_rx,
                as_rx,
                role_change_arc,
                task_manager,
            };
            nodes.insert(curp_node.id, curp_node);
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
        debug!("successfully start group");
        Self {
            nodes,
            storage_path: None,
            client_tls_config,
            server_tls_config,
        }
    }

    async fn gen_listeners(keys: impl Iterator<Item = &String>) -> HashMap<String, TcpListener> {
        join_all(
            keys.cloned()
                .map(|name| async { (name, TcpListener::bind("0.0.0.0:0").await.unwrap()) }),
        )
        .await
        .into_iter()
        .collect()
    }

    fn listeners_to_all_members_addrs(
        listeners: &HashMap<String, TcpListener>,
    ) -> HashMap<String, Vec<String>> {
        listeners
            .iter()
            .map(|(name, listener)| {
                (
                    name.clone(),
                    vec![listener.local_addr().unwrap().to_string()],
                )
            })
            .collect()
    }

    fn get_snapshot_allocator_from_cfg(config: &CurpConfig) -> Box<dyn SnapshotAllocator> {
        match config.engine_cfg {
            EngineConfig::Memory => {
                Box::<MemorySnapshotAllocator>::default() as Box<dyn SnapshotAllocator>
            }
            EngineConfig::RocksDB(_) => {
                Box::<RocksSnapshotAllocator>::default() as Box<dyn SnapshotAllocator>
            }
            _ => unreachable!(),
        }
    }

    async fn run(
        server: Arc<Rpc<TestCommand, TestRoleChange>>,
        listener: TcpListener,
        shutdown_listener: Listener,
    ) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtocolServer::from_arc(Arc::clone(&server)))
            .add_service(InnerProtocolServer::from_arc(server))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown_listener.wait(),
            )
            .await
    }

    pub async fn run_node(
        &mut self,
        listener: TcpListener,
        name: String,
        cluster_info: Arc<ClusterInfo>,
    ) {
        self.run_node_with_config(
            listener,
            name,
            cluster_info,
            Arc::new(CurpConfig::default()),
            EngineConfig::default(),
        )
        .await
    }

    pub async fn run_node_with_config(
        &mut self,
        listener: TcpListener,
        name: String,
        cluster_info: Arc<ClusterInfo>,
        config: Arc<CurpConfig>,
        xline_storage_config: EngineConfig,
    ) {
        let task_manager = Arc::new(TaskManager::new());
        let addr = listener.local_addr().unwrap().to_string();
        let snapshot_allocator = Self::get_snapshot_allocator_from_cfg(&config);

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        let (as_tx, as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            name.clone(),
            exe_tx,
            as_tx,
            xline_storage_config,
        ));

        let id = cluster_info.self_id();
        let role_change_cb = TestRoleChange::default();
        let role_change_arc = role_change_cb.get_inner_arc();
        let curp_storage = Arc::new(DB::open(&config.engine_cfg).unwrap());
        let server = Arc::new(
            Rpc::new(
                cluster_info,
                false,
                ce,
                snapshot_allocator,
                role_change_cb,
                config,
                curp_storage,
                Arc::clone(&task_manager),
                self.client_tls_config.clone(),
            )
            .await,
        );
        task_manager.spawn(TaskName::TonicServer, |n| async move {
            let _ig = Self::run(server, listener, n).await;
        });

        self.nodes.insert(
            id,
            CurpNode {
                id,
                addr,
                exe_rx,
                as_rx,
                role_change_arc,
                task_manager,
            },
        );
        let client = self.new_client().await;
        client.propose_publish(id, name, vec![]).await.unwrap();
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

    pub async fn new_client(&self) -> impl ClientApi<Error = tonic::Status, Cmd = TestCommand> {
        let addrs = self.all_addrs().cloned().collect();
        ClientBuilder::new(ClientConfig::default(), true)
            .discover_from(addrs)
            .await
            .unwrap()
            .build()
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
        self.nodes
            .values()
            .all(|node| node.task_manager.is_finished())
    }

    async fn stop(&mut self) {
        debug!("curp group stopping");

        let futs = self
            .nodes
            .values_mut()
            .map(|n| n.task_manager.shutdown(true));
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
            let channel_fut = async move {
                let ep = build_endpoint(addr, self.client_tls_config.as_ref())?;
                let channel = ep.connect().await?;
                Ok::<Channel, Box<dyn std::error::Error>>(channel)
            };
            let mut client = match channel_fut.await {
                Ok(channel) => ProtocolClient::new(channel),
                Err(e) => continue,
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
            let channel_fut = async move {
                let ep = build_endpoint(addr, self.client_tls_config.as_ref())?;
                let channel = ep.connect().await?;
                Ok::<Channel, Box<dyn std::error::Error>>(channel)
            };
            let mut client = match channel_fut.await {
                Ok(channel) => ProtocolClient::new(channel),
                Err(e) => continue,
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
        let addr = &self.nodes[id].addr;
        let channel_fut = async move {
            let ep = build_endpoint(addr, self.client_tls_config.as_ref())?;
            let channel = ep.connect().await?;
            Ok::<Channel, Box<dyn std::error::Error>>(channel)
        };
        let channel = channel_fut.await.unwrap();
        ProtocolClient::new(channel)
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
            .map(|m| Member::new(m.id, m.name, m.peer_urls, m.client_urls, m.is_learner))
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
