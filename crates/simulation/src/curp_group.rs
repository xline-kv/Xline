use std::{collections::HashMap, error::Error, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
pub use curp::rpc::{
    protocol_client::ProtocolClient, PbProposeId, ProposeRequest, ProposeResponse,
};
use curp::{
    client::{ClientApi, ClientBuilder},
    cmd::Command,
    members::{ClusterInfo, ServerId},
    rpc::{
        ConfChange, FetchClusterRequest, FetchClusterResponse, Member, ProposeConfChangeRequest,
        ProposeConfChangeResponse, ReadState,
    },
    server::{Rpc, StorageApi, DB},
    LogIndex,
};
use curp_test_utils::{
    test_cmd::{TestCE, TestCommand, TestCommandResult},
    TestRoleChange, TestRoleChangeInner,
};
use engine::{Engine, EngineType, Snapshot, SnapshotAllocator};
use itertools::Itertools;
use madsim::runtime::NodeHandle;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::debug;
use utils::{
    config::{ClientConfig, CurpConfigBuilder, EngineConfig},
    task_manager::TaskManager,
};

/// TODO: use `type CurpClient<C>  = impl ClientApi<...>` when `type_alias_impl_trait` stabilized
type CurpClient<C> = dyn ClientApi<Cmd = C, Error = tonic::Status> + Send + Sync + 'static;

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
    pub handle: NodeHandle,
    pub exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    pub as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    pub store: Arc<Mutex<Option<Arc<Engine>>>>,
    pub storage_path: PathBuf,
    pub role_change_arc: Arc<TestRoleChangeInner>,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
    pub all_members: HashMap<ServerId, String>,
    pub client_node: NodeHandle,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        assert!(n_nodes >= 3, "the number of nodes must >= 3");
        let handle = madsim::runtime::Handle::current();

        let all: HashMap<_, _> = (0..n_nodes)
            .map(|x| (format!("S{x}"), vec![format!("192.168.1.{}:2380", x + 1)]))
            .collect();
        let mut all_members = HashMap::new();

        let nodes = (0..n_nodes)
            .map(|i| {
                let name = format!("S{i}");
                let peer_url = format!("192.168.1.{}:2380", i + 1);
                let storage_path = tempfile::tempdir().unwrap().into_path();

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let store = Arc::new(Mutex::new(None));

                let cluster_info = Arc::new(ClusterInfo::from_members_map(all.clone(), [], &name));
                all_members = cluster_info
                    .all_members_peer_urls()
                    .into_iter()
                    .map(|(k, mut v)| (k, v.pop().unwrap()))
                    .collect();
                let id = cluster_info.self_id();
                let engine_cfg = EngineConfig::RocksDB(storage_path.clone());
                let store_c = Arc::clone(&store);
                let role_change_cb = TestRoleChange::default();
                let role_change_arc = role_change_cb.get_inner_arc();

                let node_handle = handle
                    .create_node()
                    .name(id.to_string())
                    .ip(format!("192.168.1.{}", i + 1).parse().unwrap())
                    .init(move || {
                        let task_manager = Arc::new(TaskManager::new());
                        let ce = Arc::new(TestCE::new(
                            name.clone(),
                            exe_tx.clone(),
                            as_tx.clone(),
                            EngineConfig::Memory,
                        ));
                        store_c.lock().replace(Arc::clone(&ce.store));
                        // we will restart the old leader.
                        // after the reboot, it may no longer be the leader.
                        let is_leader = false;
                        let curp_config = Arc::new(
                            CurpConfigBuilder::default()
                                .engine_cfg(engine_cfg.clone())
                                .log_entries_cap(10)
                                .build()
                                .unwrap(),
                        );
                        let curp_storage = Arc::new(DB::open(&curp_config.engine_cfg).unwrap());
                        let cluster_info = match curp_storage.recover_cluster_info().unwrap() {
                            Some(cl) => Arc::new(cl),
                            None => Arc::clone(&cluster_info),
                        };
                        Rpc::run_from_addr(
                            cluster_info,
                            is_leader,
                            "0.0.0.0:2380".parse().unwrap(),
                            ce,
                            Box::new(MemorySnapshotAllocator),
                            TestRoleChange {
                                inner: role_change_cb.get_inner_arc(),
                            },
                            curp_config,
                            curp_storage,
                            task_manager,
                            None,
                        )
                    })
                    .build();

                (
                    id,
                    CurpNode {
                        id,
                        addr: peer_url,
                        handle: node_handle,
                        exe_rx,
                        as_rx,
                        store,
                        storage_path,
                        role_change_arc,
                    },
                )
            })
            .collect();

        let client_node = handle
            .create_node()
            .name("client")
            .ip("192.168.2.1".parse().unwrap())
            .build();
        madsim::time::sleep(Duration::from_secs(20)).await;
        debug!("successfully start group");
        Self {
            nodes,
            all_members,
            client_node,
        }
    }

    pub fn get_node(&self, id: &ServerId) -> &CurpNode {
        &self.nodes[id]
    }

    pub async fn new_client(&self) -> SimClient<TestCommand> {
        let config = ClientConfig::default();
        let all_members = self
            .nodes
            .iter()
            .map(|(id, node)| (*id, vec![node.addr.clone()]))
            .collect();
        SimClient {
            inner: Arc::new(
                ClientBuilder::new(config, true)
                    .all_members(all_members)
                    .build()
                    .await
                    .unwrap(),
            ),
            handle: self.client_node.clone(),
        }
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

    pub async fn crash(&mut self, id: ServerId) {
        let handle = madsim::runtime::Handle::current();
        handle.kill(id.to_string());
        madsim::time::sleep(Duration::from_secs(10)).await;
        if !handle.is_exit(id.to_string()) {
            panic!("failed to crash node: {id}");
        }
    }

    pub async fn restart(&mut self, id: ServerId) {
        let handle = madsim::runtime::Handle::current();
        handle.restart(id.to_string());
    }

    pub async fn try_get_leader(&self) -> Option<(ServerId, u64)> {
        debug!("trying to get leader");
        let mut leader = None;
        let mut max_term = 0;

        let all = self.all_members.clone();
        self.client_node
            .spawn(async move {
                for addr in all.values() {
                    let addr = format!("http://{}", addr);
                    tracing::warn!("connecting to : {}", addr);
                    let mut client = if let Ok(client) = ProtocolClient::connect(addr.clone()).await
                    {
                        client
                    } else {
                        continue;
                    };

                    let FetchClusterResponse {
                        leader_id, term, ..
                    } = if let Ok(resp) = client.fetch_cluster(FetchClusterRequest::default()).await
                    {
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
            })
            .await
            .unwrap()
    }

    pub async fn get_leader(&self) -> (ServerId, u64) {
        const RETRY_INTERVAL: u64 = 100;
        loop {
            if let Some(leader) = self.try_get_leader().await {
                return leader;
            }
            debug!("failed to get leader");
            madsim::time::sleep(Duration::from_millis(RETRY_INTERVAL)).await;
        }
    }

    // get latest term and ensure every working node has the same term
    pub async fn get_term_checked(&self) -> u64 {
        let all = self.all_members.clone();
        self.client_node
            .spawn(async move {
                let mut max_term = None;
                for addr in all.values() {
                    let addr = format!("http://{}", addr);
                    let mut client = if let Ok(client) = ProtocolClient::connect(addr.clone()).await
                    {
                        client
                    } else {
                        continue;
                    };

                    let FetchClusterResponse { term, .. } = if let Ok(resp) =
                        client.fetch_cluster(FetchClusterRequest::default()).await
                    {
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
            })
            .await
            .unwrap()
    }

    /// Disconnect the node from the network.
    pub fn disable_node(&self, id: ServerId) {
        let net = madsim::net::NetSim::current();
        let node = Self::get_node_handle(id);
        net.clog_node(node.id());
    }

    /// Reconnect the node to the network.
    pub fn enable_node(&self, id: ServerId) {
        let net = madsim::net::NetSim::current();
        let node = Self::get_node_handle(id);
        net.unclog_node(node.id());
    }

    /// Disconnect the network link between two nodes
    pub fn clog_link_nodes(&self, fst: ServerId, snd: ServerId) {
        let node_fst = Self::get_node_handle(fst);
        let node_snd = Self::get_node_handle(snd);
        Self::clog_bidirectional(&node_fst, &node_snd);
    }

    /// Reconnect the network link between two nodes
    pub fn unclog_link_nodes(&self, fst: ServerId, snd: ServerId) {
        let node_fst = Self::get_node_handle(fst);
        let node_snd = Self::get_node_handle(snd);
        Self::unclog_bidirectional(&node_fst, &node_snd);
    }

    /// Disconnect the network link between the client and a list of nodes
    /// Note: This will affect SimClient
    pub fn clog_link_client_nodes<'a>(&'a self, server_ids: impl Iterator<Item = &'a ServerId>) {
        let client_node = &self.client_node;
        for server_id in server_ids {
            let server_node = Self::get_node_handle(*server_id);
            Self::clog_bidirectional(client_node, &server_node);
        }
    }

    /// Reconnect the network link between the client and a list of nodes
    pub fn unclog_link_client_nodes<'a>(&'a self, server_ids: impl Iterator<Item = &'a ServerId>) {
        let client_node = &self.client_node;
        for server_id in server_ids {
            let server_node = Self::get_node_handle(*server_id);
            Self::unclog_bidirectional(client_node, &server_node);
        }
    }

    /// Clog the network bidirectionally
    fn clog_bidirectional(node_fst: &NodeHandle, node_snd: &NodeHandle) {
        let net = madsim::net::NetSim::current();
        let id_fst = node_fst.id();
        let id_snd = node_snd.id();
        net.clog_link(id_fst, id_snd);
        net.clog_link(id_snd, id_fst);
    }

    /// Unclog the network bidirectionally
    fn unclog_bidirectional(node_fst: &NodeHandle, node_snd: &NodeHandle) {
        let net = madsim::net::NetSim::current();
        let id_fst = node_fst.id();
        let id_snd = node_snd.id();
        net.unclog_link(id_fst, id_snd);
        net.unclog_link(id_snd, id_fst);
    }

    /// Get the server node handle from ServerId
    fn get_node_handle(id: ServerId) -> NodeHandle {
        let handle = madsim::runtime::Handle::current();
        handle
            .get_node(id.to_string())
            .expect("no node with name {id} the simulator")
    }

    pub async fn get_connect(&self, id: &ServerId) -> SimProtocolClient {
        let addr = self
            .all_members
            .iter()
            .find_map(|(node_id, addr)| (node_id == id).then_some(addr))
            .unwrap();
        let addr = format!("http://{}", addr);
        SimProtocolClient {
            addr,
            handle: self.client_node.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SimProtocolClient {
    addr: String,
    handle: NodeHandle,
}

impl SimProtocolClient {
    #[inline]
    pub async fn propose(
        &mut self,
        cmd: impl tonic::IntoRequest<ProposeRequest> + 'static + Send,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        let addr = self.addr.clone();
        self.handle
            .spawn(async move {
                let mut client = ProtocolClient::connect(addr).await.unwrap();
                client.propose(cmd).await
            })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn propose_conf_change(
        &self,
        conf_change: impl tonic::IntoRequest<ProposeConfChangeRequest>,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, tonic::Status> {
        let mut req = conf_change.into_request();
        req.set_timeout(timeout);
        let addr = self.addr.clone();
        self.handle
            .spawn(async move {
                let mut client = ProtocolClient::connect(addr).await.unwrap();
                client.propose_conf_change(req).await
            })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn fetch_cluster(
        &self,
    ) -> Result<tonic::Response<FetchClusterResponse>, tonic::Status> {
        let req = FetchClusterRequest::default();
        let addr = self.addr.clone();
        self.handle
            .spawn(async move {
                let mut client = ProtocolClient::connect(addr).await.unwrap();
                client.fetch_cluster(req).await
            })
            .await
            .unwrap()
    }
}

pub struct SimClient<C: Command> {
    inner: Arc<CurpClient<C>>,
    handle: NodeHandle,
}

impl<C: Command> SimClient<C> {
    #[inline]
    pub async fn propose(
        &self,
        cmd: C,
        use_fast_path: bool,
    ) -> Result<Result<(C::ER, Option<C::ASR>), C::Error>, tonic::Status> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.propose(&cmd, None, use_fast_path).await })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, tonic::Status> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.propose_conf_change(changes).await })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, tonic::Status> {
        let inner = self.inner.clone();
        let cmd = cmd.clone();
        self.handle
            .spawn(async move { inner.fetch_read_state(&cmd).await })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn get_leader_id(&self) -> Result<ServerId, tonic::Status> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.fetch_leader_id(true).await })
            .await
            .unwrap()
    }
}

impl Drop for CurpGroup {
    fn drop(&mut self) {
        let handle = madsim::runtime::Handle::current();

        for node in self.nodes.values() {
            handle.send_ctrl_c(node.handle.id());
        }
        handle.send_ctrl_c(self.client_node.id());
        for (name, node) in &self.nodes {
            if !handle.is_exit(node.handle.id()) {
                panic!("failed to graceful shutdown {name}");
            }
        }

        let paths = self
            .nodes
            .values()
            .map(|node| node.storage_path.clone())
            .collect_vec();
        for path in paths {
            std::fs::remove_dir_all(path).unwrap();
        }

        debug!("all nodes shutdowned");
    }
}
