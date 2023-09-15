use std::{collections::HashMap, error::Error, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use curp::{
    client::{Client, ReadState},
    cmd::Command,
    error::{CommandProposeError, ProposeError},
    members::{ClusterInfo, ServerId},
    server::Rpc,
    FetchLeaderRequest, FetchLeaderResponse, LogIndex,
};
pub use curp::{protocol_client::ProtocolClient, ProposeRequest, ProposeResponse};
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
    config::{ClientConfig, CurpConfigBuilder, StorageConfig},
    shutdown,
};

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
    pub leader: Arc<Mutex<ServerId>>,
    pub all_members: HashMap<ServerId, String>,
    pub client_node: NodeHandle,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        assert!(n_nodes >= 3, "the number of nodes must >= 3");
        let handle = madsim::runtime::Handle::current();

        let all: HashMap<String, String> = (0..n_nodes)
            .map(|x| (format!("S{x}"), format!("192.168.1.{}:12345", x + 1)))
            .collect();
        let mut all_members = HashMap::new();

        let leader = Arc::new(Mutex::new(0));

        let nodes = (0..n_nodes)
            .map(|i| {
                let name = format!("S{i}");
                let addr = format!("192.168.1.{}:12345", i + 1);
                let storage_path = tempfile::tempdir().unwrap().into_path();

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let store = Arc::new(Mutex::new(None));

                let cluster_info = Arc::new(ClusterInfo::new(all.clone(), &name));
                all_members = cluster_info.all_members_addrs();
                let id = cluster_info.self_id();
                let storage_cfg = StorageConfig::RocksDB(storage_path.clone());
                let store_c = Arc::clone(&store);
                let role_change_cb = TestRoleChange::default();
                let role_change_arc = role_change_cb.get_inner_arc();

                let node_handle = handle
                    .create_node()
                    .name(id.to_string())
                    .ip(format!("192.168.1.{}", i + 1).parse().unwrap())
                    .init(move || {
                        let (trigger, _listener) = shutdown::channel();
                        let ce = TestCE::new(
                            name.clone(),
                            exe_tx.clone(),
                            as_tx.clone(),
                            StorageConfig::Memory,
                        );
                        store_c.lock().replace(Arc::clone(&ce.store));
                        let is_leader = "S0" == name;

                        Rpc::run_from_addr(
                            cluster_info.clone(),
                            is_leader,
                            "0.0.0.0:12345".parse().unwrap(),
                            ce,
                            Box::new(MemorySnapshotAllocator),
                            TestRoleChange {
                                inner: role_change_cb.get_inner_arc(),
                            },
                            Arc::new(
                                CurpConfigBuilder::default()
                                    .storage_cfg(storage_cfg.clone())
                                    .log_entries_cap(10)
                                    .build()
                                    .unwrap(),
                            ),
                            trigger,
                        )
                    })
                    .build();

                (
                    id,
                    CurpNode {
                        id,
                        addr,
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
            leader,
            all_members,
            client_node,
        }
    }

    pub fn get_node(&self, id: &ServerId) -> &CurpNode {
        &self.nodes[id]
    }

    pub async fn new_client(&self, config: ClientConfig) -> SimClient<TestCommand> {
        let all_members = self
            .nodes
            .iter()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();
        SimClient {
            inner: Arc::new(
                Client::<TestCommand>::builder()
                    .config(config)
                    .build_from_all_members(all_members)
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
        madsim::time::sleep(Duration::from_secs(2)).await;
        if !handle.is_exit(id.to_string()) {
            panic!("failed to crash node: {id}");
        }
    }

    pub async fn restart(&mut self, id: ServerId, is_leader: bool) {
        let handle = madsim::runtime::Handle::current();
        if is_leader {
            *self.leader.lock() = id;
        } else {
            *self.leader.lock() = 0;
        }
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
                    tracing::warn!("connecing to : {}", addr);
                    let mut client = if let Ok(client) = ProtocolClient::connect(addr.clone()).await
                    {
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

                    let FetchLeaderResponse { leader_id: _, term } =
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
            })
            .await
            .unwrap()
    }

    // Disconnect the node from the network.
    pub fn disable_node(&self, id: ServerId) {
        let handle = madsim::runtime::Handle::current();
        let net = madsim::net::NetSim::current();
        let Some(node) = handle.get_node(id.to_string()) else {
            panic!("no node with name {id} in the simulator")
        };
        net.clog_node(node.id());
    }

    // Reconnect the node to the network.
    pub fn enable_node(&self, id: ServerId) {
        let handle = madsim::runtime::Handle::current();
        let net = madsim::net::NetSim::current();
        let Some(node) = handle.get_node(id.to_string()) else {
            panic!("no node with name {id} the simulator")
        };
        net.unclog_node(node.id());
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
}

pub struct SimClient<C: Command> {
    inner: Arc<Client<C>>,
    handle: NodeHandle,
}

impl<C: Command + 'static> SimClient<C> {
    #[inline]
    pub async fn propose(
        &self,
        cmd: C,
        use_fast_path: bool,
    ) -> Result<(C::ER, Option<C::ASR>), CommandProposeError<C>> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.propose(cmd, use_fast_path).await })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, ProposeError> {
        let inner = self.inner.clone();
        let cmd = cmd.clone();
        self.handle
            .spawn(async move { inner.fetch_read_state(&cmd).await })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn get_leader_id(&self) -> ServerId {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.get_leader_id().await.unwrap() })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn get_leader_id_from_curp(&self) -> ServerId {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.get_leader_id_from_curp().await.unwrap() })
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
