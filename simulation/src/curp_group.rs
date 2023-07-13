use std::{collections::HashMap, error::Error, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use curp::{
    client::{Client, ReadState},
    cmd::Command,
    error::ProposeError,
    members::ClusterMember,
    server::Rpc,
    FetchLeaderRequest, FetchLeaderResponse, LogIndex, SnapshotAllocator,
};
use curp_test_utils::{
    test_cmd::{TestCE, TestCommand, TestCommandResult},
    TestRoleChange, TestRoleChangeInner,
};
use engine::{Engine, EngineType, Snapshot};
use itertools::Itertools;
use madsim::runtime::NodeHandle;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::debug;
use utils::config::{ClientTimeout, CurpConfigBuilder};

pub type ServerId = String;

pub use curp::{
    propose_response, protocol_client::ProtocolClient, ProposeRequest, ProposeResponse,
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
    pub all: HashMap<ServerId, String>,
    pub client_node: NodeHandle,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        assert!(n_nodes >= 3, "the number of nodes must >= 3");
        let handle = madsim::runtime::Handle::current();

        let all: HashMap<ServerId, String> = (0..n_nodes)
            .map(|x| (format!("S{x}"), format!("192.168.1.{}:12345", x + 1)))
            .collect();

        let leader = Arc::new(Mutex::new("S0".to_string()));

        let nodes = (0..n_nodes)
            .map(|i| {
                let id = format!("S{i}");
                let addr = format!("192.168.1.{}:12345", i + 1);
                let storage_path = tempfile::tempdir().unwrap().into_path();

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let store = Arc::new(Mutex::new(None));

                let leader = Arc::clone(&leader);

                let cluster_info = Arc::new(ClusterMember::new(all.clone(), id.clone()));

                let id_c = id.clone();
                let storage_path_c = storage_path.clone();
                let store_c = Arc::clone(&store);
                let role_change_cb = TestRoleChange::default();
                let role_change_arc = role_change_cb.get_inner_arc();

                let node_handle = handle
                    .create_node()
                    .name(format!("S{i}"))
                    .ip(format!("192.168.1.{}", i + 1).parse().unwrap())
                    .init(move || {
                        let ce = TestCE::new(id_c.clone(), exe_tx.clone(), as_tx.clone());
                        store_c.lock().replace(Arc::clone(&ce.store));
                        let is_leader = *leader.lock() == id_c;

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
                                    .data_dir(storage_path_c.clone())
                                    .log_entries_cap(10)
                                    .build()
                                    .unwrap(),
                            ),
                        )
                    })
                    .build();
                (
                    id.clone(),
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
            all,
            client_node,
        }
    }

    pub fn get_node(&self, id: &ServerId) -> &CurpNode {
        &self.nodes[id]
    }

    pub async fn new_client(&self, timeout: ClientTimeout) -> SimClient<TestCommand> {
        let addrs = self
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.addr.clone()))
            .collect();
        SimClient {
            inner: Arc::new(Client::<TestCommand>::new(None, addrs, timeout).await),
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

    pub async fn stop(self) {
        let handle = madsim::runtime::Handle::current();

        for node in self.nodes.values() {
            handle.send_ctrl_c(node.handle.id());
        }
        handle.send_ctrl_c(self.client_node.id());
        madsim::time::sleep(Duration::from_secs(10)).await;

        // check that all nodes have exited
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

    pub async fn crash(&mut self, id: &ServerId) {
        let handle = madsim::runtime::Handle::current();
        handle.kill(id);
        madsim::time::sleep(Duration::from_secs(2)).await;
        if !handle.is_exit(id) {
            panic!("failed to crash node: {id}");
        }
    }

    pub async fn restart(&mut self, id: &ServerId, is_leader: bool) {
        let handle = madsim::runtime::Handle::current();
        if is_leader {
            *self.leader.lock() = id.clone();
        } else {
            *self.leader.lock() = "".to_string();
        }
        handle.restart(id);
    }

    pub async fn try_get_leader(&self) -> Option<(ServerId, u64)> {
        debug!("trying to get leader");
        let mut leader = None;
        let mut max_term = 0;

        let all = self.all.clone();
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
        loop {
            if let Some(leader) = self.try_get_leader().await {
                return leader;
            }
            debug!("failed to get leader");
            madsim::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // get latest term and ensure every working node has the same term
    pub async fn get_term_checked(&self) -> u64 {
        let all = self.all.clone();
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
    pub fn disable_node(&self, id: &ServerId) {
        let handle = madsim::runtime::Handle::current();
        let net = madsim::net::NetSim::current();
        let Some(node)  = handle.get_node(id) else { panic!("no node with name {id} in the simulator")};
        net.clog_node(node.id());
    }

    // Reconnect the node to the network.
    pub fn enable_node(&self, id: &ServerId) {
        let handle = madsim::runtime::Handle::current();
        let net = madsim::net::NetSim::current();
        let Some(node)  = handle.get_node(id) else { panic!("no node with name {id} the simulator")};
        net.unclog_node(node.id());
    }

    pub async fn get_connect(&self, id: &ServerId) -> SimProtocolClient {
        let addr = self
            .all
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
    pub async fn propose(&self, cmd: C) -> Result<C::ER, ProposeError> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.propose(cmd).await })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn propose_indexed(&self, cmd: C) -> Result<(C::ER, C::ASR), ProposeError> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.propose_indexed(cmd).await })
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
            .spawn(async move { inner.get_leader_id().await })
            .await
            .unwrap()
    }

    #[inline]
    pub async fn get_leader_id_from_curp(&self) -> ServerId {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.get_leader_id_from_curp().await })
            .await
            .unwrap()
    }
}
