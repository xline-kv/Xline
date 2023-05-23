use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    iter,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use async_trait::async_trait;
use curp::{
    client::{Client, ReadState},
    cmd::Command,
    error::ProposeError,
    server::Rpc,
    FetchLeaderRequest, FetchLeaderResponse, LogIndex, ProtocolServer, SnapshotAllocator, TxFilter,
};
use engine::{Engine, EngineType, Snapshot};
use futures::future::join_all;
use itertools::Itertools;
use madsim::{
    runtime::{Handle, NodeHandle},
    task::ToNodeId,
};
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc};
use tracing::debug;
use utils::config::{
    default_candidate_timeout_ticks, default_cmd_workers, default_follower_timeout_ticks,
    default_gc_interval, default_heartbeat_interval, default_retry_timeout, default_rpc_timeout,
    default_server_wait_synced_timeout, ClientTimeout, CurpConfig, CurpConfigBuilder,
};

use super::{
    random_id,
    test_cmd::{TestCE, TestCommand, TestCommandResult},
};

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

#[derive(Debug)]
pub struct NotReachable;

impl Display for NotReachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Server not reachable")
    }
}

impl Error for NotReachable {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

pub struct CurpNode {
    pub id: ServerId,
    pub addr: String,
    pub handle: NodeHandle,
    pub exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    pub as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    pub store: Arc<Mutex<Option<Arc<Engine>>>>,
    pub storage_path: String,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
    pub leader: Arc<Mutex<ServerId>>,
    pub all: HashMap<ServerId, String>,
    pub client_node: NodeHandle,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        assert!(n_nodes >= 3);
        let handle = madsim::runtime::Handle::current();
        let net = madsim::net::NetSim::current();

        let all: HashMap<ServerId, String> = (0..n_nodes)
            .map(|x| (format!("S{x}"), format!("192.168.1.{}:12345", x + 1)))
            .collect();

        let leader = Arc::new(Mutex::new("S0".to_string()));

        let nodes = (0..n_nodes)
            .into_iter()
            .map(|i| {
                let id = "S{i}".to_string();
                let addr = format!("192.168.1.{}:12345", i + 1);
                let storage_path = format!("/tmp/curp-{}", random_id());

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let store = Arc::new(Mutex::new(None));

                let leader = Arc::clone(&leader);

                let mut others = all.clone();
                others.remove(&id);

                let addr_c = addr.clone();
                let id_c = id.clone();
                let storage_path_c = storage_path.clone();
                let store_c = Arc::clone(&store);

                let node_handle = handle
                    .create_node()
                    .name("S{i}")
                    .ip(format!("192.168.1.{}", i + 1).parse().unwrap())
                    .init(move || {
                        let ce = TestCE::new(id_c.clone(), exe_tx.clone(), as_tx.clone());
                        store_c.lock().replace(Arc::clone(&ce.store));
                        let is_leader = *leader.lock() == id_c;

                        Rpc::run_from_addr(
                            id_c.clone(),
                            is_leader,
                            others.clone(),
                            "0.0.0.0:12345".parse().unwrap(),
                            ce,
                            MemorySnapshotAllocator,
                            Arc::new(
                                CurpConfigBuilder::default()
                                    .data_dir(PathBuf::from(storage_path_c.clone()))
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
            inner: Arc::new(Client::<TestCommand>::new(addrs, timeout).await),
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

    pub async fn stop(mut self) {
        let handle = madsim::runtime::Handle::current();

        for node in self.nodes.values() {
            handle.send_ctrl_c(node.handle.id());
        }
        handle.send_ctrl_c(self.client_node.id());
        madsim::time::sleep(Duration::from_secs(10)).await;

        // check all nodes are exited
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
        let leader = self
            .client_node
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
            .unwrap();
        leader
    }

    pub async fn get_leader(&self) -> (ServerId, u64) {
        for _ in 0..5 {
            if let Some(leader) = self.try_get_leader().await {
                return leader;
            }
            debug!("failed to get leader");
        }
        panic!("can't get leader");
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

    pub async fn get_connect(&self, id: &ServerId) -> SimProtocalClient {
        let addr = self
            .all
            .iter()
            .find_map(|(node_id, addr)| (node_id == id).then_some(addr))
            .unwrap();
        let addr = format!("http://{}", addr);
        SimProtocalClient {
            addr,
            handle: self.client_node.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SimProtocalClient {
    addr: String,
    handle: NodeHandle,
}

impl SimProtocalClient {
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
    pub fn leader(&self) -> Option<ServerId> {
        self.inner.leader()
    }

    #[inline]
    pub fn leader_rx(&self) -> broadcast::Receiver<ServerId> {
        self.inner.leader_rx()
    }
}
