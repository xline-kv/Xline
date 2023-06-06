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
    client::Client, members::ClusterMember, server::Rpc, LogIndex, ProtocolServer,
    SnapshotAllocator, TxFilter,
};
use curp_test_utils::{
    random_id,
    test_cmd::{TestCE, TestCommand, TestCommandResult},
    TestRoleChange, TestRoleChangeInner,
};
use engine::{Engine, EngineType, Snapshot};
use futures::future::join_all;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use tokio::{net::TcpListener, runtime::Runtime, sync::mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::debug;
use utils::config::{
    default_candidate_timeout_ticks, default_cmd_workers, default_follower_timeout_ticks,
    default_gc_interval, default_heartbeat_interval, default_retry_timeout, default_rpc_timeout,
    default_server_wait_synced_timeout, ClientTimeout, CurpConfig, CurpConfigBuilder,
};

pub type ServerId = String;

pub mod proto {
    tonic::include_proto!("messagepb");
}
pub use proto::{protocol_client::ProtocolClient, ProposeRequest, ProposeResponse};

use self::proto::{FetchLeaderRequest, FetchLeaderResponse};

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

#[derive(Debug)]
struct TestTxFilter {
    reachable: Arc<AtomicBool>,
}

impl TestTxFilter {
    fn new(reachable: Arc<AtomicBool>) -> Self {
        Self { reachable }
    }
}

impl TxFilter for TestTxFilter {
    fn filter(&self) -> Option<()> {
        self.reachable.load(Ordering::Acquire).then_some(())
    }

    fn boxed_clone(&self) -> Box<dyn TxFilter> {
        Box::new(Self {
            reachable: Arc::clone(&self.reachable),
        })
    }
}

pub struct CurpNode {
    pub id: ServerId,
    pub addr: String,
    pub exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    pub as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    pub store: Arc<Engine>,
    pub rt: Runtime,
    pub switch: Arc<AtomicBool>,
    pub storage_path: String,
    pub role_change_arc: Arc<TestRoleChangeInner>,
}

pub struct CrashedCurpNode {
    pub id: ServerId,
    pub addr: String,
    pub storage_path: String,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
    pub crashed_nodes: HashMap<ServerId, CrashedCurpNode>,
    pub all: HashMap<ServerId, String>,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
        assert!(n_nodes >= 3);
        let listeners = join_all(
            iter::repeat_with(|| async { TcpListener::bind("0.0.0.0:0").await.unwrap() })
                .take(n_nodes),
        )
        .await;
        let all: HashMap<ServerId, String> = listeners
            .iter()
            .enumerate()
            .map(|(i, listener)| (format!("S{i}"), listener.local_addr().unwrap().to_string()))
            .collect();

        let nodes = listeners
            .into_iter()
            .enumerate()
            .map(|(i, listener)| {
                let id = format!("S{i}");
                let addr = listener.local_addr().unwrap().to_string();
                let storage_path = format!("/tmp/curp-{}", random_id());

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let ce = TestCE::new(id.clone(), exe_tx, as_tx);
                let store = Arc::clone(&ce.store);

                let cluster_info = Arc::new(ClusterMember::new(all.clone(), id.clone()));

                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name(format!("rt-{id}"))
                    .enable_all()
                    .build()
                    .unwrap();
                let handle = rt.handle().clone();

                // create server switch
                let switch = Arc::new(AtomicBool::new(true));
                let switch_c = Arc::clone(&switch);
                let reachable_layer = tower::filter::FilterLayer::new(move |req| {
                    if switch_c.load(Ordering::Acquire) {
                        Ok(req)
                    } else {
                        Err(NotReachable)
                    }
                });

                let id_c = id.clone();
                let switch_c = Arc::clone(&switch);
                let storage_path_c = storage_path.clone();
                let role_change_cb = TestRoleChange::default();
                let role_change_arc = role_change_cb.get_inner_arc();
                thread::spawn(move || {
                    handle.spawn(Rpc::run_from_listener(
                        cluster_info,
                        i == 0,
                        listener,
                        ce,
                        Box::new(MemorySnapshotAllocator),
                        role_change_cb,
                        Arc::new(
                            CurpConfigBuilder::default()
                                .data_dir(PathBuf::from(storage_path_c))
                                .log_entries_cap(10)
                                .build()
                                .unwrap(),
                        ),
                        Some(Box::new(TestTxFilter::new(Arc::clone(&switch_c)))),
                        Some(reachable_layer),
                    ));
                });

                (
                    id.clone(),
                    CurpNode {
                        id,
                        addr,
                        exe_rx,
                        as_rx,
                        store,
                        rt,
                        switch,
                        storage_path,
                        role_change_arc,
                    },
                )
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(300)).await;
        debug!("successfully start group");
        Self {
            nodes,
            all,
            crashed_nodes: HashMap::new(),
        }
    }

    pub fn get_node(&self, id: &ServerId) -> &CurpNode {
        &self.nodes[id]
    }

    pub async fn new_client(&self, timeout: ClientTimeout) -> Client<TestCommand> {
        Client::<TestCommand>::new(None, self.all.clone(), timeout).await
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

    pub fn stop(mut self) {
        let paths = self
            .nodes
            .values()
            .map(|node| node.storage_path.clone())
            .chain(
                self.crashed_nodes
                    .values()
                    .map(|node| node.storage_path.clone()),
            )
            .collect_vec();
        thread::spawn(move || {
            self.nodes.clear();
        })
        .join()
        .unwrap();
        for path in paths {
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    pub fn crash(&mut self, id: &ServerId) {
        let node = self.nodes.remove(id).unwrap();
        let crashed = CrashedCurpNode {
            id: node.id.clone(),
            addr: node.addr.clone(),
            storage_path: node.storage_path.clone(),
        };
        self.crashed_nodes.insert(id.clone(), crashed);
        thread::spawn(move || drop(node)).join().unwrap();
    }

    pub async fn restart(&mut self, id: &ServerId, is_leader: bool) {
        let addr = self.all.get(id).unwrap().clone();
        let listener = TcpListener::bind(&addr)
            .await
            .expect("can't restart because the original addr is taken");
        let crashed = self.crashed_nodes.remove(id).expect("no such crashed node");

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        let (as_tx, as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new(id.clone(), exe_tx, as_tx);
        let store = Arc::clone(&ce.store);

        let cluster_info = Arc::new(ClusterMember::new(self.all.clone(), id.clone()));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(format!("rt-{id}"))
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.handle().clone();

        // create server switch
        let switch = Arc::new(AtomicBool::new(true));
        let switch_c = Arc::clone(&switch);
        let reachable_layer = tower::filter::FilterLayer::new(move |req| {
            if switch_c.load(Ordering::Relaxed) {
                Ok(req)
            } else {
                Err(NotReachable)
            }
        });

        let id_c = id.clone();
        let switch_c = Arc::clone(&switch);
        let storage_path = crashed.storage_path.clone();
        let role_change_cb = TestRoleChange::default();
        let role_change_arc = role_change_cb.get_inner_arc();
        thread::spawn(move || {
            handle.spawn(Rpc::run_from_listener(
                cluster_info,
                is_leader,
                listener,
                ce,
                Box::new(MemorySnapshotAllocator),
                role_change_cb,
                Arc::new(
                    CurpConfigBuilder::default()
                        .data_dir(PathBuf::from(storage_path))
                        .build()
                        .unwrap(),
                ),
                Some(Box::new(TestTxFilter::new(Arc::clone(&switch_c)))),
                Some(reachable_layer),
            ));
        });

        let new_node = CurpNode {
            id: id.clone(),
            addr,
            exe_rx,
            as_rx,
            store,
            rt,
            switch,
            storage_path: crashed.storage_path,
            role_change_arc,
        };
        self.nodes.insert(id.clone(), new_node);
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

    pub fn disable_node(&self, id: &ServerId) {
        let node = &self.nodes[id];
        node.switch.store(false, Ordering::Relaxed);
    }

    pub fn enable_node(&self, id: &ServerId) {
        let node = &self.nodes[id];
        node.switch.store(true, Ordering::Relaxed);
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
