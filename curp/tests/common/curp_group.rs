use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    iter,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use curp::{client::Client, server::Rpc, LogIndex, ProtocolServer, TxFilter};
use futures::future::join_all;
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{net::TcpListener, runtime::Runtime, sync::mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::debug;
use utils::config::{ClientTimeout, ServerTimeout};

use crate::common::test_cmd::{TestCE, TestCommand, TestCommandResult};

pub type ServerId = String;

pub mod proto {
    tonic::include_proto!("messagepb");
}
pub use proto::{protocol_client::ProtocolClient, ProposeRequest, ProposeResponse};

use self::proto::{FetchLeaderRequest, FetchLeaderResponse};

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
    pub store: Arc<Mutex<HashMap<u32, u32>>>,
    pub rt: Runtime,
    pub switch: Arc<AtomicBool>,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
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
        let all = listeners
            .iter()
            .enumerate()
            .map(|(i, listener)| (format!("S{i}"), listener.local_addr().unwrap().to_string()))
            .collect_vec();

        let nodes = listeners
            .into_iter()
            .enumerate()
            .map(|(i, listener)| {
                let id = format!("S{i}");
                let addr = listener.local_addr().unwrap().to_string();

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let ce = TestCE::new(id.clone(), exe_tx, as_tx);
                let store = Arc::clone(&ce.store);

                let mut others = all.clone();
                others.remove(i);

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
                thread::spawn(move || {
                    handle.spawn(Rpc::run_from_listener(
                        id_c,
                        i == 0,
                        others.into_iter().collect(),
                        listener,
                        ce,
                        Arc::new(ServerTimeout::default()),
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
                    },
                )
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(300)).await;
        debug!("successfully start group");
        Self {
            nodes,
            all: all.into_iter().collect(),
        }
    }

    pub fn get_node(&self, id: &ServerId) -> &CurpNode {
        &self.nodes[id]
    }

    pub async fn new_client(&self, timeout: ClientTimeout) -> Client<TestCommand> {
        let addrs = self
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.addr.clone()))
            .collect();
        Client::<TestCommand>::new(addrs, timeout).await
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
        thread::spawn(move || {
            self.nodes.clear();
        })
        .join()
        .unwrap();
    }

    pub fn crash(&mut self, id: &ServerId) {
        let node = self.nodes.remove(id).unwrap();
        thread::spawn(move || drop(node)).join().unwrap();
    }

    pub async fn restart(&mut self, id: &ServerId) {
        let addr = self.all.get(id).unwrap().clone();
        let listener = TcpListener::bind(&addr)
            .await
            .expect("can't restart because the original addr is taken");

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        let (as_tx, as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new(id.clone(), exe_tx, as_tx);
        let store = Arc::clone(&ce.store);

        let mut others = self.all.clone();
        others.remove(id);

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
        thread::spawn(move || {
            handle.spawn(Rpc::run_from_listener(
                id_c,
                false,
                others.into_iter().collect(),
                listener,
                ce,
                Arc::new(ServerTimeout::default()),
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
