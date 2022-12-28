#![allow(
    clippy::all,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::integer_arithmetic,
    clippy::str_to_string,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::shadow_unrelated,
    dead_code,
    unused_results
)]

use std::{
    error::Error,
    fmt::Display,
    iter,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::future::join_all;
use lock_utils::parking_lot_lock::RwLockMap;
use parking_lot::RwLock;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::debug;
use tracing_test::traced_test;

use crate::{
    message::{ServerId, TermNum},
    rpc::{connect::ConnectInterface, ProposeRequest},
    server::{state::State, Rpc},
    test_utils::{
        curp_group::{CurpGroup, CurpNode},
        test_cmd::{TestCE, TestCommand, TestCommandResult},
    },
    LogIndex, ProtocolServer,
};

struct Node {
    id: ServerId,
    addr: String,
    exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    state: Arc<RwLock<State<TestCommand>>>,
    switch: Arc<AtomicBool>,
}

impl CurpNode for Node {
    fn id(&self) -> ServerId {
        self.id.clone()
    }

    fn addr(&self) -> String {
        self.addr.clone()
    }

    fn exe_rx(&mut self) -> &mut mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)> {
        &mut self.exe_rx
    }

    fn as_rx(&mut self) -> &mut mpsc::UnboundedReceiver<(TestCommand, LogIndex)> {
        &mut self.as_rx
    }
}

#[derive(Debug)]
struct NotReachable;

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

impl CurpGroup<Node> {
    async fn new(n_nodes: usize) -> Self {
        assert!(n_nodes >= 3);
        let listeners = join_all(
            iter::repeat_with(|| async { TcpListener::bind("0.0.0.0:0").await.unwrap() })
                .take(n_nodes),
        )
        .await;
        let all: Vec<_> = listeners
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

                let (exe_tx, exe_rx) = mpsc::unbounded_channel();
                let (as_tx, as_rx) = mpsc::unbounded_channel();
                let ce = TestCE::new(id.clone(), exe_tx, as_tx);

                let mut others = all.clone();
                others.remove(i);

                // create server switch
                let switch = Arc::new(AtomicBool::new(true));
                let switch_clone = Arc::clone(&switch);
                let reachable_layer = tower::filter::FilterLayer::new(move |req| {
                    if switch_clone.load(Ordering::Relaxed) {
                        Ok(req)
                    } else {
                        Err(NotReachable)
                    }
                });

                let rpc = Rpc::new_test(
                    id.clone(),
                    others.into_iter().collect(),
                    ce,
                    Arc::clone(&switch),
                );
                let state = Arc::clone(&rpc.inner.state);

                tokio::spawn(
                    tonic::transport::Server::builder()
                        .layer(reachable_layer)
                        .add_service(ProtocolServer::new(rpc))
                        .serve_with_incoming(TcpListenerStream::new(listener)),
                );

                (
                    id.clone(),
                    Node {
                        id,
                        addr,
                        exe_rx,
                        as_rx,
                        state,
                        switch,
                    },
                )
            })
            .collect();

        tokio::time::sleep(Duration::from_secs(2)).await;
        CurpGroup { nodes }
    }

    // get the latest term
    fn get_term(&self) -> TermNum {
        let mut term = 0;
        for node in self.nodes.values() {
            let state = node.state.read();
            if term < state.term {
                term = state.term;
            }
        }
        term
    }

    fn try_get_leader(&self) -> Option<String> {
        let mut leader_id = None;
        let mut term = 0;
        for node in self.nodes.values() {
            let state = node.state.read();
            if state.term > term {
                term = state.term;
                leader_id = None;
            }
            if state.is_leader() && state.term >= term {
                assert!(
                    leader_id.is_none(),
                    "there should be only 1 leader in a term, now there are two: {} {}",
                    leader_id.unwrap(),
                    node.id
                );
                leader_id = Some(node.id.clone());
            }
        }
        leader_id
    }

    async fn get_leader(&self) -> String {
        for _ in 0..5 {
            if let Some(leader) = self.try_get_leader() {
                return leader;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        panic!("can't get leader");
    }

    fn get_leader_term(&self) -> TermNum {
        let mut term = 0;
        for node in self.nodes.values() {
            let state = node.state.read();
            if state.is_leader() {
                if state.term > term {
                    term = state.term;
                }
            }
        }
        assert_ne!(term, 0, "no leader");
        term
    }

    // get latest term and ensure every node has the same term
    fn get_term_checked(&self) -> TermNum {
        let mut term = None;
        for node in self.nodes.values() {
            let node_term = node.state.map_read(|state| state.term);
            if let Some(term) = term {
                assert_eq!(term, node_term);
            } else {
                term = Some(node_term);
            }
        }
        term.unwrap()
    }

    fn disable_node(&self, id: &ServerId) {
        let node = &self.nodes[id];
        node.switch.store(false, Ordering::Relaxed);
    }

    fn enable_node(&self, id: &ServerId) {
        let node = &self.nodes[id];
        node.switch.store(true, Ordering::Relaxed);
    }
}

// Initial election
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn initial_election() {
    // watch the log while doing sync, TODO: find a better way
    let group = CurpGroup::new(3).await;

    // check whether there is exact one leader in the group
    let leader1 = group.get_leader().await;
    let term1 = group.get_term_checked();

    // check after some time, the term and the leader is still not changed
    tokio::time::sleep(Duration::from_secs(1)).await;
    let leader2 = group.try_get_leader().expect("There should be one leader");
    let term2 = group.get_term_checked();

    assert_eq!(term1, term2);
    assert_eq!(leader1, leader2);
}

// Reelect after network failure
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn reelect() {
    let group = CurpGroup::new(5).await;

    // check whether there is exact one leader in the group
    let leader1 = group.get_leader().await;
    let term1 = group.get_term_checked();

    ////////// disable leader 1
    group.disable_node(&leader1);
    debug!("disable leader {leader1}");

    // after some time, a new leader should be elected
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader2 = group.get_leader().await;
    let term2 = group.get_leader_term();

    assert_ne!(term1, term2);
    assert_ne!(leader1, leader2);

    ////////// disable leader 2
    group.disable_node(&leader2);
    debug!("disable leader {leader2}");

    // after some time, a new leader should be elected
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader3 = group.get_leader().await;
    let term3 = group.get_leader_term();

    assert_ne!(term1, term3);
    assert_ne!(term2, term3);
    assert_ne!(leader1, leader3);
    assert_ne!(leader2, leader3);

    ////////// disable leader 3
    group.disable_node(&leader3);
    debug!("disable leader {leader3}");

    // after some time, no leader should be elected
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(group.try_get_leader().is_none());

    ////////// recover network partition
    group.enable_node(&leader1);
    group.enable_node(&leader2);
    group.enable_node(&leader3);

    tokio::time::sleep(Duration::from_secs(2)).await;
    let _final_leader = group.get_leader().await;
    assert!(group.get_term_checked() > term3);
}

// Propose after reelection. Client should learn about the new server.
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn propose_after_reelect() {
    let group = CurpGroup::new(5).await;
    let client = group.new_client().await;
    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    // wait for the cmd to be synced
    tokio::time::sleep(Duration::from_secs(1)).await;

    let leader1 = group.get_leader().await;
    group.disable_node(&leader1);

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );
}

// To verify PR #86 is fixed
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn fast_round_is_slower_than_slow_round() {
    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    let cmd = Arc::new(TestCommand::new_get(vec![0]));

    let leader = group.get_leader().await;

    let connects = client.get_connects();
    let leader_connect = connects
        .iter()
        .find(|connect| connect.id() == &leader)
        .unwrap();
    leader_connect
        .propose(
            ProposeRequest::new_from_rc(Arc::clone(&cmd)).unwrap(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    // wait for the command to be synced to others
    // because followers never get the cmd from the client, it will mark the cmd done in spec pool instead of removing the cmd from it
    tokio::time::sleep(Duration::from_secs(1)).await;

    let follower_connect = connects
        .iter()
        .find(|connect| connect.id() != &leader)
        .unwrap();

    // the follower should response empty immediately
    let resp = follower_connect
        .propose(
            ProposeRequest::new_from_rc(cmd).unwrap(),
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .into_inner();
    assert!(resp.exe_result.is_none());
}
