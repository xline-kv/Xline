use std::{
    collections::HashMap,
    iter,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::anyhow;
use futures::future::join_all;
use lock_utils::parking_lot_lock::RwLockMap;
use parking_lot::RwLock;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::TcpListenerStream;

use crate::{
    client::Client,
    message::{ServerId, TermNum},
    server::{
        state::State,
        tests::test_cmd::{TestCE, TestCommand, TestCommandResult},
        Protocol, Rpc,
    },
    LogIndex, ProtocolServer,
};

struct CurpNode {
    id: ServerId,
    addr: String,
    exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    state: Arc<RwLock<State<TestCommand>>>,
    switch: Arc<AtomicBool>,
}

pub(super) struct CurpGroup {
    nodes: HashMap<ServerId, CurpNode>,
}

impl CurpGroup {
    pub(super) async fn new(n_nodes: usize) -> Self {
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
                        Err(anyhow!("server unreachable"))
                    }
                });

                let rpc = Rpc {
                    inner: Arc::new(Protocol::new_test(
                        id.clone(),
                        false,
                        others.into_iter().collect(),
                        ce,
                        Arc::clone(&switch),
                    )),
                };
                let state = Arc::clone(&rpc.inner.state);

                tokio::spawn(
                    tonic::transport::Server::builder()
                        .layer(reachable_layer)
                        .add_service(ProtocolServer::new(rpc))
                        .serve_with_incoming(TcpListenerStream::new(listener)),
                );

                (
                    id.clone(),
                    CurpNode {
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

        tokio::time::sleep(Duration::from_millis(500)).await;
        Self { nodes }
    }

    // get the latest term
    pub(super) fn get_term(&self) -> TermNum {
        let mut term = 0;
        for node in self.nodes.values() {
            let state = node.state.read();
            if term < state.term {
                term = state.term;
            }
        }
        term
    }

    pub(super) fn get_leader(&self) -> Option<String> {
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

    // get latest term and ensure every node has the same term
    pub(super) fn get_term_checked(&self) -> TermNum {
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

    pub(super) fn get_leader_term(&self) -> TermNum {
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

    pub(super) async fn new_client(&self) -> Client<TestCommand> {
        let addrs = self
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.addr.clone()))
            .collect();
        Client::<TestCommand>::new(addrs).await
    }

    pub(super) fn disable_node(&self, id: &ServerId) {
        let node = &self.nodes[id];
        node.switch.store(false, Ordering::Relaxed);
    }

    pub(super) fn enable_node(&self, id: &ServerId) {
        let node = &self.nodes[id];
        node.switch.store(true, Ordering::Relaxed);
    }

    pub(super) fn exe_rxs(
        &mut self,
    ) -> impl Iterator<Item = &mut mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>> {
        self.nodes.values_mut().map(|node| &mut node.exe_rx)
    }

    pub(super) fn as_rxs(
        &mut self,
    ) -> impl Iterator<Item = &mut mpsc::UnboundedReceiver<(TestCommand, LogIndex)>> {
        self.nodes.values_mut().map(|node| &mut node.as_rx)
    }
}
