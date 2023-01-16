use std::{collections::HashMap, iter, thread, time::Duration};

use curp::{client::Client, server::Rpc, LogIndex, ProtocolServer};
use futures::future::join_all;
use itertools::Itertools;
use tokio::{net::TcpListener, runtime::Runtime, sync::mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::debug;

use crate::common::test_cmd::{TestCE, TestCommand, TestCommandResult};

pub type ServerId = String;

mod proto {
    tonic::include_proto!("messagepb");
}
use proto::protocol_client::ProtocolClient;

use self::proto::{FetchLeaderRequest, FetchLeaderResponse};

pub struct CurpNode {
    pub id: ServerId,
    pub addr: String,
    pub exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    pub as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
    pub rt: Runtime,
}

pub struct CurpGroup {
    pub nodes: HashMap<ServerId, CurpNode>,
    all: HashMap<ServerId, String>,
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

                let mut others = all.clone();
                others.remove(i);

                let id_c = id.clone();
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name(format!("rt-{id}"))
                    .enable_all()
                    .build()
                    .unwrap();
                let handle = rt.handle().clone();
                thread::spawn(move || {
                    handle.spawn(async move {
                        let rpc = Rpc::new(id_c.clone(), i == 0, others.into_iter().collect(), ce);
                        tokio::spawn(
                            tonic::transport::Server::builder()
                                .add_service(ProtocolServer::new(rpc))
                                .serve_with_incoming(TcpListenerStream::new(listener)),
                        );
                    });
                });

                (
                    id.clone(),
                    CurpNode {
                        id,
                        addr,
                        exe_rx,
                        as_rx,
                        rt,
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

    pub async fn new_client(&self) -> Client<TestCommand> {
        let addrs = self
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.addr.clone()))
            .collect();
        Client::<TestCommand>::new(addrs).await
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

        let mut others = self.all.clone();
        others.remove(id);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(format!("rt-{id}"))
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.handle().clone();
        let id_c = id.clone();
        thread::spawn(move || {
            handle.spawn(async move {
                let rpc = Rpc::new(id_c.clone(), false, others.into_iter().collect(), ce);
                tokio::spawn(
                    tonic::transport::Server::builder()
                        .add_service(ProtocolServer::new(rpc))
                        .serve_with_incoming(TcpListenerStream::new(listener)),
                );
            });
        });

        let new_node = CurpNode {
            id: id.clone(),
            addr,
            exe_rx,
            as_rx,
            rt,
        };
        self.nodes.insert(id.clone(), new_node);
    }

    pub async fn fetch_leader(&self) -> Option<ServerId> {
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
        leader
    }
}
