use std::{
    collections::HashMap,
    iter,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use curp::{client::Client, server::Rpc, LogIndex, ProtocolServer};
use futures::future::join_all;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use utils::config::{ClientTimeout, ServerTimeout};

use crate::common::test_cmd::{TestCE, TestCommand, TestCommandResult};

pub type ServerId = String;

struct CurpNode {
    id: ServerId,
    addr: String,
    exe_rx: mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>,
    as_rx: mpsc::UnboundedReceiver<(TestCommand, LogIndex)>,
}

pub struct CurpGroup {
    nodes: HashMap<ServerId, CurpNode>,
}

impl CurpGroup {
    pub async fn new(n_nodes: usize) -> Self {
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

                let rpc = Rpc::new(
                    id.clone(),
                    false,
                    others.into_iter().collect(),
                    ce,
                    Arc::new(ServerTimeout::default()),
                );

                tokio::spawn(
                    tonic::transport::Server::builder()
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
                    },
                )
            })
            .collect();

        tokio::time::sleep(Duration::from_secs(3)).await;
        Self { nodes }
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
}
