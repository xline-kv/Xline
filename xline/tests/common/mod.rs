use std::{collections::BTreeMap, net::SocketAddr, thread};

use etcd_client::Client;
use tokio::{
    net::TcpListener,
    sync::broadcast::{self, Sender},
    time::{self, Duration},
};
use xline::server::XlineServer;

/// Cluster
pub struct Cluster {
    /// listeners of members
    listeners: BTreeMap<usize, TcpListener>,
    /// address of members
    addrs: Vec<SocketAddr>,
    /// Clients of cluster
    clients: Vec<Option<Client>>,
    /// Stop sender
    stop_tx: Option<Sender<()>>,
    /// Cluster size
    size: usize,
}

impl Cluster {
    /// New `Cluster`
    pub(crate) async fn new(size: usize) -> Self {
        let mut listeners = BTreeMap::new();
        for i in 0..size {
            listeners.insert(i, TcpListener::bind("0.0.0.0:0").await.unwrap());
        }
        let addrs = listeners
            .iter()
            .map(|l| l.1.local_addr().unwrap())
            .collect();

        Self {
            listeners,
            addrs,
            clients: vec![None; size],
            stop_tx: None,
            size,
        }
    }

    /// Start `Cluster`
    pub(crate) async fn start(&mut self) {
        let (stop_tx, _) = broadcast::channel(1);
        for i in 0..self.size {
            let mut peers = self.addrs.clone();
            peers.remove(i);
            let name = format!("server{}", i);
            let is_leader = i == 0;
            let leader_addr = self.addrs[0];
            let self_addr = self.addrs[i];
            let mut rx = stop_tx.subscribe();
            let listener = self.listeners.remove(&i).unwrap();

            thread::spawn(move || {
                tokio::runtime::Runtime::new()
                    .unwrap_or_else(|e| panic!("Create runtime error: {}", e))
                    .block_on(async move {
                        let server =
                            XlineServer::new(name, peers, is_leader, leader_addr, self_addr).await;

                        tokio::select! {
                            _ = rx.recv() => {} // tx droped, or tx send ()
                            result = server.start_from_listener(listener) => {
                                if let Err(e) = result {
                                    panic!("Server start error: {e}");
                                }
                            }
                        }
                    });
            });
        }
        self.stop_tx = Some(stop_tx);
        // Sleep 30ms, wait for the server to start
        time::sleep(Duration::from_millis(30)).await;
    }

    #[allow(dead_code)] // TODO: remove this
    /// Stop `Cluster`
    pub(crate) fn stop(&self) {
        if let Some(stop_tx) = self.stop_tx.as_ref() {
            stop_tx.send(()).unwrap();
        }
    }

    /// Create or get the client with the specified index
    pub(crate) async fn client(&mut self, i: usize) -> &mut Client {
        assert!(i < self.size);
        if self.clients[i].is_none() {
            let client = Client::connect([self.addrs[i].to_string()], None)
                .await
                .unwrap_or_else(|e| panic!("Client connect error: {:?}", e));
            self.clients[i] = Some(client);
        }
        self.clients[i].as_mut().unwrap()
    }
}
