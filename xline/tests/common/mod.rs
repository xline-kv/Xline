use etcd_rs::{Client, ClientConfig, Endpoint};
use std::{net::SocketAddr, sync::Arc, thread};
use xline::server::XlineServer;

#[allow(dead_code)] // TODO: Remove this
pub(crate) struct Cluster {
    servers: Vec<Arc<XlineServer>>,
    peers: Vec<SocketAddr>,
    service_addrs: Vec<SocketAddr>,
    clients: Vec<Option<Client>>,
    cluster_client: Option<Client>,
    size: usize,
}

#[allow(dead_code)] // TODO: Remove this
impl Cluster {
    pub(crate) fn new(size: usize) -> Self {
        let peers = (0..size)
            .map(|i| SocketAddr::from(([127, 0, 0, 1], i as u16 + 20000)))
            .collect();
        let service_addrs = (0..size)
            .map(|i| SocketAddr::from(([127, 0, 0, 1], i as u16 + 10000)))
            .collect();
        Self {
            servers: Vec::with_capacity(size),
            peers,
            service_addrs,
            clients: vec![None; size],
            cluster_client: None,
            size,
        }
    }

    pub(crate) async fn start(&mut self) {
        for i in 0..self.size {
            let mut peers = self.peers.clone();
            peers.remove(i);
            let name = format!("server{}", i);
            let addr = self.service_addrs[i];
            let is_leader = i == 0;
            let leader_address = self.peers[0];
            let self_addr = self.peers[i];
            let server = Arc::new(
                XlineServer::new(name, addr, peers, is_leader, leader_address, self_addr).await,
            );

            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    server.start().await.unwrap();
                });
            });
        }
    }

    pub(crate) async fn cluster_client(&mut self) -> &Client {
        if self.cluster_client.is_none() {
            let endpoints = self
                .service_addrs
                .iter()
                .map(|addr| Endpoint::new(format!("http://{}", addr)))
                .collect::<Vec<_>>();
            let cfg = ClientConfig::new(endpoints);
            let client = Client::connect(cfg)
                .await
                .unwrap_or_else(|e| panic!("Client connect error: {:?}", e));
            self.cluster_client = Some(client);
        }
        self.cluster_client.as_ref().unwrap()
    }

    pub(crate) async fn client(&mut self, i: usize) -> &Client {
        assert!(i < self.size);
        if self.clients[i].is_none() {
            let endpoints = vec![Endpoint::new(format!("http://{}", self.service_addrs[i]))];
            let cfg = ClientConfig::new(endpoints);
            let client = Client::connect(cfg)
                .await
                .unwrap_or_else(|e| panic!("Client connect error: {:?}", e));
            self.clients[i] = Some(client);
        }
        self.clients[i].as_ref().unwrap()
    }
}
