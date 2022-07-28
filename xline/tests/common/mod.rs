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
        Self {
            servers: Vec::with_capacity(size),
            peers: Vec::with_capacity(size),
            service_addrs: Vec::with_capacity(size),
            clients: vec![None; size],
            cluster_client: None,
            size,
        }
    }

    pub(crate) async fn start(&mut self) {
        for _ in 0..self.size {
            self.generate_available_peer_address();
        }
        for i in 0..self.size {
            let mut peers = self.peers.clone();
            peers.remove(i);
            let name = format!("server{}", i);
            let addr = self.generate_available_service_address();
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
            // let endpoints = (0..self.size)
            //     .map(|i| Endpoint::new(format!("http://127.0.0.1:{}", i + 10000)))
            //     .collect::<Vec<_>>();
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

    pub(crate) fn generate_available_peer_address(&mut self) -> SocketAddr {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind to a port");
        let addr = listener.local_addr().unwrap();
        if self.peers.contains(&addr) || self.service_addrs.contains(&addr) {
            self.generate_available_peer_address()
        } else {
            self.peers.push(addr);
            addr
        }
    }

    pub(crate) fn generate_available_service_address(&mut self) -> SocketAddr {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind to a port");
        let addr = listener.local_addr().unwrap();
        if self.peers.contains(&addr) || self.service_addrs.contains(&addr) {
            self.generate_available_service_address()
        } else {
            self.service_addrs.push(addr);
            addr
        }
    }
}
