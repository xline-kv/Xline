use etcd_rs::{Client, ClientConfig, Endpoint};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    thread,
};
use xline::server::XlineServer;

#[allow(dead_code)] // TODO: Remove this
pub(crate) struct Cluster {
    servers: Vec<Arc<XlineServer>>,
    clients: Vec<Option<Client>>,
    cluster_client: Option<Client>,
    size: usize,
}

#[allow(dead_code)] // TODO: Remove this
impl Cluster {
    pub(crate) async fn run(size: usize) -> Self {
        let peers = (0..size)
            .map(|i| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), i as u16 + 20000))
            .collect::<Vec<_>>();
        let mut servers = Vec::with_capacity(size);
        for i in 0..size {
            let mut my_peers = peers.clone();
            my_peers.remove(i);
            let name = format!("server{}", i);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), i as u16 + 10000);
            let is_leader = i == 0;
            let leader_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 20000);
            let self_addr =
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), i as u16 + 20000);

            let server = Arc::new(
                XlineServer::new(name, addr, my_peers, is_leader, leader_address, self_addr).await,
            );
            servers.push(Arc::clone(&server));

            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                // let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    server.start().await.unwrap();
                });
            });
        }

        Self {
            servers,
            clients: vec![None; size],
            cluster_client: None,
            size,
        }
    }

    pub(crate) async fn cluster_client(&mut self) -> &Client {
        if self.cluster_client.is_none() {
            let endpoints = (0..self.size)
                .map(|i| Endpoint::new(format!("http://127.0.0.1:{}", i + 10000)))
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
            let endpoints = vec![Endpoint::new(format!("http://127.0.0.1:{}", i + 10000))];
            let cfg = ClientConfig::new(endpoints);
            let client = Client::connect(cfg)
                .await
                .unwrap_or_else(|e| panic!("Client connect error: {:?}", e));
            self.clients[i] = Some(client);
        }
        self.clients[i].as_ref().unwrap()
    }
}
