use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use curp::members::ClusterInfo;
use jsonwebtoken::{DecodingKey, EncodingKey};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{
    net::TcpListener,
    runtime::Handle,
    task::block_in_place,
    time::{self, Duration},
};
use utils::config::{ClientConfig, CompactConfig, CurpConfig, ServerTimeout, StorageConfig};
use xline::{client::Client, server::XlineServer, storage::db::DB};

/// Cluster
pub struct Cluster {
    /// listeners of members
    listeners: BTreeMap<usize, TcpListener>,
    /// address of members
    all_members: HashMap<String, String>,
    /// Client of cluster
    client: Option<Client>,
    /// Xline servers
    servers: Vec<XlineServer>,
    /// Cluster size
    size: usize,
    /// storage paths
    paths: Vec<PathBuf>,
}

impl Cluster {
    /// New `Cluster`
    pub async fn new(size: usize) -> Self {
        let mut listeners = BTreeMap::new();
        for i in 0..size {
            listeners.insert(i, TcpListener::bind("0.0.0.0:0").await.unwrap());
        }
        let all_members = listeners
            .iter()
            .map(|(i, l)| (format!("server{}", i), l.local_addr().unwrap().to_string()))
            .collect();

        Self {
            listeners,
            all_members,
            client: None,
            servers: vec![],
            size,
            paths: vec![],
        }
    }

    pub fn set_paths(&mut self, paths: Vec<PathBuf>) {
        self.paths = paths;
    }

    /// Start `Cluster`
    pub async fn start(&mut self) {
        for i in 0..self.size {
            let name = format!("server{}", i);
            let is_leader = i == 0;
            let listener = self.listeners.remove(&i).unwrap();
            let path = if let Some(path) = self.paths.get(i) {
                path.clone()
            } else {
                let path = PathBuf::from(format!("/tmp/xline-{}", random_id()));
                self.paths.push(path.clone());
                path
            };
            let db: Arc<DB> = DB::open(&StorageConfig::RocksDB(path.clone())).unwrap();
            let cluster_info = ClusterInfo::new(self.all_members.clone(), &name);
            tokio::spawn(async move {
                let server = XlineServer::new(
                    cluster_info.into(),
                    is_leader,
                    CurpConfig {
                        storage_cfg: StorageConfig::RocksDB(path.join("curp")),
                        ..Default::default()
                    },
                    ClientConfig::default(),
                    ServerTimeout::default(),
                    StorageConfig::Memory,
                    CompactConfig::default(),
                );
                let result = server
                    .start_from_listener(listener, db, Self::test_key_pair())
                    .await;
                if let Err(e) = result {
                    panic!("Server start error: {e}");
                }
            });
        }
        // Sleep 30ms, wait for the server to start
        time::sleep(Duration::from_millis(300)).await;
    }

    /// Create or get the client with the specified index
    pub async fn client(&mut self) -> &mut Client {
        if self.client.is_none() {
            let client = Client::new(
                self.all_members.values().cloned().collect(),
                true,
                ClientConfig::default(),
            )
            .await
            .unwrap_or_else(|e| {
                panic!("Client connect error: {:?}", e);
            });
            self.client = Some(client);
        }
        self.client.as_mut().unwrap()
    }

    pub fn all_members(&self) -> &HashMap<String, String> {
        &self.all_members
    }

    pub fn addrs(&self) -> Vec<String> {
        self.all_members.values().cloned().collect()
    }

    async fn stop(&mut self) {
        futures::future::join_all(self.servers.iter_mut().map(|s| s.stop())).await;
        for path in &self.paths {
            let _ignore = tokio::fs::remove_dir_all(path).await;
        }
    }

    fn test_key_pair() -> Option<(EncodingKey, DecodingKey)> {
        let private_key = include_bytes!("../private.pem");
        let public_key = include_bytes!("../public.pem");
        let encoding_key = EncodingKey::from_rsa_pem(private_key).unwrap();
        let decoding_key = DecodingKey::from_rsa_pem(public_key).unwrap();
        Some((encoding_key, decoding_key))
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        block_in_place(move || {
            Handle::current().block_on(async move {
                self.stop().await;
            });
        });
    }
}

fn random_id() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}
