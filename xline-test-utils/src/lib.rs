use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use curp::members::{get_cluster_info_from_remote, ClusterInfo};
use jsonwebtoken::{DecodingKey, EncodingKey};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{
    net::TcpListener,
    time::{self, Duration},
};
use utils::config::{ClientConfig, CompactConfig, CurpConfig, ServerTimeout, StorageConfig};
use xline::{server::XlineServer, storage::db::DB};
pub use xline_client::{types, Client, ClientOptions};

/// Cluster
pub struct Cluster {
    /// listeners of members
    listeners: BTreeMap<usize, TcpListener>,
    /// address of members
    all_members: HashMap<usize, String>,
    /// Client of cluster
    client: Option<Client>,
    /// Cluster size
    size: usize,
    /// storage paths
    paths: HashMap<usize, PathBuf>,
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
            .map(|(i, l)| (*i, l.local_addr().unwrap().to_string()))
            .collect();
        Self {
            listeners,
            all_members,
            client: None,
            size,
            paths: HashMap::new(),
        }
    }

    pub fn set_paths(&mut self, paths: HashMap<usize, PathBuf>) {
        self.paths = paths;
    }

    /// Start `Cluster`
    pub async fn start(&mut self) {
        for i in 0..self.size {
            let name = format!("server{}", i);
            let is_leader = i == 0;
            let listener = self.listeners.remove(&i).unwrap();
            let path = if let Some(path) = self.paths.get(&i) {
                path.clone()
            } else {
                let path = PathBuf::from(format!("/tmp/xline-{}", random_id()));
                self.paths.insert(i, path.clone());
                path
            };
            let db: Arc<DB> = DB::open(&StorageConfig::RocksDB(path.clone())).unwrap();
            let cluster_info = ClusterInfo::new(
                self.all_members
                    .clone()
                    .into_iter()
                    .map(|(id, addr)| (format!("server{}", id), vec![addr]))
                    .collect(),
                &name,
            );
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

    pub async fn run_node(&mut self, listener: TcpListener, idx: usize) {
        let self_addr = listener.local_addr().unwrap().to_string();
        _ = self.all_members.insert(idx, self_addr.clone());
        let name = format!("server{}", idx);
        let path = if let Some(path) = self.paths.get(&idx) {
            path.clone()
        } else {
            let path = PathBuf::from(format!("/tmp/xline-{}", random_id()));
            self.paths.insert(idx, path.clone());
            path
        };
        let db: Arc<DB> = DB::open(&StorageConfig::RocksDB(path.clone())).unwrap();
        let init_cluster_info = ClusterInfo::new(
            self.all_members
                .clone()
                .into_iter()
                .map(|(id, addr)| (format!("server{id}"), vec![addr]))
                .collect(),
            &name,
        );
        let cluster_info = get_cluster_info_from_remote(
            &init_cluster_info,
            &[self_addr],
            &name,
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            let server = XlineServer::new(
                cluster_info.into(),
                false,
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

    /// Create or get the client with the specified index
    pub async fn client(&mut self) -> &mut Client {
        if self.client.is_none() {
            let client = Client::connect(
                self.all_members.values().cloned().collect::<Vec<_>>(),
                ClientOptions::default(),
            )
            .await
            .unwrap_or_else(|e| {
                panic!("Client connect error: {:?}", e);
            });
            self.client = Some(client);
        }
        self.client.as_mut().unwrap()
    }

    pub fn all_members(&self) -> &HashMap<usize, String> {
        &self.all_members
    }

    pub fn get_addr(&self, idx: usize) -> String {
        self.all_members[&idx].clone()
    }

    pub fn addrs(&self) -> Vec<String> {
        self.all_members.values().cloned().collect()
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
        for path in self.paths.values() {
            let _ignore = std::fs::remove_dir_all(path);
        }
    }
}

fn random_id() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}
