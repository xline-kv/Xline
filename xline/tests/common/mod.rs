use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use curp::{members::ClusterMember, ServerId};
use jsonwebtoken::{DecodingKey, EncodingKey};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{
    net::TcpListener,
    sync::broadcast::{self, Sender},
    time::{self, Duration},
};
use utils::config::{ClientTimeout, CurpConfig, ServerTimeout, StorageConfig};
use xline::{client::Client, server::XlineServer, storage::db::DB};

/// Cluster
pub struct Cluster {
    /// listeners of members
    listeners: BTreeMap<usize, TcpListener>,
    /// address of members
    all_members: HashMap<ServerId, String>,
    /// Client of cluster
    client: Option<Client>,
    /// Stop sender
    stop_tx: Option<Sender<()>>,
    /// Cluster size
    size: usize,
    /// storage paths
    paths: Vec<PathBuf>,
}

impl Cluster {
    /// New `Cluster`
    pub(crate) async fn new(size: usize) -> Self {
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
            stop_tx: None,
            size,
            paths: vec![],
        }
    }

    #[allow(dead_code)] // used in tests but get warning
    pub(crate) fn set_paths(&mut self, paths: Vec<PathBuf>) {
        self.paths = paths;
    }

    /// Start `Cluster`
    pub(crate) async fn start(&mut self) {
        let (stop_tx, _) = broadcast::channel(1);

        for i in 0..self.size {
            let name = format!("server{}", i);
            let is_leader = i == 0;
            let mut rx = stop_tx.subscribe();
            let listener = self.listeners.remove(&i).unwrap();
            let path = if let Some(path) = self.paths.get(i) {
                path.clone()
            } else {
                let path = PathBuf::from(format!("/tmp/xline-{}", random_id()));
                self.paths.push(path.clone());
                path
            };
            #[allow(clippy::unwrap_used)]
            let db: Arc<DB> = DB::open(&StorageConfig::RocksDB(path.clone())).unwrap();
            let cluster_info = ClusterMember::new(self.all_members.clone(), name.clone());
            tokio::spawn(async move {
                let server = XlineServer::new(
                    cluster_info.into(),
                    is_leader,
                    CurpConfig {
                        data_dir: path.join("curp"),
                        ..Default::default()
                    },
                    ClientTimeout::default(),
                    ServerTimeout::default(),
                    StorageConfig::Memory,
                );
                let signal = async {
                    let _ = rx.recv().await;
                };
                let result = server
                    .start_from_listener_shutdown(listener, signal, db, Self::test_key_pair())
                    .await;
                if let Err(e) = result {
                    panic!("Server start error: {e}");
                }
            });
        }
        self.stop_tx = Some(stop_tx);
        // Sleep 30ms, wait for the server to start
        time::sleep(Duration::from_millis(300)).await;
    }

    /// Create or get the client with the specified index
    pub(crate) async fn client(&mut self) -> &mut Client {
        if self.client.is_none() {
            let client = Client::new(self.all_members.clone(), true, ClientTimeout::default())
                .await
                .unwrap_or_else(|e| {
                    panic!("Client connect error: {:?}", e);
                });
            self.client = Some(client);
        }
        self.client.as_mut().unwrap()
    }

    #[allow(dead_code)] // used in tests but get warning
    pub fn addrs(&self) -> &HashMap<ServerId, String> {
        &self.all_members
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
        if let Some(ref stop_tx) = self.stop_tx {
            let _ = stop_tx.send(());
        }
        for path in &self.paths {
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
