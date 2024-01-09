use std::{
    collections::{BTreeMap, HashMap},
    env::temp_dir,
    path::PathBuf,
    sync::Arc,
};

use curp::members::{get_cluster_info_from_remote, ClusterInfo};
use jsonwebtoken::{DecodingKey, EncodingKey};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{
    net::TcpListener,
    runtime::Handle,
    task::block_in_place,
    time::{self, Duration},
};
use utils::config::{
    AuthConfig, ClientConfig, ClusterConfig, CompactConfig, CurpConfig, EngineConfig,
    InitialClusterState, LevelConfig, LogConfig, RotationConfig, ServerTimeout, StorageConfig,
    TraceConfig, XlineServerConfig,
};
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
    /// Xline servers
    servers: Vec<Arc<XlineServer>>,
    /// Server configs
    configs: HashMap<usize, XlineServerConfig>,
}

impl Cluster {
    /// New `Cluster`
    pub async fn new(size: usize) -> Self {
        let configs = (0..size)
            .map(|i| (i, Self::default_config()))
            .collect::<HashMap<_, _>>();
        Self::new_with_configs(configs).await
    }

    /// New `Cluster` with rocksdb
    pub async fn new_rocks(size: usize) -> Self {
        let configs = (0..size)
            .map(|i| {
                let path = temp_dir().join(random_id());
                (i, Self::default_rocks_config_with_path(path))
            })
            .collect::<HashMap<_, _>>();
        Self::new_with_configs(configs).await
    }

    pub async fn new_with_configs(configs: HashMap<usize, XlineServerConfig>) -> Self {
        let size = configs.len();
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
            servers: Vec::new(),
            configs,
        }
    }

    pub fn set_paths(&mut self, _paths: HashMap<usize, PathBuf>) {
        todo!()
        // self.paths = paths;
    }

    /// Start `Cluster`
    pub async fn start(&mut self) {
        for i in 0..self.configs.len() {
            let name = format!("server{}", i);
            let is_leader = i == 0;
            let listener = self.listeners.remove(&i).unwrap();
            let config = self.configs.get(&i).unwrap();
            let cluster_config = config.cluster();
            let cluster_info = ClusterInfo::new(
                self.all_members
                    .clone()
                    .into_iter()
                    .map(|(i, addr)| (format!("server{}", i), vec![addr]))
                    .collect(),
                &name,
            );
            assert!(matches!(
                cluster_config.initial_cluster_state(),
                InitialClusterState::New
            ));

            let db = DB::open(&config.storage().engine).unwrap();
            let server = XlineServer::new(
                cluster_info.into(),
                is_leader,
                cluster_config.curp_config().clone(),
                *cluster_config.client_config(),
                *cluster_config.server_timeout(),
                config.storage().clone(),
                *config.compact(),
            );

            tokio::spawn(async move {
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
        let config = Self::default_config();
        self.run_node_with_config(listener, idx, config).await;
    }

    pub async fn run_node_with_config(
        &mut self,
        listener: TcpListener,
        idx: usize,
        config: XlineServerConfig,
    ) {
        let self_addr = listener.local_addr().unwrap().to_string();
        _ = self.all_members.insert(idx, self_addr.clone());
        _ = self.configs.insert(idx, config);
        let config = self.configs.get(&idx).unwrap();
        let cluster_config = config.cluster();
        let name = format!("server{}", idx);
        let db: Arc<DB> = DB::open(&config.storage().engine).unwrap();
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
        let server = XlineServer::new(
            cluster_info.into(),
            false,
            cluster_config.curp_config().clone(),
            *cluster_config.client_config(),
            *cluster_config.server_timeout(),
            config.storage().clone(),
            *config.compact(),
        );
        tokio::spawn(async move {
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

    pub fn default_cluster_config() -> ClusterConfig {
        ClusterConfig::new(
            "".to_owned(),
            HashMap::new(),
            false,
            CurpConfig::default(),
            ClientConfig::default(),
            ServerTimeout::default(),
            InitialClusterState::default(),
        )
    }

    pub fn default_log_config() -> LogConfig {
        LogConfig::new(
            temp_dir().join(random_id()),
            RotationConfig::Daily,
            LevelConfig::INFO,
        )
    }

    pub fn default_config() -> XlineServerConfig {
        let cluster = Self::default_cluster_config();
        let storage = StorageConfig::default();
        let log = Self::default_log_config();
        let trace = TraceConfig::default();
        let auth = AuthConfig::default();
        let compact = CompactConfig::default();
        XlineServerConfig::new(cluster, storage, log, trace, auth, compact)
    }

    pub fn default_config_with_quota_and_rocks_path(
        path: PathBuf,
        quota: u64,
    ) -> XlineServerConfig {
        let cluster = Self::default_cluster_config();
        let storage = StorageConfig::new(EngineConfig::RocksDB(path), quota);
        let log = Self::default_log_config();
        let trace = TraceConfig::default();
        let auth = AuthConfig::default();
        let compact = CompactConfig::default();
        XlineServerConfig::new(cluster, storage, log, trace, auth, compact)
    }

    pub fn default_rocks_config_with_path(path: PathBuf) -> XlineServerConfig {
        Self::default_config_with_quota_and_rocks_path(path, 0)
    }

    pub fn default_rocks_config() -> XlineServerConfig {
        let path = temp_dir().join(random_id());
        Self::default_config_with_quota_and_rocks_path(path, 0)
    }

    pub fn default_quota_config(quota: u64) -> XlineServerConfig {
        let path = temp_dir().join(random_id());
        Self::default_config_with_quota_and_rocks_path(path, quota)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        block_in_place(move || {
            Handle::current().block_on(async move {
                for xline in self.servers.iter() {
                    xline.stop().await;
                }
                for cfg in self.configs.values() {
                    if let EngineConfig::RocksDB(ref path) = cfg.cluster().curp_config().engine_cfg
                    {
                        let _ignore = tokio::fs::remove_dir_all(path).await;
                    }
                    if let EngineConfig::RocksDB(ref path) = cfg.storage().engine {
                        let _ignore = tokio::fs::remove_dir_all(path).await;
                    }
                }
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
