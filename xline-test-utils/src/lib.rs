use std::{collections::HashMap, env::temp_dir, iter, path::PathBuf, sync::Arc};

use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{
    net::TcpListener,
    runtime::Handle,
    task::block_in_place,
    time::{self, Duration},
};
use utils::config::{
    default_quota, AuthConfig, ClusterConfig, CompactConfig, EngineConfig, InitialClusterState,
    LogConfig, StorageConfig, TraceConfig, XlineServerConfig,
};
use xline::server::XlineServer;
pub use xline_client::{types, Client, ClientOptions};

/// Cluster
pub struct Cluster {
    /// listeners of members
    listeners: Vec<TcpListener>,
    /// address of members
    all_members: Vec<String>,
    /// Server configs
    configs: Vec<XlineServerConfig>,
    /// Xline servers
    servers: Vec<Arc<XlineServer>>,
    /// Client of cluster
    client: Option<Client>,
}

impl Cluster {
    /// New `Cluster`
    pub async fn new(size: usize) -> Self {
        let configs = iter::repeat_with(XlineServerConfig::default)
            .take(size)
            .collect();
        Self::new_with_configs(configs).await
    }

    /// New `Cluster` with rocksdb
    pub async fn new_rocks(size: usize) -> Self {
        let configs = iter::repeat_with(|| {
            let path = temp_dir().join(random_id());
            Self::default_rocks_config_with_path(path)
        })
        .take(size)
        .collect();
        Self::new_with_configs(configs).await
    }

    pub async fn new_with_configs(configs: Vec<XlineServerConfig>) -> Self {
        let size = configs.len();
        let mut listeners = Vec::new();
        for _i in 0..size {
            listeners.push(TcpListener::bind("0.0.0.0:0").await.unwrap());
        }
        let all_members = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().to_string())
            .collect();
        Self {
            listeners,
            all_members,
            configs,
            servers: Vec::new(),
            client: None,
        }
    }

    /// Start `Cluster`
    pub async fn start(&mut self) {
        for (i, config) in self.configs.iter().enumerate() {
            let name = format!("server{}", i);
            let listener = self.listeners.remove(0);

            let config = Self::merge_config(
                config,
                name,
                self.all_members
                    .clone()
                    .into_iter()
                    .enumerate()
                    .map(|(i, addr)| (format!("server{}", i), vec![addr]))
                    .collect(),
                i == 0,
                InitialClusterState::New,
            );

            let server = XlineServer::new(
                config.cluster().clone(),
                config.storage().clone(),
                *config.compact(),
                config.auth().clone(),
            )
            .await
            .unwrap();

            let result = server.start_from_listener(listener).await;
            if let Err(e) = result {
                panic!("Server start error: {e}");
            }
        }
        // Sleep 30ms, wait for the server to start
        time::sleep(Duration::from_millis(300)).await;
    }

    pub async fn run_node(&mut self, listener: TcpListener) {
        let config = XlineServerConfig::default();
        self.run_node_with_config(listener, config).await;
    }

    pub async fn run_node_with_config(
        &mut self,
        listener: TcpListener,
        base_config: XlineServerConfig,
    ) {
        let idx = self.all_members.len();
        let name = format!("server{}", idx);
        let self_addr = listener.local_addr().unwrap().to_string();
        self.all_members.push(self_addr.clone());

        let members = self
            .all_members
            .clone()
            .into_iter()
            .enumerate()
            .map(|(id, addr)| (format!("server{id}"), vec![addr]))
            .collect::<HashMap<_, _>>();
        self.configs.push(base_config);

        let base_config = self.configs.last().unwrap();
        let config = Self::merge_config(
            base_config,
            name,
            members,
            false,
            InitialClusterState::Existing,
        );

        let server = XlineServer::new(
            config.cluster().clone(),
            config.storage().clone(),
            *config.compact(),
            config.auth().clone(),
        )
        .await
        .unwrap();
        let result = server.start_from_listener(listener).await;
        if let Err(e) = result {
            panic!("Server start error: {e}");
        }
    }

    /// Create or get the client with the specified index
    pub async fn client(&mut self) -> &mut Client {
        if self.client.is_none() {
            let client = Client::connect(self.all_members.clone(), ClientOptions::default())
                .await
                .unwrap_or_else(|e| {
                    panic!("Client connect error: {:?}", e);
                });
            self.client = Some(client);
        }
        self.client.as_mut().unwrap()
    }

    pub fn all_members_map(&self) -> HashMap<usize, String> {
        self.all_members.iter().cloned().enumerate().collect()
    }

    pub fn get_addr(&self, idx: usize) -> String {
        self.all_members[idx].clone()
    }

    pub fn addrs(&self) -> Vec<String> {
        self.all_members.clone()
    }

    // TODO
    // fn test_key_pair() -> Option<(EncodingKey, DecodingKey)> {
    //     let private_key = include_bytes!("../private.pem");
    //     let public_key = include_bytes!("../public.pem");
    //     let encoding_key = EncodingKey::from_rsa_pem(private_key).unwrap();
    //     let decoding_key = DecodingKey::from_rsa_pem(public_key).unwrap();
    //     Some((encoding_key, decoding_key))
    // }

    pub fn default_config_with_quota_and_rocks_path(
        path: PathBuf,
        quota: u64,
    ) -> XlineServerConfig {
        let cluster = ClusterConfig::default();
        let storage = StorageConfig::new(EngineConfig::RocksDB(path), quota);
        let log = LogConfig::default();
        let trace = TraceConfig::default();
        let auth = AuthConfig::default();
        let compact = CompactConfig::default();
        XlineServerConfig::new(cluster, storage, log, trace, auth, compact)
    }

    pub fn default_rocks_config_with_path(path: PathBuf) -> XlineServerConfig {
        Self::default_config_with_quota_and_rocks_path(path, default_quota())
    }

    pub fn default_rocks_config() -> XlineServerConfig {
        let path = temp_dir().join(random_id());
        Self::default_config_with_quota_and_rocks_path(path, default_quota())
    }

    pub fn default_quota_config(quota: u64) -> XlineServerConfig {
        let path = temp_dir().join(random_id());
        Self::default_config_with_quota_and_rocks_path(path, quota)
    }

    fn merge_config(
        base_config: &XlineServerConfig,
        name: String,
        members: HashMap<String, Vec<String>>,
        is_leader: bool,
        initial_cluster_state: InitialClusterState,
    ) -> XlineServerConfig {
        let old_cluster = base_config.cluster();
        let new_cluster = ClusterConfig::new(
            name,
            members,
            is_leader,
            old_cluster.curp_config().clone(),
            *old_cluster.client_config(),
            *old_cluster.server_timeout(),
            initial_cluster_state,
        );
        XlineServerConfig::new(
            new_cluster,
            base_config.storage().clone(),
            base_config.log().clone(),
            base_config.trace().clone(),
            base_config.auth().clone(),
            *base_config.compact(),
        )
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        block_in_place(move || {
            Handle::current().block_on(async move {
                for xline in self.servers.iter() {
                    xline.stop().await;
                }
                for cfg in &self.configs {
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
