use std::{collections::HashMap, env::temp_dir, iter, path::PathBuf, sync::Arc};

use futures::future::join_all;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{
    net::TcpListener,
    runtime::Handle,
    task::block_in_place,
    time::{self, Duration},
};
use tonic::transport::ClientTlsConfig;
use utils::config::{
    default_quota, AuthConfig, ClusterConfig, CompactConfig, EngineConfig, InitialClusterState,
    LogConfig, MetricsConfig, StorageConfig, TlsConfig, TraceConfig, XlineServerConfig,
};
use xline::server::XlineServer;
use xline_client::types::auth::{
    AuthRoleAddRequest, AuthRoleGrantPermissionRequest, AuthUserAddRequest,
    AuthUserGrantRoleRequest, Permission, PermissionType,
};
pub use xline_client::{types, Client, ClientOptions};

/// Cluster
pub struct Cluster {
    /// client and peer listeners of members
    listeners: Vec<(TcpListener, TcpListener)>,
    /// address of members
    all_members_peer_urls: Vec<String>,
    /// address of members
    all_members_client_urls: Vec<String>,
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
            listeners.push((
                TcpListener::bind("0.0.0.0:0").await.unwrap(),
                TcpListener::bind("0.0.0.0:0").await.unwrap(),
            ));
        }
        let server_tls_enabled = configs.iter().any(|c| c.tls().server_tls_enabled());
        let scheme = if server_tls_enabled { "https" } else { "http" };
        let all_members_client_urls = listeners
            .iter()
            .map(|l| format!("{scheme}://{}", l.0.local_addr().unwrap()))
            .collect();
        let all_members_peer_urls = listeners
            .iter()
            .map(|l| format!("{scheme}://{}", l.1.local_addr().unwrap()))
            .collect();
        Self {
            listeners,
            all_members_peer_urls,
            all_members_client_urls,
            configs,
            servers: Vec::new(),
            client: None,
        }
    }

    /// Start `Cluster`
    pub async fn start(&mut self) {
        let mut futs = Vec::new();
        for (i, config) in self.configs.iter().enumerate() {
            let name = format!("server{}", i);
            let (xline_listener, curp_listener) = self.listeners.remove(0);
            let self_client_url = self.get_client_url(i);
            let self_peer_url = self.get_peer_url(i);
            let config = Self::merge_config(
                config,
                name,
                self_client_url,
                self_peer_url,
                self.all_members_peer_urls
                    .clone()
                    .into_iter()
                    .enumerate()
                    .map(|(i, addr)| (format!("server{}", i), vec![addr]))
                    .collect(),
                i == 0,
                InitialClusterState::New,
            );

            let server = Arc::new(
                XlineServer::new(
                    config.cluster().clone(),
                    config.storage().clone(),
                    *config.compact(),
                    config.auth().clone(),
                    config.tls().clone(),
                )
                .await
                .unwrap(),
            );
            self.servers.push(Arc::clone(&server));

            futs.push(async move {
                let result = server
                    .start_from_listener(xline_listener, curp_listener)
                    .await;
                if let Err(e) = result {
                    panic!("Server start error: {e}");
                }
            });
        }
        join_all(futs).await;
        // Sleep 30ms, wait for the server to start
        time::sleep(Duration::from_millis(300)).await;
    }

    pub async fn run_node(&mut self, xline_listener: TcpListener, curp_listener: TcpListener) {
        let config = XlineServerConfig::default();
        self.run_node_with_config(xline_listener, curp_listener, config)
            .await;
    }

    pub async fn run_node_with_config(
        &mut self,
        xline_listener: TcpListener,
        curp_listener: TcpListener,
        base_config: XlineServerConfig,
    ) {
        let idx = self.all_members_peer_urls.len();
        let name = format!("server{}", idx);
        let server_tls_enabled = base_config.tls().server_tls_enabled();
        let scheme = if server_tls_enabled { "https" } else { "http" };
        let self_client_url = format!("{scheme}://{}", xline_listener.local_addr().unwrap());
        let self_peer_url = format!("{scheme}://{}", curp_listener.local_addr().unwrap());
        self.all_members_client_urls.push(self_client_url.clone());
        self.all_members_peer_urls.push(self_peer_url.clone());

        let peers = self
            .all_members_peer_urls
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
            self_client_url,
            self_peer_url,
            peers,
            false,
            InitialClusterState::Existing,
        );

        let server = XlineServer::new(
            config.cluster().clone(),
            config.storage().clone(),
            *config.compact(),
            config.auth().clone(),
            config.tls().clone(),
        )
        .await
        .unwrap();
        let result = server
            .start_from_listener(xline_listener, curp_listener)
            .await;
        if let Err(e) = result {
            panic!("Server start error: {e}");
        }
    }

    /// Create or get the client with the specified index
    pub async fn client(&mut self) -> &mut Client {
        if self.client.is_none() {
            let client = Client::connect(
                self.all_members_client_urls.clone(),
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

    /// Create or get the client with the specified index
    pub async fn client_with_tls_config(&mut self, client_tls_config: ClientTlsConfig) -> Client {
        let opts = ClientOptions::default().with_tls_config(client_tls_config);
        Client::connect(self.all_members_client_urls.clone(), opts)
            .await
            .unwrap_or_else(|e| {
                panic!("Client connect error: {:?}", e);
            })
    }

    pub fn all_members_client_urls_map(&self) -> HashMap<usize, String> {
        self.all_members_client_urls
            .iter()
            .cloned()
            .enumerate()
            .collect()
    }

    pub fn get_client_url(&self, idx: usize) -> String {
        self.all_members_client_urls[idx].clone()
    }

    pub fn get_peer_url(&self, idx: usize) -> String {
        self.all_members_peer_urls[idx].clone()
    }

    pub fn all_client_addrs(&self) -> Vec<String> {
        self.all_members_client_urls.clone()
    }

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
        let tls = TlsConfig::default();
        let metrics = MetricsConfig::default();
        XlineServerConfig::new(cluster, storage, log, trace, auth, compact, tls, metrics)
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
        client_url: String,
        peer_url: String,
        peers: HashMap<String, Vec<String>>,
        is_leader: bool,
        initial_cluster_state: InitialClusterState,
    ) -> XlineServerConfig {
        let old_cluster = base_config.cluster();
        let new_cluster = ClusterConfig::new(
            name,
            vec![peer_url.clone()],
            vec![peer_url],
            vec![client_url.clone()],
            vec![client_url],
            peers,
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
            base_config.tls().clone(),
            base_config.metrics().clone(),
        )
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        block_in_place(move || {
            Handle::current().block_on(async move {
                let mut handles = Vec::new();
                for xline in self.servers.drain(..) {
                    handles.push(tokio::spawn(async move {
                        xline.stop().await;
                    }));
                }
                for h in handles {
                    h.await.unwrap();
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

pub async fn set_user(
    client: &Client,
    name: &str,
    password: &str,
    role: &str,
    key: &[u8],
    range_end: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let client = client.auth_client();
    client
        .user_add(AuthUserAddRequest::new(name).with_pwd(password))
        .await?;
    client.role_add(AuthRoleAddRequest::new(role)).await?;
    client
        .user_grant_role(AuthUserGrantRoleRequest::new(name, role))
        .await?;
    if !key.is_empty() {
        client
            .role_grant_permission(AuthRoleGrantPermissionRequest::new(
                role,
                Permission::new(PermissionType::Readwrite, key).with_range_end(range_end),
            ))
            .await?;
    }
    Ok(())
}

pub async fn enable_auth(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    set_user(client, "root", "123", "root", &[], &[]).await?;
    client.auth_client().auth_enable().await?;
    Ok(())
}
