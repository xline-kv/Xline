use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    future::Future,
    hash::Hasher,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{client::Client, server::Rpc, ProtocolServer};
use jsonwebtoken::{DecodingKey, EncodingKey};
use tokio::{net::TcpListener, sync::broadcast};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;
use utils::config::{ClientTimeout, CurpConfig, StorageConfig};

use super::{
    auth_server::AuthServer,
    barriers::{IdBarrier, IndexBarrier},
    command::{Command, CommandExecutor},
    kv_server::KvServer,
    lease_server::LeaseServer,
    lock_server::LockServer,
    maintenance::MaintenanceServer,
    watch_server::{WatchServer, CHANNEL_SIZE},
};
use crate::{
    header_gen::HeaderGenerator,
    id_gen::IdGenerator,
    rpc::{
        AuthServer as RpcAuthServer, KvServer as RpcKvServer, LeaseServer as RpcLeaseServer,
        LockServer as RpcLockServer, MaintenanceServer as RpcMaintenanceServer,
        WatchServer as RpcWatchServer,
    },
    state::State,
    storage::{
        index::Index,
        kvwatcher::KvWatcher,
        lease_store::LeaseCollection,
        snapshot_allocator::{MemorySnapshotAllocator, RocksSnapshotAllocator},
        storage_api::StorageApi,
        AuthStore, KvStore, LeaseStore,
    },
};

/// Rpc Server of curp protocol
type CurpServer = Rpc<Command>;

/// Xline server
#[derive(Debug)]
pub struct XlineServer<S>
where
    S: StorageApi,
{
    /// State of current node
    state: Arc<State>,
    /// Kv storage
    kv_storage: Arc<KvStore<S>>,
    /// Auth storage
    auth_storage: Arc<AuthStore<S>>,
    /// Lease storage
    lease_storage: Arc<LeaseStore<S>>,
    /// Watcher
    watcher: Arc<KvWatcher<S>>,
    /// persistent storage
    persistent: Arc<S>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Curp server timeout
    curp_cfg: Arc<CurpConfig>,
    /// Storage config,
    storage_cfg: StorageConfig,
    /// Id generator
    id_gen: Arc<IdGenerator>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Barrier for applied index
    index_barrier: Arc<IndexBarrier>,
    /// Barrier for propose id
    id_barrier: Arc<IdBarrier>,
    /// Range request retry timeout
    range_retry_timeout: Duration,
}

impl<S> XlineServer<S>
where
    S: StorageApi,
{
    /// New `XlineServer`
    ///
    /// # Errors
    ///
    /// Return `ExecuteError::DbError` if the server cannot initialize the database
    ///
    /// # Panics
    ///
    /// panic when peers do not contain leader address
    #[inline]
    #[allow(clippy::too_many_arguments)] // TODO: refactor this use builder pattern, or just pass a reference of config
    pub async fn new(
        name: String,
        all_members: HashMap<String, String>,
        is_leader: bool,
        key_pair: Option<(EncodingKey, DecodingKey)>,
        curp_config: CurpConfig,
        client_timeout: ClientTimeout,
        range_retry_timeout: Duration,
        storage_config: StorageConfig,
        persistent: Arc<S>,
    ) -> Self {
        let url = all_members
            .get(&name)
            .unwrap_or_else(|| panic!("peer {} not found in peers {:?}", name, all_members.keys()));
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {e}"))
            .as_secs();
        let member_id = Self::calc_member_id(url, "", ts);
        let peer_urls = all_members.values().map(String::as_str).collect::<Vec<_>>();
        let cluster_id = Self::calc_cluster_id(&peer_urls, "");
        let header_gen = Arc::new(HeaderGenerator::new(cluster_id, member_id));
        let id_gen = Arc::new(IdGenerator::new(member_id));
        let leader_id = is_leader.then(|| name.clone());
        let state = Arc::new(State::new(name, leader_id, all_members.clone()));
        let curp_config = Arc::new(curp_config);
        // The ttl of a lease should larger than the 3/2 of a election timeout
        let min_ttl =
            3 * curp_config.heartbeat_interval * curp_config.candidate_timeout_ticks.cast() / 2;
        // Safe ceiling
        let min_ttl_secs = min_ttl
            .as_secs()
            .overflow_add((min_ttl.subsec_nanos() > 0).cast());
        let lease_collection = Arc::new(LeaseCollection::new(min_ttl_secs.cast()));
        let index = Arc::new(Index::new());
        let (kv_update_tx, kv_update_rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
        let kv_storage = Arc::new(KvStore::new(
            kv_update_tx.clone(),
            Arc::clone(&lease_collection),
            Arc::clone(&header_gen),
            Arc::clone(&persistent),
            Arc::clone(&index),
        ));
        let lease_storage = Arc::new(LeaseStore::new(
            Arc::clone(&lease_collection),
            Arc::clone(&state),
            Arc::clone(&header_gen),
            Arc::clone(&persistent),
            index,
            kv_update_tx,
        ));
        let auth_storage = Arc::new(AuthStore::new(
            lease_collection,
            key_pair,
            Arc::clone(&header_gen),
            Arc::clone(&persistent),
        ));
        let watcher = KvWatcher::new_arc(Arc::clone(&kv_storage), kv_update_rx);
        let client = Arc::new(Client::<Command>::new(all_members.clone(), client_timeout).await);
        let index_barrier = Arc::new(IndexBarrier::new());
        let id_barrier = Arc::new(IdBarrier::new());
        Self {
            state,
            kv_storage,
            auth_storage,
            lease_storage,
            watcher,
            persistent,
            client,
            curp_cfg: curp_config,
            storage_cfg: storage_config,
            id_gen,
            header_gen,
            index_barrier,
            id_barrier,
            range_retry_timeout,
        }
    }

    /// calculate member id
    fn calc_member_id(peer_url: &str, cluster_name: &str, now: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(peer_url.as_bytes());
        hasher.write(cluster_name.as_bytes());
        hasher.write_u64(now);
        hasher.finish()
    }

    /// calculate cluster id
    fn calc_cluster_id(member_urls: &[&str], cluster_name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        for id in member_urls {
            hasher.write(id.as_bytes());
        }
        hasher.write(cluster_name.as_bytes());
        hasher.finish()
    }

    /// Server id
    fn id(&self) -> String {
        self.state.id().to_owned()
    }

    /// Check if current node is leader
    fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    /// Start `XlineServer`
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    pub async fn start(&self, addr: SocketAddr) -> Result<()> {
        // lease storage must recover before kv storage
        self.lease_storage.recover()?;
        self.kv_storage.recover()?;
        self.auth_storage.recover()?;
        let (
            kv_server,
            lock_server,
            lease_server,
            auth_server,
            watch_server,
            maintenance_server,
            curp_server,
        ) = self.init_servers().await;
        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::from_arc(lease_server))
            .add_service(RpcAuthServer::new(auth_server))
            .add_service(RpcWatchServer::new(watch_server))
            .add_service(RpcMaintenanceServer::new(maintenance_server))
            .add_service(ProtocolServer::new(curp_server))
            .serve(addr)
            .await?)
    }

    /// Start `XlineServer` from listeners
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    pub async fn start_from_listener_shutdown<F>(
        &self,
        xline_listener: TcpListener,
        signal: F,
    ) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        // lease storage must recover before kv storage
        self.lease_storage.recover()?;
        self.kv_storage.recover()?;
        self.auth_storage.recover()?;
        let (
            kv_server,
            lock_server,
            lease_server,
            auth_server,
            watch_server,
            maintenance_server,
            curp_server,
        ) = self.init_servers().await;
        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::from_arc(lease_server))
            .add_service(RpcAuthServer::new(auth_server))
            .add_service(RpcWatchServer::new(watch_server))
            .add_service(RpcMaintenanceServer::new(maintenance_server))
            .add_service(ProtocolServer::new(curp_server))
            .serve_with_incoming_shutdown(TcpListenerStream::new(xline_listener), signal)
            .await?)
    }

    /// Leader change task
    async fn leader_change_task(
        mut rx: broadcast::Receiver<Option<String>>,
        state: Arc<State>,
        lease_storage: Arc<LeaseStore<S>>,
    ) {
        while let Ok(leader_id) = rx.recv().await {
            info!("receive new leader_id: {leader_id:?}");
            let leader_state_changed = state.set_leader_id(leader_id);
            let is_leader = state.is_leader();
            if leader_state_changed {
                if is_leader {
                    lease_storage.promote(Duration::from_secs(1)); // TODO: extend should be election timeout
                } else {
                    lease_storage.demote();
                }
            }
        }
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and `CurpServer`
    /// for the Xline Server.
    #[allow(clippy::type_complexity)] // it is easy to read
    async fn init_servers(
        &self,
    ) -> (
        KvServer<S>,
        LockServer,
        Arc<LeaseServer<S>>,
        AuthServer<S>,
        WatchServer<S>,
        MaintenanceServer<S>,
        CurpServer,
    ) {
        let curp_server = match self.storage_cfg {
            StorageConfig::Memory => {
                CurpServer::new(
                    self.id(),
                    self.is_leader(),
                    self.state.others(),
                    CommandExecutor::new(
                        Arc::clone(&self.kv_storage),
                        Arc::clone(&self.auth_storage),
                        Arc::clone(&self.lease_storage),
                        Arc::clone(&self.persistent),
                        Arc::clone(&self.index_barrier),
                        Arc::clone(&self.id_barrier),
                    ),
                    MemorySnapshotAllocator,
                    Arc::clone(&self.curp_cfg),
                    None,
                )
                .await
            }
            StorageConfig::RocksDB(_) => {
                CurpServer::new(
                    self.id(),
                    self.is_leader(),
                    self.state.others(),
                    CommandExecutor::new(
                        Arc::clone(&self.kv_storage),
                        Arc::clone(&self.auth_storage),
                        Arc::clone(&self.lease_storage),
                        Arc::clone(&self.persistent),
                        Arc::clone(&self.index_barrier),
                        Arc::clone(&self.id_barrier),
                    ),
                    RocksSnapshotAllocator,
                    Arc::clone(&self.curp_cfg),
                    None,
                )
                .await
            }
            #[allow(clippy::unimplemented)]
            _ => unimplemented!(),
        };
        let _handle = tokio::spawn({
            let state = Arc::clone(&self.state);
            let lease_storage = Arc::clone(&self.lease_storage);
            let rx = curp_server.leader_rx();
            Self::leader_change_task(rx, state, lease_storage)
        });
        (
            KvServer::new(
                Arc::clone(&self.kv_storage),
                Arc::clone(&self.auth_storage),
                Arc::clone(&self.index_barrier),
                Arc::clone(&self.id_barrier),
                self.range_retry_timeout,
                Arc::clone(&self.client),
                self.id(),
            ),
            LockServer::new(
                Arc::clone(&self.client),
                Arc::clone(&self.state),
                Arc::clone(&self.id_gen),
                self.id(),
            ),
            LeaseServer::new(
                Arc::clone(&self.lease_storage),
                Arc::clone(&self.auth_storage),
                Arc::clone(&self.client),
                self.id(),
                Arc::clone(&self.state),
                Arc::clone(&self.id_gen),
            ),
            AuthServer::new(
                Arc::clone(&self.auth_storage),
                Arc::clone(&self.client),
                self.id(),
            ),
            WatchServer::new(Arc::clone(&self.watcher), Arc::clone(&self.header_gen)),
            MaintenanceServer::new(Arc::clone(&self.persistent), Arc::clone(&self.header_gen)),
            curp_server,
        )
    }
}
