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
use curp::{
    client::Client, cmd::CommandExecutor as CurpCommandExecutor, server::Rpc, ProtocolServer,
    ServerId,
};
use event_listener::Event;
use jsonwebtoken::{DecodingKey, EncodingKey};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use utils::config::{ClientTimeout, CurpConfig, ServerTimeout, StorageConfig};

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
    revision_number::RevisionNumberGenerator,
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
type CurpServer<S> = Rpc<Command, State<S>>;

/// Xline server
#[derive(Debug)]
pub struct XlineServer {
    /// Server id
    id: String,
    /// is leader
    is_leader: bool,
    /// all Members
    all_members: Arc<HashMap<ServerId, String>>,
    /// Curp server timeout
    curp_cfg: Arc<CurpConfig>,
    /// Client timeout
    client_timeout: ClientTimeout,
    /// Storage config,
    storage_cfg: StorageConfig,
    /// Server timeout
    server_timeout: ServerTimeout,
    /// Shutdown trigger
    shutdown_trigger: Arc<Event>,
}

impl XlineServer {
    /// New `XlineServer`
    ///
    /// # Panics
    ///
    /// panic when peers do not contain leader address
    #[inline]
    #[must_use]
    pub fn new(
        name: String,
        all_members: Arc<HashMap<ServerId, String>>,
        is_leader: bool,
        curp_config: CurpConfig,
        client_timeout: ClientTimeout,
        server_timeout: ServerTimeout,
        storage_config: StorageConfig,
    ) -> Self {
        Self {
            id: name,
            is_leader,
            all_members,
            curp_cfg: Arc::new(curp_config),
            client_timeout,
            storage_cfg: storage_config,
            server_timeout,
            shutdown_trigger: Arc::new(event_listener::Event::new())
        }
    }

    /// Construct a `LeaseCollection`
    #[inline]
    fn construct_lease_collection(
        heartbeat_interval: Duration,
        candidate_timeout_ticks: u8,
    ) -> Arc<LeaseCollection> {
        let min_ttl = 3 * heartbeat_interval * candidate_timeout_ticks.cast() / 2;
        // Safe ceiling
        let min_ttl_secs = min_ttl
            .as_secs()
            .overflow_add((min_ttl.subsec_nanos() > 0).cast());
        Arc::new(LeaseCollection::new(min_ttl_secs.cast()))
    }

    /// Construct underlying storages, including `KvStore`, `LeaseStore`, `AuthStore`
    #[allow(clippy::type_complexity)] // it is easy to read
    #[inline]
    fn construct_underlying_storages<S: StorageApi>(
        &self,
        persistent: &Arc<S>,
        lease_collection: Arc<LeaseCollection>,
        header_gen: &Arc<HeaderGenerator>,
        auth_revision_gen: &Arc<RevisionNumberGenerator>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(
        Arc<KvStore<S>>,
        Arc<LeaseStore<S>>,
        Arc<AuthStore<S>>,
        Arc<KvWatcher<S>>,
    )> {
        let index = Arc::new(Index::new());
        let (kv_update_tx, kv_update_rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
        let kv_storage = Arc::new(KvStore::new(
            kv_update_tx.clone(),
            Arc::clone(&lease_collection),
            Arc::clone(header_gen),
            Arc::clone(persistent),
            Arc::clone(&index),
        ));
        let lease_storage = Arc::new(LeaseStore::new(
            Arc::clone(&lease_collection),
            Arc::clone(header_gen),
            Arc::clone(persistent),
            index,
            kv_update_tx,
            self.is_leader,
        ));
        let auth_storage = Arc::new(AuthStore::new(
            lease_collection,
            key_pair,
            Arc::clone(header_gen),
            Arc::clone(persistent),
            Arc::clone(auth_revision_gen),
        ));
        let watcher = KvWatcher::new_arc(Arc::clone(&kv_storage), kv_update_rx, Arc::clone(&self.shutdown_trigger), *self.server_timeout.sync_victims_interval());
        // lease storage must recover before kv storage
        lease_storage.recover()?;
        kv_storage.recover()?;
        auth_storage.recover()?;
        Ok((kv_storage, lease_storage, auth_storage, watcher))
    }

    /// Construct a header generator
    #[inline]
    fn construct_generator(
        name: &str,
        all_members: &HashMap<ServerId, String>,
    ) -> (
        Arc<HeaderGenerator>,
        Arc<IdGenerator>,
        Arc<RevisionNumberGenerator>,
    ) {
        let url = all_members
            .get(name)
            .unwrap_or_else(|| panic!("peer {} not found in peers {:?}", name, all_members.keys()));

        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {e}"))
            .as_secs();
        let member_id = Self::calc_member_id(url, "", ts);
        let peer_urls = all_members.values().map(String::as_str).collect::<Vec<_>>();
        let cluster_id = Self::calc_cluster_id(&peer_urls, "");
        (
            Arc::new(HeaderGenerator::new(cluster_id, member_id)),
            Arc::new(IdGenerator::new(member_id)),
            Arc::new(RevisionNumberGenerator::default()),
        )
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

    /// Start `XlineServer`
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    pub async fn start<S: StorageApi>(
        &self,
        addr: SocketAddr,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<()> {
        let (
            kv_server,
            lock_server,
            lease_server,
            auth_server,
            watch_server,
            maintenance_server,
            curp_server,
        ) = self
            .init_servers(addr.to_string(), persistent, key_pair)
            .await?;
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
    pub async fn start_from_listener_shutdown<F, S: StorageApi>(
        &self,
        xline_listener: TcpListener,
        signal: F,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let local_addr = xline_listener.local_addr()?;
        let (
            kv_server,
            lock_server,
            lease_server,
            auth_server,
            watch_server,
            maintenance_server,
            curp_server,
        ) = self
            .init_servers(local_addr.to_string(), persistent, key_pair)
            .await?;
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

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and `CurpServer`
    /// for the Xline Server.
    #[allow(clippy::type_complexity)] // it is easy to read
    async fn init_servers<S: StorageApi>(
        &self,
        address: String,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(
        KvServer<S>,
        LockServer,
        Arc<LeaseServer<S>>,
        AuthServer<S>,
        WatchServer<S>,
        MaintenanceServer<S>,
        CurpServer<S>,
    )> {
        let (header_gen, id_gen, auth_revision_gen) =
            Self::construct_generator(self.id.as_str(), &self.all_members);
        let lease_collection = Self::construct_lease_collection(
            self.curp_cfg.heartbeat_interval,
            self.curp_cfg.candidate_timeout_ticks,
        );

        let (kv_storage, lease_storage, auth_storage, watcher) =
            self.construct_underlying_storages(
                &persistent,
                lease_collection,
                &header_gen,
                &auth_revision_gen,
                key_pair,
            )?;
        let client = Arc::new(
            Client::<Command>::new(Arc::clone(&self.all_members), self.client_timeout).await,
        );
        let index_barrier = Arc::new(IndexBarrier::new());
        let id_barrier = Arc::new(IdBarrier::new());

        let state = State::new(Arc::clone(&lease_storage));
        let ce = CommandExecutor::new(
            Arc::clone(&kv_storage),
            Arc::clone(&auth_storage),
            Arc::clone(&lease_storage),
            Arc::clone(&persistent),
            Arc::clone(&index_barrier),
            Arc::clone(&id_barrier),
            header_gen.revision_arc(),
            Arc::clone(&auth_revision_gen),
        );

        Ok((
            KvServer::new(
                Arc::clone(&kv_storage),
                Arc::clone(&auth_storage),
                Arc::clone(&index_barrier),
                Arc::clone(&id_barrier),
                *self.server_timeout.range_retry_timeout(),
                Arc::clone(&client),
                self.id.clone(),
            ),
            LockServer::new(
                Arc::clone(&client),
                Arc::clone(&id_gen),
                self.id.clone(),
                address,
            ),
            LeaseServer::new(
                Arc::clone(&lease_storage),
                Arc::clone(&auth_storage),
                Arc::clone(&client),
                self.id.clone(),
                Arc::clone(&id_gen),
                Arc::clone(&self.all_members),
            ),
            AuthServer::new(
                Arc::clone(&auth_storage),
                Arc::clone(&client),
                self.id.clone(),
            ),
            WatchServer::new(Arc::clone(&watcher), Arc::clone(&header_gen)),
            MaintenanceServer::new(Arc::clone(&persistent), Arc::clone(&header_gen)),
            Self::construct_curp_server(
                self.id.clone(),
                self.is_leader,
                &self.all_members,
                ce,
                state,
                &self.storage_cfg,
                Arc::clone(&self.curp_cfg),
            )
            .await,
        ))
    }

    /// Construct a new `CurpServer`
    async fn construct_curp_server<S: StorageApi, CE: CurpCommandExecutor<Command> + 'static>(
        id: String,
        is_leader: bool,
        all_members: &HashMap<ServerId, String>,
        ce: CE,
        state: State<S>,
        storage_cfg: &StorageConfig,
        curp_cfg: Arc<CurpConfig>,
    ) -> CurpServer<S> {
        let others = Arc::new(
            all_members
                .iter()
                .filter(|&(key, _value)| id.as_str() != key.as_str())
                .map(|(server_id, addr)| (server_id.clone(), addr.clone()))
                .collect(),
        );

        match *storage_cfg {
            StorageConfig::Memory => {
                CurpServer::new(
                    id.clone(),
                    is_leader,
                    others,
                    ce,
                    MemorySnapshotAllocator,
                    state,
                    Arc::clone(&curp_cfg),
                    None,
                )
                .await
            }
            StorageConfig::RocksDB(_) => {
                CurpServer::new(
                    id.clone(),
                    is_leader,
                    others,
                    ce,
                    RocksSnapshotAllocator,
                    state,
                    Arc::clone(&curp_cfg),
                    None,
                )
                .await
            }
            #[allow(clippy::unimplemented)]
            _ => unimplemented!(),
        }
    }
}

impl Drop for XlineServer{
    #[inline]
    fn drop(&mut self) {
        self.shutdown_trigger.notify(usize::MAX);
    }
}
