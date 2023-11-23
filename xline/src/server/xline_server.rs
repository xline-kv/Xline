use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{
    client::Client,
    error::ClientError,
    members::ClusterInfo,
    rpc::{InnerProtocolServer, ProtocolServer},
    server::Rpc,
};
use dashmap::DashMap;
use engine::{MemorySnapshotAllocator, RocksSnapshotAllocator, SnapshotAllocator};
use jsonwebtoken::{DecodingKey, EncodingKey};
use tokio::{sync::mpsc::channel, task::JoinHandle};
use tonic::transport::{server::Router, Server};
use tracing::{error, warn};
use utils::{
    config::{ClientConfig, CompactConfig, CurpConfig, EngineConfig, ServerTimeout, StorageConfig},
    shutdown,
};
use xlineapi::command::Command;

use super::{
    auth_server::AuthServer,
    barriers::{IdBarrier, IndexBarrier},
    cluster_server::ClusterServer,
    command::CommandExecutor,
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
        AuthServer as RpcAuthServer, ClusterServer as RpcClusterServer, KvServer as RpcKvServer,
        LeaseServer as RpcLeaseServer, LockServer as RpcLockServer,
        MaintenanceServer as RpcMaintenanceServer, WatchServer as RpcWatchServer,
    },
    state::State,
    storage::{
        compact::{auto_compactor, compact_bg_task, COMPACT_CHANNEL_SIZE},
        index::Index,
        kv_store::KvStoreInner,
        kvwatcher::KvWatcher,
        lease_store::LeaseCollection,
        storage_api::StorageApi,
        AuthStore, KvStore, LeaseStore,
    },
};

/// Rpc Server of curp protocol
type CurpServer<S> = Rpc<Command, State<S>>;

/// Rpc Client of curp protocol
type CurpClient = Client<Command>;

/// Xline server
#[derive(Debug)]
pub struct XlineServer {
    /// cluster information
    cluster_info: Arc<ClusterInfo>,
    /// is leader
    is_leader: bool,
    /// Curp server timeout
    curp_cfg: Arc<CurpConfig>,
    /// Client config
    client_config: ClientConfig,
    /// Storage config,
    storage_cfg: StorageConfig,
    /// Compact config
    compact_cfg: CompactConfig,
    /// Server timeout
    server_timeout: ServerTimeout,
    /// Shutdown trigger
    shutdown_trigger: shutdown::Trigger,
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
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        curp_config: CurpConfig,
        client_config: ClientConfig,
        server_timeout: ServerTimeout,
        storage_config: StorageConfig,
        compact_config: CompactConfig,
    ) -> Self {
        let (shutdown_trigger, _) = shutdown::channel();
        Self {
            cluster_info,
            is_leader,
            curp_cfg: Arc::new(curp_config),
            client_config,
            storage_cfg: storage_config,
            compact_cfg: compact_config,
            server_timeout,
            shutdown_trigger,
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
    async fn construct_underlying_storages<S: StorageApi>(
        &self,
        persistent: Arc<S>,
        lease_collection: Arc<LeaseCollection>,
        header_gen: Arc<HeaderGenerator>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(
        Arc<KvStore<S>>,
        Arc<LeaseStore<S>>,
        Arc<AuthStore<S>>,
        Arc<KvWatcher<S>>,
    )> {
        let (compact_task_tx, compact_task_rx) = channel(COMPACT_CHANNEL_SIZE);
        let index = Arc::new(Index::new());
        let (kv_update_tx, kv_update_rx) = channel(CHANNEL_SIZE);
        let kv_store_inner = Arc::new(KvStoreInner::new(
            Arc::clone(&index),
            Arc::clone(&persistent),
        ));
        let kv_storage = Arc::new(KvStore::new(
            Arc::clone(&kv_store_inner),
            Arc::clone(&header_gen),
            kv_update_tx.clone(),
            compact_task_tx,
            Arc::clone(&lease_collection),
        ));
        let _hd = tokio::spawn(compact_bg_task(
            Arc::clone(&kv_storage),
            Arc::clone(&index),
            *self.compact_cfg.compact_batch_size(),
            *self.compact_cfg.compact_sleep_interval(),
            compact_task_rx,
            self.shutdown_trigger.subscribe(),
        ));
        let lease_storage = Arc::new(LeaseStore::new(
            Arc::clone(&lease_collection),
            Arc::clone(&header_gen),
            Arc::clone(&persistent),
            index,
            kv_update_tx,
            self.is_leader,
        ));
        let auth_storage = Arc::new(AuthStore::new(
            lease_collection,
            key_pair,
            header_gen,
            persistent,
        ));
        let watcher = KvWatcher::new_arc(
            kv_store_inner,
            kv_update_rx,
            *self.server_timeout.sync_victims_interval(),
            self.shutdown_trigger.subscribe(),
        );
        // lease storage must recover before kv storage
        lease_storage.recover()?;
        kv_storage.recover().await?;
        auth_storage.recover()?;
        Ok((kv_storage, lease_storage, auth_storage, watcher))
    }

    /// Construct a header generator
    #[inline]
    fn construct_generator(cluster_info: &ClusterInfo) -> (Arc<HeaderGenerator>, Arc<IdGenerator>) {
        let member_id = cluster_info.self_id();
        let cluster_id = cluster_info.cluster_id();
        (
            Arc::new(HeaderGenerator::new(cluster_id, member_id)),
            Arc::new(IdGenerator::new(member_id)),
        )
    }

    /// Init router
    ///
    /// # Errors
    ///
    /// Will return `Err` when `init_servers` return an error
    #[inline]
    pub async fn init_router<S: StorageApi>(
        &self,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(Router, Arc<CurpClient>)> {
        let (
            kv_server,
            lock_server,
            lease_server,
            auth_server,
            watch_server,
            maintenance_server,
            cluster_server,
            curp_server,
            curp_client,
        ) = self.init_servers(persistent, key_pair).await?;
        let router = Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::from_arc(lease_server))
            .add_service(RpcAuthServer::new(auth_server))
            .add_service(RpcWatchServer::new(watch_server))
            .add_service(RpcMaintenanceServer::new(maintenance_server))
            .add_service(RpcClusterServer::new(cluster_server))
            .add_service(ProtocolServer::from_arc(Arc::clone(&curp_server)))
            .add_service(InnerProtocolServer::from_arc(curp_server));
        #[cfg(not(madsim))]
        let router = {
            let (mut reporter, health_server) = tonic_health::server::health_reporter();
            reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
            router.add_service(health_server)
        };
        Ok((router, curp_client))
    }

    /// Start `XlineServer`
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    #[cfg(madsim)]
    pub async fn start_from_single_addr<S: StorageApi>(
        &self,
        addr: SocketAddr,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<JoinHandle<Result<(), tonic::transport::Error>>> {
        let mut shutdown_listener = self.shutdown_trigger.subscribe();
        let signal = async move {
            shutdown_listener.wait_self_shutdown().await;
        };
        let (router, curp_client) = self.init_router(persistent, key_pair).await?;
        let handle = tokio::spawn(async move { router.serve_with_shutdown(addr, signal).await });
        if let Err(e) = self.publish(curp_client).await {
            warn!("publish name to cluster failed: {:?}", e);
        };
        Ok(handle)
    }

    /// Start `XlineServer`
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    #[cfg(not(madsim))]
    pub async fn start<S: StorageApi>(
        &self,
        addrs: Vec<SocketAddr>,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<JoinHandle<Result<(), tonic::transport::Error>>> {
        let mut shutdown_listener = self.shutdown_trigger.subscribe();
        let signal = async move {
            let _r = shutdown_listener.wait_self_shutdown().await;
        };
        let (router, curp_client) = self.init_router(persistent, key_pair).await?;
        let incoming = bind_addrs(addrs.into_iter())?;
        let handle =
            tokio::spawn(
                async move { router.serve_with_incoming_shutdown(incoming, signal).await },
            );
        if let Err(e) = self.publish(curp_client).await {
            warn!("publish name to cluster failed: {:?}", e);
        };
        Ok(handle)
    }

    /// Start `XlineServer` from listeners
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    #[cfg(not(madsim))]
    pub async fn start_from_listener<S: StorageApi>(
        &self,
        xline_listener: tokio::net::TcpListener,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<()> {
        let mut shutdown_listener = self.shutdown_trigger.subscribe();
        let signal = async move {
            shutdown_listener.wait_self_shutdown().await;
        };
        let (router, curp_client) = self.init_router(persistent, key_pair).await?;
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(xline_listener);
        let _h =
            tokio::spawn(
                async move { router.serve_with_incoming_shutdown(incoming, signal).await },
            );
        if let Err(e) = self.publish(curp_client).await {
            warn!("publish name to cluster failed: {:?}", e);
        };
        Ok(())
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and `CurpServer`
    /// for the Xline Server.
    #[allow(clippy::type_complexity, clippy::too_many_lines)] // it is easy to read
    async fn init_servers<S: StorageApi>(
        &self,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(
        KvServer<S>,
        LockServer,
        Arc<LeaseServer<S>>,
        AuthServer,
        WatchServer<S>,
        MaintenanceServer<S>,
        ClusterServer,
        Arc<CurpServer<S>>,
        Arc<CurpClient>,
    )> {
        let (header_gen, id_gen) = Self::construct_generator(&self.cluster_info);
        let lease_collection = Self::construct_lease_collection(
            self.curp_cfg.heartbeat_interval,
            self.curp_cfg.candidate_timeout_ticks,
        );

        let (kv_storage, lease_storage, auth_storage, watcher) = self
            .construct_underlying_storages(
                Arc::clone(&persistent),
                lease_collection,
                Arc::clone(&header_gen),
                key_pair,
            )
            .await?;

        let index_barrier = Arc::new(IndexBarrier::new());
        let id_barrier = Arc::new(IdBarrier::new());
        let compact_events = Arc::new(DashMap::new());
        let ce = CommandExecutor::new(
            Arc::clone(&kv_storage),
            Arc::clone(&auth_storage),
            Arc::clone(&lease_storage),
            Arc::clone(&persistent),
            Arc::clone(&index_barrier),
            Arc::clone(&id_barrier),
            header_gen.general_revision_arc(),
            header_gen.auth_revision_arc(),
            Arc::clone(&compact_events),
            self.storage_cfg.quota,
        );
        let snapshot_allocator: Box<dyn SnapshotAllocator> = match self.storage_cfg.engine {
            EngineConfig::Memory => Box::<MemorySnapshotAllocator>::default(),
            EngineConfig::RocksDB(_) => Box::<RocksSnapshotAllocator>::default(),
            #[allow(clippy::unimplemented)]
            _ => unimplemented!(),
        };

        let client = Arc::new(
            CurpClient::builder()
                .local_server_id(self.cluster_info.self_id())
                .config(self.client_config)
                .build_from_all_members(self.cluster_info.all_members_addrs(), None)
                .await?,
        );

        let auto_compactor = if let Some(auto_config_cfg) = *self.compact_cfg.auto_compact_config()
        {
            Some(
                auto_compactor(
                    self.is_leader,
                    Arc::clone(&client),
                    header_gen.general_revision_arc(),
                    self.shutdown_trigger.subscribe(),
                    auto_config_cfg,
                )
                .await,
            )
        } else {
            None
        };

        let state = State::new(Arc::clone(&lease_storage), auto_compactor);
        let curp_server = CurpServer::new(
            Arc::clone(&self.cluster_info),
            self.is_leader,
            ce,
            snapshot_allocator,
            state,
            Arc::clone(&self.curp_cfg),
            self.shutdown_trigger.clone(),
        )
        .await;

        Ok((
            KvServer::new(
                Arc::clone(&kv_storage),
                Arc::clone(&auth_storage),
                index_barrier,
                id_barrier,
                *self.server_timeout.range_retry_timeout(),
                *self.server_timeout.compact_timeout(),
                Arc::clone(&client),
                compact_events,
            ),
            LockServer::new(
                Arc::clone(&client),
                Arc::clone(&id_gen),
                self.cluster_info.self_addrs(),
            ),
            LeaseServer::new(
                lease_storage,
                auth_storage,
                Arc::clone(&client),
                id_gen,
                Arc::clone(&self.cluster_info),
                self.shutdown_trigger.subscribe(),
            ),
            AuthServer::new(Arc::clone(&client)),
            WatchServer::new(
                watcher,
                Arc::clone(&header_gen),
                *self.server_timeout.watch_progress_notify_interval(),
                self.shutdown_trigger.subscribe(),
            ),
            MaintenanceServer::new(
                kv_storage,
                Arc::clone(&client),
                persistent,
                Arc::clone(&header_gen),
                Arc::clone(&self.cluster_info),
            ),
            ClusterServer::new(Arc::clone(&client), header_gen),
            Arc::new(curp_server),
            client,
        ))
    }

    /// Publish the name of current node to cluster
    async fn publish(&self, curp_client: Arc<CurpClient>) -> Result<(), ClientError<Command>> {
        curp_client
            .publish(self.cluster_info.self_id(), self.cluster_info.self_name())
            .await
    }

    /// Check if `XlineServer` is stopped
    #[inline]
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.shutdown_trigger.is_closed()
    }

    /// Stop `XlineServer`
    #[inline]
    pub async fn stop(&self) {
        self.shutdown_trigger.self_shutdown_and_wait().await;
    }
}

impl Drop for XlineServer {
    #[inline]
    fn drop(&mut self) {
        if !self.is_stopped() {
            let task_number = self.shutdown_trigger.receiver_count();
            error!(
                "Xline server is not stopped, there are {} tasks not stopped",
                task_number
            );
        }
    }
}

/// Bind multiple addresses
#[cfg(not(madsim))]
fn bind_addrs<T: Iterator<Item = SocketAddr>>(
    addrs: T,
) -> Result<impl futures::Stream<Item = Result<hyper::server::conn::AddrStream, std::io::Error>>> {
    let incoming = addrs
        .map(|addr| {
            tonic::transport::server::TcpIncoming::new(addr, true, None)
                .map_err(|e| anyhow::anyhow!("Failed to bind to {}, err: {e}", addr))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(futures::stream::select_all(incoming.into_iter()))
}
