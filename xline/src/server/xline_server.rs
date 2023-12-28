use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{
    client::ClientBuilder as CurpClientBuilder,
    members::{get_cluster_info_from_remote, ClusterInfo},
    rpc::{InnerProtocolServer, ProtocolServer},
    server::Rpc,
};
use dashmap::DashMap;
use engine::{MemorySnapshotAllocator, RocksSnapshotAllocator, SnapshotAllocator};
#[cfg(not(madsim))]
use futures::Stream;
use itertools::Itertools;
use jsonwebtoken::{DecodingKey, EncodingKey};
#[cfg(not(madsim))]
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::{fs, sync::mpsc::channel};
#[cfg(not(madsim))]
use tonic::transport::server::Connected;
use tonic::transport::{server::Router, Server};
use tracing::{info, warn};
use utils::{
    config::{
        AuthConfig, ClusterConfig, CompactConfig, EngineConfig, InitialClusterState, StorageConfig,
    },
    task_manager::{tasks::TaskName, TaskManager},
};
use xlineapi::command::{Command, CurpClient};

use super::{
    auth_server::AuthServer,
    barriers::{IdBarrier, IndexBarrier},
    cluster_server::ClusterServer,
    command::{Alarmer, CommandExecutor},
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
        db::DB,
        index::Index,
        kv_store::KvStoreInner,
        kvwatcher::KvWatcher,
        lease_store::LeaseCollection,
        storage_api::StorageApi,
        AlarmStore, AuthStore, KvStore, LeaseStore,
    },
};

/// Rpc Server of curp protocol
type CurpServer<S> = Rpc<Command, State<S, Arc<CurpClient>>>;

/// Xline server
#[derive(Debug)]
pub struct XlineServer {
    /// Cluster information
    cluster_info: Arc<ClusterInfo>,
    /// Cluster Config
    cluster_config: ClusterConfig,
    /// Storage config,
    storage_config: StorageConfig,
    /// Compact config
    compact_config: CompactConfig,
    /// Auth config
    auth_config: AuthConfig,
    /// Task Manager
    task_manager: Arc<TaskManager>,
}

impl XlineServer {
    /// New `XlineServer`
    /// # Errors
    /// Return error if init cluster info failed
    #[inline]
    pub async fn new(
        cluster_config: ClusterConfig,
        storage_config: StorageConfig,
        compact_config: CompactConfig,
        auth_config: AuthConfig,
    ) -> Result<Self> {
        let cluster_info = Arc::new(Self::init_cluster_info(&cluster_config).await?);
        Ok(Self {
            cluster_info,
            cluster_config,
            storage_config,
            compact_config,
            auth_config,
            task_manager: Arc::new(TaskManager::new()),
        })
    }

    /// Init cluster info from cluster config
    async fn init_cluster_info(cluster_config: &ClusterConfig) -> Result<ClusterInfo> {
        let server_addr_str = cluster_config
            .members()
            .get(cluster_config.name())
            .ok_or_else(|| {
                anyhow!(
                    "node name {} not found in cluster peers",
                    cluster_config.name()
                )
            })?;
        let server_addr = server_addr_str
            .iter()
            .map(|addr| {
                // TODO: update this after we support https
                let address = if let Some(address) = addr.strip_prefix("http://") {
                    address
                } else {
                    addr
                };
                address.to_socket_addrs()
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect_vec();
        info!("name = {:?}", cluster_config.name());
        info!("server_addr = {server_addr:?}");
        info!("cluster_peers = {:?}", cluster_config.members());

        let name = cluster_config.name().clone();
        let all_members = cluster_config.members().clone();
        let init_cluster_info = ClusterInfo::new(all_members, &name);
        Ok(match *cluster_config.initial_cluster_state() {
            InitialClusterState::New => init_cluster_info,
            InitialClusterState::Existing => get_cluster_info_from_remote(
                &init_cluster_info,
                server_addr_str,
                &name,
                *cluster_config.client_config().wait_synced_timeout(),
            )
            .await
            .ok_or_else(|| anyhow!("Failed to get cluster info from remote"))?,
            _ => unreachable!("xline only supports two initial cluster states: new, existing"),
        })
    }

    /// Construct a `LeaseCollection`
    #[inline]
    #[allow(clippy::arithmetic_side_effects)] // never overflow
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
        Arc<AlarmStore<S>>,
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
        self.task_manager.spawn(TaskName::CompactBg, |n| {
            compact_bg_task(
                Arc::clone(&kv_storage),
                Arc::clone(&index),
                *self.compact_config.compact_batch_size(),
                *self.compact_config.compact_sleep_interval(),
                compact_task_rx,
                n,
            )
        });
        let lease_storage = Arc::new(LeaseStore::new(
            Arc::clone(&lease_collection),
            Arc::clone(&header_gen),
            Arc::clone(&persistent),
            index,
            kv_update_tx,
            *self.cluster_config.is_leader(),
        ));
        let auth_storage = Arc::new(AuthStore::new(
            lease_collection,
            key_pair,
            Arc::clone(&header_gen),
            Arc::clone(&persistent),
        ));
        let alarm_storage = Arc::new(AlarmStore::new(header_gen, persistent));

        let watcher = KvWatcher::new_arc(
            kv_store_inner,
            kv_update_rx,
            *self.cluster_config.server_timeout().sync_victims_interval(),
            &self.task_manager,
        );
        // lease storage must recover before kv storage
        lease_storage.recover()?;
        kv_storage.recover().await?;
        auth_storage.recover()?;
        alarm_storage.recover()?;
        Ok((
            kv_storage,
            lease_storage,
            auth_storage,
            alarm_storage,
            watcher,
        ))
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
            .add_service(ProtocolServer::new(curp_server.clone()))
            .add_service(InnerProtocolServer::new(curp_server));
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
    pub async fn start_from_single_addr(
        &self,
        addr: SocketAddr,
        persistent: Arc<S>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>> {
        let n = self
            .task_manager
            .get_shutdown_listener(TaskName::TonicServer);
        let persistent = DB::open(&self.storage_config.engine)?;
        let key_pair = Self::read_key_pair(
            self.auth_config.auth_private_key().as_ref(),
            self.auth_config.auth_public_key().as_ref(),
        )
        .await?;
        let (router, curp_client) = self.init_router(persistent, key_pair).await?;
        let handle = tokio::spawn(async move { router.serve_with_shutdown(addr, n.wait()).await });
        if let Err(e) = self.publish(curp_client).await {
            warn!("publish name to cluster failed: {:?}", e);
        };
        Ok(handle)
    }

    /// inner start method shared by `start` and `start_from_listener`
    #[cfg(not(madsim))]
    async fn start_inner<I, IO, IE>(&self, incoming: I) -> Result<()>
    where
        I: Stream<Item = Result<IO, IE>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
        IO::ConnectInfo: Clone + Send + Sync + 'static,
        IE: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        let persistent = DB::open(&self.storage_config.engine)?;
        let key_pair = Self::read_key_pair(
            self.auth_config.auth_private_key().as_ref(),
            self.auth_config.auth_public_key().as_ref(),
        )
        .await?;
        let (router, curp_client) = self.init_router(persistent, key_pair).await?;
        self.task_manager
            .spawn(TaskName::TonicServer, |n| async move {
                let _ig = router
                    .serve_with_incoming_shutdown(incoming, n.wait())
                    .await;
            });
        if let Err(e) = self.publish(curp_client).await {
            warn!("publish name to cluster failed: {e:?}");
        };
        Ok(())
    }

    /// Start `XlineServer`
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    #[cfg(not(madsim))]
    pub async fn start(&self) -> Result<()> {
        let server_addr_iter = self
            .cluster_config
            .members()
            .get(self.cluster_config.name())
            .ok_or_else(|| {
                anyhow!(
                    "node name {} not found in cluster peers",
                    self.cluster_config.name()
                )
            })?
            .iter()
            .map(|addr| {
                // TODO: update this after we support https
                let address = if let Some(address) = addr.strip_prefix("http://") {
                    address
                } else {
                    addr
                };
                address.to_socket_addrs()
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten();
        let incoming = bind_addrs(server_addr_iter)?;

        self.start_inner(incoming).await
    }

    /// Start `XlineServer` from listeners
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    #[cfg(not(madsim))]
    pub async fn start_from_listener(&self, xline_listener: tokio::net::TcpListener) -> Result<()> {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(xline_listener);
        self.start_inner(incoming).await
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and `CurpServer`
    /// for the Xline Server.
    #[allow(clippy::type_complexity, clippy::too_many_lines)] // it is easy to read
    #[allow(clippy::as_conversions)] // cast to dyn
    #[allow(trivial_casts)] // same as above
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
        CurpServer<S>,
        Arc<CurpClient>,
    )> {
        let (header_gen, id_gen) = Self::construct_generator(&self.cluster_info);
        let lease_collection = Self::construct_lease_collection(
            self.cluster_config.curp_config().heartbeat_interval,
            self.cluster_config.curp_config().candidate_timeout_ticks,
        );

        let (kv_storage, lease_storage, auth_storage, alarm_storage, watcher) = self
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
        let ce = Arc::new(CommandExecutor::new(
            Arc::clone(&kv_storage),
            Arc::clone(&auth_storage),
            Arc::clone(&lease_storage),
            Arc::clone(&alarm_storage),
            Arc::clone(&persistent),
            Arc::clone(&index_barrier),
            Arc::clone(&id_barrier),
            header_gen.general_revision_arc(),
            header_gen.auth_revision_arc(),
            Arc::clone(&compact_events),
            self.storage_config.quota,
        ));
        let snapshot_allocator: Box<dyn SnapshotAllocator> = match self.storage_config.engine {
            EngineConfig::Memory => Box::<MemorySnapshotAllocator>::default(),
            EngineConfig::RocksDB(_) => Box::<RocksSnapshotAllocator>::default(),
            #[allow(clippy::unimplemented)]
            _ => unimplemented!(),
        };

        let auto_compactor =
            if let Some(auto_config_cfg) = *self.compact_config.auto_compact_config() {
                Some(
                    auto_compactor(
                        *self.cluster_config.is_leader(),
                        header_gen.general_revision_arc(),
                        auto_config_cfg,
                        Arc::clone(&self.task_manager),
                    )
                    .await,
                )
            } else {
                None
            };

        let auto_compactor_c = auto_compactor.clone();

        let state = State::new(Arc::clone(&lease_storage), auto_compactor);

        let curp_config = Arc::new(self.cluster_config.curp_config().clone());
        let curp_server = CurpServer::new(
            Arc::clone(&self.cluster_info),
            *self.cluster_config.is_leader(),
            Arc::clone(&ce),
            snapshot_allocator,
            state,
            Arc::clone(&curp_config),
            Arc::clone(&self.task_manager),
        )
        .await;

        let client = Arc::new(
            CurpClientBuilder::new(*self.cluster_config.client_config())
                .cluster_version(self.cluster_info.cluster_version())
                .all_members(self.cluster_info.all_members_addrs())
                .bypass(self.cluster_info.self_id(), curp_server.clone())
                .build::<Command>()
                .await?,
        ) as Arc<CurpClient>;

        if let Some(compactor) = auto_compactor_c {
            compactor.set_compactable(Arc::clone(&client)).await;
        }
        ce.set_alarmer(Alarmer::new(
            self.cluster_info.self_id(),
            Arc::clone(&client),
        ));
        let raw_curp = curp_server.raw_curp();

        let server_timeout = self.cluster_config.server_timeout();
        Ok((
            KvServer::new(
                Arc::clone(&kv_storage),
                Arc::clone(&auth_storage),
                index_barrier,
                id_barrier,
                *server_timeout.range_retry_timeout(),
                *server_timeout.compact_timeout(),
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
                &self.task_manager,
            ),
            AuthServer::new(Arc::clone(&client)),
            WatchServer::new(
                watcher,
                Arc::clone(&header_gen),
                *server_timeout.watch_progress_notify_interval(),
                Arc::clone(&self.task_manager),
            ),
            MaintenanceServer::new(
                kv_storage,
                Arc::clone(&client),
                persistent,
                Arc::clone(&header_gen),
                Arc::clone(&self.cluster_info),
                raw_curp,
                ce,
                alarm_storage,
            ),
            ClusterServer::new(Arc::clone(&client), header_gen),
            curp_server,
            client,
        ))
    }

    /// Publish the name of current node to cluster
    async fn publish(&self, curp_client: Arc<CurpClient>) -> Result<(), tonic::Status> {
        curp_client
            .propose_publish(self.cluster_info.self_id(), self.cluster_info.self_name())
            .await
    }

    /// Stop `XlineServer`
    #[inline]
    pub async fn stop(&self) {
        self.task_manager.shutdown(true).await;
    }

    /// Read key pair from file
    async fn read_key_pair(
        private_key_path: Option<&PathBuf>,
        public_key_path: Option<&PathBuf>,
    ) -> Result<Option<(EncodingKey, DecodingKey)>> {
        match (private_key_path, public_key_path) {
            (Some(private), Some(public)) => {
                let encoding_key = EncodingKey::from_rsa_pem(&fs::read(private).await?)?;
                let decoding_key = DecodingKey::from_rsa_pem(&fs::read(public).await?)?;
                Ok(Some((encoding_key, decoding_key)))
            }
            (None, None) => Ok(None),
            _ => Err(anyhow!(
                "private key path and public key path must be both set or both unset"
            )),
        }
    }
}

/// Bind multiple addresses
#[cfg(not(madsim))]
fn bind_addrs<T: Iterator<Item = SocketAddr>>(
    addrs: T,
) -> Result<impl Stream<Item = Result<hyper::server::conn::AddrStream, std::io::Error>>> {
    let incoming = addrs
        .map(|addr| {
            tonic::transport::server::TcpIncoming::new(addr, true, None)
                .map_err(|e| anyhow::anyhow!("Failed to bind to {}, err: {e}", addr))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(futures::stream::select_all(incoming.into_iter()))
}
