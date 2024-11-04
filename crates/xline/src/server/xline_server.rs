use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp::{
    client::ClientBuilder as CurpClientBuilder,
    member::{ClusterId, MembershipInfo},
    rpc::{InnerProtocolServer, NodeMetadata, ProtocolServer},
    server::{Rpc, DB as CurpDB},
};
use dashmap::DashMap;
use engine::{MemorySnapshotAllocator, RocksSnapshotAllocator, SnapshotAllocator};
#[cfg(not(madsim))]
use futures::Stream;
use jsonwebtoken::{DecodingKey, EncodingKey};
use tokio::fs;
#[cfg(not(madsim))]
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(not(madsim))]
use tonic::transport::{
    server::Connected, Certificate, ClientTlsConfig, Identity, ServerTlsConfig,
};
use tonic::transport::{server::Router, Server};
#[cfg(not(madsim))]
use tracing::info;
use utils::{
    barrier::IdBarrier,
    config::{AuthConfig, ClusterConfig, CompactConfig, EngineConfig, StorageConfig, TlsConfig},
    task_manager::{tasks::TaskName, TaskManager},
};
#[cfg(madsim)]
use utils::{ClientTlsConfig, ServerTlsConfig};
use xlineapi::command::{Command, CurpClient};

use super::{
    auth_server::AuthServer,
    auth_wrapper::AuthWrapper,
    cluster_server::ClusterServer,
    command::{Alarmer, CommandExecutor},
    kv_server::KvServer,
    lease_server::LeaseServer,
    lock_server::LockServer,
    maintenance::MaintenanceServer,
    watch_server::{WatchServer, CHANNEL_SIZE},
};
use crate::{
    conflict::{XlineSpeculativePools, XlineUncommittedPools},
    header_gen::HeaderGenerator,
    id_gen::IdGenerator,
    metrics::Metrics,
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
        AlarmStore, AuthStore, KvStore, LeaseStore,
    },
};

/// Rpc Server of curp protocol
pub(crate) type CurpServer = Rpc<Command, CommandExecutor, State<Arc<CurpClient>>>;

/// Xline server
#[derive(Debug)]
pub struct XlineServer {
    /// Membership information
    membership_info: MembershipInfo,
    /// Cluster Config
    cluster_config: ClusterConfig,
    /// Storage config,
    storage_config: StorageConfig,
    /// Compact config
    compact_config: CompactConfig,
    /// Auth config
    auth_config: AuthConfig,
    /// Client tls config
    client_tls_config: Option<ClientTlsConfig>,
    /// Server tls config
    #[cfg_attr(madsim, allow(unused))]
    server_tls_config: Option<ServerTlsConfig>,
    /// Task Manager
    task_manager: Arc<TaskManager>,
    /// Curp storage
    curp_storage: Arc<CurpDB<Command>>,
}

impl XlineServer {
    #[cfg_attr(madsim, allow(clippy::unused_async))]
    /// New `XlineServer`
    ///
    /// # Errors
    ///
    /// Return error if init cluster info failed
    #[inline]
    pub async fn new(
        cluster_config: ClusterConfig,
        storage_config: StorageConfig,
        compact_config: CompactConfig,
        auth_config: AuthConfig,
        #[cfg_attr(madsim, allow(unused_variables))] tls_config: TlsConfig,
    ) -> Result<Self> {
        #[cfg(not(madsim))]
        let (client_tls_config, server_tls_config) = Self::read_tls_config(&tls_config).await?;
        #[cfg(madsim)]
        let (client_tls_config, server_tls_config) = (None, None);
        let curp_storage = Arc::new(CurpDB::open(&cluster_config.curp_config().engine_cfg)?);

        let init_members = cluster_config
            .initial_membership_info()
            .clone()
            .into_iter()
            .map(|(name, conf)| {
                let meta =
                    NodeMetadata::new(name, conf.peer_urls().clone(), conf.client_urls().clone());
                (*conf.id(), meta)
            })
            .collect();
        let membership_info = MembershipInfo::new(*cluster_config.node_id(), init_members);

        Ok(Self {
            cluster_config,
            storage_config,
            compact_config,
            auth_config,
            client_tls_config,
            server_tls_config,
            task_manager: Arc::new(TaskManager::new()),
            curp_storage,
            membership_info,
        })
    }

    /// Construct a `LeaseCollection`
    #[inline]
    #[allow(clippy::arithmetic_side_effects)] // never overflow
    fn construct_lease_collection(
        heartbeat_interval: Duration,
        candidate_timeout_ticks: u8,
    ) -> Arc<LeaseCollection> {
        let min_ttl = 3 * heartbeat_interval * candidate_timeout_ticks.numeric_cast() / 2;
        // Safe ceiling
        let min_ttl_secs = min_ttl
            .as_secs()
            .overflow_add(u64::from(min_ttl.subsec_nanos() > 0));
        Arc::new(LeaseCollection::new(min_ttl_secs.numeric_cast()))
    }

    /// Construct underlying storages, including `KvStore`, `LeaseStore`,
    /// `AuthStore`
    #[allow(clippy::type_complexity)] // it is easy to read
    #[inline]
    async fn construct_underlying_storages(
        &self,
        db: Arc<DB>,
        lease_collection: Arc<LeaseCollection>,
        header_gen: Arc<HeaderGenerator>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(
        Arc<KvStore>,
        Arc<LeaseStore>,
        Arc<AuthStore>,
        Arc<AlarmStore>,
        Arc<KvWatcher>,
    )> {
        let (compact_task_tx, compact_task_rx) = flume::bounded(COMPACT_CHANNEL_SIZE);
        let index = Arc::new(Index::new());
        let (kv_update_tx, kv_update_rx) = flume::bounded(CHANNEL_SIZE);
        let kv_store_inner = Arc::new(KvStoreInner::new(Arc::clone(&index), Arc::clone(&db)));
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
                index,
                *self.compact_config.compact_batch_size(),
                *self.compact_config.compact_sleep_interval(),
                compact_task_rx,
                n,
            )
        });
        let lease_storage = Arc::new(LeaseStore::new(
            Arc::clone(&lease_collection),
            Arc::clone(&header_gen),
            Arc::clone(&db),
            kv_update_tx,
            *self.cluster_config.is_leader(),
        ));
        let auth_storage = Arc::new(AuthStore::new(
            lease_collection,
            key_pair,
            Arc::clone(&header_gen),
            Arc::clone(&db),
        ));
        let alarm_storage = Arc::new(AlarmStore::new(header_gen, db));

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
    fn construct_generator(
        membership_info: &MembershipInfo,
    ) -> (Arc<HeaderGenerator>, Arc<IdGenerator>) {
        let member_id = membership_info.node_id;
        let cluster_id = membership_info.cluster_id();
        (
            Arc::new(HeaderGenerator::new(cluster_id, member_id)),
            Arc::new(IdGenerator::new(member_id)),
        )
    }

    /// Init xline and curp router
    ///
    /// # Errors
    ///
    /// Will return `Err` when `init_servers` return an error
    #[inline]
    pub async fn init_router(
        &self,
        db: Arc<DB>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(Router, Router, Arc<CurpClient>)> {
        let (
            kv_server,
            lock_server,
            lease_server,
            auth_server,
            watch_server,
            maintenance_server,
            cluster_server,
            curp_server,
            auth_wrapper,
            curp_client,
        ) = self.init_servers(db, key_pair).await?;
        let mut builder = Server::builder();
        #[cfg(not(madsim))]
        if let Some(ref cfg) = self.server_tls_config {
            builder = builder.tls_config(cfg.clone())?;
        }
        let xline_router = builder
            .clone()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::from_arc(lease_server))
            .add_service(RpcAuthServer::new(auth_server))
            .add_service(RpcWatchServer::new(watch_server))
            .add_service(RpcMaintenanceServer::new(maintenance_server))
            .add_service(RpcClusterServer::new(cluster_server))
            .add_service(ProtocolServer::new(auth_wrapper.clone()));
        let curp_router = builder
            .add_service(ProtocolServer::new(curp_server.clone()))
            .add_service(InnerProtocolServer::new(curp_server));
        #[cfg(not(madsim))]
        let xline_router = {
            let (mut reporter, health_server) = tonic_health::server::health_reporter();
            reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
            xline_router.add_service(health_server)
        };
        Ok((xline_router, curp_router, curp_client))
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
        xline_addr: std::net::SocketAddr,
        curp_addr: std::net::SocketAddr,
    ) -> Result<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>> {
        let n1 = self
            .task_manager
            .get_shutdown_listener(TaskName::TonicServer)
            .unwrap_or_else(|| unreachable!("cluster should never shutdown before start"));
        let n2 = n1.clone();
        let db = DB::open(&self.storage_config.engine)?;
        let key_pair = Self::read_key_pair(&self.auth_config).await?;
        let (xline_router, curp_router, _curp_client) = self.init_router(db, key_pair).await?;
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = xline_router.serve_with_shutdown(xline_addr, n1.wait()) => {},
                _ = curp_router.serve_with_shutdown(curp_addr, n2.wait()) => {},
            }
            Ok(())
        });

        Ok(handle)
    }

    /// inner start method shared by `start` and `start_from_listener`
    #[cfg(not(madsim))]
    async fn start_inner<I1, I2, IO, IE>(&self, xline_incoming: I1, curp_incoming: I2) -> Result<()>
    where
        I1: Stream<Item = Result<IO, IE>> + Send + 'static,
        I2: Stream<Item = Result<IO, IE>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
        IO::ConnectInfo: Clone + Send + Sync + 'static,
        IE: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        let db = DB::open(&self.storage_config.engine)?;
        let key_pair = Self::read_key_pair(&self.auth_config).await?;
        let (xline_router, curp_router, _curp_client) = self.init_router(db, key_pair).await?;
        self.task_manager
            .spawn(TaskName::TonicServer, |n1| async move {
                let n2 = n1.clone();
                tokio::select! {
                    _ = xline_router.serve_with_incoming_shutdown(xline_incoming, n1.wait()) => {},
                    _ = curp_router.serve_with_incoming_shutdown(curp_incoming, n2.wait()) => {},
                }
            });

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
        let client_listen_urls = self.cluster_config.client_listen_urls();
        let peer_listen_urls = self.cluster_config.peer_listen_urls();
        let xline_incoming = bind_addrs(client_listen_urls)?;
        let curp_incoming = bind_addrs(peer_listen_urls)?;
        info!("start xline server on {:?}", client_listen_urls);
        info!("start curp server on {:?}", peer_listen_urls);
        self.start_inner(xline_incoming, curp_incoming).await
    }

    /// Start `XlineServer` from listeners
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    #[cfg(not(madsim))]
    pub async fn start_from_listener(
        &self,
        xline_listener: tokio::net::TcpListener,
        curp_listener: tokio::net::TcpListener,
    ) -> Result<()> {
        let xline_incoming = tokio_stream::wrappers::TcpListenerStream::new(xline_listener);
        let curp_incoming = tokio_stream::wrappers::TcpListenerStream::new(curp_listener);
        self.start_inner(xline_incoming, curp_incoming).await
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and
    /// `CurpServer` for the Xline Server.
    #[allow(
        clippy::type_complexity, // it is easy to read
        clippy::too_many_lines, // TODO: split this into multiple functions
        clippy::as_conversions, // cast to dyn
        trivial_casts // same as above
    )]
    async fn init_servers(
        &self,
        db: Arc<DB>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Result<(
        KvServer,
        LockServer,
        Arc<LeaseServer>,
        AuthServer,
        WatchServer,
        MaintenanceServer,
        ClusterServer,
        CurpServer,
        AuthWrapper,
        Arc<CurpClient>,
    )> {
        let (header_gen, id_gen) = Self::construct_generator(&self.membership_info);
        let lease_collection = Self::construct_lease_collection(
            self.cluster_config.curp_config().heartbeat_interval,
            self.cluster_config.curp_config().candidate_timeout_ticks,
        );

        let (kv_storage, lease_storage, auth_storage, alarm_storage, watcher) = self
            .construct_underlying_storages(
                Arc::clone(&db),
                Arc::clone(&lease_collection),
                Arc::clone(&header_gen),
                key_pair,
            )
            .await?;

        let id_barrier = Arc::new(IdBarrier::new());
        let compact_events = Arc::new(DashMap::new());
        let ce = Arc::new(CommandExecutor::new(
            Arc::clone(&kv_storage),
            Arc::clone(&auth_storage),
            Arc::clone(&lease_storage),
            Arc::clone(&alarm_storage),
            Arc::clone(&db),
            Arc::clone(&id_barrier),
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
            self.membership_info.clone(),
            *self.cluster_config.is_leader(),
            Arc::clone(&ce),
            snapshot_allocator,
            state,
            Arc::clone(&curp_config),
            Arc::clone(&self.curp_storage),
            Arc::clone(&self.task_manager),
            self.client_tls_config.clone(),
            XlineSpeculativePools::new(Arc::clone(&lease_collection)).into_inner(),
            XlineUncommittedPools::new(lease_collection).into_inner(),
        );

        let client = Arc::new(
            CurpClientBuilder::new(*self.cluster_config.client_config(), true)
                .tls_config(self.client_tls_config.clone())
                .init_nodes(
                    self.membership_info
                        .init_members
                        .values()
                        .cloned()
                        .map(NodeMetadata::into_peer_urls),
                )
                .bypass(self.membership_info.node_id, curp_server.clone())
                .build::<Command>()?,
        ) as Arc<CurpClient>;

        if let Some(compactor) = auto_compactor_c {
            compactor.set_compactable(Arc::clone(&client)).await;
        }
        ce.set_alarmer(Alarmer::new(
            self.membership_info.node_id,
            Arc::clone(&client),
        ));
        let raw_curp = curp_server.raw_curp();

        Metrics::register_callback()?;

        let server_timeout = self.cluster_config.server_timeout();
        let self_addrs: Vec<_> = self
            .membership_info
            .init_members
            .get(&self.membership_info.node_id)
            .cloned()
            .map(NodeMetadata::into_peer_urls)
            .into_iter()
            .flatten()
            .collect();
        Ok((
            KvServer::new(
                Arc::clone(&kv_storage),
                Arc::clone(&auth_storage),
                *server_timeout.compact_timeout(),
                Arc::clone(&client),
                compact_events,
            ),
            LockServer::new(
                Arc::clone(&client),
                Arc::clone(&auth_storage),
                Arc::clone(&id_gen),
                &self_addrs,
                self.client_tls_config.as_ref(),
            ),
            LeaseServer::new(
                lease_storage,
                Arc::clone(&auth_storage),
                Arc::clone(&client),
                id_gen,
                self.client_tls_config.clone(),
                &self.task_manager,
            ),
            AuthServer::new(Arc::clone(&client), Arc::clone(&auth_storage)),
            WatchServer::new(
                watcher,
                Arc::clone(&header_gen),
                *server_timeout.watch_progress_notify_interval(),
                Arc::clone(&self.task_manager),
            ),
            MaintenanceServer::new(
                kv_storage,
                Arc::clone(&auth_storage),
                Arc::clone(&client),
                db,
                Arc::clone(&header_gen),
                raw_curp,
                ce,
                alarm_storage,
            ),
            ClusterServer::new(Arc::clone(&client), header_gen),
            curp_server.clone(),
            AuthWrapper::new(curp_server, auth_storage),
            client,
        ))
    }

    /// Stop `XlineServer`
    #[inline]
    pub async fn stop(&self) {
        self.task_manager.shutdown(true).await;
    }

    /// Read key pair from file
    async fn read_key_pair(auth_config: &AuthConfig) -> Result<Option<(EncodingKey, DecodingKey)>> {
        match (
            auth_config.auth_private_key().as_ref(),
            auth_config.auth_public_key().as_ref(),
        ) {
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

    /// Read tls cert and key from file
    #[cfg(not(madsim))]
    async fn read_tls_config(
        tls_config: &TlsConfig,
    ) -> Result<(Option<ClientTlsConfig>, Option<ServerTlsConfig>)> {
        let client_tls_config = match (
            tls_config.client_ca_cert_path().as_ref(),
            tls_config.client_cert_path().as_ref(),
            tls_config.client_key_path().as_ref(),
        ) {
            (Some(ca_path), Some(cert_path), Some(key_path)) => {
                let ca = fs::read(ca_path).await?;
                let cert = fs::read(cert_path).await?;
                let key = fs::read(key_path).await?;
                Some(
                    ClientTlsConfig::new()
                        .ca_certificate(Certificate::from_pem(ca))
                        .identity(Identity::from_pem(cert, key)),
                )
            }
            (Some(ca_path), None, None) => {
                let ca = fs::read(ca_path).await?;
                Some(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))
            }
            (_, Some(_), None) | (_, None, Some(_)) => {
                return Err(anyhow!(
                    "client_cert_path and client_key_path must be both set"
                ))
            }
            _ => None,
        };
        let server_tls_config = match (
            tls_config.peer_ca_cert_path().as_ref(),
            tls_config.peer_cert_path().as_ref(),
            tls_config.peer_key_path().as_ref(),
        ) {
            (Some(ca_path), Some(cert_path), Some(key_path)) => {
                let ca = fs::read(ca_path).await?;
                let cert = fs::read_to_string(cert_path).await?;
                let key = fs::read_to_string(key_path).await?;
                Some(
                    ServerTlsConfig::new()
                        .client_ca_root(Certificate::from_pem(ca))
                        .identity(Identity::from_pem(cert, key)),
                )
            }
            (None, Some(cert_path), Some(key_path)) => {
                let cert = fs::read_to_string(cert_path).await?;
                let key = fs::read_to_string(key_path).await?;
                Some(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
            }
            (_, Some(_), None) | (_, None, Some(_)) => {
                return Err(anyhow!("peer_cert_path and peer_key_path must be both set"))
            }
            _ => None,
        };
        Ok((client_tls_config, server_tls_config))
    }
}

/// Bind multiple addresses
#[cfg(not(madsim))]
fn bind_addrs(
    addrs: &[String],
) -> Result<impl Stream<Item = Result<tokio::net::TcpStream, std::io::Error>>> {
    use std::net::ToSocketAddrs;
    if addrs.is_empty() {
        return Err(anyhow!("No address to bind"));
    }
    let incoming = addrs
        .iter()
        .map(|addr| {
            let address = match addr.split_once("://") {
                Some((_, address)) => address,
                None => addr,
            };
            address.to_socket_addrs()
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .map(|addr| {
            tonic::transport::server::TcpIncoming::new(addr, true, None)
                .map_err(|e| anyhow::anyhow!("Failed to bind to {}, err: {e}", addr))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(futures::stream::select_all(incoming))
}
