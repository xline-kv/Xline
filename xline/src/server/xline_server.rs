use std::{collections::HashMap, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use curp::{client::Client, server::Rpc, ProtocolServer};
use jsonwebtoken::{DecodingKey, EncodingKey};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;
use utils::config::{ClientTimeout, CurpConfig};

use super::{
    auth_server::AuthServer,
    command::{Command, CommandExecutor},
    kv_server::KvServer,
    lease_server::LeaseServer,
    lock_server::LockServer,
    watch_server::WatchServer,
};
use crate::{
    header_gen::HeaderGenerator,
    id_gen::IdGenerator,
    rpc::{
        AuthServer as RpcAuthServer, KvServer as RpcKvServer, LeaseServer as RpcLeaseServer,
        LockServer as RpcLockServer, WatchServer as RpcWatchServer,
    },
    state::State,
    storage::{storage_api::StorageApi, AuthStore, KvStore, LeaseStore},
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

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
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Curp server timeout
    curp_cfg: Arc<CurpConfig>,
    /// Id generator
    id_gen: Arc<IdGenerator>,
}

impl<S> XlineServer<S>
where
    S: StorageApi + Clone,
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
    pub async fn new(
        name: String,
        all_members: HashMap<String, String>,
        is_leader: bool,
        key_pair: Option<(EncodingKey, DecodingKey)>,
        curp_config: CurpConfig,
        client_timeout: ClientTimeout,
        storage: Arc<S>,
    ) -> Self {
        // TODO: temporary solution, need real cluster id and member id
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let id_gen = Arc::new(IdGenerator::new(0));
        let leader_id = is_leader.then(|| name.clone());
        let state = Arc::new(State::new(name, leader_id, all_members.clone()));
        let curp_config = Arc::new(curp_config);
        let (del_tx, del_rx) = mpsc::channel(CHANNEL_SIZE);
        let (lease_cmd_tx, lease_cmd_rx) = mpsc::channel(CHANNEL_SIZE);
        let lease_storage = Arc::new(LeaseStore::new(
            del_tx,
            lease_cmd_rx,
            Arc::clone(&state),
            Arc::clone(&header_gen),
            Arc::clone(&storage),
        ));
        let kv_storage = Arc::new(KvStore::new(
            lease_cmd_tx.clone(),
            del_rx,
            Arc::clone(&header_gen),
            Arc::clone(&storage),
        ));
        let auth_storage = Arc::new(AuthStore::new(
            lease_cmd_tx,
            key_pair,
            header_gen,
            Arc::clone(&storage),
        ));
        let client = Arc::new(Client::<Command>::new(all_members.clone(), client_timeout).await);
        Self {
            state,
            kv_storage,
            auth_storage,
            lease_storage,
            client,
            curp_cfg: curp_config,
            id_gen,
        }
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
        let (kv_server, lock_server, lease_server, auth_server, watch_server, curp_server) =
            self.init_servers().await;
        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::from_arc(lease_server))
            .add_service(RpcAuthServer::new(auth_server))
            .add_service(RpcWatchServer::new(watch_server))
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
        let (kv_server, lock_server, lease_server, auth_server, watch_server, curp_server) =
            self.init_servers().await;
        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::from_arc(lease_server))
            .add_service(RpcAuthServer::new(auth_server))
            .add_service(RpcWatchServer::new(watch_server))
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
        LockServer<S>,
        Arc<LeaseServer<S>>,
        AuthServer<S>,
        WatchServer<S>,
        CurpServer,
    ) {
        let curp_server = CurpServer::new(
            self.id(),
            self.is_leader(),
            self.state.others(),
            CommandExecutor::new(
                Arc::clone(&self.kv_storage),
                Arc::clone(&self.auth_storage),
                Arc::clone(&self.lease_storage),
            ),
            Arc::clone(&self.curp_cfg),
            None,
        )
        .await;
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
                Arc::clone(&self.state),
                Arc::clone(&self.client),
                self.id(),
            ),
            LockServer::new(
                Arc::clone(&self.kv_storage),
                Arc::clone(&self.client),
                Arc::clone(&self.state),
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
            WatchServer::new(self.kv_storage.kv_watcher()),
            curp_server,
        )
    }
}
