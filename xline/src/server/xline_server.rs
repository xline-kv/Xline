use std::{collections::HashMap, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use curp::{client::Client, server::Rpc, ProtocolServer};
use jsonwebtoken::{DecodingKey, EncodingKey};
use parking_lot::RwLock;
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;
use utils::{
    config::{ClientTimeout, ServerTimeout},
    parking_lot_lock::RwLockMap,
};

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
    storage::{AuthStore, KvStore, LeaseStore},
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

/// Rpc Server of curp protocol
type CurpServer = Rpc<Command>;

/// Xline server
#[allow(dead_code)] // Remove this after feature is completed
#[derive(Debug)]
pub struct XlineServer {
    /// State of current node
    state: Arc<RwLock<State>>,
    /// Kv storage
    kv_storage: Arc<KvStore>,
    /// Auth storage
    auth_storage: Arc<AuthStore>,
    /// Lease storage
    lease_storage: Arc<LeaseStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Curp server timeout
    server_timeout: Arc<ServerTimeout>,
    /// Id generator
    id_gen: Arc<IdGenerator>,
}

impl XlineServer {
    /// New `XlineServer`
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
        server_timeout: ServerTimeout,
        client_timeout: ClientTimeout,
    ) -> Self {
        // TODO: temporary solution, need real cluster id and member id
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let id_gen = Arc::new(IdGenerator::new(0));
        let leader_id = is_leader.then(|| name.clone());
        let state = Arc::new(RwLock::new(State::new(
            name,
            leader_id,
            all_members.clone(),
        )));
        let server_timeout = Arc::new(server_timeout);
        let (del_tx, del_rx) = mpsc::channel(CHANNEL_SIZE);
        let (lease_cmd_tx, lease_cmd_rx) = mpsc::channel(CHANNEL_SIZE);
        let lease_storage = Arc::new(LeaseStore::new(
            del_tx,
            lease_cmd_rx,
            Arc::clone(&state),
            Arc::clone(&header_gen),
        ));
        let kv_storage = Arc::new(KvStore::new(
            lease_cmd_tx.clone(),
            del_rx,
            Arc::clone(&header_gen),
        ));
        let auth_storage = Arc::new(AuthStore::new(
            lease_cmd_tx,
            key_pair,
            Arc::clone(&header_gen),
        ));
        let client = Arc::new(Client::<Command>::new(all_members.clone(), client_timeout).await);

        Self {
            state,
            kv_storage,
            auth_storage,
            lease_storage,
            client,
            header_gen,
            server_timeout,
            id_gen,
        }
    }

    /// Server id
    fn id(&self) -> String {
        self.state.read().id().to_owned()
    }

    /// Check if current node is leader
    fn is_leader(&self) -> bool {
        self.state.read().is_leader()
    }

    /// Start `XlineServer`
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    pub async fn start(&self, addr: SocketAddr) -> Result<()> {
        let (kv_server, lock_server, lease_server, auth_server, watch_server, curp_server) =
            self.init_servers();
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
    pub async fn start_from_listener_shoutdown<F>(
        &self,
        xline_listener: TcpListener,
        signal: F,
    ) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let (kv_server, lock_server, lease_server, auth_server, watch_server, curp_server) =
            self.init_servers();
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
        state: Arc<RwLock<State>>,
        lease_storage: Arc<LeaseStore>,
    ) {
        while let Ok(leader_id) = rx.recv().await {
            info!("receive new leader_id: {leader_id:?}");
            let (leader_state_changed, is_leader) = state.map_write(|mut s| {
                let is_leader_before = s.is_leader();
                s.set_leader_id(leader_id);
                let is_leader_after = s.is_leader();
                (is_leader_before ^ is_leader_after, is_leader_after)
            });
            if leader_state_changed {
                if is_leader {
                    lease_storage.promote(Duration::from_secs(1)); // TODO: extend shoud be election timeout
                } else {
                    lease_storage.demote();
                }
            }
        }
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and `CurpServer`
    /// for the Xline Server.
    fn init_servers(
        &self,
    ) -> (
        KvServer,
        LockServer,
        Arc<LeaseServer>,
        AuthServer,
        WatchServer,
        CurpServer,
    ) {
        let curp_server = CurpServer::new(
            self.id(),
            self.is_leader(),
            self.state.read().others(),
            CommandExecutor::new(
                Arc::clone(&self.kv_storage),
                Arc::clone(&self.auth_storage),
                Arc::clone(&self.lease_storage),
            ),
            Arc::clone(&self.server_timeout),
        );
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
