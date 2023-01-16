use std::{collections::HashMap, future::Future, net::SocketAddr, sync::Arc};

use anyhow::Result;
use curp::{client::Client, server::Rpc, ProtocolServer};
use jsonwebtoken::{DecodingKey, EncodingKey};
use parking_lot::RwLock;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
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
    rpc::{
        AuthServer as RpcAuthServer, KvServer as RpcKvServer, LeaseServer as RpcLeaseServer,
        LockServer as RpcLockServer, WatchServer as RpcWatchServer,
    },
    state::State,
    storage::{AuthStore, KvStore},
};

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
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Curp server timeout
    server_timeout: Arc<ServerTimeout>,
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
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let kv_storage = Arc::new(KvStore::new(Arc::clone(&header_gen)));
        let auth_storage = Arc::new(AuthStore::new(key_pair, Arc::clone(&header_gen)));
        let client = Arc::new(Client::<Command>::new(all_members.clone(), client_timeout).await);
        let leader_id = is_leader.then(|| name.clone());
        let state = Arc::new(RwLock::new(State::new(name, leader_id, all_members)));
        let server_timeout = Arc::new(server_timeout);
        Self {
            state,
            kv_storage,
            auth_storage,
            client,
            header_gen,
            server_timeout,
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
            .add_service(RpcLeaseServer::new(lease_server))
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
            .add_service(RpcLeaseServer::new(lease_server))
            .add_service(RpcAuthServer::new(auth_server))
            .add_service(RpcWatchServer::new(watch_server))
            .add_service(ProtocolServer::new(curp_server))
            .serve_with_incoming_shutdown(TcpListenerStream::new(xline_listener), signal)
            .await?)
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and `CurpServer`
    /// for the Xline Server.
    fn init_servers(
        &self,
    ) -> (
        KvServer,
        LockServer,
        LeaseServer,
        AuthServer,
        WatchServer,
        CurpServer,
    ) {
        let curp_server = CurpServer::new(
            self.id(),
            self.is_leader(),
            self.state.read().others(),
            CommandExecutor::new(Arc::clone(&self.kv_storage), Arc::clone(&self.auth_storage)),
            Arc::clone(&self.server_timeout),
        );
        let mut rx = curp_server.leader_rx();
        let _handle = tokio::spawn({
            let state_clone = Arc::clone(&self.state);
            async move {
                while let Ok(leader_id) = rx.recv().await {
                    state_clone.map_write(|mut state| state.set_leader_id(leader_id));
                }
            }
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
                Arc::clone(&self.kv_storage),
                Arc::clone(&self.client),
                self.id(),
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
