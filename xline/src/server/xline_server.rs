use std::{collections::HashMap, future::Future, net::SocketAddr, sync::Arc};

use anyhow::Result;
use curp::{client::Client, server::Rpc, ProtocolServer};
use jsonwebtoken::{DecodingKey, EncodingKey};
use parking_lot::Mutex;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

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
    /// Server name
    name: String,
    /// Address of peers
    peers: Vec<SocketAddr>,
    /// Kv storage
    kv_storage: Arc<KvStore>,
    /// Auth storage
    auth_storage: Arc<AuthStore>,
    /// Lease storage
    lease_storage: Arc<LeaseStore>,
    /// Consensus Server
    //node: Arc<DefaultServer<Command, CommandExecutor>>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// If current node is leader when it starts
    /// TODO: remove this when leader selection is supported
    is_leader: Arc<Mutex<bool>>,
    /// Address of self node
    self_addr: SocketAddr,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
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
        peers: Vec<SocketAddr>,
        is_leader: bool,
        self_addr: SocketAddr,
        key_pair: Option<(EncodingKey, DecodingKey)>,
    ) -> Self {
        // TODO: temporary solution, need real cluster id and member id
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        // TODO: need to change by curp
        let is_leader = Arc::new(Mutex::new(is_leader));
        let (del_tx, del_rx) = mpsc::channel(CHANNEL_SIZE);
        let lease_storage = Arc::new(LeaseStore::new(
            del_tx,
            Arc::clone(&is_leader),
            Arc::clone(&header_gen),
        ));
        let kv_storage = Arc::new(KvStore::new(
            Arc::clone(&lease_storage),
            del_rx,
            Arc::clone(&header_gen),
        ));
        let auth_storage = Arc::new(AuthStore::new(
            Arc::clone(&lease_storage),
            key_pair,
            Arc::clone(&header_gen),
        ));

        let mut all_members: HashMap<_, _> = peers
            .iter()
            .map(|addr| (addr.to_string(), addr.to_string()))
            .collect();
        let _ignore = all_members.insert(self_addr.to_string(), self_addr.to_string());

        let client = Arc::new(Client::<Command>::new(all_members).await);

        Self {
            name,
            peers,
            kv_storage,
            auth_storage,
            lease_storage,
            client,
            is_leader,
            self_addr,
            header_gen,
        }
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
        (
            KvServer::new(
                Arc::clone(&self.kv_storage),
                Arc::clone(&self.client),
                self.name.clone(),
            ),
            LockServer::new(
                Arc::clone(&self.kv_storage),
                Arc::clone(&self.client),
                self.name.clone(),
            ),
            LeaseServer::new(
                Arc::clone(&self.lease_storage),
                Arc::clone(&self.client),
                self.name.clone(),
                Arc::clone(&self.is_leader),
            ),
            AuthServer::new(
                Arc::clone(&self.auth_storage),
                Arc::clone(&self.client),
                self.name.clone(),
            ),
            WatchServer::new(self.kv_storage.kv_watcher()),
            CurpServer::new(
                self.self_addr.to_string(),
                *self.is_leader.lock(),
                self.peers
                    .iter()
                    .map(|peer| (peer.to_string(), peer.to_string()))
                    .collect(),
                CommandExecutor::new(
                    Arc::clone(&self.kv_storage),
                    Arc::clone(&self.auth_storage),
                    Arc::clone(&self.lease_storage),
                ),
            ),
        )
    }
}
