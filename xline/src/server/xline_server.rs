use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use curp::{client::Client, server::Rpc, ProtocolServer};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::{
    command::{Command, CommandExecutor},
    kv_server::KvServer,
    lease_server::LeaseServer,
    lock_server::LockServer,
    watch_server::WatchServer,
};
use crate::{
    rpc::{
        KvServer as RpcKvServer, LeaseServer as RpcLeaseServer, LockServer as RpcLockServer,
        WatchServer as RpcWatchServer,
    },
    storage::KvStore,
};

/// Rpc Server of curp protocol
type CurpServer = Rpc<Command, CommandExecutor>;

/// Xline server
#[allow(dead_code)] // Remove this after feature is completed
#[derive(Debug)]
pub struct XlineServer {
    /// Server name
    name: String,
    /// Address of peers
    peers: Vec<SocketAddr>,
    /// Kv storage
    storage: Arc<KvStore>,
    /// Consensus Server
    //node: Arc<DefaultServer<Command, CommandExecutor>>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// If current node is leader when it starts
    /// TODO: remove this when leader selection is supported
    is_leader: bool,
    /// Address of self node
    self_addr: SocketAddr,
}

impl XlineServer {
    /// New `XlineServer`
    #[inline]
    pub async fn new(
        name: String,
        peers: Vec<SocketAddr>,
        is_leader: bool,
        self_addr: SocketAddr,
    ) -> Self {
        let storage = Arc::new(KvStore::new());

        let mut all_members = peers.clone();
        all_members.push(self_addr);
        all_members.sort();
        let all_members = all_members
            .into_iter()
            .map(|addr| (addr.to_string(), addr.to_string()))
            .collect();

        let client = Arc::new(Client::<Command>::new(all_members).await);

        Self {
            name,
            peers,
            storage,
            client,
            is_leader,
            self_addr,
        }
    }

    /// Start `XlineServer`
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    pub async fn start(&self, addr: SocketAddr) -> Result<()> {
        let (kv_server, lock_server, lease_server, watch_server, curp_server) = self.init_servers();
        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::new(lease_server))
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
    pub async fn start_from_listener(&self, xline_listener: TcpListener) -> Result<()> {
        let (kv_server, lock_server, lease_server, watch_server, curp_server) = self.init_servers();
        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::new(lease_server))
            .add_service(RpcWatchServer::new(watch_server))
            .add_service(ProtocolServer::new(curp_server))
            .serve_with_incoming(TcpListenerStream::new(xline_listener))
            .await?)
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer`, `WatchServer` and `CurpServer`
    /// for the Xline Server.
    fn init_servers(&self) -> (KvServer, LockServer, LeaseServer, WatchServer, CurpServer) {
        (
            KvServer::new(
                Arc::clone(&self.storage),
                Arc::clone(&self.client),
                self.name.clone(),
            ),
            LockServer::new(
                Arc::clone(&self.storage),
                Arc::clone(&self.client),
                self.name.clone(),
            ),
            LeaseServer::new(
                Arc::clone(&self.storage),
                Arc::clone(&self.client),
                self.name.clone(),
            ),
            WatchServer::new(self.storage.kv_watcher()),
            CurpServer::new(
                self.self_addr.to_string(),
                self.is_leader,
                self.peers
                    .iter()
                    .map(|peer| (peer.to_string(), peer.to_string()))
                    .collect(),
                CommandExecutor::new(Arc::clone(&self.storage)),
            ),
        )
    }
}
