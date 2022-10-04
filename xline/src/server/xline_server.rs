use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
//use log::debug;
//use prost::Message;

use curp::{client::Client, server::Rpc};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::rpc::{
    KvServer as RpcKvServer, LeaseServer as RpcLeaseServer, LockServer as RpcLockServer,
    WatchServer as RpcWatchServer,
};

use super::{
    command::{Command, CommandExecutor},
    kv_server::KvServer,
    lease_server::LeaseServer,
    lock_server::LockServer,
    watch_server::WatchServer,
};

use crate::storage::KvStore;

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
    /// Leader index
    leader_index: usize,
    /// Address of self node
    self_addr: SocketAddr,
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
        leader_addr: SocketAddr,
        self_addr: SocketAddr,
    ) -> Self {
        let storage = Arc::new(KvStore::new());

        let mut all_members = peers.clone();
        all_members.push(self_addr);
        all_members.sort();

        let leader_index = all_members.binary_search(&leader_addr).unwrap_or_else(|_| {
            panic!("leader address should be in the collection of peers and self address")
        });

        let client = Arc::new(Client::<Command>::new(leader_index, all_members).await);

        Self {
            name,
            peers,
            storage,
            client,
            is_leader,
            leader_index,
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
        let (kv_server, lock_server, lease_server, watch_server) = self.init_servers();

        let storage = Arc::clone(&self.storage);
        let peers = self.peers.clone();
        let is_leader = self.is_leader;
        let self_addr = self.self_addr;

        let _handle = tokio::spawn(async move {
            let cmd_executor = CommandExecutor::new(storage);
            Rpc::<Command, CommandExecutor>::run(
                is_leader,
                0,
                peers.into_iter().map(|p| p.to_string()).collect(),
                Some(self_addr.port()),
                cmd_executor,
            )
            .await
        });

        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::new(lease_server))
            .add_service(RpcWatchServer::new(watch_server))
            .serve(addr)
            .await?)
    }

    /// Start `XlineServer` from listeners
    ///
    /// # Errors
    ///
    /// Will return `Err` when `tonic::Server` serve return an error
    #[inline]
    pub async fn start_from_listener(
        &self,
        xline_listener: TcpListener,
        curp_listener: TcpListener,
    ) -> Result<()> {
        let (kv_server, lock_server, lease_server, watch_server) = self.init_servers();

        let storage = Arc::clone(&self.storage);
        let peers = self.peers.clone();
        let is_leader = self.is_leader;

        let _handle = tokio::spawn(async move {
            let cmd_executor = CommandExecutor::new(storage);
            Rpc::<Command, CommandExecutor>::run_from_listener(
                is_leader,
                0,
                peers.into_iter().map(|p| p.to_string()).collect(),
                curp_listener,
                cmd_executor,
            )
            .await
        });

        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::new(lease_server))
            .add_service(RpcWatchServer::new(watch_server))
            .serve_with_incoming(TcpListenerStream::new(xline_listener))
            .await?)
    }

    /// Init `KvServer`, `LockServer`, `LeaseServer` and `WatchServer` for the
    /// Xline Server.
    fn init_servers(&self) -> (KvServer, LockServer, LeaseServer, WatchServer) {
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
        )
    }
}
