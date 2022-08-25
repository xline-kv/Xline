use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
//use log::debug;
//use prost::Message;

use curp::{client::Client, server::Rpc};
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
    /// Address of server
    addr: SocketAddr,
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
    pub async fn new(
        name: String,
        addr: SocketAddr,
        peers: Vec<SocketAddr>,
        is_leader: bool,
        leader_addr: SocketAddr,
        self_addr: SocketAddr,
    ) -> Self {
        let storage = Arc::new(KvStore::new());
        let storage_clone = Arc::clone(&storage);

        let mut all_members = peers.clone();
        all_members.push(self_addr);
        all_members.sort();

        let leader_index = all_members
            .binary_search(&leader_addr)
            .expect("leader address should be in the collection of peers and self address");

        let client = Arc::new(
            Client::<Command>::new(
                leader_index,
                all_members.into_iter().map(|m| m.to_string()).collect(),
            )
            .await,
        );

        let peers_clone = peers.clone();
        let _handle = tokio::spawn(async move {
            let cmd_executor = CommandExecutor::new(storage_clone);
            Rpc::<Command, CommandExecutor>::run(
                is_leader,
                0,
                peers_clone.into_iter().map(|p| p.to_string()).collect(),
                Some(self_addr.port()),
                cmd_executor,
            )
            .await
        });
        Self {
            name,
            addr,
            peers,
            storage: Arc::clone(&storage),
            client,
            is_leader,
            leader_index,
            self_addr,
        }
    }

    /// Start `XlineServer`
    pub async fn start(&self) -> Result<()> {
        let kv_server = KvServer::new(
            Arc::clone(&self.storage),
            Arc::clone(&self.client),
            self.name.clone(),
        );

        let lock_server = LockServer::new(
            Arc::clone(&self.storage),
            Arc::clone(&self.client),
            self.name.clone(),
        );

        let lease_server = LeaseServer::new(
            Arc::clone(&self.storage),
            Arc::clone(&self.client),
            self.name.clone(),
        );
        let watch_server = WatchServer::new(self.storage.kv_watcher());

        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::new(lease_server))
            .add_service(RpcWatchServer::new(watch_server))
            .serve(self.addr)
            .await?)
    }
}
