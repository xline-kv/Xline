use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
//use log::debug;
//use prost::Message;

use curp::{client::Client, server::RpcServerWrap};
use tonic::transport::Server;

use crate::rpc::{
    KvServer as RpcKvServer, LeaseServer as RpcLeaseServer, LockServer as RpcLockServer,
};

use super::{
    command::{Command, CommandExecutor},
    kv_server::KvServer,
    lease_server::LeaseServer,
    lock_server::LockServer,
};

use crate::storage::KvStore;

/// Xline server
#[allow(dead_code)] // Remove this after feature is completed
#[derive(Debug)]
pub(crate) struct XlineServer {
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
    /// Leader address
    leader_address: SocketAddr,
    /// Address of self node
    self_addr: SocketAddr,
}

impl XlineServer {
    /// New `XlineServer`
    pub(crate) async fn new(
        name: String,
        addr: SocketAddr,
        peers: Vec<SocketAddr>,
        is_leader: bool,
        leader_address: SocketAddr,
        self_addr: SocketAddr,
    ) -> Self {
        let storage = Arc::new(KvStore::new());
        let storage_clone = Arc::clone(&storage);

        let mut all_members = peers.clone();
        all_members.push(self_addr);

        let client = Arc::new(
            Client::<Command>::new(&leader_address, &all_members)
                .await
                .unwrap_or_else(|| panic!("Failed to create client")),
        );

        let peers_clone = peers.clone();
        let _handle = tokio::spawn(async move {
            let cmd_executor = CommandExecutor::new(storage_clone);
            RpcServerWrap::<Command, CommandExecutor>::run(
                is_leader,
                0,
                peers_clone,
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
            leader_address,
            self_addr,
        }
    }

    /// Start `XlineServer`
    pub(crate) async fn start(&self) -> Result<()> {
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

        Ok(Server::builder()
            .add_service(RpcLockServer::new(lock_server))
            .add_service(RpcKvServer::new(kv_server))
            .add_service(RpcLeaseServer::new(lease_server))
            .serve(self.addr)
            .await?)
    }
}
