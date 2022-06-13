use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use log::debug;
use prost::Message;

use curp::{client::Client, cmd::ProposeId, error::ProposeError, server::RpcServerWrap};
use tonic::transport::Server;
use uuid::Uuid;

use crate::rpc::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, Kv, KvServer,
    PutRequest, PutResponse, RangeRequest, RangeResponse, Request, RequestOp, Response, ResponseOp,
    TxnRequest, TxnResponse,
};

use super::command::{Command, CommandExecutor, CommandResponse, KeyRange, SyncResponse};

use crate::storage::KvStore;

/// Xline server
#[allow(dead_code)] // Remove this after feature is completed
                    //#[derive(Debug)]
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

/// Xline Server Inner
//#[derive(Debug)]
#[allow(dead_code)] // Remove this after feature is completed
struct XlineRpcServer {
    /// KV storage
    storage: Arc<KvStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Consensus configuration
    //config: Configure,
    /// Server name
    name: String,
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
        let rpc_server = XlineRpcServer {
            storage: Arc::clone(&self.storage),
            client: Arc::clone(&self.client),
            //config: self.config.clone(),
            name: self.name.clone(),
        };
        Ok(Server::builder()
            .add_service(KvServer::new(rpc_server))
            .serve(self.addr)
            .await?)
    }
}

impl XlineRpcServer {
    /// Parse `ResponseOp`
    fn parse_response_op(response_op: ResponseOp) -> Response {
        if let Some(response) = response_op.response {
            response
        } else {
            panic!("Receive empty ResponseOp");
        }
    }
    /// Propose request and get result
    async fn propose(
        &self,
        propose_id: ProposeId,
        request: Request,
    ) -> Result<(CommandResponse, SyncResponse), ProposeError> {
        let key_range = match request {
            Request::RequestRange(ref req) => KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            },
            Request::RequestPut(ref req) => KeyRange {
                start: req.key.clone(),
                end: vec![],
            },
            Request::RequestDeleteRange(ref req) => KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            },
            Request::RequestTxn(_) => {
                panic!("Unsupported request");
            }
        };
        let range_request_op = RequestOp {
            request: Some(request),
        };
        let cmd = Command::new(
            vec![key_range],
            range_request_op.encode_to_vec(),
            propose_id,
        );
        let response = self.client.propose_indexed(cmd.clone()).await;
        response
    }

    /// Update revision of `ResponseHeader`
    fn update_header_revision(mut response: Response, revision: i64) -> Response {
        match response {
            Response::ResponseRange(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
            }
            Response::ResponsePut(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
            }
            Response::ResponseDeleteRange(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
            }
            Response::ResponseTxn(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
            }
        }
        response
    }

    /// Generate propose id
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }
}

#[tonic::async_trait]
impl Kv for XlineRpcServer {
    /// Range gets the keys in the range from the key-value store.
    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        debug!("Receive RangeRequest {:?}", request);

        let range_request = request.into_inner();
        let propose_id = self.generate_propose_id();
        let result = self
            .propose(propose_id.clone(), Request::RequestRange(range_request))
            .await;

        match result {
            Ok((res_op, sync_res)) => {
                let res = XlineRpcServer::parse_response_op(res_op.decode());
                let revision = sync_res.revision();
                debug!("Get revision {:?} for RangeRequest", revision);
                let res = Self::update_header_revision(res, revision);
                if let Response::ResponseRange(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(e) => panic!("Failed to receive response from KV storage, {e}"),
        }
    }

    /// Put puts the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        debug!("Receive PutRequest {:?}", request);
        let put_request = request.into_inner();
        let propose_id = self.generate_propose_id();
        let result = self
            .propose(propose_id.clone(), Request::RequestPut(put_request))
            .await;
        match result {
            Ok((res_op, sync_res)) => {
                let res = XlineRpcServer::parse_response_op(res_op.decode());
                let revision = sync_res.revision();
                debug!("Get revision {:?} for PutRequest", revision);
                let res = Self::update_header_revision(res, revision);
                if let Response::ResponsePut(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(e) => panic!("Failed to receive response from KV storage, {e}"),
        }
    }

    /// DeleteRange deletes the given range from the key-value store.
    /// A delete request increments the revision of the key-value store
    /// and generates a delete event in the event history for every deleted key.
    async fn delete_range(
        &self,
        request: tonic::Request<DeleteRangeRequest>,
    ) -> Result<tonic::Response<DeleteRangeResponse>, tonic::Status> {
        debug!("Receive DeleteRangeRequest {:?}", request);
        let delete_range_request = request.into_inner();
        let propose_id = self.generate_propose_id();
        let result = self
            .propose(
                propose_id.clone(),
                Request::RequestDeleteRange(delete_range_request),
            )
            .await;
        match result {
            Ok((res_op, sync_res)) => {
                let res = XlineRpcServer::parse_response_op(res_op.decode());
                let revision = sync_res.revision();
                debug!("Get revision {:?} for PutRequest", revision);
                let res = Self::update_header_revision(res, revision);
                if let Response::ResponseDeleteRange(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(e) => panic!("Failed to receive response from KV storage, {e}"),
        }
    }

    /// Txn processes multiple requests in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed request.
    /// It is not allowed to modify the same key several times within one txn.
    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        debug!("Receive TxnRequest {:?}", request);
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Not Implemented".to_owned(),
        ))
    }

    /// Compact compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    async fn compact(
        &self,
        request: tonic::Request<CompactionRequest>,
    ) -> Result<tonic::Response<CompactionResponse>, tonic::Status> {
        debug!("Receive CompactionRequest {:?}", request);
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Not Implemented".to_owned(),
        ))
    }
}
