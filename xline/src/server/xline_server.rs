use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use log::debug;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tonic::transport::Server;

use crate::rpc::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, Kv, KvServer,
    PutRequest, PutResponse, RangeRequest, RangeResponse, Request, RequestOp, Response, ResponseOp,
    TxnRequest, TxnResponse,
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
    /// Address of members
    members: Vec<SocketAddr>,
    /// Kv storage
    storage: Arc<Storage>,
}

/// server storage
#[derive(Debug)]
struct Storage {
    /// KV storage
    storage: KvStore,
}

impl Storage {
    /// Send execution request
    async fn send_req(&self, req: ExecutionRequset) {
        self.storage.send_req(req).await;
    }
}

/// Xline Server Inner
#[derive(Debug)]
struct XlineRpcServer {
    /// KV storage
    storage: Arc<Storage>,
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
}

impl XlineServer {
    /// New `XlineServer`
    pub(crate) fn new(name: String, addr: SocketAddr, members: Vec<SocketAddr>) -> Self {
        Self {
            name,
            addr,
            members,
            storage: Arc::new(Storage {
                storage: KvStore::new(),
            }),
        }
    }

    /// Start `XlineServer`
    pub(crate) async fn start(&self) -> Result<()> {
        let rpc_server = XlineRpcServer {
            storage: Arc::clone(&self.storage),
        };
        Ok(Server::builder()
            .add_service(KvServer::new(rpc_server))
            .serve(self.addr)
            .await?)
    }
}

/// Command to run consensus protocal
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Command {
    /// Key of request
    key: String,
    ///
    /// Encoded request data
    request: Vec<u8>,
}

impl Command {
    /// New `Command`
    pub(crate) fn new(key: String, request: Vec<u8>) -> Self {
        Self { key, request }
    }

    /*
    /// Get key of `Command`
    pub(crate) fn key(&self) -> &String {
        &self.key
    }
    /// Get request of `Command`
    pub(crate) fn request(&self) -> &Vec<u8> {
        &self.request
    }
    */

    /// Consume `Command` and get ownership of each field
    pub(crate) fn unpack(self) -> (String, Vec<u8>) {
        let Self { key, request } = self;
        (key, request)
    }
}

/// Execution Request
#[derive(Debug)]
pub(crate) struct ExecutionRequset {
    /// Command to execute
    command: Command,
    /// Execution result sender
    notifier: Sender<ResponseOp>,
}

impl ExecutionRequset {
    /// New `ExecutionRequest`
    pub(crate) fn new(command: Command) -> (Self, Receiver<ResponseOp>) {
        let (notifier, receiver) = channel();
        (Self { command, notifier }, receiver)
    }
    /// Consume `ExecutionRequest` and get ownership of each field
    pub(crate) fn unpack(self) -> (Command, Sender<ResponseOp>) {
        let Self { command, notifier } = self;
        (command, notifier)
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
        let key = std::str::from_utf8(&range_request.key.clone())
            .unwrap_or_else(|_| panic!("Failed to convert Vec<u8> to String"))
            .to_owned();
        let range_request_op = RequestOp {
            request: Some(Request::RequestRange(range_request)),
        };
        let cmd = Command::new(key, range_request_op.encode_to_vec());
        let (execution_req, receiver) = ExecutionRequset::new(cmd);
        self.storage.send_req(execution_req).await;
        match receiver.await {
            Ok(res_op) => {
                let res = XlineRpcServer::parse_response_op(res_op);
                if let Response::ResponseRange(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(_) => panic!("Failed to receive response from KV storage"),
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
        let key = std::str::from_utf8(&put_request.key.clone())
            .unwrap_or_else(|_| panic!("Failed to convert Vec<u8> to String"))
            .to_owned();
        let put_request_op = RequestOp {
            request: Some(Request::RequestPut(put_request)),
        };
        let cmd = Command::new(key, put_request_op.encode_to_vec());
        let (execution_req, receiver) = ExecutionRequset::new(cmd);
        self.storage.send_req(execution_req).await;
        match receiver.await {
            Ok(res_op) => {
                let res = XlineRpcServer::parse_response_op(res_op);
                if let Response::ResponsePut(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(_) => panic!("Failed to receive response from KV storage"),
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
        let key = std::str::from_utf8(&delete_range_request.key.clone())
            .unwrap_or_else(|_| panic!("Failed to convert Vec<u8> to String"))
            .to_owned();
        let delete_range_request_op = RequestOp {
            request: Some(Request::RequestDeleteRange(delete_range_request)),
        };
        let cmd = Command::new(key, delete_range_request_op.encode_to_vec());
        let (execution_req, receiver) = ExecutionRequset::new(cmd);
        self.storage.send_req(execution_req).await;
        match receiver.await {
            Ok(res_op) => {
                let res = XlineRpcServer::parse_response_op(res_op);
                if let Response::ResponseDeleteRange(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(_) => panic!("Failed to receive response from KV storage"),
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
