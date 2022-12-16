use std::sync::Arc;

use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use tokio::time::Duration;
use tracing::debug;
use uuid::Uuid;

use super::{
    command::{Command, CommandResponse, KeyRange, SyncResponse},
    kv_server::KvServer,
};
use crate::{
    rpc::{
        Compare, CompareResult, CompareTarget, DeleteRangeRequest, Lock, LockRequest, LockResponse,
        PutRequest, Request, RequestOp, RequestWithToken, Response, TargetUnion, TxnRequest,
        UnlockRequest, UnlockResponse,
    },
    storage::KvStore,
};

/// Lock Server
//#[derive(Debug)]
#[allow(dead_code)] // Remove this after feature is completed
pub(crate) struct LockServer {
    /// KV storage
    storage: Arc<KvStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
}

impl LockServer {
    /// New `LockServer`
    pub(crate) fn new(storage: Arc<KvStore>, client: Arc<Client<Command>>, name: String) -> Self {
        Self {
            storage,
            client,
            name,
        }
    }

    /// Propose request and get result
    async fn propose(
        &self,
        propose_id: ProposeId,
        request: Request,
    ) -> Result<(CommandResponse, SyncResponse), ProposeError> {
        let key_ranges = match request {
            Request::RequestRange(_) | Request::RequestPut(_) => {
                unreachable!("Propose RequestRange and RequestPut from LockServer is not allowed")
            }
            Request::RequestDeleteRange(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            }],
            Request::RequestTxn(ref req) => req
                .compare
                .iter()
                .map(|cmp| KeyRange {
                    start: cmp.key.clone(),
                    end: cmp.range_end.clone(),
                })
                .collect(),
        };
        let request_op = RequestOp {
            request: Some(request),
        };
        let wrapper = RequestWithToken::new(request_op.into());
        let cmd = Command::new(key_ranges, wrapper, propose_id);
        self.client.propose_indexed(cmd.clone()).await
    }

    /// Generate propose id
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }
}

#[tonic::async_trait]
impl Lock for LockServer {
    /// Lock acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to etcd only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    async fn lock(
        &self,
        request: tonic::Request<LockRequest>,
    ) -> Result<tonic::Response<LockResponse>, tonic::Status> {
        debug!("Receive LockRequest {:?}", request);
        let lock_request = request.into_inner();
        let key = lock_request.name;

        #[allow(clippy::as_conversions)] // Converting Enum to i32 is safe.
        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Create as i32,
            key: key.clone(),
            range_end: vec![],
            target_union: Some(TargetUnion::CreateRevision(0)),
        };

        let success = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: key.clone(),
                value: vec![0],
                ..PutRequest::default()
            })),
        };

        let txn_request = TxnRequest {
            compare: vec![compare],
            success: vec![success],
            failure: vec![],
        };
        loop {
            let result = self
                .propose(
                    self.generate_propose_id(),
                    Request::RequestTxn(txn_request.clone()),
                )
                .await;
            match result {
                Ok((res_op, sync_res)) => {
                    let mut res = KvServer::parse_response_op(res_op.decode().into());
                    let revision = sync_res.revision();
                    debug!("Get revision {:?} for LockRequest", revision);
                    KvServer::update_header_revision(&mut res, revision);
                    if let Response::ResponseTxn(response) = res {
                        if response.succeeded {
                            let resp = LockResponse {
                                header: response.header,
                                key: key.clone(),
                            };
                            return Ok(tonic::Response::new(resp));
                        }
                    } else {
                        panic!("Receive wrong response {:?} for LockRequest", res);
                    }
                }
                Err(e) => panic!("Failed to receive response from KV storage, {e}"),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    /// Unlock takes a key returned by Lock and releases the hold on lock. The
    /// next Lock caller waiting for the lock will then be woken up and given
    /// ownership of the lock.
    async fn unlock(
        &self,
        request: tonic::Request<UnlockRequest>,
    ) -> Result<tonic::Response<UnlockResponse>, tonic::Status> {
        debug!("Receive UnlockRequest {:?}", request);
        let unlock_request = request.into_inner();
        let key = unlock_request.key;

        let delete_request = DeleteRangeRequest {
            key: key.clone(),
            range_end: vec![],
            ..DeleteRangeRequest::default()
        };
        let result = self
            .propose(
                self.generate_propose_id(),
                Request::RequestDeleteRange(delete_request.clone()),
            )
            .await;
        match result {
            Ok((res_op, sync_res)) => {
                let mut res = KvServer::parse_response_op(res_op.decode().into());
                let revision = sync_res.revision();
                debug!("Get revision {:?} for UnlockRequest", revision);
                KvServer::update_header_revision(&mut res, revision);
                if let Response::ResponseDeleteRange(response) = res {
                    Ok(tonic::Response::new(UnlockResponse {
                        header: response.header,
                    }))
                } else {
                    panic!("Receive wrong response {:?} for LockRequest", res);
                }
            }
            Err(e) => panic!("Failed to receive response from KV storage, {e}"),
        }
    }
}
