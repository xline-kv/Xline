/// Lease
mod lease;
/// Lease heap
mod lease_heap;

use std::{collections::HashMap, sync::Arc};

use clippy_utilities::Cast;
use curp::{cmd::ProposeId, error::ExecuteError};
use lock_utils::parking_lot_lock::MutexMap;
use log::debug;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::header_gen::HeaderGenerator;
use crate::rpc::{
    LeaseGrantRequest, LeaseGrantResponse, LeaseRevokeRequest, LeaseRevokeResponse,
    RequestWithToken, RequestWrapper, ResponseHeader, ResponseWrapper,
};
use crate::server::command::{CommandResponse, SyncResponse};

use self::lease_heap::{LeaseHeap, LeaseWithTime};

use super::req_ctx::RequestCtx;

pub(crate) use self::lease::Lease;

/// Max lease ttl
const MAX_LEASE_TTL: i64 = 9_000_000_000;
/// Min lease ttl
const MIN_LEASE_TTL: i64 = 1; // TODO: this num should calculated by electionticks and heartbeat

/// Message that will be sent to kv store
pub(crate) struct DeleteMessage {
    /// keys
    keys: Vec<Vec<u8>>,
    /// Send response message after delete
    res_sender: oneshot::Sender<()>,
}

impl DeleteMessage {
    /// New `DeleteMessage`
    pub(crate) fn new(keys: Vec<Vec<u8>>) -> (Self, oneshot::Receiver<()>) {
        let (res_sender, res_receiver) = oneshot::channel();
        (Self { keys, res_sender }, res_receiver)
    }

    /// Unpack `DeleteMessage`
    pub(crate) fn unpack(self) -> (Vec<Vec<u8>>, oneshot::Sender<()>) {
        (self.keys, self.res_sender)
    }
}

/// KV store
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct LeaseStore {
    /// Lease store Backend
    inner: Arc<LeaseStoreBackend>,
}

/// KV store inner
#[derive(Debug)]
pub(crate) struct LeaseStoreBackend {
    /// lease id to lease
    lease_map: Mutex<HashMap<i64, Lease>>,
    /// key to lease id
    item_map: Mutex<HashMap<Vec<u8>, i64>>,
    /// lease heap
    expired_queue: Mutex<LeaseHeap>,
    /// delete channel
    del_tx: mpsc::Sender<DeleteMessage>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<ProposeId, RequestCtx>>,
    /// Current node is leader or not
    is_leader: Arc<Mutex<bool>>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
}

impl LeaseStore {
    /// New `KvStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new(
        del_tx: mpsc::Sender<DeleteMessage>,
        is_leader: Arc<Mutex<bool>>,
        header_gen: Arc<HeaderGenerator>,
    ) -> Self {
        let inner = Arc::new(LeaseStoreBackend::new(del_tx, is_leader, header_gen));
        Self { inner }
    }

    /// execute a auth request
    pub(crate) fn execute(
        &self,
        id: ProposeId,
        request: RequestWithToken,
    ) -> Result<CommandResponse, ExecuteError> {
        self.inner
            .handle_lease_requests(id, request.request)
            .map(CommandResponse::new)
    }

    /// sync a auth request
    pub(crate) async fn after_sync(&self, id: &ProposeId) -> SyncResponse {
        SyncResponse::new(self.inner.sync_request(id).await)
    }

    /// Check if the node is leader
    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    /// Attach key to lease
    pub(crate) fn attach(&self, lease_id: i64, key: Vec<u8>) -> Result<(), ExecuteError> {
        self.inner.lease_map.map_lock(|mut map| {
            if let Some(lease) = map.get_mut(&lease_id) {
                lease.insert_key(key.clone());
                self.inner.item_map.map_lock(|mut item_map| {
                    let _ignore = item_map.insert(key, lease_id);
                });
                Ok(())
            } else {
                Err(ExecuteError::InvalidCommand("lease not found".to_owned()))
            }
        })
    }

    /// Detach key from lease
    pub(crate) fn detach(&self, lease_id: i64, key: &[u8]) -> Result<(), ExecuteError> {
        self.inner
            .lease_map
            .map_lock(|mut map| match map.get_mut(&lease_id) {
                Some(lease) => {
                    lease.remove_key(key);
                    self.inner.item_map.map_lock(|mut item_map| {
                        let _ignore = item_map.remove(key);
                    });
                    Ok(())
                }
                None => Err(ExecuteError::InvalidCommand("lease not found".to_owned())),
            })
    }

    /// Get lease id by given key
    pub(crate) fn get_lease(&self, key: &[u8]) -> i64 {
        self.inner
            .item_map
            .map_lock(|map| map.get(key).copied().unwrap_or(0))
    }

    /// Get lease by id
    pub(crate) fn look_up(&self, lease_id: i64) -> Option<Lease> {
        self.inner
            .lease_map
            .map_lock(|map| map.get(&lease_id).cloned())
    }

    /// Get all leases
    pub(crate) fn leases(&self) -> Vec<Lease> {
        self.inner.lease_map.map_lock(|map| {
            let mut leases = map.values().cloned().collect::<Vec<_>>();
            leases.sort_by_key(Lease::remaining);
            leases
        })
    }

    /// Find expired leases
    pub(crate) fn find_expired_leases(&self) -> Vec<i64> {
        let mut expired_leases = vec![];
        self.inner.expired_queue.map_lock(|mut queue| {
            while let Some(le) = queue.peek() {
                if le.expired() {
                    #[allow(clippy::unwrap_used)] // queue.peek() returns Some
                    let item = queue.pop().unwrap();
                    expired_leases.push(item.id());
                } else {
                    break;
                }
            }
        });
        self.inner.lease_map.map_lock(|map| {
            expired_leases
                .into_iter()
                .filter(|id| map.contains_key(id))
                .collect::<Vec<_>>()
        })
    }

    /// Get keys attached to a lease
    pub(crate) fn get_keys(&self, lease_id: i64) -> Vec<Vec<u8>> {
        self.inner
            .lease_map
            .map_lock(|map| map.get(&lease_id).map(Lease::keys).unwrap_or_default())
    }

    /// Keep alive a lease
    pub(crate) fn keep_alive(&self, lease_id: i64) -> Result<i64, tonic::Status> {
        if !self.is_leader() {
            return Err(tonic::Status::aborted("not leader"));
        }

        let lease = self.inner.lease_map.map_lock(|mut map| {
            map.get_mut(&lease_id).map_or_else(
                || Err(tonic::Status::not_found("lease not found")),
                |lease| {
                    if lease.expired() {
                        return Err(tonic::Status::aborted("lease expired"));
                    }
                    lease.refresh(0);
                    Ok(lease.clone())
                },
            )
        })?;
        let le = LeaseWithTime::new(
            lease.id(),
            lease
                .expiry()
                .unwrap_or_else(|| panic!("expiry should be some")),
        );
        self.inner
            .expired_queue
            .map_lock(|mut queue| queue.insert_or_update(le));
        Ok(lease.ttl().as_secs().cast())
    }

    /// Generate `ResponseHeader`
    pub(crate) fn gen_header(&self) -> ResponseHeader {
        self.inner.header_gen.gen_header()
    }
}

impl LeaseStoreBackend {
    /// New `KvStoreBackend`
    pub(crate) fn new(
        del_tx: mpsc::Sender<DeleteMessage>,
        is_leader: Arc<Mutex<bool>>,
        header_gen: Arc<HeaderGenerator>,
    ) -> Self {
        Self {
            lease_map: Mutex::new(HashMap::new()),
            item_map: Mutex::new(HashMap::new()),
            expired_queue: Mutex::new(LeaseHeap::new()),
            sp_exec_pool: Mutex::new(HashMap::new()),
            del_tx,
            is_leader,
            header_gen,
        }
    }

    /// Check if the node is leader
    fn is_leader(&self) -> bool {
        *self.is_leader.lock()
    }

    /// Handle kv requests
    fn handle_lease_requests(
        &self,
        id: ProposeId,
        wrapper: RequestWrapper,
    ) -> Result<ResponseWrapper, ExecuteError> {
        debug!("Receive request {:?}", wrapper);
        #[allow(clippy::wildcard_enum_match_arm)]
        let res = match wrapper {
            RequestWrapper::LeaseGrantRequest(ref req) => {
                debug!("Receive LeaseGrantRequest {:?}", req);
                self.handle_lease_grant_request(req).map(Into::into)
            }
            RequestWrapper::LeaseRevokeRequest(ref req) => {
                debug!("Receive LeaseRevokeRequest {:?}", req);
                self.handle_lease_revoke_request(req).map(Into::into)
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };
        self.sp_exec_pool.map_lock(|mut pool| {
            let _prev = pool.insert(id, RequestCtx::new(wrapper, res.is_err()));
        });
        res
    }

    /// Handle `LeaseGrantRequest`
    fn handle_lease_grant_request(
        &self,
        req: &LeaseGrantRequest,
    ) -> Result<LeaseGrantResponse, ExecuteError> {
        if req.id == 0 {
            return Err(ExecuteError::InvalidCommand("lease not found".to_owned()));
        }

        if req.ttl > MAX_LEASE_TTL {
            return Err(ExecuteError::InvalidCommand(format!(
                "lease ttl too large: {}",
                req.ttl
            )));
        }

        if self.lease_map.map_lock(|map| map.contains_key(&req.id)) {
            return Err(ExecuteError::InvalidCommand(format!(
                "lease already exists: {}",
                req.id
            )));
        }

        Ok(LeaseGrantResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            id: req.id,
            ttl: req.ttl,
            error: "".to_owned(),
        })
    }

    /// Handle `LeaseRevokeRequest`
    fn handle_lease_revoke_request(
        &self,
        req: &LeaseRevokeRequest,
    ) -> Result<LeaseRevokeResponse, ExecuteError> {
        self.lease_map.map_lock(|map| {
            if map.contains_key(&req.id) {
                Ok(LeaseRevokeResponse {
                    header: Some(self.header_gen.gen_header_without_revision()),
                })
            } else {
                Err(ExecuteError::InvalidCommand("lease not found".to_owned()))
            }
        })
    }

    /// Sync `RequestWithToken`
    async fn sync_request(&self, id: &ProposeId) -> i64 {
        let ctx = self.sp_exec_pool.lock().remove(id).unwrap_or_else(|| {
            panic!("Failed to get speculative execution propose id {:?}", id);
        });
        if ctx.met_err() {
            return self.header_gen.revision();
        }
        let wrapper = ctx.req();
        #[allow(clippy::wildcard_enum_match_arm)]
        match wrapper {
            RequestWrapper::LeaseGrantRequest(req) => {
                debug!("Sync LeaseGrantRequest {:?}", req);
                self.sync_lease_grant_request(&req);
            }
            RequestWrapper::LeaseRevokeRequest(req) => {
                debug!("Sync LeaseRevokeRequest {:?}", req);
                self.sync_lease_revoke_request(&req).await;
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };
        self.header_gen.revision()
    }

    /// Sync `LeaseGrantRequest`
    fn sync_lease_grant_request(&self, req: &LeaseGrantRequest) {
        if (req.id == 0)
            || (req.ttl > MAX_LEASE_TTL)
            || (self.lease_map.map_lock(|map| map.contains_key(&req.id)))
        {
            return;
        }
        let mut lease = Lease::new(req.id, req.ttl.max(MIN_LEASE_TTL).cast());
        if self.is_leader() {
            lease.refresh(0);
            let le = LeaseWithTime::new(
                lease.id(),
                lease
                    .expiry()
                    .unwrap_or_else(|| panic!("expiry should be some")),
            );
            self.expired_queue
                .map_lock(|mut heap| heap.insert_or_update(le));
        } else {
            lease.forever();
        }
        let _ignore = self.lease_map.map_lock(|mut map| map.insert(req.id, lease));
        // TODO Persistence
    }

    /// Sync `LeaseRevokeRequest`
    async fn sync_lease_revoke_request(&self, req: &LeaseRevokeRequest) {
        let lease = match self.lease_map.map_lock(|mut map| map.remove(&req.id)) {
            Some(l) => l,
            None => return,
        };
        let mut keys = lease.keys();
        if keys.is_empty() {
            return;
        }
        keys.sort(); // all node delete in same order
        let (msg, rx) = DeleteMessage::new(keys);
        assert!(
            self.del_tx.send(msg).await.is_ok(),
            "Failed to send delete keys"
        );
        assert!(rx.await.is_ok(), "Failed to receive delete keys response");
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, time::Duration};

    use tracing::info;

    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_lease_storage() -> Result<(), Box<dyn Error>> {
        let (del_tx, mut del_rx) = mpsc::channel(128);
        let is_leader = Arc::new(Mutex::new(true));
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let ls = LeaseStore::new(del_tx, is_leader, header_gen);
        let _handle = tokio::spawn(async move {
            while let Some(msg) = del_rx.recv().await {
                let (keys, tx) = msg.unpack();
                info!("Delete keys {:?}", keys);
                assert!(tx.send(()).is_ok(), "Failed to send delete keys response");
            }
        });

        let req1 = RequestWithToken::new(LeaseGrantRequest { ttl: 10, id: 1 }.into());
        let _ignore1 = exe_and_sync_req(&ls, req1).await?;

        let lo = ls.look_up(1);
        assert!(lo.is_some());
        #[allow(clippy::unwrap_used)] // checked
        let lo = lo.unwrap();
        assert_eq!(lo.id(), 1);
        assert_eq!(lo.ttl(), Duration::from_secs(10));
        assert_eq!(ls.leases().len(), 1);

        let attach_non_existing_lease = ls.attach(0, "key".into());
        assert!(attach_non_existing_lease.is_err());
        let attach_existing_lease = ls.attach(1, "key".into());
        assert!(attach_existing_lease.is_ok());

        let req2 = RequestWithToken::new(LeaseRevokeRequest { id: 1 }.into());
        let _ignore2 = exe_and_sync_req(&ls, req2).await?;
        assert!(ls.look_up(1).is_none());
        assert!(ls.leases().is_empty());

        Ok(())
    }

    async fn exe_and_sync_req(
        ls: &LeaseStore,
        req: RequestWithToken,
    ) -> Result<ResponseWrapper, ExecuteError> {
        let id = ProposeId::new("testid".to_owned());
        let cmd_res = ls.execute(id.clone(), req)?;
        let _ignore = ls.after_sync(&id).await;
        Ok(cmd_res.decode())
    }
}
