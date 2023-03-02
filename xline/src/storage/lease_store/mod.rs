/// Lease
mod lease;
/// Lease heap
mod lease_queue;
/// Lease cmd, used by other storages
mod message;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use clippy_utilities::Cast;
use curp::cmd::ProposeId;
use log::debug;
use parking_lot::{Mutex, RwLock};
use prost::Message;
use tokio::sync::mpsc;
use utils::parking_lot_lock::MutexMap;

use self::lease_queue::LeaseQueue;
pub(crate) use self::{
    lease::Lease,
    message::{DeleteMessage, LeaseMessage},
};
use super::{storage_api::StorageApi, ExecuteError, RequestCtx};
use crate::{
    header_gen::HeaderGenerator,
    rpc::{
        LeaseGrantRequest, LeaseGrantResponse, LeaseRevokeRequest, LeaseRevokeResponse, PbLease,
        RequestWithToken, RequestWrapper, ResponseHeader, ResponseWrapper,
    },
    server::command::{CommandResponse, SyncResponse},
    state::State,
};

/// Max lease ttl
const MAX_LEASE_TTL: i64 = 9_000_000_000;
/// Min lease ttl
const MIN_LEASE_TTL: i64 = 1; // TODO: this num should calculated by election ticks and heartbeat

/// Lease store
#[derive(Debug)]
pub(crate) struct LeaseStore<DB>
where
    DB: StorageApi,
{
    /// Lease store Backend
    inner: Arc<LeaseStoreBackend<DB>>,
}

/// Collection of lease related data
#[derive(Debug)]
struct LeaseCollection {
    /// lease id to lease
    lease_map: HashMap<i64, Lease>,
    /// key to lease id
    item_map: HashMap<Vec<u8>, i64>,
    /// lease queue
    expired_queue: LeaseQueue,
}

impl LeaseCollection {
    /// New `LeaseCollection`
    fn new() -> Self {
        Self {
            lease_map: HashMap::new(),
            item_map: HashMap::new(),
            expired_queue: LeaseQueue::new(),
        }
    }

    /// Find expired leases
    fn find_expired_leases(&mut self) -> Vec<i64> {
        let mut expired_leases = vec![];
        while let Some(expiry) = self.expired_queue.peek() {
            if *expiry <= Instant::now() {
                #[allow(clippy::unwrap_used)] // queue.peek() returns Some
                let id = self.expired_queue.pop().unwrap();
                if self.lease_map.contains_key(&id) {
                    expired_leases.push(id);
                }
            } else {
                break;
            }
        }
        expired_leases
    }

    /// Renew lease
    fn renew(&mut self, lease_id: i64) -> Result<i64, ExecuteError> {
        self.lease_map.get_mut(&lease_id).map_or_else(
            || Err(ExecuteError::lease_not_found(lease_id)),
            |lease| {
                if lease.expired() {
                    return Err(ExecuteError::lease_expired(lease_id));
                }
                let expiry = lease.refresh(Duration::default());
                let _ignore = self.expired_queue.update(lease_id, expiry);
                Ok(lease.ttl().as_secs().cast())
            },
        )
    }

    /// Attach key to lease
    fn attach(&mut self, lease_id: i64, key: Vec<u8>) -> Result<(), ExecuteError> {
        self.lease_map.get_mut(&lease_id).map_or_else(
            || Err(ExecuteError::lease_not_found(lease_id)),
            |lease| {
                lease.insert_key(key.clone());
                let _ignore = self.item_map.insert(key, lease_id);
                Ok(())
            },
        )
    }

    /// Detach key from lease
    fn detach(&mut self, lease_id: i64, key: &[u8]) -> Result<(), ExecuteError> {
        self.lease_map.get_mut(&lease_id).map_or_else(
            || Err(ExecuteError::lease_not_found(lease_id)),
            |lease| {
                lease.remove_key(key);
                let _ignore = self.item_map.remove(key);
                Ok(())
            },
        )
    }

    /// Check if a lease exists
    fn contains_lease(&self, lease_id: i64) -> bool {
        self.lease_map.contains_key(&lease_id)
    }

    /// Grant a lease
    fn grant(&mut self, lease_id: i64, ttl: i64, is_leader: bool) -> PbLease {
        let mut lease = Lease::new(lease_id, ttl.max(MIN_LEASE_TTL).cast());
        if is_leader {
            let expiry = lease.refresh(Duration::ZERO);
            let _ignore = self.expired_queue.insert(lease_id, expiry);
        } else {
            lease.forever();
        }
        let _ignore = self.lease_map.insert(lease_id, lease.clone());
        PbLease {
            id: lease.id(),
            ttl: lease.ttl().as_secs().cast(),
            remaining_ttl: lease.remaining_ttl().as_secs().cast(),
        }
    }

    /// Revokes a lease
    fn revoke(&mut self, lease_id: i64) -> Option<Lease> {
        self.lease_map.remove(&lease_id)
    }

    /// Demote current node
    fn demote(&mut self) {
        self.lease_map.values_mut().for_each(Lease::forever);
        self.expired_queue.clear();
    }

    /// Promote current node
    fn promote(&mut self, extend: Duration) {
        for lease in self.lease_map.values_mut() {
            let expiry = lease.refresh(extend);
            let _ignore = self.expired_queue.insert(lease.id(), expiry);
        }
    }
}

/// Lease store inner
#[derive(Debug)]
pub(crate) struct LeaseStoreBackend<DB>
where
    DB: StorageApi,
{
    /// Table name
    table: String,
    /// lease collection
    lease_collection: RwLock<LeaseCollection>,
    /// Db to store lease
    db: Arc<DB>,
    /// delete channel
    del_tx: mpsc::Sender<DeleteMessage>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<ProposeId, RequestCtx>>,
    /// Current node is leader or not
    state: Arc<State>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
}

impl<DB> LeaseStore<DB>
where
    DB: StorageApi,
{
    /// New `LeaseStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new(
        del_tx: mpsc::Sender<DeleteMessage>,
        mut lease_cmd_rx: mpsc::Receiver<LeaseMessage>,
        state: Arc<State>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
    ) -> Self {
        let inner = Arc::new(LeaseStoreBackend::new(del_tx, state, header_gen, db));
        let _handle = tokio::spawn({
            let inner = Arc::clone(&inner);
            async move {
                while let Some(lease_msg) = lease_cmd_rx.recv().await {
                    match lease_msg {
                        LeaseMessage::Attach(tx, lease_id, key) => {
                            assert!(
                                tx.send(inner.attach(lease_id, key)).is_ok(),
                                "receiver is closed"
                            );
                        }
                        LeaseMessage::Detach(tx, lease_id, key) => {
                            assert!(
                                tx.send(inner.detach(lease_id, &key)).is_ok(),
                                "receiver is closed"
                            );
                        }
                        LeaseMessage::GetLease(tx, key) => {
                            assert!(tx.send(inner.get_lease(&key)).is_ok(), "receiver is closed");
                        }
                        LeaseMessage::LookUp(tx, lease_id) => {
                            assert!(
                                tx.send(inner.look_up(lease_id)).is_ok(),
                                "receiver is closed"
                            );
                        }
                    }
                }
            }
        });
        Self { inner }
    }

    /// execute a lease request
    pub(crate) fn execute(
        &self,
        id: ProposeId,
        request: RequestWithToken,
    ) -> Result<CommandResponse, ExecuteError> {
        self.inner
            .handle_lease_requests(id, request.request)
            .map(CommandResponse::new)
    }

    /// sync a lease request
    pub(crate) async fn after_sync(&self, id: &ProposeId) -> Result<SyncResponse, ExecuteError> {
        self.inner.sync_request(id).await.map(SyncResponse::new)
    }

    /// Check if the node is leader
    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    /// Get lease by id
    pub(crate) fn look_up(&self, lease_id: i64) -> Option<Lease> {
        self.inner.look_up(lease_id)
    }

    /// Get all leases
    pub(crate) fn leases(&self) -> Vec<Lease> {
        let mut leases = self
            .inner
            .lease_collection
            .read()
            .lease_map
            .values()
            .cloned()
            .collect::<Vec<_>>();
        leases.sort_by_key(Lease::remaining);
        leases
    }

    /// Find expired leases
    pub(crate) fn find_expired_leases(&self) -> Vec<i64> {
        self.inner.lease_collection.write().find_expired_leases()
    }

    /// Get keys attached to a lease
    pub(crate) fn get_keys(&self, lease_id: i64) -> Vec<Vec<u8>> {
        self.inner
            .lease_collection
            .read()
            .lease_map
            .get(&lease_id)
            .map(Lease::keys)
            .unwrap_or_default()
    }

    /// Keep alive a lease
    pub(crate) fn keep_alive(&self, lease_id: i64) -> Result<i64, ExecuteError> {
        if !self.is_leader() {
            return Err(ExecuteError::lease_not_leader());
        }
        self.inner.lease_collection.write().renew(lease_id)
    }

    /// Generate `ResponseHeader`
    pub(crate) fn gen_header(&self) -> ResponseHeader {
        self.inner.header_gen.gen_header()
    }

    /// Demote current node
    pub(crate) fn demote(&self) {
        self.inner.lease_collection.write().demote();
    }

    /// Promote current node
    pub(crate) fn promote(&self, extend: Duration) {
        self.inner.lease_collection.write().promote(extend);
    }
}

impl<DB> LeaseStoreBackend<DB>
where
    DB: StorageApi,
{
    /// New `LeaseStoreBackend`
    pub(crate) fn new(
        del_tx: mpsc::Sender<DeleteMessage>,
        state: Arc<State>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
    ) -> Self {
        Self {
            table: "lease".to_owned(),
            lease_collection: RwLock::new(LeaseCollection::new()),
            db,
            sp_exec_pool: Mutex::new(HashMap::new()),
            del_tx,
            state,
            header_gen,
        }
    }

    /// Check if the node is leader
    fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    /// Attach key to lease
    pub(crate) fn attach(&self, lease_id: i64, key: Vec<u8>) -> Result<(), ExecuteError> {
        self.lease_collection.write().attach(lease_id, key)
    }

    /// Detach key from lease
    pub(crate) fn detach(&self, lease_id: i64, key: &[u8]) -> Result<(), ExecuteError> {
        self.lease_collection.write().detach(lease_id, key)
    }

    /// Get lease id by given key
    pub(crate) fn get_lease(&self, key: &[u8]) -> i64 {
        self.lease_collection
            .read()
            .item_map
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    /// Get lease by id
    pub(crate) fn look_up(&self, lease_id: i64) -> Option<Lease> {
        self.lease_collection
            .read()
            .lease_map
            .get(&lease_id)
            .cloned()
    }

    /// Handle lease requests
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
            return Err(ExecuteError::lease_not_found(0));
        }
        if req.ttl > MAX_LEASE_TTL {
            return Err(ExecuteError::lease_ttl_too_large(req.ttl));
        }
        if self.lease_collection.read().contains_lease(req.id) {
            return Err(ExecuteError::lease_already_exists(req.id));
        }

        Ok(LeaseGrantResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            id: req.id,
            ttl: req.ttl,
            error: String::new(),
        })
    }

    /// Handle `LeaseRevokeRequest`
    fn handle_lease_revoke_request(
        &self,
        req: &LeaseRevokeRequest,
    ) -> Result<LeaseRevokeResponse, ExecuteError> {
        if self.lease_collection.read().contains_lease(req.id) {
            Ok(LeaseRevokeResponse {
                header: Some(self.header_gen.gen_header_without_revision()),
            })
        } else {
            Err(ExecuteError::lease_not_found(req.id))
        }
    }

    /// Sync `RequestWithToken`
    async fn sync_request(&self, id: &ProposeId) -> Result<i64, ExecuteError> {
        let ctx = self.sp_exec_pool.lock().remove(id).unwrap_or_else(|| {
            panic!("Failed to get speculative execution propose id {id:?}");
        });
        if ctx.met_err() {
            return Ok(self.header_gen.revision());
        }
        let wrapper = ctx.req();
        #[allow(clippy::wildcard_enum_match_arm)]
        match wrapper {
            RequestWrapper::LeaseGrantRequest(req) => {
                debug!("Sync LeaseGrantRequest {:?}", req);
                self.sync_lease_grant_request(&req)?;
            }
            RequestWrapper::LeaseRevokeRequest(req) => {
                debug!("Sync LeaseRevokeRequest {:?}", req);
                self.sync_lease_revoke_request(&req).await?;
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };
        Ok(self.header_gen.revision())
    }

    /// Sync `LeaseGrantRequest`
    fn sync_lease_grant_request(&self, req: &LeaseGrantRequest) -> Result<(), ExecuteError> {
        let lease = self
            .lease_collection
            .write()
            .grant(req.id, req.ttl, self.is_leader());
        self.insert(lease.id, &lease)
    }

    /// Insert a `PbLease`
    fn insert(&self, lease_id: i64, kv: &PbLease) -> Result<(), ExecuteError> {
        let key = lease_id.encode_to_vec();
        let value = kv.encode_to_vec();
        self.db.insert(&self.table, key, value)
    }

    /// Delete a `PbLease` by `lease_id`
    fn delete(&self, lease_id: i64) -> Result<(), ExecuteError> {
        let key = lease_id.encode_to_vec();
        self.db
            .delete(&self.table, key)
            .map_err(|e| ExecuteError::DbError(format!("Failed to delete Lease, error: {e}")))?;
        Ok(())
    }

    /// Sync `LeaseRevokeRequest`
    async fn sync_lease_revoke_request(
        &self,
        req: &LeaseRevokeRequest,
    ) -> Result<(), ExecuteError> {
        let mut keys = match self.lease_collection.read().lease_map.get(&req.id) {
            Some(l) => l.keys(),
            None => return Err(ExecuteError::lease_not_found(req.id)),
        };
        if !keys.is_empty() {
            keys.sort(); // all node delete in same order
            let (msg, rx) = DeleteMessage::new(keys);
            assert!(
                self.del_tx.send(msg).await.is_ok(),
                "Failed to send delete keys"
            );
            assert!(rx.await.is_ok(), "Failed to receive delete keys response");
        }
        let _ignore = self.lease_collection.write().revoke(req.id);
        self.delete(req.id)
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, time::Duration};

    use tracing::info;

    use super::*;
    use crate::storage::db::DBProxy;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_lease_storage() -> Result<(), Box<dyn Error>> {
        let (del_tx, mut del_rx) = mpsc::channel(128);
        let (_, lease_cmd_rx) = mpsc::channel(128);
        let state = Arc::new(State::default());
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));

        let lease_db = DBProxy::new(true)?;
        #[allow(clippy::unwrap_used)] // safe unwrap
        let lease_store = LeaseStore::new(del_tx, lease_cmd_rx, state, header_gen, lease_db);
        let _handle = tokio::spawn(async move {
            while let Some(msg) = del_rx.recv().await {
                let (keys, tx) = msg.unpack();
                info!("Delete keys {:?}", keys);
                assert!(tx.send(()).is_ok(), "Failed to send delete keys response");
            }
        });

        let req1 = RequestWithToken::new(LeaseGrantRequest { ttl: 10, id: 1 }.into());
        let _ignore1 = exe_and_sync_req(&lease_store, req1).await?;

        let lo = lease_store.look_up(1);
        assert!(lo.is_some());
        #[allow(clippy::unwrap_used)] // checked
        let lo = lo.unwrap();
        assert_eq!(lo.id(), 1);
        assert_eq!(lo.ttl(), Duration::from_secs(10));
        assert_eq!(lease_store.leases().len(), 1);

        let attach_non_existing_lease = lease_store.inner.attach(0, "key".into());
        assert!(attach_non_existing_lease.is_err());
        let attach_existing_lease = lease_store.inner.attach(1, "key".into());
        assert!(attach_existing_lease.is_ok());

        let req2 = RequestWithToken::new(LeaseRevokeRequest { id: 1 }.into());
        let _ignore2 = exe_and_sync_req(&lease_store, req2).await?;
        assert!(lease_store.look_up(1).is_none());
        assert!(lease_store.leases().is_empty());

        Ok(())
    }

    async fn exe_and_sync_req(
        ls: &LeaseStore<DBProxy>,
        req: RequestWithToken,
    ) -> Result<ResponseWrapper, ExecuteError> {
        let id = ProposeId::new("testid".to_owned());
        let cmd_res = ls.execute(id.clone(), req)?;
        let _ignore = ls.after_sync(&id).await;
        Ok(cmd_res.decode())
    }
}
