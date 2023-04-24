/// Lease
mod lease;
/// Lease related structs collection
mod lease_collection;
/// Lease heap
mod lease_queue;

use std::{sync::Arc, time::Duration};

use log::debug;
use prost::Message;
use tokio::sync::mpsc;

pub(crate) use self::{lease::Lease, lease_collection::LeaseCollection};
use super::{
    db::WriteOp,
    index::{Index, IndexOperate},
    kv_store::KV_TABLE,
    storage_api::StorageApi,
    ExecuteError,
};
use crate::{
    header_gen::HeaderGenerator,
    revision_number::RevisionNumber,
    rpc::{
        Event, EventType, KeyValue, LeaseGrantRequest, LeaseGrantResponse, LeaseRevokeRequest,
        LeaseRevokeResponse, PbLease, RequestWithToken, RequestWrapper, ResponseHeader,
        ResponseWrapper,
    },
    server::command::{CommandResponse, SyncResponse},
    state::State,
    storage::Revision,
};

/// Lease table name
pub(crate) const LEASE_TABLE: &str = "lease";
/// Max lease ttl
const MAX_LEASE_TTL: i64 = 9_000_000_000;

/// Lease store
#[derive(Debug)]
pub(crate) struct LeaseStore<DB>
where
    DB: StorageApi,
{
    /// Lease store Backend
    inner: Arc<LeaseStoreBackend<DB>>,
}

/// Lease store inner
#[derive(Debug)]
struct LeaseStoreBackend<DB>
where
    DB: StorageApi,
{
    /// lease collection
    lease_collection: Arc<LeaseCollection>,
    /// Db to store lease
    db: Arc<DB>,
    /// Key to revision index
    index: Arc<Index>,
    /// Current node is leader or not
    state: Arc<State>,
    /// Revision
    revision: Arc<RevisionNumber>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// KV update sender
    kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
}

impl<DB> LeaseStore<DB>
where
    DB: StorageApi,
{
    /// New `LeaseStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new(
        lease_collection: Arc<LeaseCollection>,
        state: Arc<State>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
        index: Arc<Index>,
        kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
    ) -> Self {
        Self {
            inner: Arc::new(LeaseStoreBackend::new(
                lease_collection,
                state,
                header_gen,
                db,
                index,
                kv_update_tx,
            )),
        }
    }

    /// execute a lease request
    pub(crate) fn execute(
        &self,
        request: &RequestWithToken,
    ) -> Result<CommandResponse, ExecuteError> {
        self.inner
            .handle_lease_requests(&request.request)
            .map(CommandResponse::new)
    }

    /// sync a lease request
    pub(crate) async fn after_sync(
        &self,
        request: &RequestWithToken,
    ) -> Result<(SyncResponse, Vec<WriteOp>), ExecuteError> {
        self.inner
            .sync_request(&request.request)
            .await
            .map(|(rev, ops)| (SyncResponse::new(rev), ops))
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
        self.inner.lease_collection.leases()
    }

    /// Find expired leases
    pub(crate) fn find_expired_leases(&self) -> Vec<i64> {
        self.inner.lease_collection.find_expired_leases()
    }

    /// Get keys attached to a lease
    pub(crate) fn get_keys(&self, lease_id: i64) -> Vec<Vec<u8>> {
        self.inner
            .lease_collection
            .look_up(lease_id)
            .map(|l| l.keys())
            .unwrap_or_default()
    }

    /// Keep alive a lease
    pub(crate) fn keep_alive(&self, lease_id: i64) -> Result<i64, ExecuteError> {
        if !self.is_leader() {
            return Err(ExecuteError::lease_not_leader());
        }
        self.inner.lease_collection.renew(lease_id)
    }

    /// Generate `ResponseHeader`
    pub(crate) fn gen_header(&self) -> ResponseHeader {
        self.inner.header_gen.gen_header()
    }

    /// Demote current node
    pub(crate) fn demote(&self) {
        self.inner.lease_collection.demote();
    }

    /// Promote current node
    pub(crate) fn promote(&self, extend: Duration) {
        self.inner.lease_collection.promote(extend);
    }

    /// Recover data form persistent storage
    pub(crate) fn recover(&self) -> Result<(), ExecuteError> {
        self.inner.recover_from_current_db()
    }
}

impl<DB> LeaseStoreBackend<DB>
where
    DB: StorageApi,
{
    /// New `LeaseStoreBackend`
    fn new(
        lease_collection: Arc<LeaseCollection>,
        state: Arc<State>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
        index: Arc<Index>,
        kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
    ) -> Self {
        Self {
            lease_collection,
            db,
            state,
            revision: header_gen.revision_arc(),
            header_gen,
            index,
            kv_update_tx,
        }
    }

    /// Check if the node is leader
    fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    /// Detach key from lease
    fn detach(&self, lease_id: i64, key: &[u8]) -> Result<(), ExecuteError> {
        self.lease_collection.detach(lease_id, key)
    }

    /// Get lease id by given key
    fn get_lease(&self, key: &[u8]) -> i64 {
        self.lease_collection.get_lease(key)
    }

    /// Get lease by id
    fn look_up(&self, lease_id: i64) -> Option<Lease> {
        self.lease_collection.look_up(lease_id)
    }

    /// Recover data form persistent storage
    fn recover_from_current_db(&self) -> Result<(), ExecuteError> {
        let leases = self.get_all()?;
        for lease in leases {
            let _ignore = self.lease_collection.grant(lease.id, lease.ttl, false);
        }
        Ok(())
    }

    /// Handle lease requests
    fn handle_lease_requests(
        &self,
        wrapper: &RequestWrapper,
    ) -> Result<ResponseWrapper, ExecuteError> {
        debug!("Receive request {:?}", wrapper);
        #[allow(clippy::wildcard_enum_match_arm)]
        let res = match *wrapper {
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
        if self.lease_collection.contains_lease(req.id) {
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
        if self.lease_collection.contains_lease(req.id) {
            Ok(LeaseRevokeResponse {
                header: Some(self.header_gen.gen_header_without_revision()),
            })
        } else {
            Err(ExecuteError::lease_not_found(req.id))
        }
    }

    /// Sync `RequestWithToken`
    async fn sync_request(
        &self,
        wrapper: &RequestWrapper,
    ) -> Result<(i64, Vec<WriteOp>), ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        let ops = match *wrapper {
            RequestWrapper::LeaseGrantRequest(ref req) => {
                debug!("Sync LeaseGrantRequest {:?}", req);
                self.sync_lease_grant_request(req)
            }
            RequestWrapper::LeaseRevokeRequest(ref req) => {
                debug!("Sync LeaseRevokeRequest {:?}", req);
                self.sync_lease_revoke_request(req).await?
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };
        Ok((self.header_gen.revision(), ops))
    }

    /// Sync `LeaseGrantRequest`
    fn sync_lease_grant_request(&self, req: &LeaseGrantRequest) -> Vec<WriteOp> {
        let lease = self
            .lease_collection
            .grant(req.id, req.ttl, self.is_leader());
        vec![WriteOp::PutLease(lease)]
    }

    /// Get all `PbLease`
    fn get_all(&self) -> Result<Vec<PbLease>, ExecuteError> {
        self.db
            .get_all(LEASE_TABLE)
            .map_err(|e| ExecuteError::DbError(format!("Failed to get all leases, error: {e}")))?
            .into_iter()
            .map(|(_, v)| {
                PbLease::decode(&mut v.as_slice()).map_err(|e| {
                    ExecuteError::DbError(format!("Failed to decode lease, error: {e}"))
                })
            })
            .collect()
    }

    /// Sync `LeaseRevokeRequest`
    async fn sync_lease_revoke_request(
        &self,
        req: &LeaseRevokeRequest,
    ) -> Result<Vec<WriteOp>, ExecuteError> {
        let mut ops = Vec::new();
        ops.push(WriteOp::DeleteLease(req.id));

        let keys = match self.lease_collection.look_up(req.id) {
            Some(l) => l.keys(),
            None => return Err(ExecuteError::lease_not_found(req.id)),
        };

        if keys.is_empty() {
            let _ignore = self.lease_collection.revoke(req.id);
            return Ok(Vec::new());
        }

        let revision = self.revision.next();
        let (prev_keys, del_revs): (Vec<Vec<u8>>, Vec<Revision>) = keys
            .into_iter()
            .zip(0..)
            .map(|(key, sub_revision)| {
                let (prev_rev, del_rev) = self
                    .index
                    .delete(&key, &[], revision, sub_revision)
                    .pop()
                    .unwrap_or_else(|| panic!("delete one key should return 1 result"));
                (prev_rev.encode_to_vec(), del_rev)
            })
            .unzip();
        let prev_kvs: Vec<KeyValue> = self
            .db
            .get_values(KV_TABLE, &prev_keys)?
            .into_iter()
            .flatten()
            .map(|v| KeyValue::decode(v.as_slice()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                ExecuteError::DbError(format!("Failed to decode key-value from DB, error: {e}"))
            })?;
        assert_eq!(prev_kvs.len(), del_revs.len());
        for kv in &prev_kvs {
            let lease_id = self.get_lease(&kv.key);
            self.detach(lease_id, kv.key.as_slice())?;
        }
        prev_kvs
            .iter()
            .zip(del_revs.into_iter())
            .for_each(|(kv, del_rev)| {
                let del_kv = KeyValue {
                    key: kv.key.clone(),
                    mod_revision: del_rev.revision(),
                    ..KeyValue::default()
                };
                ops.push(WriteOp::PutKeyValue(del_rev, del_kv.encode_to_vec()));
            });

        let updates = prev_kvs
            .into_iter()
            .map(|prev| {
                let kv = KeyValue {
                    key: prev.key.clone(),
                    mod_revision: revision,
                    ..Default::default()
                };
                Event {
                    #[allow(clippy::as_conversions)] // This cast is always valid
                    r#type: EventType::Delete as i32,
                    kv: Some(kv),
                    prev_kv: Some(prev),
                }
            })
            .collect();

        let _ignore = self.lease_collection.revoke(req.id);
        assert!(
            self.kv_update_tx.send((revision, updates)).await.is_ok(),
            "Failed to send updates to KV watcher"
        );
        Ok(ops)
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, time::Duration};

    use utils::config::StorageConfig;

    use super::*;
    use crate::storage::db::DB;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_lease_storage() -> Result<(), Box<dyn Error>> {
        let db = DB::open(&StorageConfig::Memory)?;
        let lease_store = init_store(db);

        let req1 = RequestWithToken::new(LeaseGrantRequest { ttl: 10, id: 1 }.into());
        let _ignore1 = exe_and_sync_req(&lease_store, &req1).await?;

        let lo = lease_store.look_up(1).unwrap();
        assert_eq!(lo.id(), 1);
        assert_eq!(lo.ttl(), Duration::from_secs(10));
        assert_eq!(lease_store.leases().len(), 1);

        let attach_non_existing_lease = lease_store.inner.lease_collection.attach(0, "key".into());
        assert!(attach_non_existing_lease.is_err());
        let attach_existing_lease = lease_store.inner.lease_collection.attach(1, "key".into());
        assert!(attach_existing_lease.is_ok());
        lease_store.inner.detach(1, "key".as_bytes())?;

        let req2 = RequestWithToken::new(LeaseRevokeRequest { id: 1 }.into());
        let _ignore2 = exe_and_sync_req(&lease_store, &req2).await?;
        assert!(lease_store.look_up(1).is_none());
        assert!(lease_store.leases().is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_recover() -> Result<(), ExecuteError> {
        let db = DB::open(&StorageConfig::Memory)?;
        let store = init_store(Arc::clone(&db));

        let req1 = RequestWithToken::new(LeaseGrantRequest { ttl: 10, id: 1 }.into());
        let _ignore1 = exe_and_sync_req(&store, &req1).await?;
        store.inner.lease_collection.attach(1, "key".into())?;

        let new_store = init_store(db);
        assert!(new_store.look_up(1).is_none());
        new_store.inner.recover_from_current_db()?;

        let lease1 = store.look_up(1).unwrap();
        let lease2 = new_store.look_up(1).unwrap();

        assert_eq!(lease1.id(), lease2.id());
        assert_eq!(lease1.ttl(), lease2.ttl());
        assert!(!lease1.keys().is_empty());
        assert!(lease2.keys().is_empty()); // keys will be recovered when recover kv store

        Ok(())
    }

    fn init_store(db: Arc<DB>) -> LeaseStore<DB> {
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let (kv_update_tx, _) = mpsc::channel(1);
        let state = Arc::new(State::default());
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let index = Arc::new(Index::new());
        LeaseStore::new(lease_collection, state, header_gen, db, index, kv_update_tx)
    }

    async fn exe_and_sync_req(
        ls: &LeaseStore<DB>,
        req: &RequestWithToken,
    ) -> Result<ResponseWrapper, ExecuteError> {
        let cmd_res = ls.execute(req)?;
        let (_ignore, ops) = ls.after_sync(req).await?;
        ls.inner.db.flush_ops(ops)?;
        Ok(cmd_res.decode())
    }
}
