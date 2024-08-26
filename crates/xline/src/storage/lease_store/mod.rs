#![allow(clippy::multiple_inherent_impl)]

/// Lease
mod lease;
/// Lease related structs collection
mod lease_collection;
/// Lease heap
mod lease_queue;

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use clippy_utilities::OverflowArithmetic;
use engine::TransactionApi;
use log::debug;
use parking_lot::RwLock;
use prost::Message;
use utils::table_names::LEASE_TABLE;
use xlineapi::{
    command::{CommandResponse, SyncResponse},
    execute_error::ExecuteError,
};

pub(crate) use self::{lease::Lease, lease_collection::LeaseCollection};
use super::{
    db::{WriteOp, DB},
    index::IndexOperate,
    storage_api::XlineStorageOps,
};
use crate::{
    header_gen::HeaderGenerator,
    revision_number::RevisionNumberGeneratorState,
    rpc::{
        Event, LeaseGrantRequest, LeaseGrantResponse, LeaseLeasesRequest, LeaseLeasesResponse,
        LeaseRevokeRequest, LeaseRevokeResponse, LeaseStatus, PbLease, RequestWrapper,
        ResponseHeader, ResponseWrapper,
    },
    storage::KvStore,
};

/// Max lease ttl
const MAX_LEASE_TTL: i64 = 9_000_000_000;

/// Lease store
#[derive(Debug)]
pub(crate) struct LeaseStore {
    /// lease collection
    lease_collection: Arc<LeaseCollection>,
    /// Db to store lease
    db: Arc<DB>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// KV update sender
    kv_update_tx: flume::Sender<(i64, Vec<Event>)>,
    /// Primary flag
    is_primary: AtomicBool,
    /// cache unsynced lease id
    unsynced_cache: Arc<RwLock<HashSet<i64>>>,
    /// notify sync event
    sync_event: event_listener::Event,
}

impl LeaseStore {
    /// New `LeaseStore`
    pub(crate) fn new(
        lease_collection: Arc<LeaseCollection>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
        kv_update_tx: flume::Sender<(i64, Vec<Event>)>,
        is_leader: bool,
    ) -> Self {
        Self {
            lease_collection,
            db,
            header_gen,
            kv_update_tx,
            is_primary: AtomicBool::new(is_leader),
            unsynced_cache: Arc::new(RwLock::new(HashSet::new())),
            sync_event: event_listener::Event::new(),
        }
    }

    /// execute a lease request
    pub(crate) fn execute(
        &self,
        request: &RequestWrapper,
    ) -> Result<CommandResponse, ExecuteError> {
        self.handle_lease_requests(request)
            .map(CommandResponse::new)
    }

    /// sync a lease request
    pub(crate) fn after_sync<T, I>(
        &self,
        request: &RequestWrapper,
        revision_gen: &RevisionNumberGeneratorState<'_>,
        txn_db: &T,
        index: &I,
    ) -> Result<(SyncResponse, Vec<WriteOp>), ExecuteError>
    where
        T: XlineStorageOps + TransactionApi,
        I: IndexOperate,
    {
        let next_revision = revision_gen.get().overflow_add(1);
        let updated = self.sync_request(request, next_revision, txn_db, index)?;
        let rev = if updated {
            revision_gen.next()
        } else {
            revision_gen.get()
        };
        // TODO: return only a `SyncResponse`
        Ok((SyncResponse::new(rev), vec![]))
    }

    /// Get lease by id
    pub(crate) fn look_up(&self, lease_id: i64) -> Option<Lease> {
        self.lease_collection.look_up(lease_id)
    }

    /// Get all leases
    pub(crate) fn leases(&self) -> Vec<Lease> {
        self.lease_collection.leases()
    }

    /// Find expired leases
    pub(crate) fn find_expired_leases(&self) -> Vec<i64> {
        self.lease_collection.find_expired_leases()
    }

    /// Keep alive a lease
    pub(crate) fn keep_alive(&self, lease_id: i64) -> Result<i64, ExecuteError> {
        self.lease_collection.renew(lease_id)
    }

    /// Generate `ResponseHeader`
    pub(crate) fn gen_header(&self) -> ResponseHeader {
        self.header_gen.gen_header()
    }

    /// Demote current node
    pub(crate) fn demote(&self) {
        self.lease_collection.demote();
        self.is_primary.store(false, Ordering::Release);
    }

    /// Promote current node
    pub(crate) fn promote(&self, extend: Duration) {
        self.is_primary.store(true, Ordering::Release);
        self.lease_collection.promote(extend);
    }

    /// Recover data form persistent storage
    pub(crate) fn recover(&self) -> Result<(), ExecuteError> {
        let leases = self.get_all()?;
        for lease in leases {
            let _ignore = self.lease_collection.grant(lease.id, lease.ttl, false);
        }
        Ok(())
    }

    /// Check whether the current lease storage is primary or not
    pub(crate) fn is_primary(&self) -> bool {
        self.is_primary.load(Ordering::Relaxed)
    }

    /// Make lease synced, remove it from `unsynced_cache`
    pub(crate) fn mark_lease_synced(&self, wrapper: &RequestWrapper) {
        #[allow(clippy::wildcard_enum_match_arm)] // only the following two type are allowed
        let lease_id = match *wrapper {
            RequestWrapper::LeaseGrantRequest(ref req) => req.id,
            RequestWrapper::LeaseRevokeRequest(ref req) => req.id,
            _ => {
                return;
            }
        };

        _ = self.unsynced_cache.write().remove(&lease_id);
        let _ignore = self.sync_event.notify(usize::MAX);
    }

    /// Wait for the lease id to be removed from the cache
    pub(crate) async fn wait_synced(&self, lease_id: i64) {
        loop {
            let contains_id = self.unsynced_cache.read().contains(&lease_id);
            if contains_id {
                self.sync_event.listen().await;
            } else {
                break;
            }
        }
    }
}

impl LeaseStore {
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
            RequestWrapper::LeaseRevokeRequest(req) => {
                debug!("Receive LeaseRevokeRequest {:?}", req);
                self.handle_lease_revoke_request(req).map(Into::into)
            }
            RequestWrapper::LeaseLeasesRequest(req) => {
                debug!("Receive LeaseLeasesRequest {:?}", req);
                Ok(self.handle_lease_leases_request(req).into())
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
            return Err(ExecuteError::LeaseNotFound(0));
        }
        if req.ttl > MAX_LEASE_TTL {
            return Err(ExecuteError::LeaseTtlTooLarge(req.ttl));
        }
        if self.lease_collection.contains_lease(req.id) {
            return Err(ExecuteError::LeaseAlreadyExists(req.id));
        }

        _ = self.unsynced_cache.write().insert(req.id);

        Ok(LeaseGrantResponse {
            header: Some(self.header_gen.gen_header()),
            id: req.id,
            ttl: req.ttl,
            error: String::new(),
        })
    }

    /// Handle `LeaseRevokeRequest`
    fn handle_lease_revoke_request(
        &self,
        req: LeaseRevokeRequest,
    ) -> Result<LeaseRevokeResponse, ExecuteError> {
        if self.lease_collection.contains_lease(req.id) {
            _ = self.unsynced_cache.write().insert(req.id);

            Ok(LeaseRevokeResponse {
                header: Some(self.header_gen.gen_header()),
            })
        } else {
            Err(ExecuteError::LeaseNotFound(req.id))
        }
    }

    /// Handle `LeaseRevokeRequest`
    fn handle_lease_leases_request(&self, _req: LeaseLeasesRequest) -> LeaseLeasesResponse {
        let leases = self
            .leases()
            .into_iter()
            .map(|lease| LeaseStatus { id: lease.id() })
            .collect();

        LeaseLeasesResponse {
            header: Some(self.header_gen.gen_header()),
            leases,
        }
    }

    /// Sync `RequestWithToken`
    fn sync_request<T, I>(
        &self,
        wrapper: &RequestWrapper,
        revision: i64,
        txn_db: &T,
        index: &I,
    ) -> Result<bool, ExecuteError>
    where
        T: XlineStorageOps + TransactionApi,
        I: IndexOperate,
    {
        #[allow(clippy::wildcard_enum_match_arm)]
        let updated = match *wrapper {
            RequestWrapper::LeaseGrantRequest(ref req) => {
                debug!("Sync LeaseGrantRequest {:?}", req);
                self.sync_lease_grant_request(req, txn_db)?;
                false
            }
            RequestWrapper::LeaseRevokeRequest(ref req) => {
                debug!("Sync LeaseRevokeRequest {:?}", req);
                self.sync_lease_revoke_request(req, revision, txn_db, index)?
            }
            RequestWrapper::LeaseLeasesRequest(ref req) => {
                debug!("Sync LeaseLeasesRequest {:?}", req);
                false
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };
        Ok(updated)
    }

    /// Sync `LeaseGrantRequest`
    fn sync_lease_grant_request<T: XlineStorageOps>(
        &self,
        req: &LeaseGrantRequest,
        txn_db: &T,
    ) -> Result<(), ExecuteError> {
        let lease = self
            .lease_collection
            .grant(req.id, req.ttl, self.is_primary());
        txn_db.write_op(WriteOp::PutLease(lease))
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
    #[allow(clippy::trivially_copy_pass_by_ref)] // we can only get a reference in the caller
    fn sync_lease_revoke_request<T, I>(
        &self,
        req: &LeaseRevokeRequest,
        revision: i64,
        txn_db: &T,
        index: &I,
    ) -> Result<bool, ExecuteError>
    where
        T: XlineStorageOps + TransactionApi,
        I: IndexOperate,
    {
        let mut updates = Vec::new();
        txn_db.write_op(WriteOp::DeleteLease(req.id))?;

        let del_keys = match self.lease_collection.look_up(req.id) {
            Some(l) => l.keys(),
            None => return Err(ExecuteError::LeaseNotFound(req.id)),
        };

        if del_keys.is_empty() {
            let _ignore = self.lease_collection.revoke(req.id);
            return Ok(false);
        }

        for (key, mut sub_revision) in del_keys.iter().zip(0..) {
            let deleted =
                KvStore::delete_keys(txn_db, index, key, &[], revision, &mut sub_revision)?;
            KvStore::detach_leases(&deleted, &self.lease_collection);
            let mut del_event = KvStore::new_deletion_events(revision, deleted);
            updates.append(&mut del_event);
        }

        let _ignore = self.lease_collection.revoke(req.id);
        assert!(
            self.kv_update_tx.send((revision, updates)).is_ok(),
            "Failed to send updates to KV watcher"
        );

        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, time::Duration};

    use test_macros::abort_on_panic;
    use utils::config::EngineConfig;

    use super::*;
    use crate::{
        revision_number::RevisionNumberGenerator,
        storage::{
            db::DB,
            index::{Index, IndexState},
            storage_api::XlineStorageOps,
        },
    };

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_lease_storage() -> Result<(), Box<dyn Error>> {
        let db = DB::open(&EngineConfig::Memory)?;
        let index = Index::new();
        let (lease_store, rev_gen) = init_store(db);
        let rev_gen_state = rev_gen.state();

        let req1 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 1 });
        let _ignore1 = exe_and_sync_req(&lease_store, index.state(), &req1, &rev_gen_state)?;

        let lo = lease_store.look_up(1).unwrap();
        assert_eq!(lo.id(), 1);
        assert_eq!(lo.ttl(), Duration::from_secs(10));
        assert_eq!(lease_store.leases().len(), 1);

        let attach_non_existing_lease = lease_store.lease_collection.attach(0, "key".into());
        assert!(attach_non_existing_lease.is_err());
        let attach_existing_lease = lease_store.lease_collection.attach(1, "key".into());
        assert!(attach_existing_lease.is_ok());
        lease_store.lease_collection.detach(1, "key".as_bytes())?;

        let req2 = RequestWrapper::from(LeaseRevokeRequest { id: 1 });
        let _ignore2 = exe_and_sync_req(&lease_store, index.state(), &req2, &rev_gen_state)?;
        assert!(lease_store.look_up(1).is_none());
        assert!(lease_store.leases().is_empty());

        let req3 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 3 });
        let req4 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 4 });
        let req5 = RequestWrapper::from(LeaseRevokeRequest { id: 3 });
        let req6 = RequestWrapper::from(LeaseLeasesRequest {});
        let _ignore3 = exe_and_sync_req(&lease_store, index.state(), &req3, &rev_gen_state)?;
        let _ignore4 = exe_and_sync_req(&lease_store, index.state(), &req4, &rev_gen_state)?;
        let resp_1 = exe_and_sync_req(&lease_store, index.state(), &req6, &rev_gen_state)?;

        let ResponseWrapper::LeaseLeasesResponse(leases_1) = resp_1 else {
            panic!("wrong response type: {resp_1:?}");
        };
        assert_eq!(leases_1.leases[0].id, 3);
        assert_eq!(leases_1.leases[1].id, 4);

        let _ignore5 = exe_and_sync_req(&lease_store, index.state(), &req5, &rev_gen_state)?;
        let resp_2 = exe_and_sync_req(&lease_store, index.state(), &req6, &rev_gen_state)?;
        let ResponseWrapper::LeaseLeasesResponse(leases_2) = resp_2 else {
            panic!("wrong response type: {resp_2:?}");
        };
        assert_eq!(leases_2.leases[0].id, 4);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lease_sync() -> Result<(), Box<dyn Error>> {
        let db = DB::open(&EngineConfig::Memory)?;
        let txn = db.transaction();
        let index = Index::new();
        let (lease_store, rev_gen) = init_store(Arc::clone(&db));
        let rev_gen_state = rev_gen.state();
        let wait_duration = Duration::from_millis(1);

        let req1 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 1 });
        let _ignore1 = lease_store.execute(&req1)?;

        assert!(
            tokio::time::timeout(wait_duration, lease_store.wait_synced(1))
                .await
                .is_err(),
            "the future should block until the lease is synced"
        );

        let (_ignore, ops) = lease_store.after_sync(&req1, &rev_gen_state, &txn, &index)?;
        lease_store.db.write_ops(ops)?;
        lease_store.mark_lease_synced(&req1);

        assert!(
            tokio::time::timeout(wait_duration, lease_store.wait_synced(1))
                .await
                .is_ok(),
            "the future should complete immediately after the lease is synced"
        );

        let req2 = RequestWrapper::from(LeaseRevokeRequest { id: 1 });
        let _ignore2 = lease_store.execute(&req2)?;

        assert!(
            tokio::time::timeout(wait_duration, lease_store.wait_synced(1))
                .await
                .is_err(),
            "the future should block until the lease is synced"
        );

        let (_ignore, ops) = lease_store.after_sync(&req2, &rev_gen_state, &txn, &index)?;
        lease_store.db.write_ops(ops)?;
        lease_store.mark_lease_synced(&req2);

        assert!(
            tokio::time::timeout(wait_duration, lease_store.wait_synced(1))
                .await
                .is_ok(),
            "the future should complete immediately after the lease is synced"
        );

        Ok(())
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_recover() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let index = Index::new();
        let (store, rev_gen) = init_store(Arc::clone(&db));
        let rev_gen_state = rev_gen.state();

        let req1 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 1 });
        let _ignore1 = exe_and_sync_req(&store, index.state(), &req1, &rev_gen_state)?;
        store.lease_collection.attach(1, "key".into())?;

        let (new_store, _) = init_store(db);
        assert!(new_store.look_up(1).is_none());
        new_store.recover()?;

        let lease1 = store.look_up(1).unwrap();
        let lease2 = new_store.look_up(1).unwrap();

        assert_eq!(lease1.id(), lease2.id());
        assert_eq!(lease1.ttl(), lease2.ttl());
        assert!(!lease1.keys().is_empty());
        assert!(lease2.keys().is_empty()); // keys will be recovered when recover kv store

        Ok(())
    }

    fn init_store(db: Arc<DB>) -> (LeaseStore, RevisionNumberGenerator) {
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let (kv_update_tx, _) = flume::bounded(1);
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        (
            LeaseStore::new(lease_collection, header_gen, db, kv_update_tx, true),
            RevisionNumberGenerator::new(1),
        )
    }

    fn exe_and_sync_req(
        ls: &LeaseStore,
        index: IndexState,
        req: &RequestWrapper,
        rev_gen: &RevisionNumberGeneratorState<'_>,
    ) -> Result<ResponseWrapper, ExecuteError> {
        let cmd_res = ls.execute(req)?;
        let txn = ls.db.transaction();
        let (_ignore, _ops) = ls.after_sync(req, rev_gen, &txn, &index)?;
        txn.commit()
            .map_err(|e| ExecuteError::DbError(e.to_string()))?;
        index.commit();
        rev_gen.commit();
        Ok(cmd_res.into_inner())
    }
}
