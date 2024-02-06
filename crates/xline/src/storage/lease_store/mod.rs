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

use log::debug;
use parking_lot::RwLock;
use prost::Message;
use tokio::sync::mpsc;
use utils::table_names::LEASE_TABLE;
use xlineapi::{
    command::{CommandResponse, SyncResponse},
    execute_error::ExecuteError,
};

pub(crate) use self::{lease::Lease, lease_collection::LeaseCollection};
use super::{db::WriteOp, index::Index, storage_api::StorageApi};
use crate::{
    header_gen::HeaderGenerator,
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
pub(crate) struct LeaseStore<DB>
where
    DB: StorageApi,
{
    /// lease collection
    lease_collection: Arc<LeaseCollection>,
    /// Db to store lease
    db: Arc<DB>,
    /// Key to revision index
    index: Arc<Index>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// KV update sender
    kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
    /// Primary flag
    is_primary: AtomicBool,
    /// cache unsynced lease id
    unsynced_cache: Arc<RwLock<HashSet<i64>>>,
    /// notify sync event
    sync_event: event_listener::Event,
}

impl<DB> LeaseStore<DB>
where
    DB: StorageApi,
{
    /// New `LeaseStore`
    pub(crate) fn new(
        lease_collection: Arc<LeaseCollection>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
        index: Arc<Index>,
        kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
        is_leader: bool,
    ) -> Self {
        Self {
            lease_collection,
            db,
            index,
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
    pub(crate) async fn after_sync(
        &self,
        request: &RequestWrapper,
        revision: i64,
    ) -> Result<(SyncResponse, Vec<WriteOp>), ExecuteError> {
        self.sync_request(request, revision)
            .await
            .map(|(rev, ops)| (SyncResponse::new(rev), ops))
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

    /// Get keys attached to a lease
    pub(crate) fn get_keys(&self, lease_id: i64) -> Vec<Vec<u8>> {
        self.lease_collection
            .look_up(lease_id)
            .map(|l| l.keys())
            .unwrap_or_default()
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
        self.sync_event.notify(usize::MAX);
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

impl<DB> LeaseStore<DB>
where
    DB: StorageApi,
{
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
            RequestWrapper::LeaseLeasesRequest(ref req) => {
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
        req: &LeaseRevokeRequest,
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
    fn handle_lease_leases_request(&self, _req: &LeaseLeasesRequest) -> LeaseLeasesResponse {
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
    async fn sync_request(
        &self,
        wrapper: &RequestWrapper,
        revision: i64,
    ) -> Result<(i64, Vec<WriteOp>), ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        let ops = match *wrapper {
            RequestWrapper::LeaseGrantRequest(ref req) => {
                debug!("Sync LeaseGrantRequest {:?}", req);
                self.sync_lease_grant_request(req)
            }
            RequestWrapper::LeaseRevokeRequest(ref req) => {
                debug!("Sync LeaseRevokeRequest {:?}", req);
                self.sync_lease_revoke_request(req, revision).await?
            }
            RequestWrapper::LeaseLeasesRequest(ref req) => {
                debug!("Sync LeaseLeasesRequest {:?}", req);
                vec![]
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };
        Ok((revision, ops))
    }

    /// Sync `LeaseGrantRequest`
    fn sync_lease_grant_request(&self, req: &LeaseGrantRequest) -> Vec<WriteOp> {
        let lease = self
            .lease_collection
            .grant(req.id, req.ttl, self.is_primary());
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
        revision: i64,
    ) -> Result<Vec<WriteOp>, ExecuteError> {
        let mut ops = Vec::new();
        let mut updates = Vec::new();
        ops.push(WriteOp::DeleteLease(req.id));

        let del_keys = match self.lease_collection.look_up(req.id) {
            Some(l) => l.keys(),
            None => return Err(ExecuteError::LeaseNotFound(req.id)),
        };

        if del_keys.is_empty() {
            let _ignore = self.lease_collection.revoke(req.id);
            return Ok(Vec::new());
        }

        for (key, sub_revision) in del_keys.iter().zip(0..) {
            let (mut del_ops, mut del_event) = KvStore::<DB>::delete_keys(
                &self.index,
                &self.lease_collection,
                key,
                &[],
                revision,
                sub_revision,
            );
            ops.append(&mut del_ops);
            updates.append(&mut del_event);
        }

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

    use test_macros::abort_on_panic;
    use utils::config::EngineConfig;

    use super::*;
    use crate::storage::db::DB;

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_lease_storage() -> Result<(), Box<dyn Error>> {
        let db = DB::open(&EngineConfig::Memory)?;
        let lease_store = init_store(db);
        let revision_gen = lease_store.header_gen.general_revision_arc();

        let req1 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 1 });
        let _ignore1 = exe_and_sync_req(&lease_store, &req1, -1).await?;

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
        let _ignore2 = exe_and_sync_req(&lease_store, &req2, revision_gen.next()).await?;
        assert!(lease_store.look_up(1).is_none());
        assert!(lease_store.leases().is_empty());

        let req3 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 3 });
        let req4 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 4 });
        let req5 = RequestWrapper::from(LeaseRevokeRequest { id: 3 });
        let req6 = RequestWrapper::from(LeaseLeasesRequest {});
        let _ignore3 = exe_and_sync_req(&lease_store, &req3, -1).await?;
        let _ignore4 = exe_and_sync_req(&lease_store, &req4, -1).await?;
        let resp_1 = exe_and_sync_req(&lease_store, &req6, -1).await?;

        let ResponseWrapper::LeaseLeasesResponse(leases_1) = resp_1 else { panic!("wrong response type: {resp_1:?}"); };
        assert_eq!(leases_1.leases[0].id, 3);
        assert_eq!(leases_1.leases[1].id, 4);

        let _ignore5 = exe_and_sync_req(&lease_store, &req5, -1).await?;
        let resp_2 = exe_and_sync_req(&lease_store, &req6, -1).await?;
        let ResponseWrapper::LeaseLeasesResponse(leases_2) = resp_2 else { panic!("wrong response type: {resp_2:?}"); };
        assert_eq!(leases_2.leases[0].id, 4);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lease_sync() -> Result<(), Box<dyn Error>> {
        let db = DB::open(&EngineConfig::Memory)?;
        let lease_store = init_store(db);
        let wait_duration = Duration::from_millis(1);

        let req1 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 1 });
        let _ignore1 = lease_store.execute(&req1)?;

        assert!(
            tokio::time::timeout(wait_duration, lease_store.wait_synced(1))
                .await
                .is_err(),
            "the future should block until the lease is synced"
        );

        let (_ignore, ops) = lease_store.after_sync(&req1, -1).await?;
        _ = lease_store.db.flush_ops(ops)?;
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

        let (_ignore, ops) = lease_store.after_sync(&req2, -1).await?;
        _ = lease_store.db.flush_ops(ops)?;
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
        let store = init_store(Arc::clone(&db));

        let req1 = RequestWrapper::from(LeaseGrantRequest { ttl: 10, id: 1 });
        let _ignore1 = exe_and_sync_req(&store, &req1, -1).await?;
        store.lease_collection.attach(1, "key".into())?;

        let new_store = init_store(db);
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

    fn init_store(db: Arc<DB>) -> LeaseStore<DB> {
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let (kv_update_tx, _) = mpsc::channel(1);
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let index = Arc::new(Index::new());
        LeaseStore::new(lease_collection, header_gen, db, index, kv_update_tx, true)
    }

    async fn exe_and_sync_req(
        ls: &LeaseStore<DB>,
        req: &RequestWrapper,
        revision: i64,
    ) -> Result<ResponseWrapper, ExecuteError> {
        let cmd_res = ls.execute(req)?;
        let (_ignore, ops) = ls.after_sync(req, revision).await?;
        _ = ls.db.flush_ops(ops)?;
        Ok(cmd_res.into_inner())
    }
}
