use std::sync::Arc;

use curp::{
    cmd::Command as CurpCommand,
    rpc::PoolEntry,
    server::{SpObject, UcpObject},
};
use utils::interval_map::Interval;
use xlineapi::{
    command::{Command, KeyRange},
    interval::BytesAffine,
    RequestWrapper,
};

use crate::storage::lease_store::{Lease, LeaseCollection};

use self::{
    spec_pool::{ExclusiveSpecPool, KvSpecPool, LeaseSpecPool},
    uncommitted_pool::{ExclusiveUncomPool, KvUncomPool, LeaseUncomPool},
};
// TODO: Refine code to improve reusability for different conflict pool types
/// Speculative pool implementations
pub(crate) mod spec_pool;
/// Uncommitted pool implementations
pub(crate) mod uncommitted_pool;

/// Tests
#[cfg(test)]
mod tests;

/// Returns command intervals
fn intervals<C>(lease_collection: &LeaseCollection, entry: &C) -> Vec<Interval<BytesAffine>>
where
    C: AsRef<Command>,
{
    intervals_kv(entry)
        .into_iter()
        .chain(intervals_lease(lease_collection, entry))
        .collect()
}

/// Gets KV intervals of a kv request
fn intervals_kv<C>(entry: &C) -> impl IntoIterator<Item = Interval<BytesAffine>>
where
    C: AsRef<Command>,
{
    entry.as_ref().keys().into_iter().map(Into::into)
}

/// Gets KV intervals of a lease request
///
/// We also needs to handle `LeaseRevokeRequest` in KV conflict pools,
/// as a revoke may delete keys associated with the lease id. Therefore,
/// we should insert these keys into the KV conflict pool as well.
fn intervals_lease<C>(
    lease_collection: &LeaseCollection,
    entry: &C,
) -> impl IntoIterator<Item = Interval<BytesAffine>>
where
    C: AsRef<Command>,
{
    let id = if let RequestWrapper::LeaseRevokeRequest(ref req) = *entry.as_ref().request() {
        lease_collection.look_up(req.id)
    } else {
        None
    };

    id.into_iter()
        .flat_map(Lease::into_keys)
        .map(KeyRange::new_one_key)
        .map(Into::into)
}

/// Returns `true` if this command conflicts with all other commands
fn is_exclusive_cmd(cmd: &Command) -> bool {
    matches!(
        *cmd.request(),
        RequestWrapper::CompactionRequest(_)
            | RequestWrapper::AuthEnableRequest(_)
            | RequestWrapper::AuthDisableRequest(_)
            | RequestWrapper::AuthRoleAddRequest(_)
            | RequestWrapper::AuthRoleDeleteRequest(_)
            | RequestWrapper::AuthRoleGrantPermissionRequest(_)
            | RequestWrapper::AuthRoleRevokePermissionRequest(_)
            | RequestWrapper::AuthUserAddRequest(_)
            | RequestWrapper::AuthUserChangePasswordRequest(_)
            | RequestWrapper::AuthUserDeleteRequest(_)
            | RequestWrapper::AuthUserGrantRoleRequest(_)
            | RequestWrapper::AuthUserRevokeRoleRequest(_)
            | RequestWrapper::AuthenticateRequest(_)
            | RequestWrapper::AlarmRequest(_)
    )
}

/// Gets all lease id
/// * lease ids in the requests field
/// * lease ids associated with the keys
pub(super) fn all_leases(lease_collection: &LeaseCollection, req: &PoolEntry<Command>) -> Vec<i64> {
    req.leases()
        .into_iter()
        .chain(lookup_lease(lease_collection, req))
        .collect()
}

/// Lookups lease ids from lease collection
///
/// We also needs to handle `PutRequest` and `DeleteRangeRequest` in
/// lease conflict pools, as they may conflict with a `LeaseRevokeRequest`.
/// Therefore, we should lookup the lease ids from lease collection.
fn lookup_lease(lease_collection: &LeaseCollection, req: &PoolEntry<Command>) -> Vec<i64> {
    req.request()
        .keys()
        .into_iter()
        .flat_map(|key| lease_collection.get_lease_by_range(key))
        .collect()
}

/// Xline speculative pools wrapper
pub(crate) struct XlineSpeculativePools<C>(Vec<SpObject<C>>);

impl<C> XlineSpeculativePools<C> {
    /// Convert into sp objects
    pub(crate) fn into_inner(self) -> Vec<SpObject<C>> {
        self.0
    }
}

impl XlineSpeculativePools<Command> {
    /// Creates a new [`XlineSpeculativePools<Command>`].
    pub(crate) fn new(lease_collection: Arc<LeaseCollection>) -> Self {
        let kv_sp = Box::new(KvSpecPool::new(Arc::clone(&lease_collection)));
        let lease_sp = Box::new(LeaseSpecPool::new(lease_collection));
        let exclusive_sp = Box::<ExclusiveSpecPool>::default();
        Self(vec![kv_sp, lease_sp, exclusive_sp])
    }
}

/// Xline uncommitted pools wrapper
pub(crate) struct XlineUncommittedPools<C>(Vec<UcpObject<C>>);

impl<C> XlineUncommittedPools<C> {
    /// Convert into sp objects
    pub(crate) fn into_inner(self) -> Vec<UcpObject<C>> {
        self.0
    }
}

impl XlineUncommittedPools<Command> {
    /// Creates a new [`XlineUncommittedPools<Command>`].
    pub(crate) fn new(lease_collection: Arc<LeaseCollection>) -> Self {
        let kv_ucp = Box::new(KvUncomPool::new(Arc::clone(&lease_collection)));
        let lease_ucp = Box::new(LeaseUncomPool::new(lease_collection));
        let exclusive_ucp = Box::<ExclusiveUncomPool>::default();
        Self(vec![kv_ucp, lease_ucp, exclusive_ucp])
    }
}
