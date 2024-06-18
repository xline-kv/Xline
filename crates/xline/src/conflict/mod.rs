use std::sync::Arc;

use curp::{
    cmd::Command as CurpCommand,
    server::{conflict::CommandEntry, SpObject, UcpObject},
};
use utils::interval_map::Interval;
use xlineapi::{
    command::{Command, KeyRange},
    interval::BytesAffine,
    RequestBackend, RequestWrapper,
};

use crate::storage::lease_store::{Lease, LeaseCollection};

use self::{
    spec_pool::{ExclusiveSpecPool, KvSpecPool, LeaseSpecPool},
    uncommitted_pool::{ExclusiveUncomPool, KvUncomPool, LeaseUncomPool},
};

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
    let intervals = entry.as_ref().keys().into_iter();
    let lease_intervals = entry
        .as_ref()
        .leases()
        .into_iter()
        .filter_map(|id| lease_collection.look_up(id))
        .flat_map(Lease::into_keys)
        .map(KeyRange::new_one_key);

    intervals.chain(lease_intervals).map(Into::into).collect()
}

/// Filter kv commands
fn filter_kv<C>(entry: C) -> Option<C>
where
    C: AsRef<Command>,
{
    matches!(
        entry.as_ref().request().backend(),
        RequestBackend::Kv | RequestBackend::Lease
    )
    .then_some(entry)
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
pub(super) fn all_leases(
    lease_collection: &LeaseCollection,
    req: &CommandEntry<Command>,
) -> Vec<i64> {
    req.leases()
        .into_iter()
        .chain(lookup_lease(lease_collection, req))
        .collect()
}

/// Lookups lease ids from lease collection
fn lookup_lease(lease_collection: &LeaseCollection, req: &CommandEntry<Command>) -> Vec<i64> {
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
