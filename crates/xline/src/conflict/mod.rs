use curp::{
    cmd::Command as CurpCommand,
    server::{SpObject, UcpObject},
};
use utils::interval_map::Interval;
use xlineapi::{command::Command, interval::BytesAffine, RequestBackend, RequestWrapper};

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
fn intervals<C>(entry: &C) -> Vec<Interval<BytesAffine>>
where
    C: AsRef<Command>,
{
    entry
        .as_ref()
        .keys()
        .iter()
        .cloned()
        .map(Into::into)
        .collect()
}

/// Filter kv commands
fn filter_kv<C>(entry: C) -> Option<C>
where
    C: AsRef<Command>,
{
    matches!(entry.as_ref().request().backend(), RequestBackend::Kv).then_some(entry)
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

/// Xline speculative pools wrapper
pub(crate) struct XlineSpeculativePools<C>(Vec<SpObject<C>>);

impl<C> XlineSpeculativePools<C> {
    /// Convert into sp objects
    pub(crate) fn into_inner(self) -> Vec<SpObject<C>> {
        self.0
    }
}

impl Default for XlineSpeculativePools<Command> {
    fn default() -> Self {
        let kv_sp = Box::<KvSpecPool>::default();
        let lease_sp = Box::<LeaseSpecPool>::default();
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

impl Default for XlineUncommittedPools<Command> {
    fn default() -> Self {
        let kv_ucp = Box::<KvUncomPool>::default();
        let lease_ucp = Box::<LeaseUncomPool>::default();
        let exclusive_ucp = Box::<ExclusiveUncomPool>::default();
        Self(vec![kv_ucp, lease_ucp, exclusive_ucp])
    }
}
