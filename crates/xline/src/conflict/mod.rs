use curp::cmd::Command as CurpCommand;
use utils::interval_map::Interval;
use xlineapi::{command::Command, interval::BytesAffine, RequestBackend, RequestWrapper};

/// Speculative pool implementations
pub(crate) mod spec_pool;
/// Uncommitted pool implementations
pub(crate) mod uncommitted_pool;

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
fn is_xor_cmd(cmd: &Command) -> bool {
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
