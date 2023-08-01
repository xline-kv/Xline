use std::sync::Arc;

use curp::{
    client::Client,
    error::{CommandProposeError, ProposeError},
};

use super::{
    command::{CommandResponse, SyncResponse},
    Command,
};

/// Propose and convert error into `tonic::Status`
#[allow(clippy::wildcard_enum_match_arm)] // can't satisfy clippy
pub(super) async fn propose(
    client: &Arc<Client<Command>>,
    cmd: Command,
) -> Result<CommandResponse, tonic::Status> {
    client.propose(cmd).await.map_err(|err| match err {
        CommandProposeError::Execute(e) | CommandProposeError::AfterSync(e) => {
            tonic::Status::from(e)
        }
        _ => {
            unreachable!("propose err {err:?}")
        }
    })
}

/// Propose indexed and convert error into `tonic::Status`
#[allow(clippy::wildcard_enum_match_arm)] // can't satisfy clippy
pub(super) async fn propose_indexed(
    client: &Arc<Client<Command>>,
    cmd: Command,
) -> Result<(CommandResponse, SyncResponse), tonic::Status> {
    client.propose_indexed(cmd).await.map_err(|err| match err {
        CommandProposeError::Execute(e) | CommandProposeError::AfterSync(e) => e.into(),
        CommandProposeError::Propose(ProposeError::SyncedError(e)) => {
            tonic::Status::unknown(e.to_string())
        }
        _ => {
            unreachable!("propose err {err:?}")
        }
    })
}
