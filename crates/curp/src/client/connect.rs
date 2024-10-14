use std::collections::BTreeSet;

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::Stream;

use crate::{
    members::ServerId,
    rpc::{Change, MembershipResponse, ReadState, WaitLearnerResponse},
};

use super::retry::Context;

/// The response of propose command, deserialized from [`crate::rpc::ProposeResponse`] or
/// [`crate::rpc::WaitSyncedResponse`].
#[allow(type_alias_bounds)] // that's not bad
pub(crate) type ProposeResponse<C: Command> = Result<(C::ER, Option<C::ASR>), C::Error>;

/// `ClientApi`, a higher wrapper for `ConnectApi`, providing some methods for communicating to
/// the whole curp cluster. Automatically discovery curp server to update it's quorum.
#[async_trait]
#[allow(clippy::module_name_repetitions)] // better than just Api
pub trait ClientApi {
    /// The client error
    type Error;

    /// The command type
    type Cmd: Command;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        token: Option<&String>, // TODO: Allow external custom interceptors, do not pass token in parameters
        use_fast_path: bool,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error>;

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), Self::Error>;

    /// Send move leader request
    async fn move_leader(&self, node_id: ServerId) -> Result<(), Self::Error>;

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &Self::Cmd) -> Result<ReadState, Self::Error>;

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    ///
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<MembershipResponse, Self::Error>;

    /// Fetch leader id
    #[inline]
    async fn fetch_leader_id(&self, linearizable: bool) -> Result<ServerId, Self::Error> {
        self.fetch_cluster(linearizable)
            .await
            .map(|resp| resp.leader_id)
    }

    /// Performs membership change
    async fn change_membership(&self, changes: Vec<Change>) -> Result<(), Self::Error>;

    /// Send wait learner of the give ids, returns a stream of updating response stream
    async fn wait_learner(
        &self,
        node_ids: BTreeSet<u64>,
    ) -> Result<Box<dyn Stream<Item = Result<WaitLearnerResponse, Self::Error>> + Send>, Self::Error>;
}

/// This trait override some unrepeatable methods in ClientApi, and a client with this trait will be able to retry.
#[async_trait]
pub(crate) trait RepeatableClientApi {
    /// The client error
    type Error;

    /// The command type
    type Cmd: Command;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        token: Option<&String>,
        use_fast_path: bool,
        ctx: Context,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error>;

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self, ctx: Context) -> Result<(), Self::Error>;

    /// Send move leader request
    async fn move_leader(&self, node_id: u64, ctx: Context) -> Result<(), Self::Error>;

    /// Send fetch read state from leader
    async fn fetch_read_state(
        &self,
        cmd: &Self::Cmd,
        ctx: Context,
    ) -> Result<ReadState, Self::Error>;

    /// Performs membership change
    async fn change_membership(
        &self,
        changes: Vec<Change>,
        ctx: Context,
    ) -> Result<MembershipResponse, Self::Error>;

    /// Send wait learner of the give ids, returns a stream of updating response stream
    async fn wait_learner(
        &self,
        node_ids: BTreeSet<u64>,
        ctx: Context,
    ) -> Result<
        Box<dyn Stream<Item = Result<WaitLearnerResponse, tonic::Status>> + Send>,
        Self::Error,
    >;
}
