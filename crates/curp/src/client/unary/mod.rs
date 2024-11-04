/// Client propose implementation
mod propose_impl;

use std::{collections::BTreeSet, marker::PhantomData};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::Stream;
use tracing::{debug, warn};

use super::{
    config::Config,
    connect::{ProposeResponse, RepeatableClientApi},
    retry::Context,
};
use crate::{
    member::Membership,
    rpc::{
        Change, ChangeMembershipRequest, CurpError, FetchReadStateRequest, MembershipChange,
        MembershipResponse, MoveLeaderRequest, ReadState, ShutdownRequest, WaitLearnerRequest,
        WaitLearnerResponse,
    },
};

/// The unary client
#[derive(Debug)]
pub(super) struct Unary<C: Command> {
    /// Unary config
    config: Config,
    /// marker
    phantom: PhantomData<C>,
}

impl<C: Command> Unary<C> {
    /// Create an unary client
    pub(super) fn new(config: Config) -> Self {
        Self {
            config,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<C: Command> RepeatableClientApi for Unary<C> {
    /// The error is generated from server
    type Error = CurpError;

    /// The command type
    type Cmd = C;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        token: Option<&String>,
        use_fast_path: bool,
        ctx: Context,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error> {
        if cmd.is_read_only() {
            self.propose_read_only(cmd, token, use_fast_path, &ctx)
                .await
        } else {
            self.propose_mutative(cmd, token, use_fast_path, &ctx).await
        }
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self, ctx: Context) -> Result<(), Self::Error> {
        let req = ShutdownRequest::new(ctx.propose_id(), 0);
        let timeout = self.config.wait_synced_timeout();
        let _resp = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.shutdown(req, timeout).await })
            .await?;

        Ok(())
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: u64, ctx: Context) -> Result<(), Self::Error> {
        let req = MoveLeaderRequest::new(node_id, 0);
        let timeout = self.config.wait_synced_timeout();
        let _resp = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.move_leader(req, timeout).await })
            .await?;

        Ok(())
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(
        &self,
        cmd: &Self::Cmd,
        ctx: Context,
    ) -> Result<ReadState, Self::Error> {
        // Same as fast_round, we blame the serializing error to the server even
        // thought it is the local error
        let req = FetchReadStateRequest::new(cmd, 0).map_err(|ser_err| {
            warn!("serializing error: {ser_err}");
            CurpError::from(ser_err)
        })?;
        let timeout = self.config.wait_synced_timeout();
        let state = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.fetch_read_state(req, timeout).await })
            .await?
            .into_inner()
            .read_state
            .unwrap_or_else(|| unreachable!("read_state must be set in fetch read state response"));

        Ok(state)
    }

    async fn change_membership(
        &self,
        changes: Vec<Change>,
        ctx: Context,
    ) -> Result<Option<MembershipResponse>, Self::Error> {
        if Self::change_applied(ctx.cluster_state().membership(), &changes) {
            debug!("membership already applied, skipping changes");
            return Ok(None);
        }
        let changes = changes
            .into_iter()
            .map(|c| MembershipChange { change: Some(c) })
            .collect();
        let cluster_version = ctx.cluster_state().cluster_version();
        let req = ChangeMembershipRequest {
            cluster_version,
            changes,
        };
        let timeout = self.config.wait_synced_timeout();
        let resp = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.change_membership(req, timeout).await })
            .await?
            .into_inner();

        Ok(Some(resp))
    }

    /// Send wait learner of the give ids, returns a stream of updating response stream
    async fn wait_learner(
        &self,
        node_ids: BTreeSet<u64>,
        ctx: Context,
    ) -> Result<
        Box<dyn Stream<Item = Result<WaitLearnerResponse, tonic::Status>> + Send>,
        Self::Error,
    > {
        let node_ids = node_ids.into_iter().collect();
        let req = WaitLearnerRequest { node_ids };
        let timeout = self.config.wait_synced_timeout();
        let resp = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.wait_learner(req, timeout).await })
            .await?
            .into_inner();

        Ok(resp)
    }
}

impl<C: Command> Unary<C> {
    /// Check if the changes already applied to the cluster membership
    ///
    /// TODO: Currently we do not send any request if the changes are already satisfied. However,
    /// this may lead to some semantic ambiguity. For example, the id of a `Change::Remove` might
    /// be invalid, but we still assume it has completed. A better implementation might be send a
    /// full membership state to the cluster.
    fn change_applied(membership: &Membership, changes: &[Change]) -> bool {
        changes.iter().all(|change| match *change {
            Change::Add(ref node) => membership.nodes.get(&node.node_id) == node.meta.as_ref(),
            Change::Remove(id) => !membership.nodes.contains_key(&id),
            Change::Promote(id) => membership.contains_member(id),
            Change::Demote(id) => !membership.contains_member(id),
        })
    }
}
