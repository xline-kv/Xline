/// Client propose implementation
mod propose_impl;

use std::marker::PhantomData;

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use tracing::warn;

use super::{
    config::Config,
    connect::{ProposeResponse, RepeatableClientApi},
    retry::Context,
};
use crate::{
    members::ServerId,
    rpc::{
        AddLearnerRequest, AddMemberRequest, CurpError, FetchReadStateRequest, MoveLeaderRequest,
        PublishRequest, ReadState, RemoveLearnerRequest, RemoveMemberRequest, ShutdownRequest,
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
        let req = ShutdownRequest::new(ctx.propose_id(), ctx.cluster_state().cluster_version());
        let timeout = self.config.wait_synced_timeout();
        let _resp = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.shutdown(req, timeout).await })
            .await?;

        Ok(())
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
        ctx: Context,
    ) -> Result<(), Self::Error> {
        let req = PublishRequest::new(ctx.propose_id(), node_id, node_name, node_client_urls);
        let timeout = self.config.wait_synced_timeout();
        let _resp = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.publish(req, timeout).await })
            .await?;

        Ok(())
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: u64, ctx: Context) -> Result<(), Self::Error> {
        let req = MoveLeaderRequest::new(node_id, ctx.cluster_state().cluster_version());
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
        let req = FetchReadStateRequest::new(cmd, ctx.cluster_state().cluster_version()).map_err(
            |ser_err| {
                warn!("serializing error: {ser_err}");
                CurpError::from(ser_err)
            },
        )?;
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

    /// Add some learners to the cluster.
    async fn add_learner(&self, addrs: Vec<String>, ctx: Context) -> Result<Vec<u64>, Self::Error> {
        let req = AddLearnerRequest { node_addrs: addrs };
        let timeout = self.config.wait_synced_timeout();
        let resp = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.add_learner(req, timeout).await })
            .await?;

        Ok(resp.into_inner().node_ids)
    }

    /// Remove some learners from the cluster.
    async fn remove_learner(&self, ids: Vec<u64>, ctx: Context) -> Result<(), Self::Error> {
        let req = RemoveLearnerRequest { node_ids: ids };
        let timeout = self.config.wait_synced_timeout();
        let _ig = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.remove_learner(req, timeout).await })
            .await?;

        Ok(())
    }

    /// Add some members to the cluster.
    async fn add_member(&self, ids: Vec<u64>, ctx: Context) -> Result<(), Self::Error> {
        let req = AddMemberRequest { node_ids: ids };
        let timeout = self.config.wait_synced_timeout();
        let _ig = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.add_member(req, timeout).await })
            .await?;

        Ok(())
    }

    /// Add some members to the cluster.
    async fn remove_member(&self, ids: Vec<u64>, ctx: Context) -> Result<(), Self::Error> {
        let req = RemoveMemberRequest { node_ids: ids };
        let timeout = self.config.wait_synced_timeout();
        let _ig = ctx
            .cluster_state()
            .map_leader(|conn| async move { conn.remove_member(req, timeout).await })
            .await?;

        Ok(())
    }
}
