use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use curp_external_api::cmd::Command;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tonic::Response;

use crate::member::Membership;
use crate::quorum::QuorumSet;
use crate::rpc;
use crate::rpc::connect::fetch::FetchApi;
use crate::rpc::connect::ConnectApi;
use crate::rpc::protocol_client::ProtocolClient;
use crate::rpc::CurpError;
use crate::rpc::FetchMembershipRequest;

use super::Unary;

impl<C: Command> Unary<C> {
    /// Fetches the latest membership config and updates the state
    pub(crate) async fn fetch_and_update_state(&self) -> Result<(), CurpError> {
        let (leader_id, membership, connects) = self.fetch_membership().await?;
        self.state
            .update_membership(leader_id, membership, connects)
            .await;

        Ok(())
    }

    /// Fetches the latest membership config
    ///
    /// Returns the leader id, membership, and rpc connects
    #[allow(clippy::pattern_type_mismatch)]
    async fn fetch_membership(
        &self,
    ) -> Result<(u64, Membership, HashMap<u64, Arc<dyn ConnectApi>>), CurpError> {
        let timeout = self.config.wait_synced_timeout;
        let leaders = self.fetch_leaders().await;
        for (leader_id, (addr, term)) in leaders {
            let client = ProtocolClient::connect(addr).await?;
            let membership = client
                .fetch_membership(FetchMembershipRequest {}, timeout)
                .await?
                .into_inner()
                .into_membership();
            let mut connects = membership
                .nodes
                .clone()
                .into_iter()
                // TODO: enable tls
                .map(|(id, a)| async move { (id, rpc::connect(id, vec![a], None).await) })
                .collect::<FuturesUnordered<_>>()
                .filter_map(|(id, res)| async move { res.ok().map(|c| (id, c)) })
                .collect::<HashMap<_, _>>()
                .await;
            let ids = connects
                .iter_mut()
                .map(|(id, c)| async move { (id, c.read_index(timeout).await) })
                .collect::<FuturesUnordered<_>>()
                .filter_map(|(id, res)| async move { res.ok().map(|r| (id, r.into_inner())) })
                .filter_map(|(id, res)| async move { (res.term == term).then_some(id) })
                .collect::<Vec<_>>()
                .await;
            if membership.as_joint().is_quorum(ids) {
                return Ok((leader_id, membership, connects));
            }
        }
        Err(CurpError::internal("no valid membership"))
    }

    /// Fetch all potiential leaders with the cluster configuration cached on
    /// the client
    ///
    /// Returns a map of leader id to (leader address, term)
    async fn fetch_leaders(&self) -> BTreeMap<u64, (String, u64)> {
        let timeout = self.config.wait_synced_timeout;
        let results: Vec<_> = self
            .state
            .for_each_server1(|api| async move {
                api.fetch_membership(FetchMembershipRequest {}, timeout)
                    .await
            })
            .await
            .collect()
            .await;

        results
            .into_iter()
            .filter_map(|(_id, res)| res.ok().map(Response::into_inner))
            .map(|resp| {
                #[allow(clippy::expect_used)]
                let leader_addr = resp
                    .nodes
                    .into_iter()
                    .find_map(|n| (n.node_id == resp.leader_id).then_some(n.addr))
                    .expect("leader id should always exist");
                (resp.leader_id, (leader_addr, resp.term))
            })
            .collect()
    }
}
