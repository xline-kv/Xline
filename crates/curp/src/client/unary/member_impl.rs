use async_trait::async_trait;
use curp_external_api::cmd::Command;

use crate::client::connect::MemberClientApi;
use crate::rpc::AddLearnerRequest;
use crate::rpc::RemoveLearnerRequest;

use super::Unary;

#[async_trait]
impl<C: Command> MemberClientApi for Unary<C> {
    /// Add some learners to the cluster.
    async fn add_learner(&self, addrs: Vec<String>) -> Result<Vec<u64>, Self::Error> {
        let req = AddLearnerRequest { node_addrs: addrs };
        let timeout = self.config.wait_synced_timeout;
        let resp = self
            .map_leader(|conn| async move { conn.add_learner(req, timeout).await })
            .await?;

        Ok(resp.into_inner().node_ids)
    }

    /// Remove some learners from the cluster.
    async fn remove_learner(&self, ids: Vec<u64>) -> Result<(), Self::Error> {
        let req = RemoveLearnerRequest { node_ids: ids };
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.remove_learner(req, timeout).await })
            .await?;

        Ok(())
    }
}
