use async_trait::async_trait;

use crate::client::connect::LeaderStateUpdate;
use crate::client::connect::MemberClientApi;
use crate::client::connect::RepeatableClientApi;
use crate::rpc::CurpError;

use super::Retry;

#[async_trait]
impl<Api> MemberClientApi for Retry<Api>
where
    Api: RepeatableClientApi<Error = CurpError> + LeaderStateUpdate + Send + Sync + 'static,
{
    /// Add some learners to the cluster.
    async fn add_learner(&self, addrs: Vec<String>) -> Result<Vec<u64>, Self::Error> {
        self.retry::<_, _>(|client| client.add_learner(addrs.clone()))
            .await
    }

    /// Remove some learners from the cluster.
    async fn remove_learner(&self, ids: Vec<u64>) -> Result<(), Self::Error> {
        self.retry::<_, _>(|client| client.remove_learner(ids.clone()))
            .await
    }
}
