use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::Future;
use futures::FutureExt;
use futures::StreamExt;

use crate::quorum::QuorumSet;
use crate::rpc::connect::ConnectApi;

use super::State;

impl State {
    /// Execute an operation on each follower, until a quorum is reached.
    ///
    /// Parameters:
    /// - f: Operation to execute on each follower's connection
    /// - filter: Function to filter on each response
    /// - quorum: Function to determine if a quorum is reached, use functions in `QuorumSet` trait
    ///
    /// Returns `true` if then given quorum is reached.
    pub(crate) async fn for_each_follower_with_quorum<R, Fut: Future<Output = R>, F, Q>(
        &self,
        mut f: impl FnMut(Arc<dyn ConnectApi>) -> Fut,
        mut filter: F,
        mut expect_quorum: Q,
    ) -> bool
    where
        F: FnMut(R) -> bool,
        Q: FnMut(&dyn QuorumSet<Vec<u64>>, Vec<u64>) -> bool,
    {
        let mutable_r = self.mutable.read().await;
        let qs = mutable_r.membership.membership().as_joint();
        // FIXME: make sure that the client state contains a leader id
        #[allow(clippy::expect_used)]
        let leader_id = mutable_r.leader.expect("leader is not set");

        #[allow(clippy::pattern_type_mismatch)]
        let stream: FuturesUnordered<_> = mutable_r
            .membership
            .member_connects()
            .filter(|(id, _)| *id != leader_id)
            .map(|(id, conn)| f(Arc::clone(conn)).map(move |r| (id, r)))
            .collect();

        let mut filtered =
            stream.filter_map(|(id, r)| futures::future::ready(filter(r).then_some(id)));

        let mut ids = vec![leader_id];
        while let Some(id) = filtered.next().await {
            ids.push(id);
            if expect_quorum(&qs, ids.clone()) {
                return true;
            }
        }

        false
    }

    /// Take an async function and map to all server, returning `FuturesUnordered<F>`
    pub(crate) async fn for_each_server1<R, Fut: Future<Output = R>>(
        &self,
        mut f: impl FnMut(Arc<dyn ConnectApi>) -> Fut,
    ) -> FuturesUnordered<impl Future<Output = (u64, R)>> {
        let mutable_r = self.mutable.read().await;
        mutable_r
            .membership
            .member_connects()
            .map(|(id, conn)| f(Arc::clone(conn)).map(move |r| (id, r)))
            .collect()
    }
}
