use std::sync::Arc;
use std::time::Duration;

use curp_external_api::cmd::Command;
use futures::Future;
use futures::Stream;
use tonic::Response;
use tonic::Status;

use crate::client::connect::ClientError;
use crate::client::ClientApi;
use crate::client::ProposeResponse;
use crate::quorum::QuorumSet;
use crate::response::ResponseReceiver;
use crate::rpc::connect::ConnectApi;
use crate::rpc::CurpError;
use crate::rpc::OpResponse;
use crate::rpc::ProposeId;
use crate::rpc::ProposeRequest;
use crate::rpc::RecordRequest;

use super::Unary;

/// Propose result type
type ProposeResult =
    Result<Response<Box<dyn Stream<Item = Result<OpResponse, Status>> + Send>>, CurpError>;

impl<C: Command> Unary<C> {
    #[allow(unused)] // FIXME: Enable this after new membership change on the server has been
    // implemented
    /// Send propose to the whole cluster, `use_fast_path` set to `false` to
    /// fallback into ordered requests (event the requests are commutative).
    async fn propose1(
        &self,
        propose_id: ProposeId,
        cmd: &<Self as ClientApi>::Cmd,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<<Self as ClientApi>::Cmd>, <Self as ClientError>::Error> {
        let cmd_arc = Arc::new(cmd);
        let term = self.state.term().await;
        let propose_req = ProposeRequest::new::<C>(
            propose_id,
            cmd_arc.as_ref(),
            self.state.cluster_version().await,
            term,
            !use_fast_path,
            self.tracker.read().first_incomplete(),
        );
        let timeout = self.config.propose_timeout;
        let propose_fut = self.map_leader(|conn| async move {
            conn.propose_stream(propose_req, token.cloned(), timeout)
                .await
        });
        if cmd.is_read_only() {
            let read_index_fut = self.read_index(term, timeout);
            Self::propose_read_only1(propose_fut, read_index_fut).await
        } else {
            let record_fut = self.record(propose_id, cmd_arc.as_ref(), timeout);
            Self::propose_mutative1(propose_fut, record_fut, use_fast_path).await
        }
    }

    /// Propose for mutative commands
    async fn propose_mutative1<PF, RF>(
        propose_fut: PF,
        record_fut: RF,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<C>, CurpError>
    where
        PF: Future<Output = ProposeResult>,
        RF: Future<Output = bool>,
    {
        let (propose_res, fast_path_failed) = tokio::join!(propose_fut, record_fut);
        let resp_stream = propose_res?.into_inner();
        let mut response_rx = ResponseReceiver::new(resp_stream);
        response_rx
            .recv::<C>(fast_path_failed || !use_fast_path)
            .await
    }

    /// Propose for read only commands
    ///
    /// For read-only commands, we only need to send propose to leader
    async fn propose_read_only1<PF, RF>(
        propose_fut: PF,
        read_index_fut: RF,
    ) -> Result<ProposeResponse<C>, CurpError>
    where
        PF: Future<Output = ProposeResult>,
        RF: Future<Output = bool>,
    {
        let (propose_res, success) = tokio::join!(propose_fut, read_index_fut);
        if !success {
            return Err(CurpError::WrongClusterVersion(()));
        }
        let resp_stream = propose_res?.into_inner();
        let mut response_rx = ResponseReceiver::new(resp_stream);
        response_rx.recv::<C>(false).await
    }

    /// Build a record future
    fn record(
        &self,
        propose_id: ProposeId,
        cmd: &<Self as ClientApi>::Cmd,
        timeout: Duration,
    ) -> impl Future<Output = bool> + '_ {
        let record_req = RecordRequest::new::<C>(propose_id, cmd);
        let record = move |conn: Arc<dyn ConnectApi>| {
            let record_req_c = record_req.clone();
            async move { conn.record(record_req_c, timeout).await }
        };
        self.state.for_each_follower_with_quorum(
            record,
            |res| res.is_ok(),
            |qs, ids| QuorumSet::is_super_quorum(qs, ids),
        )
    }

    /// Build a `read_index` future
    fn read_index(&self, term: u64, timeout: Duration) -> impl Future<Output = bool> + '_ {
        let read_index =
            move |conn: Arc<dyn ConnectApi>| async move { conn.read_index(timeout).await };
        self.state.for_each_follower_with_quorum(
            read_index,
            move |res| res.is_ok_and(|r| r.get_ref().term == term),
            |qs, ids| QuorumSet::is_quorum(qs, ids),
        )
    }
}
