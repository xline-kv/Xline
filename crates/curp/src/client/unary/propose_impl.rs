use std::pin::Pin;

use curp_external_api::cmd::Command;
use futures::{future, stream, FutureExt, Stream, StreamExt};

use crate::{
    client::ProposeResponse,
    members::ServerId,
    quorum,
    rpc::{CurpError, OpResponse, ProposeId, ProposeRequest, RecordRequest, ResponseOp},
    super_quorum,
};

use super::Unary;

/// A stream of propose events
type EventStream<'a, C> = Box<dyn Stream<Item = Result<ProposeEvent<C>, CurpError>> + Send + 'a>;

/// An event returned by the cluster during propose
enum ProposeEvent<C: Command> {
    /// Speculative execution result
    SpecExec {
        /// conflict returned by the leader
        conflict_l: bool,
        /// Speculative execution result
        er: Result<C::ER, C::Error>,
    },
    /// After sync result
    AfterSync {
        /// After sync result
        asr: Result<C::ASR, C::Error>,
    },
    /// Record result
    Record {
        /// conflict returned by the follower
        conflict: bool,
    },
}

impl<C: Command> Unary<C> {
    /// Propose for mutative commands
    pub(super) async fn propose_mutative(
        &self,
        cmd: &C,
        propose_id: ProposeId,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<C>, CurpError> {
        let stream = self
            .send_propose_mutative(cmd, propose_id, use_fast_path, token)
            .await?;
        let mut stream = Box::into_pin(stream);
        let first_two_events = (
            Self::next_event(&mut stream).await?,
            Self::next_event(&mut stream).await?,
        );
        match first_two_events {
            (ProposeEvent::SpecExec { er, .. }, ProposeEvent::AfterSync { asr })
            | (ProposeEvent::AfterSync { asr }, ProposeEvent::SpecExec { er, .. }) => {
                Ok(Self::combine_er_asr(er, asr))
            }
            (ProposeEvent::SpecExec { conflict_l, er }, ProposeEvent::Record { conflict })
            | (ProposeEvent::Record { conflict }, ProposeEvent::SpecExec { conflict_l, er }) => {
                let require_asr = !use_fast_path || conflict | conflict_l;
                Self::with_spec_exec(stream, er, require_asr).await
            }
            (ProposeEvent::AfterSync { asr }, ProposeEvent::Record { .. })
            | (ProposeEvent::Record { .. }, ProposeEvent::AfterSync { asr }) => {
                Self::with_after_sync(stream, asr).await
            }
            _ => unreachable!("no other possible events"),
        }
    }

    /// Propose for read only commands
    ///
    /// For read-only commands, we only need to send propose to leader
    ///
    /// TODO: Provide an implementation that delegates the read index to the leader for batched
    /// processing.
    pub(super) async fn propose_read_only(
        &self,
        cmd: &C,
        propose_id: ProposeId,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<C>, CurpError> {
        let leader_id = self.leader_id().await?;
        let stream = self
            .send_leader_propose(cmd, leader_id, propose_id, use_fast_path, token)
            .await?;
        let mut stream_pinned = Box::into_pin(stream);
        if !self.send_read_index(leader_id).await {
            return Err(CurpError::WrongClusterVersion(()));
        }
        if use_fast_path {
            let event = Self::next_event(&mut stream_pinned).await?;
            match event {
                ProposeEvent::SpecExec { conflict_l, er } => {
                    Self::with_spec_exec(stream_pinned, er, conflict_l).await
                }
                ProposeEvent::AfterSync { asr } => Self::with_after_sync(stream_pinned, asr).await,
                ProposeEvent::Record { .. } => unreachable!("leader does not returns record event"),
            }
        } else {
            let leader_events = (
                Self::next_event(&mut stream_pinned).await?,
                Self::next_event(&mut stream_pinned).await?,
            );
            match leader_events {
                (ProposeEvent::SpecExec { er, .. }, ProposeEvent::AfterSync { asr })
                | (ProposeEvent::AfterSync { asr }, ProposeEvent::SpecExec { er, .. }) => {
                    Ok(Self::combine_er_asr(er, asr))
                }
                _ => unreachable!("no other possible events"),
            }
        }
    }

    /// Send propose to the cluster
    ///
    /// Returns a stream that combines the propose stream and record request
    async fn send_propose_mutative(
        &self,
        cmd: &C,
        propose_id: ProposeId,
        use_fast_path: bool,
        token: Option<&String>,
    ) -> Result<EventStream<'_, C>, CurpError> {
        let leader_id = self.leader_id().await?;
        let leader_stream = self
            .send_leader_propose(cmd, leader_id, propose_id, use_fast_path, token)
            .await?;
        let follower_stream = self.send_record(cmd, leader_id, propose_id).await;
        let select = stream::select(Box::into_pin(leader_stream), Box::into_pin(follower_stream));

        Ok(Box::new(select))
    }

    /// Send propose request to the leader
    async fn send_leader_propose(
        &self,
        cmd: &C,
        leader_id: ServerId,
        propose_id: ProposeId,
        use_fast_path: bool,
        token: Option<&String>,
    ) -> Result<EventStream<'_, C>, CurpError> {
        let term = self.state.term().await;
        let propose_req = ProposeRequest::new::<C>(
            propose_id,
            cmd,
            self.state.cluster_version().await,
            term,
            !use_fast_path,
            self.tracker.read().first_incomplete(),
        );
        let timeout = self.config.propose_timeout;
        let token = token.cloned();
        let stream = self
            .state
            .map_server(leader_id, move |conn| async move {
                conn.propose_stream(propose_req, token, timeout).await
            })
            .map(Self::flatten_propose_stream_result)
            .map(Box::into_pin)
            .flatten_stream();

        Ok(Box::new(stream))
    }

    /// Send read index requests to the cluster
    ///
    /// Returns `true` if the read index is successful
    async fn send_read_index(&self, leader_id: ServerId) -> bool {
        let term = self.state.term().await;
        let connects_len = self.state.connects_len().await;
        let quorum = quorum(connects_len);
        let expect = quorum.wrapping_sub(1);
        let timeout = self.config.propose_timeout;

        self.state
            .for_each_follower(
                leader_id,
                |conn| async move { conn.read_index(timeout).await },
            )
            .await
            .filter_map(|res| future::ready(res.ok()))
            .filter(|resp| future::ready(resp.get_ref().term == term))
            .take(expect)
            .count()
            .map(|c| c >= expect)
            .await
    }

    /// Send record requests to the cluster
    ///
    /// Returns a stream that yield a single event
    async fn send_record(
        &self,
        cmd: &C,
        leader_id: ServerId,
        propose_id: ProposeId,
    ) -> EventStream<'_, C> {
        let connects_len = self.state.connects_len().await;
        let superquorum = super_quorum(connects_len);
        let timeout = self.config.propose_timeout;
        let record_req = RecordRequest::new::<C>(propose_id, cmd);
        let expect = superquorum.wrapping_sub(1);
        let stream = self
            .state
            .for_each_follower(leader_id, |conn| {
                let record_req_c = record_req.clone();
                async move { conn.record(record_req_c, timeout).await }
            })
            .await
            .filter_map(|res| future::ready(res.ok()))
            .filter(|resp| future::ready(!resp.get_ref().conflict))
            .take(expect)
            .count()
            .map(move |c| ProposeEvent::Record {
                conflict: c < expect,
            })
            .map(Ok)
            .into_stream();

        Box::new(stream)
    }

    /// Flattens the result of `ConnectApi::propose_stream`
    ///
    /// It is considered a propose failure when the stream returns a `CurpError`
    #[allow(clippy::type_complexity)] // copied from the return value of `ConnectApi::propose_stream`
    fn flatten_propose_stream_result(
        result: Result<
            tonic::Response<Box<dyn Stream<Item = Result<OpResponse, tonic::Status>> + Send>>,
            CurpError,
        >,
    ) -> EventStream<'static, C> {
        match result {
            Ok(stream) => {
                let pinned_stream = Box::into_pin(stream.into_inner());
                Box::new(
                    pinned_stream.map(|r| r.map_err(CurpError::from).map(ProposeEvent::<C>::from)),
                )
            }
            Err(e) => Box::new(future::ready(Err(e)).into_stream()),
        }
    }

    /// Combines the results of speculative execution and after-sync replication.
    fn combine_er_asr(
        er: Result<C::ER, C::Error>,
        asr: Result<C::ASR, C::Error>,
    ) -> ProposeResponse<C> {
        er.and_then(|e| asr.map(|a| (e, Some(a))))
    }

    /// Handles speculative execution and record processing.
    async fn with_spec_exec(
        mut stream: Pin<EventStream<'_, C>>,
        er: Result<C::ER, C::Error>,
        require_asr: bool,
    ) -> Result<ProposeResponse<C>, CurpError> {
        if require_asr {
            let event = Self::next_event(&mut stream).await?;
            let ProposeEvent::AfterSync { asr } = event else {
                unreachable!("event should only be asr");
            };
            Ok(Self::combine_er_asr(er, asr))
        } else {
            Ok(er.map(|e| (e, None)))
        }
    }

    /// Handles after-sync and record processing.
    async fn with_after_sync(
        mut stream: Pin<EventStream<'_, C>>,
        asr: Result<C::ASR, C::Error>,
    ) -> Result<ProposeResponse<C>, CurpError> {
        let event = Self::next_event(&mut stream).await?;
        let ProposeEvent::SpecExec { er, .. } = event else {
            unreachable!("event should only be er");
        };
        Ok(Self::combine_er_asr(er, asr))
    }

    /// Retrieves the next event from the stream.
    async fn next_event(
        stream: &mut Pin<EventStream<'_, C>>,
    ) -> Result<ProposeEvent<C>, CurpError> {
        stream
            .next()
            .await
            .transpose()?
            .ok_or(CurpError::internal("propose stream closed"))
    }
}

// Converts the propose stream response to event
// TODO: The deserialization structure need to be simplified
#[allow(clippy::expect_used)] // too verbose to write unreachables
impl<C: Command> From<OpResponse> for ProposeEvent<C> {
    fn from(resp: OpResponse) -> Self {
        match resp.op.expect("op should always exist") {
            ResponseOp::Propose(resp) => Self::SpecExec {
                conflict_l: resp.conflict,
                er: resp
                    .map_result::<C, _, _>(Result::transpose)
                    .ok()
                    .flatten()
                    .expect("er deserialization should never fail"),
            },
            ResponseOp::Synced(resp) => Self::AfterSync {
                asr: resp
                    .map_result::<C, _, _>(|res| res)
                    .ok()
                    .flatten()
                    .expect("asr deserialization should never fail"),
            },
        }
    }
}
