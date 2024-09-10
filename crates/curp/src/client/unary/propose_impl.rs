use std::{pin::Pin, sync::Arc};

use curp_external_api::cmd::Command;
use futures::{future, stream, FutureExt, Stream, StreamExt};

use crate::{
    client::{connect::ProposeResponse, retry::Context},
    quorum::QuorumSet,
    rpc::{connect::ConnectApi, CurpError, OpResponse, ProposeRequest, RecordRequest, ResponseOp},
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
        token: Option<&String>,
        use_fast_path: bool,
        ctx: &Context,
    ) -> Result<ProposeResponse<C>, CurpError> {
        let stream = self.send_propose_mutative(cmd, use_fast_path, token, ctx);
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
        token: Option<&String>,
        use_fast_path: bool,
        ctx: &Context,
    ) -> Result<ProposeResponse<C>, CurpError> {
        let stream = self.send_leader_propose(cmd, use_fast_path, token, ctx);
        let mut stream_pinned = Box::into_pin(stream);
        if !self.send_read_index(ctx).await {
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
    fn send_propose_mutative(
        &self,
        cmd: &C,
        use_fast_path: bool,
        token: Option<&String>,
        ctx: &Context,
    ) -> EventStream<'_, C> {
        let leader_stream = self.send_leader_propose(cmd, use_fast_path, token, ctx);
        let follower_stream = self.send_record(cmd, ctx);
        let select = stream::select(Box::into_pin(leader_stream), Box::into_pin(follower_stream));

        Box::new(select)
    }

    /// Send propose request to the leader
    fn send_leader_propose(
        &self,
        cmd: &C,
        use_fast_path: bool,
        token: Option<&String>,
        ctx: &Context,
    ) -> EventStream<'_, C> {
        let term = ctx.cluster_state().term();
        let propose_req = ProposeRequest::new::<C>(
            ctx.propose_id(),
            cmd,
            ctx.cluster_state().cluster_version(),
            term,
            !use_fast_path,
            ctx.first_incomplete(),
        );
        let timeout = self.config.propose_timeout();
        let token = token.cloned();
        let stream = ctx
            .cluster_state()
            .map_leader(move |conn| async move {
                conn.propose_stream(propose_req, token, timeout).await
            })
            .map(Self::flatten_propose_stream_result)
            .map(Box::into_pin)
            .flatten_stream();

        Box::new(stream)
    }

    /// Send read index requests to the cluster
    ///
    /// Returns `true` if the read index is successful
    async fn send_read_index(&self, ctx: &Context) -> bool {
        let term = ctx.cluster_state().term();
        let timeout = self.config.propose_timeout();
        let read_index =
            move |conn: Arc<dyn ConnectApi>| async move { conn.read_index(timeout).await };

        ctx.cluster_state()
            .for_each_follower_with_quorum(
                read_index,
                move |res| res.is_ok_and(|resp| resp.get_ref().term == term),
                |qs, ids| QuorumSet::is_quorum(qs, ids),
            )
            .await
    }

    /// Send record requests to the cluster
    ///
    /// Returns a stream that yield a single event
    fn send_record(&self, cmd: &C, ctx: &Context) -> EventStream<'_, C> {
        let timeout = self.config.propose_timeout();
        let record_req = RecordRequest::new::<C>(ctx.propose_id(), cmd);
        let record = move |conn: Arc<dyn ConnectApi>| {
            let record_req_c = record_req.clone();
            async move { conn.record(record_req_c, timeout).await }
        };

        let stream = ctx
            .cluster_state()
            .for_each_follower_with_quorum(
                record,
                |res| res.is_ok_and(|resp| !resp.get_ref().conflict),
                |qs, ids| QuorumSet::is_super_quorum(qs, ids),
            )
            .map(move |conflict| ProposeEvent::Record { conflict })
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
