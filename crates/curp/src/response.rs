use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};

use curp_external_api::cmd::Command;
use futures::Stream;
use tokio_stream::StreamExt;
use tonic::Status;

use crate::rpc::{CurpError, OpResponse, ProposeResponse, ResponseOp, SyncedResponse};

/// The response sender
#[derive(Debug)]
pub(super) struct ResponseSender {
    /// The stream sender
    tx: flume::Sender<Result<OpResponse, Status>>,
    /// Whether the command will be speculatively executed
    conflict: AtomicBool,
}

impl ResponseSender {
    /// Creates a new `ResponseSender`
    pub(super) fn new(tx: flume::Sender<Result<OpResponse, Status>>) -> ResponseSender {
        ResponseSender {
            tx,
            conflict: AtomicBool::new(false),
        }
    }

    /// Gets whether the command associated with this sender will be
    /// speculatively executed
    pub(super) fn is_conflict(&self) -> bool {
        self.conflict.load(Ordering::SeqCst)
    }

    /// Sets the the command associated with this sender will be
    /// speculatively executed
    pub(super) fn set_conflict(&self, conflict: bool) {
        let _ignore = self.conflict.fetch_or(conflict, Ordering::SeqCst);
    }

    /// Sends propose result
    pub(super) fn send_propose(&self, resp: ProposeResponse) {
        let resp = OpResponse {
            op: Some(ResponseOp::Propose(resp)),
        };
        // Ignore the result because the client might close the receiving stream
        let _ignore = self.tx.try_send(Ok(resp));
    }

    /// Sends after sync result
    pub(super) fn send_synced(&self, resp: SyncedResponse) {
        let resp = OpResponse {
            op: Some(ResponseOp::Synced(resp)),
        };
        // Ignore the result because the client might close the receiving stream
        let _ignore = self.tx.try_send(Ok(resp));
    }
}

/// Receiver for obtaining execution or after sync results
pub(crate) struct ResponseReceiver {
    /// The response stream
    resp_stream: Pin<Box<dyn Stream<Item = tonic::Result<OpResponse>> + Send>>,
}

impl ResponseReceiver {
    /// Creates a new [`ResponseReceiver`].
    pub(crate) fn new(
        resp_stream: Box<dyn Stream<Item = tonic::Result<OpResponse>> + Send>,
    ) -> Self {
        Self {
            resp_stream: Box::into_pin(resp_stream),
        }
    }

    /// Receives the results
    pub(crate) async fn recv<C: Command>(
        &mut self,
        both: bool,
    ) -> Result<Result<(C::ER, Option<C::ASR>), C::Error>, CurpError> {
        let fst = self.recv_resp().await?;

        match fst {
            ResponseOp::Propose(propose_resp) => {
                let conflict = propose_resp.conflict;
                let er_result = propose_resp.map_result::<C, _, _>(|res| {
                    res.map(|er| er.unwrap_or_else(|| unreachable!()))
                })?;
                if let Err(e) = er_result {
                    return Ok(Err(e));
                }
                if conflict || both {
                    let snd = self.recv_resp().await?;
                    let ResponseOp::Synced(synced_resp) = snd else {
                        unreachable!()
                    };
                    let asr_result = synced_resp
                        .map_result::<C, _, _>(|res| res.unwrap_or_else(|| unreachable!()))?;
                    return Ok(er_result.and_then(|er| asr_result.map(|asr| (er, Some(asr)))));
                }
                Ok(er_result.map(|er| (er, None)))
            }
            ResponseOp::Synced(synced_resp) => {
                let asr_result = synced_resp
                    .map_result::<C, _, _>(|res| res.unwrap_or_else(|| unreachable!()))?;
                if let Err(e) = asr_result {
                    return Ok(Err(e));
                }
                let snd = self.recv_resp().await?;
                let ResponseOp::Propose(propose_resp) = snd else {
                    unreachable!("op: {snd:?}")
                };
                let er_result = propose_resp.map_result::<C, _, _>(|res| {
                    res.map(|er| er.unwrap_or_else(|| unreachable!()))
                })?;
                Ok(er_result.and_then(|er| asr_result.map(|asr| (er, Some(asr)))))
            }
        }
    }

    /// Receives a single response from stream
    async fn recv_resp(&mut self) -> Result<ResponseOp, CurpError> {
        let resp = self
            .resp_stream
            .next()
            .await
            .ok_or(CurpError::internal("stream reaches on an end".to_owned()))??;
        Ok(resp
            .op
            .unwrap_or_else(|| unreachable!("op should always exist")))
    }
}
