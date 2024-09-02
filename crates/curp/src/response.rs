use std::sync::atomic::{AtomicBool, Ordering};

use curp_external_api::cmd::Command;
use tonic::Status;

use crate::rpc::{OpResponse, ProposeResponse, ResponseOp, SyncedResponse};

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

    /// Sends the error result
    pub(super) fn send_err<C: Command>(&self, err: C::Error) {
        let er = ProposeResponse::new_result::<C>(&Err(err.clone()), false);
        let asr = SyncedResponse::new_result::<C>(&Err(err));
        self.send_propose(er);
        self.send_synced(asr);
    }
}
