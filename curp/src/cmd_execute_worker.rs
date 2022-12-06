use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::{
    channel::key_spmc::SpmcKeyBasedReceiver,
    cmd::{Command, CommandExecutor},
    error::ExecuteError,
    LogIndex,
};

/// Number of execute workers
pub(crate) const N_EXECUTE_WORKERS: usize = 8;

/// Worker that execute commands
pub(crate) async fn execute_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: SpmcKeyBasedReceiver<C::K, Option<ExecuteMessage<C>>>,
    ce: Arc<CE>,
) {
    while let Ok((msg_wrapped, done)) = dispatch_rx.recv().await {
        #[allow(clippy::unwrap_used)]
        // it's a hack to bypass the map_msg(you can't do await in map_msg)
        // TODO: is there a better way to mark a spmc msg done instead of sending the msg back
        let ExecuteMessage { cmd, er_tx } = msg_wrapped.map_msg(Option::take).unwrap();

        match er_tx {
            ExecuteResultSender::Execute(tx) => {
                let er = cmd.execute(ce.as_ref()).await;
                debug!("cmd {:?} is executed", cmd.id());
                let _ignore = tx.send(er); // it's ok to ignore the result here because sometimes the result is not needed
            }
            ExecuteResultSender::AfterSync(tx, index) => {
                let asr = cmd.after_sync(ce.as_ref(), index).await;
                debug!("cmd {:?} after sync is called", cmd.id());
                let _ignore = tx.send(asr); // it's ok to ignore the result here because sometimes the result is not needed
            }
            ExecuteResultSender::ExecuteAndAfterSync(tx, index) => {
                let er = cmd.execute(ce.as_ref()).await;
                debug!("cmd {:?} is executed", cmd.id());
                #[allow(clippy::if_then_some_else_none)] // you can't do await in closure
                let asr = if er.is_ok() {
                    Some(cmd.after_sync(ce.as_ref(), index).await)
                } else {
                    None
                };
                let _ignore = tx.send((er, asr)); // it's ok to ignore the result here because sometimes the result is not needed
            }
        }

        if let Err(e) = done.send(msg_wrapped) {
            warn!("{e}");
        }
    }
}

/// Messages sent to the background cmd execution task
pub(crate) struct ExecuteMessage<C: Command + 'static> {
    /// The cmd to be executed
    pub(crate) cmd: Arc<C>,
    /// Send execution result
    er_tx: ExecuteResultSender<C>,
}

/// Channel for transferring execution results
pub(crate) enum ExecuteResultSender<C: Command + 'static> {
    /// Only call `execute`
    Execute(oneshot::Sender<Result<C::ER, ExecuteError>>),
    /// Only call `after_sync`
    AfterSync(oneshot::Sender<Result<C::ASR, ExecuteError>>, LogIndex),
    /// Call both `execute` and `after_sync`
    #[allow(clippy::type_complexity)] // though complex, it's quite clear
    ExecuteAndAfterSync(
        oneshot::Sender<(
            Result<C::ER, ExecuteError>,
            Option<Result<C::ASR, ExecuteError>>,
        )>,
        LogIndex,
    ),
}

/// Send cmd to background execute cmd task
pub(crate) struct CmdExecuteSender<C: Command + 'static>(mpsc::UnboundedSender<ExecuteMessage<C>>);

impl<C: Command + 'static> Clone for CmdExecuteSender<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<C: Command + 'static> CmdExecuteSender<C> {
    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    pub(crate) fn send_exe(&self, cmd: Arc<C>) -> oneshot::Receiver<Result<C::ER, ExecuteError>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.0.send(ExecuteMessage {
            cmd,
            er_tx: ExecuteResultSender::Execute(tx),
        }) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }

    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    pub(crate) fn send_after_sync(
        &self,
        cmd: Arc<C>,
        index: LogIndex,
    ) -> oneshot::Receiver<Result<C::ASR, ExecuteError>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.0.send(ExecuteMessage {
            cmd,
            er_tx: ExecuteResultSender::AfterSync(tx, index),
        }) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }

    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    #[allow(clippy::type_complexity)] // though complex, it's quite clear
    pub(crate) fn send_exe_and_after_sync(
        &self,
        cmd: Arc<C>,
        index: LogIndex,
    ) -> oneshot::Receiver<(
        Result<C::ER, ExecuteError>,
        Option<Result<C::ASR, ExecuteError>>,
    )> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.0.send(ExecuteMessage {
            cmd,
            er_tx: ExecuteResultSender::ExecuteAndAfterSync(tx, index),
        }) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }
}

/// Create a channel to send cmds to background cmd execute workers
pub(crate) fn cmd_execute_channel<C: Command + 'static>() -> (
    CmdExecuteSender<C>,
    mpsc::UnboundedReceiver<ExecuteMessage<C>>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    (CmdExecuteSender(tx), rx)
}
