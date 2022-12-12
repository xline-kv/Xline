use std::sync::Arc;

use futures::future::OptionFuture;
use tokio::sync::oneshot;
use tracing::{debug, error, warn};

use crate::{
    cmd::{Command, CommandExecutor},
    conflict_checked_mpmc,
    conflict_checked_mpmc::{ConflictCheckedMsg, DoneNotifier},
    error::ExecuteError,
    LogIndex,
};

/// Number of execute workers
pub(super) const N_EXECUTE_WORKERS: usize = 8;

/// Worker that execute commands
#[allow(clippy::type_complexity)]
pub(crate) async fn execute_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: CmdExeReceiver<C>,
    ce: Arc<CE>,
) {
    while let Ok((ExecuteMessage { cmd, er_tx }, done)) = dispatch_rx.0.recv_async().await {
        match er_tx {
            ExeResultSender::Execute(tx) => {
                let er = cmd.execute(ce.as_ref()).await;
                debug!("cmd {:?} is executed", cmd.id());
                let _ignore = tx.send(er); // it's ok to ignore the result here because sometimes the result is not needed
            }
            ExeResultSender::AfterSync(tx, index) => {
                let asr = cmd.after_sync(ce.as_ref(), index).await;
                debug!("cmd {:?} after sync is called", cmd.id());
                let _ignore = tx.send(asr); // it's ok to ignore the result here because sometimes the result is not needed
            }
            ExeResultSender::ExecuteAndAfterSync(tx, index) => {
                let er = cmd.execute(ce.as_ref()).await;
                debug!("cmd {:?} is executed", cmd.id());
                let asr: OptionFuture<_> = er
                    .is_ok()
                    .then(|| async {
                        let asr = cmd.after_sync(ce.as_ref(), index).await;
                        debug!("cmd {:?} call after sync", cmd.id());
                        asr
                    })
                    .into();
                let _ignore = tx.send((er, asr.await)); // it's ok to ignore the result here because sometimes the result is not needed
            }
        }
        if let Err(e) = done.notify() {
            warn!("{e}");
        }
    }
    error!("execute worker stopped unexpectedly");
}

/// Messages sent to the background cmd execution task
struct ExecuteMessage<C: Command + 'static> {
    /// The cmd to be executed
    cmd: Arc<C>,
    /// Send execution result
    er_tx: ExeResultSender<C>,
}

impl<C: Command + 'static> ConflictCheckedMsg for ExecuteMessage<C> {
    type Token = C;

    fn token(&self) -> Arc<Self::Token> {
        Arc::clone(&self.cmd)
    }
}

impl<C: Command + 'static> ExecuteMessage<C> {
    /// Create a new exe msg
    fn new(cmd: Arc<C>, er_tx: ExeResultSender<C>) -> Self {
        Self { cmd, er_tx }
    }
}

/// Channel for transferring execution results
enum ExeResultSender<C: Command + 'static> {
    /// Only call `execute`
    Execute(oneshot::Sender<Result<C::ER, ExecuteError>>),
    /// Only call `after_sync`
    AfterSync(oneshot::Sender<Result<C::ASR, ExecuteError>>, LogIndex),
    /// Call both `execute` and `after_sync`
    #[allow(clippy::type_complexity)] // though complex, it's quite clear
    ExecuteAndAfterSync(
        oneshot::Sender<(
            Result<C::ER, ExecuteError>,
            Option<Result<C::ASR, ExecuteError>>, // why option: if execution fails, after sync will not be called
        )>,
        LogIndex,
    ),
}

/// Send cmd to background execute cmd task
#[derive(Clone)]
pub(crate) struct CmdExeSender<C: Command + 'static>(flume::Sender<ExecuteMessage<C>>);

/// Recv cmds that need to be executed or after synced
#[derive(Clone)]
pub(crate) struct CmdExeReceiver<C: Command + 'static>(
    flume::Receiver<(ExecuteMessage<C>, DoneNotifier)>,
);

impl<C: Command + 'static> CmdExeSender<C> {
    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    pub(super) fn send_exe(&self, cmd: Arc<C>) -> oneshot::Receiver<Result<C::ER, ExecuteError>> {
        let (tx, rx) = oneshot::channel();
        let msg = ExecuteMessage::new(cmd, ExeResultSender::Execute(tx));
        if let Err(e) = self.0.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }

    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    pub(super) fn send_after_sync(
        &self,
        cmd: Arc<C>,
        index: LogIndex,
    ) -> oneshot::Receiver<Result<C::ASR, ExecuteError>> {
        let (tx, rx) = oneshot::channel();
        let msg = ExecuteMessage::new(cmd, ExeResultSender::AfterSync(tx, index));
        if let Err(e) = self.0.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }

    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    #[allow(clippy::type_complexity)] // though complex, it's quite clear
    pub(super) fn send_exe_and_after_sync(
        &self,
        cmd: Arc<C>,
        index: LogIndex,
    ) -> oneshot::Receiver<(
        Result<C::ER, ExecuteError>,
        Option<Result<C::ASR, ExecuteError>>,
    )> {
        let (tx, rx) = oneshot::channel();
        let msg = ExecuteMessage::new(cmd, ExeResultSender::ExecuteAndAfterSync(tx, index));
        if let Err(e) = self.0.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }
}

/// Create a channel to send cmds to background cmd execute workers. The channel guarantees the execution order and that after sync is called after execution completes
pub(crate) fn cmd_exe_channel<C: Command + 'static>() -> (CmdExeSender<C>, CmdExeReceiver<C>) {
    let (tx, rx) = conflict_checked_mpmc::channel();
    (CmdExeSender(tx), CmdExeReceiver(rx))
}
