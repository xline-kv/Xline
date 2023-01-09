use std::sync::Arc;

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use tokio::sync::oneshot;
use tracing::{debug, error, warn};
use utils::parking_lot_lock::RwLockMap;

use crate::{
    cmd::{Command, CommandExecutor, ConflictCheck},
    conflict_checked_mpmc,
    conflict_checked_mpmc::{ConflictCheckedMsg, DoneNotifier},
    error::ExecuteError,
    server::cmd_board::CmdBoardRef,
    LogIndex,
};

/// Number of execute workers
pub(super) const N_EXECUTE_WORKERS: usize = 8;

/// Worker that execute commands
pub(super) async fn execute_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: CmdExeReceiver<C>,
    cmd_board: CmdBoardRef<C>,
    ce: Arc<CE>,
) {
    while let Ok((msg, done)) = dispatch_rx.recv().await {
        match msg {
            ExecuteMessage::Execute { cmd, er_tx } => {
                let er = ce.execute(cmd.as_ref()).await;
                debug!("cmd {:?} is executed", cmd.id());
                cmd_board.write().insert_er(cmd.id(), er.clone()); // insert er
                let _ignore = er_tx.send(er); // it's ok to ignore the result here because sometimes the result is not needed
            }
            ExecuteMessage::AfterSync { cmd, index } => {
                /// Different conditions in after sync
                enum Condition {
                    /// Call both exe and after sync
                    ExeAndAfterSync,
                    /// Call after sync only
                    AfterSync,
                    /// Do nothing
                    Nothing,
                }
                let condition = cmd_board.map_write(|mut board_w| {
                    #[allow(clippy::unwrap_used)]
                    // We can unwrap here because ExecuteMessage::AfterSync must arrive later than ExecuteMessage::Execute because a cmd conflicts itself
                    if board_w.needs_exe.remove(cmd.id()) {
                        // The cmd needs both execution and after sync
                        Condition::ExeAndAfterSync
                    } else if board_w.er_buffer.get(cmd.id()).unwrap().is_ok() {
                        // execution succeeded, we can do call after sync now
                        Condition::AfterSync
                    } else {
                        Condition::Nothing
                    }
                });

                match condition {
                    Condition::ExeAndAfterSync => {
                        let er = ce.execute(cmd.as_ref()).await;
                        let er_ok = er.is_ok();
                        cmd_board.write().insert_er(cmd.id(), er);
                        debug!("cmd {:?} is executed", cmd.id());

                        if er_ok {
                            let asr = ce.after_sync(cmd.as_ref(), index).await;
                            cmd_board.write().insert_asr(cmd.id(), asr);
                            debug!("cmd {:?} after sync is called", cmd.id());
                        }
                    }
                    Condition::AfterSync => {
                        let asr = ce.after_sync(cmd.as_ref(), index).await;
                        cmd_board.write().insert_asr(cmd.id(), asr);
                        debug!("cmd {:?} after sync is called", cmd.id());
                    }
                    Condition::Nothing => {}
                }
            }
            ExecuteMessage::Reset => {
                ce.reset().await;
            }
        }
        if let Err(e) = done.notify() {
            warn!("{e}");
        }
    }
    error!("execute worker stopped unexpectedly");
}

/// Messages sent to the background cmd execution task
enum ExecuteMessage<C: Command + 'static> {
    /// The cmd is ready for execution
    Execute {
        /// The cmd to be executed
        cmd: Arc<C>,
        /// Send execution result
        er_tx: oneshot::Sender<Result<C::ER, ExecuteError>>,
    },
    /// The cmd is ready for after sync
    AfterSync {
        /// The cmd to be executed to be after aynced
        cmd: Arc<C>,
        /// Index of the cmd
        index: LogIndex,
    },
    /// We need to reset the ce state
    Reset,
}

/// Token of `ExecuteMessage`, used for conflict checking
enum Token<C> {
    /// Is a regular cmd exe or after sync
    Cmd(Arc<C>),
    /// Is a reset message
    Reset,
}

impl<C: Command> ConflictCheck for Token<C> {
    fn is_conflict(&self, other: &Self) -> bool {
        match (self, other) {
            (&Token::Cmd(ref cmd1), &Token::Cmd(ref cmd2)) => cmd1.is_conflict(cmd2),
            // Reset should conflict with all others
            _ => true,
        }
    }
}

impl<C: Command + 'static> ConflictCheckedMsg for ExecuteMessage<C> {
    type Token = Token<C>;

    fn token(&self) -> Self::Token {
        match *self {
            ExecuteMessage::AfterSync { ref cmd, .. } | ExecuteMessage::Execute { ref cmd, .. } => {
                Token::Cmd(Arc::clone(cmd))
            }
            ExecuteMessage::Reset => Token::Reset,
        }
    }
}

/// Send cmd to background execute cmd task
#[derive(Clone)]
pub(super) struct CmdExeSender<C: Command + 'static>(flume::Sender<ExecuteMessage<C>>);

/// Recv cmds that need to be executed or after synced
#[derive(Clone)]
pub(super) struct CmdExeReceiver<C: Command + 'static>(
    flume::Receiver<(ExecuteMessage<C>, DoneNotifier)>,
);

/// Send cmd to background execution worker
#[cfg_attr(test, automock)]
pub(super) trait CmdExeSenderInterface<C: Command + 'static>: Send + Sync + 'static {
    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    fn send_exe(&self, cmd: Arc<C>) -> oneshot::Receiver<Result<C::ER, ExecuteError>>;

    /// Send after sync event to the background cmd executor so that after sync can be called
    fn send_after_sync(&self, cmd: Arc<C>, index: LogIndex);

    /// Send flush
    fn send_reset(&self);
}

impl<C: Command + 'static> CmdExeSenderInterface<C> for CmdExeSender<C> {
    fn send_exe(&self, cmd: Arc<C>) -> oneshot::Receiver<Result<C::ER, ExecuteError>> {
        let (tx, rx) = oneshot::channel();
        let msg = ExecuteMessage::Execute { cmd, er_tx: tx };
        if let Err(e) = self.0.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }

    fn send_after_sync(&self, cmd: Arc<C>, index: LogIndex) {
        let msg = ExecuteMessage::AfterSync { cmd, index };
        if let Err(e) = self.0.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
    }

    fn send_reset(&self) {
        let msg = ExecuteMessage::Reset;
        if let Err(e) = self.0.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
    }
}

/// Cmd exe recv interface
#[cfg_attr(test, automock)]
#[async_trait]
trait CmdExeReceiverInterface<C: Command + 'static> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<(ExecuteMessage<C>, DoneNotifier), flume::RecvError>;
}

#[async_trait]
impl<C: Command + 'static> CmdExeReceiverInterface<C> for CmdExeReceiver<C> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<(ExecuteMessage<C>, DoneNotifier), flume::RecvError> {
        self.0.recv_async().await
    }
}

/// Create a channel to send cmds to background cmd execute workers. The channel guarantees the execution order and that after sync is called after execution completes
pub(super) fn cmd_exe_channel<C: Command + 'static>() -> (CmdExeSender<C>, CmdExeReceiver<C>) {
    let (tx, rx) = conflict_checked_mpmc::channel();
    (CmdExeSender(tx), CmdExeReceiver(rx))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use parking_lot::RwLock;
    use tokio::{sync::mpsc, time::Instant};
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        server::cmd_board::CommandBoard,
        test_utils::{
            sleep_millis, sleep_secs,
            test_cmd::{TestCE, TestCommand},
        },
    };

    // This should happen in fast path in most cases
    #[traced_test]
    #[tokio::test]
    async fn fast_path_normal() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let (exe_tx, exe_rx) = cmd_exe_channel::<TestCommand>();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        tokio::spawn(execute_worker(exe_rx, Arc::clone(&cmd_board), Arc::new(ce)));

        let cmd = Arc::new(TestCommand::default());
        exe_tx.send_exe(Arc::clone(&cmd));
        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        exe_tx.send_after_sync(Arc::clone(&cmd), 1);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
    }

    // When the execution takes more time than sync, as should be called after exe has finished
    #[traced_test]
    #[tokio::test]
    async fn fast_path_cond1() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new("S1".to_owned(), er_tx, as_tx));
        let (exe_tx, exe_rx) = cmd_exe_channel::<TestCommand>();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        tokio::spawn(execute_worker(
            exe_rx.clone(),
            Arc::clone(&cmd_board),
            Arc::clone(&ce),
        ));

        let begin = Instant::now();
        let cmd = Arc::new(TestCommand::default().set_exe_dur(Duration::from_secs(1)));
        exe_tx.send_exe(Arc::clone(&cmd));

        // at 500ms, sync has completed, call after sync, this will be dispatched to the second exe_worker
        sleep_millis(500).await;
        exe_tx.send_after_sync(Arc::clone(&cmd), 1);

        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);

        assert!((Instant::now() - begin) >= Duration::from_secs(1));
    }

    // When the execution takes more time than sync and fails, as should not be called
    #[traced_test]
    #[tokio::test]
    async fn fast_path_cond2() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new("S1".to_owned(), er_tx, as_tx));
        let (exe_tx, exe_rx) = cmd_exe_channel::<TestCommand>();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        tokio::spawn(execute_worker(
            exe_rx.clone(),
            Arc::clone(&cmd_board),
            Arc::clone(&ce),
        ));

        let cmd = Arc::new(
            TestCommand::default()
                .set_exe_dur(Duration::from_secs(1))
                .set_exe_should_fail(),
        );
        exe_tx.send_exe(Arc::clone(&cmd));

        // at 500ms, sync has completed
        sleep_millis(500).await;
        exe_tx.send_after_sync(Arc::clone(&cmd), 1);

        // at 1500ms, as should not be called
        sleep_secs(1).await;
        assert!(er_rx.try_recv().is_err());
        assert!(as_rx.try_recv().is_err());
    }

    // This should happen in slow path in most cases
    #[traced_test]
    #[tokio::test]
    async fn slow_path_normal() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let (exe_tx, exe_rx) = cmd_exe_channel::<TestCommand>();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        tokio::spawn(execute_worker(exe_rx, Arc::clone(&cmd_board), Arc::new(ce)));

        let cmd = Arc::new(TestCommand::default());
        {
            cmd_board.write().needs_exe.insert(cmd.id().clone());
        }

        exe_tx.send_after_sync(Arc::clone(&cmd), 1);

        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
    }

    // When exe fails
    #[traced_test]
    #[tokio::test]
    async fn slow_path_exe_fails() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let (exe_tx, exe_rx) = cmd_exe_channel::<TestCommand>();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        tokio::spawn(execute_worker(exe_rx, Arc::clone(&cmd_board), Arc::new(ce)));

        let cmd = Arc::new(TestCommand::default().set_exe_should_fail());
        {
            cmd_board.write().needs_exe.insert(cmd.id().clone());
        }

        exe_tx.send_after_sync(Arc::clone(&cmd), 1);

        sleep_millis(100).await;
        assert!(er_rx.try_recv().is_err());
        assert!(as_rx.try_recv().is_err());
    }
}
