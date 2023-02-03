//! `exe` stands for execution
//! `as` stands for after sync

use std::{iter, sync::Arc};

use async_trait::async_trait;
use event_listener::Event;
#[cfg(test)]
use mockall::automock;
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{debug, error, warn};
use utils::parking_lot_lock::{MutexMap, RwLockMap};

use super::spec_pool::SpecPoolRef;
use crate::{
    cmd::{Command, CommandExecutor, ConflictCheck},
    conflict_checked_mpmc,
    conflict_checked_mpmc::{ConflictCheckedMsg, DoneNotifier},
    server::cmd_board::CmdBoardRef,
    LogIndex,
};

/// Number of execute workers
const N_EXECUTE_WORKERS: usize = 4;

/// Number of after sync workers
const N_AFTER_SYNC_WORKERS: usize = 4;

/// Worker that execute commands
async fn execute_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: CmdExeReceiver<C>,
    cmd_board: CmdBoardRef<C>,
    spec: SpecPoolRef<C>,
    ce: Arc<CE>,
) {
    while let Ok((msg, done)) = dispatch_rx.recv().await {
        match msg {
            ExeMsg::Execute { cmd, er_tx } => {
                let er = ce.execute(cmd.as_ref()).await.map_err(|e| e.to_string());
                debug!("cmd {:?} is speculatively executed", cmd.id());

                spec.map_lock(|mut spec_l| spec_l.remove(cmd.id())); // clean spec pool
                let _ignore = er_tx.send(er.clone()); // it's ok to ignore the result here because sometimes the result is not needed

                // insert er to cmd_board and check if after sync is already needed
                let index = {
                    let mut board_w = cmd_board.write();
                    let er_ok = er.is_ok();
                    board_w.insert_er(cmd.id(), er);

                    if !er_ok {
                        // execution failed, no need to do as
                        let _ig = board_w.needs_as.remove(cmd.id());
                        done.notify();
                        continue;
                    }

                    if let Some(index) = board_w.needs_as.remove(cmd.id()) {
                        index
                    } else {
                        // mark a cmd done only when after sync is called to ensure as and exe order of conflicted commands
                        assert!(
                            board_w
                                .done_notifiers
                                .insert(cmd.id().clone(), done)
                                .is_none(),
                            "done_notifier should not be inserted twice"
                        );
                        continue;
                    }
                };

                let asr = ce
                    .after_sync(cmd.as_ref(), index)
                    .await
                    .map_err(|e| e.to_string());
                cmd_board.write().insert_asr(cmd.id(), asr);
                debug!("cmd {:?} after sync is called", cmd.id());
            }
            ExeMsg::Reset => {
                ce.reset().await;
                debug!("command executor has been reset");
            }
        }
        done.notify();
    }
    error!("execute worker stopped unexpectedly");
}

/// Worker that do after sync
async fn after_sync_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: CmdAsReceiver<C>,
    cmd_board: CmdBoardRef<C>,
    spec: SpecPoolRef<C>,
    ce: Arc<CE>,
) {
    while let Ok((AsMsg { cmd, index }, done)) = dispatch_rx.recv().await {
        /// Different conditions in after sync
        enum Condition {
            /// Call both exe and after sync
            ExeAndAfterSync,
            /// Call after sync only
            AfterSync(DoneNotifier),
            /// Do nothing
            Nothing,
        }
        let condition = cmd_board.map_write(|mut board_w| {
            if board_w.needs_exe.remove(cmd.id()) {
                // The cmd needs both execution and after sync
                Condition::ExeAndAfterSync
            } else if let Some(er) = board_w.er_buffer.get(cmd.id()) {
                if er.is_ok() {
                    #[allow(clippy::unwrap_used)]
                    // execution succeeded, we can do call after sync now
                    Condition::AfterSync(board_w.done_notifiers.remove(cmd.id()).unwrap())
                } else {
                    // execution failed, we should not call after sync at all
                    Condition::Nothing
                }
            } else {
                // execution has not finished yet, we add the cmd to needs_as so that it will be called when execution finishes
                // this rarely happens
                assert!(
                    board_w.needs_as.insert(cmd.id().clone(), index).is_none(),
                    "cmd should not be inserted into needs_as twice"
                );
                Condition::Nothing
            }
        });

        match condition {
            Condition::ExeAndAfterSync => {
                let er = ce.execute(cmd.as_ref()).await.map_err(|e| e.to_string());
                let er_ok = er.is_ok();
                cmd_board.write().insert_er(cmd.id(), er);
                debug!("cmd {:?} is executed", cmd.id());

                spec.map_lock(|mut spec_l| spec_l.remove(cmd.id())); // clean spec pool

                if er_ok {
                    let asr = ce
                        .after_sync(cmd.as_ref(), index)
                        .await
                        .map_err(|e| e.to_string());
                    cmd_board.write().insert_asr(cmd.id(), asr);
                    debug!("cmd {:?} after sync is called", cmd.id());
                }
            }
            Condition::AfterSync(exe_d) => {
                let asr = ce
                    .after_sync(cmd.as_ref(), index)
                    .await
                    .map_err(|e| e.to_string());
                cmd_board.write().insert_asr(cmd.id(), asr);
                debug!("cmd {:?} after sync is called", cmd.id());
                exe_d.notify();
            }
            Condition::Nothing => {}
        }
        done.notify();
    }
    error!("after sync worker stopped unexpectedly");
}

/// Messages sent to the background cmd execution task
enum ExeMsg<C: Command + 'static> {
    /// The cmd needs to be executed
    Execute {
        /// The cmd to be executed
        cmd: Arc<C>,
        /// Send execution result
        er_tx: oneshot::Sender<Result<C::ER, String>>,
    },
    /// We need to reset the ce state
    Reset,
}

/// Token of `ExeMsg`, used for conflict checking
enum ExeMsgToken<C> {
    /// Is a regular cmd exe or after sync
    Cmd(Arc<C>),
    /// Is a reset message
    Reset,
}

impl<C: Command> ConflictCheck for ExeMsgToken<C> {
    fn is_conflict(&self, other: &Self) -> bool {
        #[allow(clippy::pattern_type_mismatch)] // clash with clippy::needless_borrowed_reference
        match (&self, &other) {
            (ExeMsgToken::Cmd(ref cmd1), ExeMsgToken::Cmd(ref cmd2)) => cmd1.is_conflict(cmd2),
            // Reset should conflict with all others
            _ => true,
        }
    }
}

impl<C: Command + 'static> ConflictCheckedMsg for ExeMsg<C> {
    type Token = ExeMsgToken<C>;

    fn token(&self) -> Self::Token {
        match *self {
            ExeMsg::Execute { ref cmd, .. } => ExeMsgToken::Cmd(Arc::clone(cmd)),
            ExeMsg::Reset => ExeMsgToken::Reset,
        }
    }
}

/// Messages sent to the background after sync task
struct AsMsg<C: Command + 'static> {
    /// The cmd to be after synced
    cmd: Arc<C>,
    /// Log index of the cmd
    index: LogIndex,
}

/// Token of `AsMsg`
struct AsMsgToken<C>(Arc<C>);

impl<C: Command + 'static> ConflictCheck for AsMsgToken<C> {
    fn is_conflict(&self, other: &Self) -> bool {
        self.0.is_conflict(&other.0)
    }
}

impl<C: Command + 'static> ConflictCheckedMsg for AsMsg<C> {
    type Token = AsMsgToken<C>;

    fn token(&self) -> Self::Token {
        AsMsgToken(Arc::clone(&self.cmd))
    }
}

/// Send cmd to background execute cmd task
#[derive(Clone)]
pub(super) struct CmdExeSender<C: Command + 'static> {
    /// Send tasks to execute workers
    exe_tx: flume::Sender<ExeMsg<C>>,
    /// Send tasks to after sync workers
    as_tx: flume::Sender<AsMsg<C>>,
}

/// Recv cmds that need to be executed
#[derive(Clone)]
struct CmdExeReceiver<C: Command + 'static>(flume::Receiver<(ExeMsg<C>, DoneNotifier)>);

/// Recv cmds that need to be after synced
#[derive(Clone)]
struct CmdAsReceiver<C: Command + 'static>(flume::Receiver<(AsMsg<C>, DoneNotifier)>);

/// Send cmd to background execution worker
#[cfg_attr(test, automock)]
pub(super) trait CmdExeSenderInterface<C: Command + 'static>: Send + Sync + 'static {
    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    fn send_exe(&self, cmd: Arc<C>) -> oneshot::Receiver<Result<C::ER, String>>;

    /// Send after sync event to the background cmd executor so that after sync can be called
    fn send_after_sync(&self, cmd: Arc<C>, index: LogIndex);

    /// Send flush
    fn send_reset(&self);
}

impl<C: Command + 'static> CmdExeSenderInterface<C> for CmdExeSender<C> {
    fn send_exe(&self, cmd: Arc<C>) -> oneshot::Receiver<Result<C::ER, String>> {
        let (tx, rx) = oneshot::channel();
        let msg = ExeMsg::Execute { cmd, er_tx: tx };
        if let Err(e) = self.exe_tx.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
        rx
    }

    fn send_after_sync(&self, cmd: Arc<C>, index: LogIndex) {
        let msg = AsMsg { cmd, index };
        if let Err(e) = self.as_tx.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
    }

    fn send_reset(&self) {
        let msg = ExeMsg::Reset;
        if let Err(e) = self.exe_tx.send(msg) {
            warn!("failed to send cmd to background execute cmd task, {e}");
        }
    }
}

/// Cmd exe recv interface
#[cfg_attr(test, automock)]
#[async_trait]
trait CmdExeReceiverInterface<C: Command + 'static> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<(ExeMsg<C>, DoneNotifier), flume::RecvError>;
}

/// Cmd after sync recv interface
#[cfg_attr(test, automock)]
#[async_trait]
trait CmdAsReceiverInterface<C: Command + 'static> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<(AsMsg<C>, DoneNotifier), flume::RecvError>;
}

#[async_trait]
impl<C: Command + 'static> CmdExeReceiverInterface<C> for CmdExeReceiver<C> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<(ExeMsg<C>, DoneNotifier), flume::RecvError> {
        self.0.recv_async().await
    }
}

#[async_trait]
impl<C: Command + 'static> CmdAsReceiverInterface<C> for CmdAsReceiver<C> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<(AsMsg<C>, DoneNotifier), flume::RecvError> {
        self.0.recv_async().await
    }
}

/// Run cmd execute workers. Returns a channel to interact with these workers. The channel guarantees the execution order and that after sync is called after execution completes.
pub(super) fn start_cmd_workers<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    cmd_executor: CE,
    spec: SpecPoolRef<C>,
    cmd_board: CmdBoardRef<C>,
    shutdown_trigger: Arc<Event>,
) -> CmdExeSender<C> {
    let (exe_tx, exe_rx) = conflict_checked_mpmc::channel();
    let (as_tx, as_rx) = conflict_checked_mpmc::channel();
    let cmd_executor = Arc::new(cmd_executor);
    let bg_exe_worker_handles: Vec<JoinHandle<_>> = iter::repeat((
        exe_rx,
        Arc::clone(&spec),
        Arc::clone(&cmd_board),
        Arc::clone(&cmd_executor),
    ))
    .take(N_EXECUTE_WORKERS)
    .map(|(rx, spec_c, cmd_board_c, ce)| {
        tokio::spawn(execute_worker(CmdExeReceiver(rx), cmd_board_c, spec_c, ce))
    })
    .collect();
    let bg_as_worker_handles: Vec<JoinHandle<_>> =
        iter::repeat((as_rx, spec, cmd_board, cmd_executor))
            .take(N_AFTER_SYNC_WORKERS)
            .map(|(rx, spec_c, cmd_board_c, ce)| {
                tokio::spawn(after_sync_worker(
                    CmdAsReceiver(rx),
                    cmd_board_c,
                    spec_c,
                    ce,
                ))
            })
            .collect();
    let _ig = tokio::spawn(async move {
        shutdown_trigger.listen().await;
        for handle in bg_exe_worker_handles {
            handle.abort();
        }
        for handle in bg_as_worker_handles {
            handle.abort();
        }
    });

    CmdExeSender { exe_tx, as_tx }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use parking_lot::{Mutex, RwLock};
    use tokio::{sync::mpsc, time::Instant};
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        server::{cmd_board::CommandBoard, spec_pool::SpeculativePool},
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
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let exe_tx = start_cmd_workers(ce, spec_pool, cmd_board, Arc::new(Event::new()));

        let cmd = Arc::new(TestCommand::default());
        exe_tx.send_exe(Arc::clone(&cmd));
        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        exe_tx.send_after_sync(Arc::clone(&cmd), 1);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
    }

    // When the execution takes more time than sync, `as` should be called after exe has finished
    #[traced_test]
    #[tokio::test]
    async fn fast_path_cond1() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            Arc::clone(&cmd_board),
            Arc::new(Event::new()),
        );

        let begin = Instant::now();
        let cmd = Arc::new(TestCommand::default().set_exe_dur(Duration::from_secs(1)));
        exe_tx.send_exe(Arc::clone(&cmd));

        // at 500ms, sync has completed, call after sync, then needs_as will be updated
        sleep_millis(500).await;
        exe_tx.send_after_sync(Arc::clone(&cmd), 1);

        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);

        assert!((Instant::now() - begin) >= Duration::from_secs(1));
    }

    // When the execution takes more time than sync and fails, after sync should not be called
    #[traced_test]
    #[tokio::test]
    async fn fast_path_cond2() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            Arc::clone(&cmd_board),
            Arc::new(Event::new()),
        );

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
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            Arc::clone(&cmd_board),
            Arc::new(Event::new()),
        );

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
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            Arc::clone(&cmd_board),
            Arc::new(Event::new()),
        );

        let cmd = Arc::new(TestCommand::default().set_exe_should_fail());
        {
            cmd_board.write().needs_exe.insert(cmd.id().clone());
        }

        exe_tx.send_after_sync(Arc::clone(&cmd), 1);

        sleep_millis(100).await;
        assert!(er_rx.try_recv().is_err());
        assert!(as_rx.try_recv().is_err());
    }

    // If cmd1 and cmd2 conflict, order will be (cmd1 exe) -> (cmd1 as) -> (cmd2 exe) -> (cmd2 as)
    #[traced_test]
    #[tokio::test]
    async fn conflict_cmd_order() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            Arc::clone(&cmd_board),
            Arc::new(Event::new()),
        );

        let cmd1 = Arc::new(TestCommand::new_put(vec![1], 1));
        let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
        exe_tx.send_exe(Arc::clone(&cmd1));
        exe_tx.send_exe(Arc::clone(&cmd2));

        // cmd1 exe done
        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        sleep_millis(100).await;

        // cmd2 will not be executed
        assert!(er_rx.try_recv().is_err());
        assert!(as_rx.try_recv().is_err());

        // cmd1 and cmd2 after sync
        exe_tx.send_after_sync(Arc::clone(&cmd1), 1);
        exe_tx.send_after_sync(Arc::clone(&cmd2), 2);

        assert_eq!(er_rx.recv().await.unwrap().1, vec![1]);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
        assert_eq!(as_rx.recv().await.unwrap().1, 2);
    }
}
