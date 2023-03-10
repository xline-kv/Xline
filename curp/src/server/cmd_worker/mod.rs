//! `exe` stands for execution
//! `as` stands for after sync

use std::{fmt::Debug, iter, sync::Arc};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
#[cfg(test)]
use mockall::automock;
use tokio::task::JoinHandle;
use tracing::{debug, error};

use self::conflict_checked_mpmc::Task;
use super::{curp_node::UncommittedPoolRef, spec_pool::SpecPoolRef};
use crate::{
    cmd::{Command, CommandExecutor},
    server::{cmd_board::CmdBoardRef, cmd_worker::conflict_checked_mpmc::TaskType},
};

/// The special conflict checked mpmc
mod conflict_checked_mpmc;

/// Number of execute workers
const N_WORKERS: usize = 8;

/// Event for command executor
enum CEEvent<C> {
    /// The cmd is ready for speculative execution
    SpecExeReady(Arc<C>),
    /// The cmd is ready for after sync
    ASReady(Arc<C>, usize),
    /// Reset the command executor
    Reset,
}

impl<C: Command> Debug for CEEvent<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::SpecExeReady(ref cmd) => f.debug_tuple("SpecExeReady").field(cmd.id()).finish(),
            Self::ASReady(ref cmd, ref index) => f
                .debug_tuple("ASReady")
                .field(cmd.id())
                .field(index)
                .finish(),
            Self::Reset => write!(f, "Reset"),
        }
    }
}

/// Worker that execute commands
async fn cmd_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: TaskRx<C>,
    done_tx: flume::Sender<(Task<C>, bool)>,
    cb: CmdBoardRef<C>,
    sp: SpecPoolRef<C>,
    ucp: UncommittedPoolRef<C>,
    ce: Arc<CE>,
) {
    while let Ok(task) = dispatch_rx.recv().await {
        let succeeded = match *task.inner() {
            TaskType::SpecExe(ref cmd) => {
                let er = ce.execute(cmd.as_ref()).await.map_err(|e| e.to_string());
                let er_ok = er.is_ok();
                debug!("cmd {:?} is speculatively executed", cmd.id());
                cb.write().insert_er(cmd.id(), er);
                er_ok
            }
            TaskType::AS(ref cmd, index) => {
                let asr = ce
                    .after_sync(cmd.as_ref(), index.numeric_cast())
                    .await
                    .map_err(|e| e.to_string());
                let asr_ok = asr.is_ok();
                cb.write().insert_asr(cmd.id(), asr);
                sp.lock().remove(cmd.id());
                let _ig = ucp.lock().remove(cmd.id());
                debug!("cmd {:?} after sync is called", cmd.id());
                asr_ok
            }
            TaskType::Reset => {
                ce.reset().await;
                debug!("command executor has been reset");
                true
            }
        };
        if let Err(e) = done_tx.send((task, succeeded)) {
            error!("can't mark a task done, the channel could be closed, {e}");
        }
    }
    error!("cmd worker exits unexpectedly");
}

/// Send event to background command executor workers
#[derive(Debug, Clone)]
pub(super) struct CEEventTx<C: Command + 'static>(flume::Sender<CEEvent<C>>);

/// Recv cmds that need to be executed
#[derive(Clone)]
struct TaskRx<C: Command + 'static>(flume::Receiver<Task<C>>);

/// Send cmd to background execution worker
#[cfg_attr(test, automock)]
pub(super) trait CEEventTxApi<C: Command + 'static>: Send + Sync + 'static {
    /// Send cmd to background cmd worker for speculative execution
    fn send_sp_exe(&self, cmd: Arc<C>);

    /// Send after sync event to the background cmd worker so that after sync can be called
    fn send_after_sync(&self, cmd: Arc<C>, index: usize);

    /// Send reset
    fn send_reset(&self);
}

impl<C: Command + 'static> CEEventTxApi<C> for CEEventTx<C> {
    fn send_sp_exe(&self, cmd: Arc<C>) {
        let event = CEEvent::SpecExeReady(cmd);
        if let Err(e) = self.0.send(event) {
            error!("failed to send cmd exe event to background cmd worker, {e}");
        }
    }

    fn send_after_sync(&self, cmd: Arc<C>, index: usize) {
        let event = CEEvent::ASReady(cmd, index);
        if let Err(e) = self.0.send(event) {
            error!("failed to send cmd as event to background cmd worker, {e}");
        }
    }

    fn send_reset(&self) {
        let msg = CEEvent::Reset;
        if let Err(e) = self.0.send(msg) {
            error!("failed to send reset event to background cmd worker, {e}");
        }
    }
}

/// Cmd exe recv interface
#[cfg_attr(test, automock)]
#[async_trait]
trait TaskRxApi<C: Command + 'static> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<Task<C>, flume::RecvError>;
}

#[async_trait]
impl<C: Command + 'static> TaskRxApi<C> for TaskRx<C> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<Task<C>, flume::RecvError> {
        self.0.recv_async().await
    }
}

/// Run cmd execute workers. Returns a channel to interact with these workers. The channel guarantees the execution order and that after sync is called after execution completes.
pub(super) fn start_cmd_workers<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    cmd_executor: CE,
    spec_pool: SpecPoolRef<C>,
    uncommitted_pool: UncommittedPoolRef<C>,
    cmd_board: CmdBoardRef<C>,
    shutdown_trigger: Arc<event_listener::Event>,
) -> CEEventTx<C> {
    let (event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
    #[allow(clippy::shadow_unrelated)] // false positive
    let bg_worker_handles: Vec<JoinHandle<_>> = iter::repeat((
        task_rx,
        done_tx,
        cmd_board,
        spec_pool,
        uncommitted_pool,
        Arc::new(cmd_executor),
    ))
    .take(N_WORKERS)
    .map(|(task_rx, done_tx, cb, sp, ucp, ce)| {
        tokio::spawn(cmd_worker(TaskRx(task_rx), done_tx, cb, sp, ucp, ce))
    })
    .collect();

    let _ig = tokio::spawn(async move {
        shutdown_trigger.listen().await;
        for handle in bg_worker_handles {
            handle.abort();
        }
    });

    CEEventTx(event_tx)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use parking_lot::{Mutex, RwLock};
    use tokio::{sync::mpsc, time::Instant};
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        server::{cmd_board::CommandBoard, curp_node::UncommittedPool, spec_pool::SpeculativePool},
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
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            uncommitted_pool,
            Arc::clone(&cmd_board),
            Arc::new(event_listener::Event::new()),
        );

        let cmd = Arc::new(TestCommand::default());

        exe_tx.send_sp_exe(Arc::clone(&cmd));
        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        exe_tx.send_after_sync(Arc::clone(&cmd), 1);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
    }

    // When the execution takes more time than sync, `as` should be called after exe has finished
    #[traced_test]
    #[tokio::test]
    async fn fast_path_cond1() {
        let (er_tx, _er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            uncommitted_pool,
            Arc::clone(&cmd_board),
            Arc::new(event_listener::Event::new()),
        );

        let begin = Instant::now();
        let cmd = Arc::new(TestCommand::default().set_exe_dur(Duration::from_secs(1)));

        exe_tx.send_sp_exe(Arc::clone(&cmd));

        // at 500ms, sync has completed, call after sync, then needs_as will be updated
        sleep_millis(500).await;
        exe_tx.send_after_sync(Arc::clone(&cmd), 1);

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
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            uncommitted_pool,
            Arc::clone(&cmd_board),
            Arc::new(event_listener::Event::new()),
        );

        let cmd = Arc::new(
            TestCommand::default()
                .set_exe_dur(Duration::from_secs(1))
                .set_exe_should_fail(),
        );

        exe_tx.send_sp_exe(Arc::clone(&cmd));

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
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            uncommitted_pool,
            Arc::clone(&cmd_board),
            Arc::new(event_listener::Event::new()),
        );

        let cmd = Arc::new(TestCommand::default());

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
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            uncommitted_pool,
            Arc::clone(&cmd_board),
            Arc::new(event_listener::Event::new()),
        );
        let cmd = Arc::new(TestCommand::default().set_exe_should_fail());

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
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            uncommitted_pool,
            Arc::clone(&cmd_board),
            Arc::new(event_listener::Event::new()),
        );

        let cmd1 = Arc::new(TestCommand::new_put(vec![1], 1));
        let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
        exe_tx.send_sp_exe(Arc::clone(&cmd1));
        exe_tx.send_sp_exe(Arc::clone(&cmd2));

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

    #[traced_test]
    #[tokio::test]
    async fn reset_will_wipe_all_states_and_outdated_cmds() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let exe_tx = start_cmd_workers(
            ce,
            spec_pool,
            uncommitted_pool,
            Arc::clone(&cmd_board),
            Arc::new(event_listener::Event::new()),
        );

        let cmd1 =
            Arc::new(TestCommand::new_put(vec![1], 1).set_exe_dur(Duration::from_millis(50)));
        let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
        exe_tx.send_sp_exe(Arc::clone(&cmd1));
        exe_tx.send_sp_exe(Arc::clone(&cmd2));

        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        exe_tx.send_reset();

        let cmd3 = Arc::new(TestCommand::new_get(vec![1]));
        exe_tx.send_after_sync(cmd3, 1);

        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        // there will be only one after sync results
        assert!(as_rx.recv().await.is_some());
        assert!(as_rx.try_recv().is_err());
    }
}
