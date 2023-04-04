//! `exe` stands for execution
//! `as` stands for after sync

use std::{fmt::Debug, iter, sync::Arc};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
#[cfg(test)]
use mockall::automock;
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{debug, error};

use self::conflict_checked_mpmc::Task;
use super::raw_curp::RawCurp;
use crate::{
    cmd::{Command, CommandExecutor},
    server::cmd_worker::conflict_checked_mpmc::TaskType,
    snapshot::{Snapshot, SnapshotMeta},
    LogIndex,
};

/// The special conflict checked mpmc
pub(super) mod conflict_checked_mpmc;

/// Event for command executor
pub(super) enum CEEvent<C> {
    /// The cmd is ready for speculative execution
    SpecExeReady(Arc<C>),
    /// The cmd is ready for after sync
    ASReady(Arc<C>, LogIndex),
    /// Reset the command executor, send(()) when finishes
    Reset(Option<Snapshot>, oneshot::Sender<()>),
    /// Take a snapshot
    Snapshot(SnapshotMeta, oneshot::Sender<Snapshot>),
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
            Self::Reset(ref ss, _) => {
                if ss.is_none() {
                    write!(f, "Reset(None)")
                } else {
                    write!(f, "Reset(Some(_))")
                }
            }
            Self::Snapshot(meta, _) => f.debug_tuple("Snapshot").field(&meta).finish(),
        }
    }
}

/// Worker that execute commands
async fn cmd_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: TaskRx<C>,
    done_tx: flume::Sender<(Task<C>, bool)>,
    curp: Arc<RawCurp<C>>,
    ce: Arc<CE>,
) {
    let (cb, sp, ucp) = (curp.cmd_board(), curp.spec_pool(), curp.uncommitted_pool());
    let id = curp.id();
    while let Ok(mut task) = dispatch_rx.recv().await {
        #[allow(clippy::pattern_type_mismatch)] // can't get away with it
        let succeeded = match task.take() {
            TaskType::SpecExe(cmd) => {
                let er = ce.execute(cmd.as_ref()).await.map_err(|e| e.to_string());
                let er_ok = er.is_ok();
                debug!("{id} cmd({}) is speculatively executed", cmd.id());
                cb.write().insert_er(cmd.id(), er);
                er_ok
            }
            TaskType::AS(ref cmd, index) => {
                let need_run = cb
                    .read()
                    .er_buffer
                    .get(cmd.id())
                    .map_or(false, Result::is_ok);
                let asr = ce
                    .after_sync(cmd.as_ref(), index, need_run)
                    .await
                    .map_err(|e| e.to_string());
                let asr_ok = asr.is_ok();
                if need_run {
                    cb.write().insert_asr(cmd.id(), asr);
                }
                sp.lock().remove(cmd.id());
                let _ig = ucp.lock().remove(cmd.id());
                debug!("{id} cmd({}) after sync is called", cmd.id());
                asr_ok
            }
            TaskType::Reset(snapshot, finish_tx) => {
                if let Some(snapshot) = snapshot {
                    let meta = snapshot.meta;
                    #[allow(clippy::expect_used)] // only in debug
                    if let Err(e) = ce
                        .reset(Some((snapshot.into_inner(), meta.last_included_index)))
                        .await
                    {
                        error!("reset failed, {e}");
                    } else {
                        debug_assert_eq!(
                            ce.last_applied()
                                .expect("failed to get last_applied from ce"),
                            meta.last_included_index,
                            "inconsistent last_applied"
                        );
                        debug!("{id}'s command executor has been reset by a snapshot");
                        curp.reset_by_snapshot(meta);
                    }
                } else {
                    if let Err(e) = ce.reset(None).await {
                        error!("reset failed, {e}");
                    }
                    debug!("{id}'s command executor has been restored to the initial state");
                }
                let _ig = finish_tx.send(());
                true
            }
            #[allow(clippy::unwrap_used)]
            TaskType::Snapshot(meta, tx) => match ce.snapshot().await {
                Ok(snapshot) => {
                    debug_assert_eq!(ce.last_applied().unwrap(), meta.last_included_index); // sanity check
                    if tx.send(Snapshot::new(meta, snapshot)).is_err() {
                        error!("snapshot oneshot closed");
                    }
                    true
                }
                Err(e) => {
                    error!("snapshot failed, {e}");
                    false
                }
            },
        };
        if let Err(e) = done_tx.send((task, succeeded)) {
            error!("can't mark a task done, the channel could be closed, {e}");
        }
    }
    error!("cmd worker exits unexpectedly");
}

/// Send event to background command executor workers
#[derive(Debug, Clone)]
pub(super) struct CEEventTx<C: Command>(flume::Sender<CEEvent<C>>);

/// Recv cmds that need to be executed
#[derive(Clone)]
struct TaskRx<C: Command>(flume::Receiver<Task<C>>);

/// Send cmd to background execution worker
#[cfg_attr(test, automock)]
pub(super) trait CEEventTxApi<C: Command + 'static>: Send + Sync + 'static {
    /// Send cmd to background cmd worker for speculative execution
    fn send_sp_exe(&self, cmd: Arc<C>);

    /// Send after sync event to the background cmd worker so that after sync can be called
    fn send_after_sync(&self, cmd: Arc<C>, index: LogIndex);

    /// Send reset
    fn send_reset(&self, snapshot: Option<Snapshot>) -> oneshot::Receiver<()>;

    /// Send snapshot
    fn send_snapshot(&self, meta: SnapshotMeta) -> oneshot::Receiver<Snapshot>;
}

impl<C: Command + 'static> CEEventTxApi<C> for CEEventTx<C> {
    fn send_sp_exe(&self, cmd: Arc<C>) {
        let event = CEEvent::SpecExeReady(cmd);
        if let Err(e) = self.0.send(event) {
            error!("failed to send cmd exe event to background cmd worker, {e}");
        }
    }

    fn send_after_sync(&self, cmd: Arc<C>, index: LogIndex) {
        let event = CEEvent::ASReady(cmd, index);
        if let Err(e) = self.0.send(event) {
            error!("failed to send cmd as event to background cmd worker, {e}");
        }
    }

    fn send_reset(&self, snapshot: Option<Snapshot>) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let msg = CEEvent::Reset(snapshot, tx);
        if let Err(e) = self.0.send(msg) {
            error!("failed to send reset event to background cmd worker, {e}");
        }
        rx
    }

    fn send_snapshot(&self, meta: SnapshotMeta) -> oneshot::Receiver<Snapshot> {
        let (tx, rx) = oneshot::channel();
        let msg = CEEvent::Snapshot(meta, tx);
        if let Err(e) = self.0.send(msg) {
            error!("failed to send snapshot event to background cmd worker, {e}");
        }
        rx
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

/// Run cmd execute workers. Each cmd execute worker will continually fetch task to perform from `task_rx`.
pub(super) fn start_cmd_workers<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    cmd_executor: CE,
    curp: Arc<RawCurp<C>>,
    task_rx: flume::Receiver<Task<C>>,
    done_tx: flume::Sender<(Task<C>, bool)>,
    shutdown_trigger: Arc<event_listener::Event>,
) {
    let n_workers: usize = curp.cfg().cmd_workers.numeric_cast();
    #[allow(clippy::shadow_unrelated)] // false positive
    let bg_worker_handles: Vec<JoinHandle<_>> =
        iter::repeat((task_rx, done_tx, curp, Arc::new(cmd_executor)))
            .take(n_workers)
            .map(|(task_rx, done_tx, curp, ce)| {
                tokio::spawn(cmd_worker(TaskRx(task_rx), done_tx, curp, ce))
            })
            .collect();

    let _ig = tokio::spawn(async move {
        shutdown_trigger.listen().await;
        for handle in bg_worker_handles {
            handle.abort();
        }
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{sync::mpsc, time::Instant};
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        log_entry::LogEntry,
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
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let cmd = Arc::new(TestCommand::default());

        ce_event_tx.send_sp_exe(Arc::clone(&cmd));
        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        ce_event_tx.send_after_sync(Arc::clone(&cmd), 1);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
    }

    // When the execution takes more time than sync, `as` should be called after exe has finished
    #[traced_test]
    #[tokio::test]
    async fn fast_path_cond1() {
        let (er_tx, _er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let begin = Instant::now();
        let cmd = Arc::new(TestCommand::default().set_exe_dur(Duration::from_secs(1)));

        ce_event_tx.send_sp_exe(Arc::clone(&cmd));

        // at 500ms, sync has completed, call after sync, then needs_as will be updated
        sleep_millis(500).await;
        ce_event_tx.send_after_sync(Arc::clone(&cmd), 1);

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
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let cmd = Arc::new(
            TestCommand::default()
                .set_exe_dur(Duration::from_secs(1))
                .set_exe_should_fail(),
        );

        ce_event_tx.send_sp_exe(Arc::clone(&cmd));

        // at 500ms, sync has completed
        sleep_millis(500).await;
        ce_event_tx.send_after_sync(Arc::clone(&cmd), 1);

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
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let cmd = Arc::new(TestCommand::default());

        ce_event_tx.send_after_sync(Arc::clone(&cmd), 1);

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
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let cmd = Arc::new(TestCommand::default().set_exe_should_fail());

        ce_event_tx.send_after_sync(Arc::clone(&cmd), 1);

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
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let cmd1 = Arc::new(TestCommand::new_put(vec![1], 1));
        let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
        ce_event_tx.send_sp_exe(Arc::clone(&cmd1));
        ce_event_tx.send_sp_exe(Arc::clone(&cmd2));

        // cmd1 exe done
        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        sleep_millis(100).await;

        // cmd2 will not be executed
        assert!(er_rx.try_recv().is_err());
        assert!(as_rx.try_recv().is_err());

        // cmd1 and cmd2 after sync
        ce_event_tx.send_after_sync(Arc::clone(&cmd1), 1);
        ce_event_tx.send_after_sync(Arc::clone(&cmd2), 2);

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
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let cmd1 =
            Arc::new(TestCommand::new_put(vec![1], 1).set_exe_dur(Duration::from_millis(50)));
        let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
        ce_event_tx.send_sp_exe(Arc::clone(&cmd1));
        ce_event_tx.send_sp_exe(Arc::clone(&cmd2));

        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        ce_event_tx.send_reset(None);

        let cmd3 = Arc::new(TestCommand::new_get(vec![1]));
        ce_event_tx.send_after_sync(cmd3, 1);

        assert_eq!(er_rx.recv().await.unwrap().1, vec![]);

        // there will be only one after sync results
        assert!(as_rx.recv().await.is_some());
        assert!(as_rx.try_recv().is_err());
    }

    #[traced_test]
    #[tokio::test]
    async fn test_snapshot() {
        // ce1
        let (er_tx, mut _er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut _as_rx) = mpsc::unbounded_channel();
        let ce1 = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        let curp = RawCurp::new_test(3, ce_event_tx.clone());
        curp.handle_append_entries(
            1,
            "S3".to_owned(),
            0,
            0,
            vec![LogEntry::new(1, 1, Arc::new(TestCommand::default()))],
            0,
        )
        .unwrap();
        start_cmd_workers(
            ce1,
            Arc::new(curp),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        let cmd1 =
            Arc::new(TestCommand::new_put(vec![1], 1).set_exe_dur(Duration::from_millis(50)));

        ce_event_tx.send_after_sync(Arc::clone(&cmd1), 1);

        let snapshot = ce_event_tx
            .send_snapshot(SnapshotMeta {
                last_included_index: 1,
                last_included_term: 0,
            })
            .await
            .unwrap();

        // ce2
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut _as_rx) = mpsc::unbounded_channel();
        let ce2 = TestCE::new("S1".to_owned(), er_tx, as_tx);
        let (ce_event_tx, task_rx, done_tx) = conflict_checked_mpmc::channel();
        start_cmd_workers(
            ce2,
            Arc::new(RawCurp::new_test(3, ce_event_tx.clone())),
            task_rx,
            done_tx,
            Arc::new(event_listener::Event::new()),
        );

        ce_event_tx.send_reset(Some(snapshot)).await.unwrap();

        let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
        ce_event_tx.send_after_sync(Arc::clone(&cmd2), 2);
        assert_eq!(er_rx.recv().await.unwrap().1, vec![1]);
    }
}
