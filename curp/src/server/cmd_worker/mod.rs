//! `exe` stands for execution
//! `as` stands for after sync

use std::{fmt::Debug, iter, sync::Arc};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
#[cfg(test)]
use mockall::automock;
use tokio::sync::oneshot;
use tracing::{debug, error, info};
use utils::shutdown;

use self::conflict_checked_mpmc::Task;
use super::raw_curp::RawCurp;
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    role_change::RoleChange,
    rpc::ConfChangeType,
    server::cmd_worker::conflict_checked_mpmc::TaskType,
    snapshot::{Snapshot, SnapshotMeta},
};

/// The special conflict checked mpmc
pub(super) mod conflict_checked_mpmc;

/// Event for command executor
pub(super) enum CEEvent<C> {
    /// The cmd is ready for speculative execution
    SpecExeReady(Arc<LogEntry<C>>),
    /// The cmd is ready for after sync
    ASReady(Arc<LogEntry<C>>),
    /// Reset the command executor, send(()) when finishes
    Reset(Option<Snapshot>, oneshot::Sender<()>),
    /// Take a snapshot
    Snapshot(SnapshotMeta, oneshot::Sender<Snapshot>),
}

impl<C: Command> Debug for CEEvent<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::SpecExeReady(ref entry) => f.debug_tuple("SpecExeReady").field(entry).finish(),
            Self::ASReady(ref entry) => f.debug_tuple("ASReady").field(entry).finish(),
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
async fn cmd_worker<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    dispatch_rx: impl TaskRxApi<C>,
    done_tx: flume::Sender<(Task<C>, bool)>,
    curp: Arc<RawCurp<C, RC>>,
    ce: Arc<CE>,
    // This task will safely exit when the dispatch_rx is dropped, but we still
    // need to keep the shutdown_listener here to notify the shutdown trigger
    _shutdown_listener: shutdown::Listener,
) {
    while let Ok(mut task) = dispatch_rx.recv().await {
        let succeeded = match task.take() {
            TaskType::SpecExe(entry, pre_err) => {
                worker_exe(entry, pre_err, ce.as_ref(), curp.as_ref()).await
            }
            TaskType::AS(entry, prepare) => {
                worker_as(entry, prepare, ce.as_ref(), curp.as_ref()).await
            }
            TaskType::Reset(snapshot, finish_tx) => {
                worker_reset(snapshot, finish_tx, ce.as_ref(), curp.as_ref()).await
            }
            TaskType::Snapshot(meta, tx) => {
                worker_snapshot(meta, tx, ce.as_ref(), curp.as_ref()).await
            }
        };
        if let Err(e) = done_tx.send((task, succeeded)) {
            if !curp.is_shutdown() {
                error!("can't mark a task done, the channel could be closed, {e}");
            }
        }
    }
    debug!("cmd worker exits");
}

/// Cmd worker execute handler
async fn worker_exe<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    entry: Arc<LogEntry<C>>,
    pre_err: Option<C::Error>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    let (cb, sp, ucp) = (curp.cmd_board(), curp.spec_pool(), curp.uncommitted_pool());
    let id = curp.id();
    let success = match entry.entry_data {
        EntryData::Command(ref cmd) => {
            let er = if let Some(err_msg) = pre_err {
                Err(err_msg)
            } else {
                ce.execute(cmd).await
            };
            let er_ok = er.is_ok();
            cb.write().insert_er(entry.propose_id, er);
            if !er_ok {
                sp.lock().remove(&entry.propose_id);
                let _ig = ucp.lock().remove(&entry.propose_id);
            }
            debug!(
                "{id} cmd({}) is speculatively executed, exe status: {er_ok}",
                entry.propose_id
            );
            er_ok
        }
        EntryData::ConfChange(_)
        | EntryData::Shutdown
        | EntryData::Empty
        | EntryData::SetName(_, _) => true,
    };
    if !success {
        ce.trigger(entry.inflight_id(), entry.index);
    }
    success
}

/// Cmd worker after sync handler
async fn worker_as<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    entry: Arc<LogEntry<C>>,
    prepare: Option<C::PR>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    let (cb, sp, ucp) = (curp.cmd_board(), curp.spec_pool(), curp.uncommitted_pool());
    let id = curp.id();
    let success = match entry.entry_data {
        EntryData::Command(ref cmd) => {
            let Some(prepare) = prepare else {
            unreachable!("prepare should always be Some(_) when entry is a command");
        };
            let asr = ce.after_sync(cmd.as_ref(), entry.index, prepare).await;
            let asr_ok = asr.is_ok();
            cb.write().insert_asr(entry.propose_id, asr);
            sp.lock().remove(&entry.propose_id);
            let _ig = ucp.lock().remove(&entry.propose_id);
            debug!("{id} cmd({}) after sync is called", entry.propose_id);
            asr_ok
        }
        EntryData::Shutdown => {
            curp.enter_shutdown();
            if curp.is_leader() {
                curp.shutdown_trigger().mark_leader_notified();
            }
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
            }
            cb.write().notify_shutdown();
            true
        }
        EntryData::ConfChange(ref conf_change) => {
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
                return false;
            }
            let change = conf_change.first().unwrap_or_else(|| {
                unreachable!("conf change should always have at least one change")
            });
            let shutdown_self =
                change.change_type() == ConfChangeType::Remove && change.node_id == id;
            cb.write().insert_conf(entry.propose_id);
            sp.lock().remove(&entry.propose_id);
            let _ig = ucp.lock().remove(&entry.propose_id);
            if shutdown_self {
                curp.shutdown_trigger().self_shutdown();
            }
            true
        }
        EntryData::SetName(node_id, ref name) => {
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
                return false;
            }
            curp.cluster().set_name(node_id, name.clone());
            true
        }
        EntryData::Empty => true,
    };
    ce.trigger(entry.inflight_id(), entry.index);
    success
}

/// Cmd worker reset handler
async fn worker_reset<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    snapshot: Option<Snapshot>,
    finish_tx: oneshot::Sender<()>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    let id = curp.id();
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

/// Cmd worker snapshot handler
async fn worker_snapshot<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    meta: SnapshotMeta,
    tx: oneshot::Sender<Snapshot>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    match ce.snapshot().await {
        Ok(snapshot) => {
            debug_assert!(
                ce.last_applied()
                    .is_ok_and(|last_applied| last_applied <= meta.last_included_index),
                " the `last_as` should always be less than or equal to the `last_exe`"
            ); // sanity check
            let snapshot = Snapshot::new(meta, snapshot);
            debug!("{} takes a snapshot, {snapshot:?}", curp.id());
            if tx.send(snapshot).is_err() {
                error!("snapshot oneshot closed");
            }
            true
        }
        Err(e) => {
            error!("snapshot failed, {e}");
            false
        }
    }
}

/// Send event to background command executor workers
#[derive(Debug, Clone)]
pub(super) struct CEEventTx<C: Command>(flume::Sender<CEEvent<C>>, shutdown::Listener);

/// Recv cmds that need to be executed
#[derive(Clone)]
struct TaskRx<C: Command>(flume::Receiver<Task<C>>);

/// Send cmd to background execution worker
#[cfg_attr(test, automock)]
pub(super) trait CEEventTxApi<C: Command>: Send + Sync + 'static {
    /// Send cmd to background cmd worker for speculative execution
    fn send_sp_exe(&self, entry: Arc<LogEntry<C>>);

    /// Send after sync event to the background cmd worker so that after sync can be called
    fn send_after_sync(&self, entry: Arc<LogEntry<C>>);

    /// Send reset
    fn send_reset(&self, snapshot: Option<Snapshot>) -> oneshot::Receiver<()>;

    /// Send snapshot
    fn send_snapshot(&self, meta: SnapshotMeta) -> oneshot::Receiver<Snapshot>;
}

impl<C: Command> CEEventTx<C> {
    /// Send ce event
    fn send_event(&self, event: CEEvent<C>) {
        if let Err(e) = self.0.send(event) {
            if self.1.is_shutdown() {
                info!("send event after current node shutdown");
                return;
            }
            error!("failed to send cmd exe event to background cmd worker, {e}");
        }
    }
}

impl<C: Command> CEEventTxApi<C> for CEEventTx<C> {
    fn send_sp_exe(&self, entry: Arc<LogEntry<C>>) {
        let event = CEEvent::SpecExeReady(Arc::clone(&entry));
        self.send_event(event);
    }

    fn send_after_sync(&self, entry: Arc<LogEntry<C>>) {
        let event = CEEvent::ASReady(Arc::clone(&entry));
        self.send_event(event);
    }

    fn send_reset(&self, snapshot: Option<Snapshot>) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let event = CEEvent::Reset(snapshot, tx);
        self.send_event(event);
        rx
    }

    fn send_snapshot(&self, meta: SnapshotMeta) -> oneshot::Receiver<Snapshot> {
        let (tx, rx) = oneshot::channel();
        let event = CEEvent::Snapshot(meta, tx);
        self.send_event(event);
        rx
    }
}

/// Cmd exe recv interface
#[cfg_attr(test, automock)]
#[async_trait]
trait TaskRxApi<C: Command> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<Task<C>, flume::RecvError>;
}

#[async_trait]
impl<C: Command> TaskRxApi<C> for TaskRx<C> {
    /// Recv execute msg and done notifier
    async fn recv(&self) -> Result<Task<C>, flume::RecvError> {
        self.0.recv_async().await
    }
}

/// Run cmd execute workers. Each cmd execute worker will continually fetch task to perform from `task_rx`.
pub(super) fn start_cmd_workers<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    cmd_executor: Arc<CE>,
    curp: Arc<RawCurp<C, RC>>,
    task_rx: flume::Receiver<Task<C>>,
    done_tx: flume::Sender<(Task<C>, bool)>,
    shutdown_listener: shutdown::Listener,
) {
    let n_workers: usize = curp.cfg().cmd_workers.numeric_cast();
    #[allow(clippy::shadow_unrelated)] // false positive
    iter::repeat((task_rx, done_tx, curp, cmd_executor, shutdown_listener))
        .take(n_workers)
        .for_each(|(task_rx, done_tx, curp, ce, shutdown_listener)| {
            let _cmd_worker = tokio::spawn(cmd_worker(
                TaskRx(task_rx),
                done_tx,
                curp,
                ce,
                shutdown_listener,
            ));
        });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use curp_test_utils::{
        mock_role_change, sleep_millis, sleep_secs,
        test_cmd::{TestCE, TestCommand},
    };
    use test_macros::abort_on_panic;
    use tokio::{sync::mpsc, time::Instant};
    use tracing_test::traced_test;
    use utils::config::{default_quota, EngineConfig, StorageConfig};

    use super::*;
    use crate::{log_entry::LogEntry, rpc::ProposeId};

    // This should happen in fast path in most cases
    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn fast_path_normal() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (t, l) = shutdown::channel();
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce), t.clone());
        start_cmd_workers(
            Arc::clone(&ce),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        let entry = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 0),
            Arc::new(TestCommand::default()),
        ));

        ce_event_tx.send_sp_exe(Arc::clone(&entry));
        assert_eq!(er_rx.recv().await.unwrap().1.values, Vec::<u32>::new());

        ce_event_tx.send_after_sync(entry);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
        t.self_shutdown();
    }

    // When the execution takes more time than sync, `as` should be called after exe has finished
    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn fast_path_cond1() {
        let (er_tx, _er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (t, l) = shutdown::channel();
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce), t.clone());
        start_cmd_workers(
            Arc::clone(&ce),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        let begin = Instant::now();
        let entry = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 0),
            Arc::new(TestCommand::default().set_exe_dur(Duration::from_secs(1))),
        ));

        ce_event_tx.send_sp_exe(Arc::clone(&entry));

        // at 500ms, sync has completed, call after sync, then needs_as will be updated
        sleep_millis(500).await;
        ce_event_tx.send_after_sync(entry);

        assert_eq!(as_rx.recv().await.unwrap().1, 1);

        assert!((Instant::now() - begin) >= Duration::from_secs(1));
        t.self_shutdown();
    }

    // When the execution takes more time than sync and fails, after sync should not be called
    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn fast_path_cond2() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (t, l) = shutdown::channel();
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce), t.clone());
        start_cmd_workers(
            Arc::clone(&ce),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        let entry = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 0),
            Arc::new(
                TestCommand::default()
                    .set_exe_dur(Duration::from_secs(1))
                    .set_exe_should_fail(),
            ),
        ));

        ce_event_tx.send_sp_exe(Arc::clone(&entry));

        // at 500ms, sync has completed
        sleep_millis(500).await;
        ce_event_tx.send_after_sync(entry);

        // at 1500ms, as should not be called
        sleep_secs(1).await;
        assert!(er_rx.try_recv().is_err());
        assert!(as_rx.try_recv().is_err());
        t.self_shutdown();
    }

    // This should happen in slow path in most cases
    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn slow_path_normal() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (t, l) = shutdown::channel();
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce), t.clone());
        start_cmd_workers(
            Arc::clone(&ce),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        let entry = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 0),
            Arc::new(TestCommand::default()),
        ));

        ce_event_tx.send_after_sync(entry);

        assert_eq!(er_rx.recv().await.unwrap().1.revisions, Vec::<i64>::new());
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
        t.self_shutdown();
    }

    // When exe fails
    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn slow_path_exe_fails() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (t, l) = shutdown::channel();
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce), t.clone());
        start_cmd_workers(
            Arc::clone(&ce),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        let entry = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 0),
            Arc::new(TestCommand::default().set_exe_should_fail()),
        ));

        ce_event_tx.send_after_sync(entry);

        sleep_millis(100).await;
        let er = er_rx.try_recv();
        assert!(er.is_err(), "The execute command result is {er:?}");
        let asr = as_rx.try_recv();
        assert!(asr.is_err(), "The after sync result is {asr:?}");
        t.self_shutdown();
    }

    // If cmd1 and cmd2 conflict, order will be (cmd1 exe) -> (cmd1 as) -> (cmd2 exe) -> (cmd2 as)
    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn conflict_cmd_order() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (t, l) = shutdown::channel();
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce), t.clone());
        start_cmd_workers(
            Arc::clone(&ce),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        let entry1 = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 0),
            Arc::new(TestCommand::new_put(vec![1], 1)),
        ));
        let entry2 = Arc::new(LogEntry::new(
            2,
            1,
            ProposeId(0, 1),
            Arc::new(TestCommand::new_get(vec![1])),
        ));

        ce_event_tx.send_sp_exe(Arc::clone(&entry1));
        ce_event_tx.send_sp_exe(Arc::clone(&entry2));

        // cmd1 exe done
        assert_eq!(er_rx.recv().await.unwrap().1.revisions, Vec::<i64>::new());

        sleep_millis(100).await;

        // cmd2 will not be executed
        assert!(er_rx.try_recv().is_err());
        assert!(as_rx.try_recv().is_err());

        // cmd1 and cmd2 after sync
        ce_event_tx.send_after_sync(entry1);
        ce_event_tx.send_after_sync(entry2);

        assert_eq!(er_rx.recv().await.unwrap().1.revisions, vec![1]);
        assert_eq!(as_rx.recv().await.unwrap().1, 1);
        assert_eq!(as_rx.recv().await.unwrap().1, 2);
        t.self_shutdown();
    }

    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn reset_will_wipe_all_states_and_outdated_cmds() {
        let (er_tx, mut er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut as_rx) = mpsc::unbounded_channel();
        let ce = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (t, l) = shutdown::channel();
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce), t.clone());
        start_cmd_workers(
            Arc::clone(&ce),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        let entry1 = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 0),
            Arc::new(TestCommand::new_put(vec![1], 1).set_as_dur(Duration::from_millis(50))),
        ));
        let entry2 = Arc::new(LogEntry::new(
            2,
            1,
            ProposeId(0, 1),
            Arc::new(TestCommand::new_get(vec![1])),
        ));
        ce_event_tx.send_sp_exe(Arc::clone(&entry1));
        ce_event_tx.send_sp_exe(Arc::clone(&entry2));

        assert_eq!(er_rx.recv().await.unwrap().1.revisions, Vec::<i64>::new());

        ce_event_tx.send_reset(None);

        let entry3 = Arc::new(LogEntry::new(
            3,
            1,
            ProposeId(0, 2),
            Arc::new(TestCommand::new_get(vec![1])),
        ));

        ce_event_tx.send_after_sync(entry3);

        assert_eq!(er_rx.recv().await.unwrap().1.revisions, Vec::<i64>::new());

        // there will be only one after sync results
        assert!(as_rx.recv().await.is_some());
        assert!(as_rx.try_recv().is_err());
        t.self_shutdown();
    }

    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn test_snapshot() {
        let (t, l) = shutdown::channel();

        // ce1
        let (er_tx, mut _er_rx) = mpsc::unbounded_channel();
        let (as_tx, mut _as_rx) = mpsc::unbounded_channel();
        let ce1 = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce1), t.clone());
        let curp = RawCurp::new_test(3, ce_event_tx.clone(), mock_role_change());
        let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
        curp.handle_append_entries(
            1,
            s2_id,
            0,
            0,
            vec![LogEntry::new(
                1,
                1,
                ProposeId(0, 0),
                Arc::new(TestCommand::default()),
            )],
            0,
        )
        .unwrap();
        start_cmd_workers(
            Arc::clone(&ce1),
            Arc::new(curp),
            task_rx,
            done_tx,
            l.clone(),
        );

        let entry = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 1),
            Arc::new(TestCommand::new_put(vec![1], 1).set_exe_dur(Duration::from_millis(50))),
        ));

        ce_event_tx.send_after_sync(entry);

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
        let ce2 = Arc::new(TestCE::new(
            "S1".to_owned(),
            er_tx,
            as_tx,
            StorageConfig::new(EngineConfig::Memory, default_quota()),
        ));
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&ce2), t.clone());
        start_cmd_workers(
            Arc::clone(&ce2),
            Arc::new(RawCurp::new_test(
                3,
                ce_event_tx.clone(),
                mock_role_change(),
            )),
            task_rx,
            done_tx,
            l,
        );

        ce_event_tx.send_reset(Some(snapshot)).await.unwrap();

        let entry = Arc::new(LogEntry::new(
            1,
            1,
            ProposeId(0, 2),
            Arc::new(TestCommand::new_get(vec![1])),
        ));
        ce_event_tx.send_after_sync(entry);
        assert_eq!(er_rx.recv().await.unwrap().1.revisions, vec![1]);
        t.self_shutdown();
    }
}
