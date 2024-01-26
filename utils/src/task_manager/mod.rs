use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
};

use clippy_utilities::OverflowArithmetic;
use dashmap::DashMap;
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{debug, info};

use self::tasks::{TaskName, ALL_EDGES};

/// Task names and edges
pub mod tasks;

/// Task manager
#[derive(Debug)]
pub struct TaskManager {
    /// All tasks
    tasks: Arc<DashMap<TaskName, Task>>,
    /// State of task manager
    state: Arc<AtomicU8>,
    /// Cluster shutdown tracker
    cluster_shutdown_tracker: Arc<ClusterShutdownTracker>,
}

/// Cluster shutdown tracker
#[derive(Debug, Default)]
pub struct ClusterShutdownTracker {
    /// Cluster shutdown notify
    notify: Notify,
    /// State of mpsc channel.
    mpmc_channel_shutdown: AtomicBool,
    /// Count of sync follower tasks.
    sync_follower_task_count: AtomicU8,
    /// Shutdown Applied
    leader_notified: AtomicBool,
}

impl ClusterShutdownTracker {
    /// Create a new `ClusterShutdownTracker`
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            notify: Notify::new(),
            mpmc_channel_shutdown: AtomicBool::new(false),
            sync_follower_task_count: AtomicU8::new(0),
            leader_notified: AtomicBool::new(false),
        }
    }

    /// Mark mpmc channel shutdown
    #[inline]
    pub fn mark_mpmc_channel_shutdown(&self) {
        self.mpmc_channel_shutdown.store(true, Ordering::Relaxed);
        self.notify.notify_one();
        debug!("mark mpmc channel shutdown");
    }

    /// Sync follower task count inc
    #[inline]
    pub fn sync_follower_task_count_inc(&self) {
        let n = self
            .sync_follower_task_count
            .fetch_add(1, Ordering::Relaxed);
        debug!("sync follower task count inc to: {}", n.overflow_add(1));
    }

    /// Sync follower task count dec
    #[inline]
    pub fn sync_follower_task_count_dec(&self) {
        let c = self
            .sync_follower_task_count
            .fetch_sub(1, Ordering::Relaxed);
        if c == 1 {
            self.notify.notify_one();
        }
        debug!("sync follower task count dec to: {}", c.overflow_sub(1));
    }

    /// Mark leader notified
    #[inline]
    pub fn mark_leader_notified(&self) {
        self.leader_notified.store(true, Ordering::Relaxed);
        self.notify.notify_one();
        debug!("mark leader notified");
    }

    /// Check if the cluster shutdown condition is met
    fn check(&self) -> bool {
        let mpmc_channel_shutdown = self.mpmc_channel_shutdown.load(Ordering::Relaxed);
        let sync_follower_task_count = self.sync_follower_task_count.load(Ordering::Relaxed);
        let leader_notified = self.leader_notified.load(Ordering::Relaxed);
        mpmc_channel_shutdown && sync_follower_task_count == 0 && leader_notified
    }
}

impl TaskManager {
    /// Create a new `TaskManager`
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let tasks = Arc::new(DashMap::new());
        for name in TaskName::iter() {
            let task = Task::new(name);
            _ = tasks.insert(name, task);
        }
        for (from, to) in ALL_EDGES {
            _ = tasks.get_mut(&from).map(|mut t| t.depend_by.push(to));
            _ = tasks
                .get_mut(&to)
                .map(|mut t| t.depend_cnt = t.depend_cnt.overflow_add(1));
        }
        let state = Arc::new(AtomicU8::new(0));
        let cluster_shutdown_tracker = Arc::new(ClusterShutdownTracker::new());
        Self {
            tasks,
            state,
            cluster_shutdown_tracker,
        }
    }

    /// Check if task manager is shutdown
    #[must_use]
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.state.load(Ordering::Acquire) != 0
    }

    /// Get shutdown listener
    #[must_use]
    #[inline]
    pub fn get_shutdown_listener(&self, name: TaskName) -> Listener {
        let task = self
            .tasks
            .get(&name)
            .unwrap_or_else(|| unreachable!("task {:?} should exist", name));
        Listener::new(
            Arc::clone(&self.state),
            Arc::clone(&task.notifier),
            Arc::clone(&self.cluster_shutdown_tracker),
        )
    }

    /// Spawn a task
    #[inline]
    pub fn spawn<FN, F>(&self, name: TaskName, f: FN)
    where
        F: Future<Output = ()> + Send + 'static,
        FN: FnOnce(Listener) -> F,
    {
        if self.is_shutdown() {
            return;
        }
        info!("spawn {name:?}");
        let mut task = self
            .tasks
            .get_mut(&name)
            .unwrap_or_else(|| unreachable!("task {:?} should exist", name));
        let listener = Listener::new(
            Arc::clone(&self.state),
            Arc::clone(&task.notifier),
            Arc::clone(&self.cluster_shutdown_tracker),
        );
        let handle = tokio::spawn(f(listener));
        task.handle.push(handle);
    }

    /// Get root tasks queue
    fn root_tasks_queue(tasks: &DashMap<TaskName, Task>) -> VecDeque<TaskName> {
        tasks
            .iter()
            .filter_map(|task| (task.depend_cnt == 0).then_some(task.name))
            .collect()
    }

    /// Inner shutdown task
    async fn inner_shutdown(tasks: Arc<DashMap<TaskName, Task>>, state: Arc<AtomicU8>) {
        let mut queue = Self::root_tasks_queue(&tasks);
        state.store(1, Ordering::Release);
        while let Some(v) = queue.pop_front() {
            let Some((_name,mut task)) = tasks.remove(&v) else {
                continue;
            };
            task.notifier.notify_waiters();
            for handle in task.handle.drain(..) {
                handle
                    .await
                    .unwrap_or_else(|e| unreachable!("background task should not panic: {e}"));
            }
            for child in task.depend_by.drain(..) {
                let Some(mut child_task) = tasks.get_mut(&child) else {
                    continue;
                };
                child_task.depend_cnt = child_task.depend_cnt.overflow_sub(1);
                if child_task.depend_cnt == 0 {
                    queue.push_back(child);
                }
            }
        }
        info!("all tasks have been shutdown");
    }

    /// Shutdown current node
    #[inline]
    pub async fn shutdown(&self, wait: bool) {
        let tasks = Arc::clone(&self.tasks);
        let state = Arc::clone(&self.state);
        let h = tokio::spawn(Self::inner_shutdown(tasks, state));
        if wait {
            h.await
                .unwrap_or_else(|e| unreachable!("shutdown task should not panic: {e}"));
        }
    }

    /// Shutdown cluster
    #[inline]
    pub fn cluster_shutdown(&self) {
        let tasks = Arc::clone(&self.tasks);
        let state = Arc::clone(&self.state);
        let tracker = Arc::clone(&self.cluster_shutdown_tracker);
        let _ig = tokio::spawn(async move {
            info!("cluster shutdown start");
            state.store(2, Ordering::Release);
            for name in [TaskName::SyncFollower, TaskName::ConflictCheckedMpmc] {
                _ = tasks.get(&name).map(|n| n.notifier.notify_waiters());
            }
            loop {
                if tracker.check() {
                    break;
                }
                tracker.notify.notified().await;
            }
            info!("cluster shutdown check passed, start shutdown");
            Self::inner_shutdown(tasks, state).await;
        });
    }

    /// Mark mpmc channel shutdown
    #[inline]
    pub fn mark_leader_notified(&self) {
        self.cluster_shutdown_tracker.mark_leader_notified();
    }

    /// Check if all tasks are finished
    #[inline]
    #[must_use]
    pub fn is_finished(&self) -> bool {
        for t in self.tasks.iter() {
            for h in &t.handle {
                if !h.is_finished() {
                    return false;
                }
            }
        }
        true
    }
}

impl Default for TaskManager {
    #[must_use]
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Task
#[derive(Debug)]
struct Task {
    /// Task name
    name: TaskName,
    /// Task shutdown notifier
    notifier: Arc<Notify>,
    /// Task handles
    handle: Vec<JoinHandle<()>>,
    /// All tasks that depend on this task
    depend_by: Vec<TaskName>,
    /// Count of tasks that this task depends on
    depend_cnt: usize,
}

impl Drop for Task {
    fn drop(&mut self) {
        info!("drop task: {:?}", self.name);
    }
}

impl Task {
    /// Create a new `Task`
    fn new(name: TaskName) -> Self {
        let notifier = Arc::new(Notify::new());
        Self {
            name,
            notifier,
            handle: vec![],
            depend_by: vec![],
            depend_cnt: 0,
        }
    }
}

/// State of task manager
#[derive(Debug, Clone, Copy)]
#[allow(clippy::exhaustive_enums)]
pub enum State {
    /// Running
    Running,
    /// Shutdown current node
    Shutdown,
    /// Shutdown cluster
    ClusterShutdown,
}

/// Listener of task manager
#[derive(Debug, Clone)]
pub struct Listener {
    /// Shutdown notify
    notify: Arc<Notify>,
    /// State of task manager
    state: Arc<AtomicU8>,
    /// Cluster shutdown tracker
    cluster_shutdown_tracker: Arc<ClusterShutdownTracker>,
}

impl Listener {
    /// Create a new `Listener`
    fn new(
        state: Arc<AtomicU8>,
        notify: Arc<Notify>,
        cluster_shutdown_tracker: Arc<ClusterShutdownTracker>,
    ) -> Self {
        Self {
            notify,
            state,
            cluster_shutdown_tracker,
        }
    }

    /// Get current state
    fn state(&self) -> State {
        let state = self.state.load(Ordering::Acquire);
        match state {
            0 => State::Running,
            1 => State::Shutdown,
            2 => State::ClusterShutdown,
            _ => unreachable!("invalid state: {}", state),
        }
    }

    /// Wait for self shutdown
    #[inline]
    pub async fn wait(&self) {
        let state = self.state();
        if matches!(state, State::Shutdown) {
            return;
        }
        self.notify.notified().await;
    }

    /// Wait for shutdown state
    #[inline]
    pub async fn wait_state(&self) -> State {
        let state = self.state();
        if matches!(state, State::Shutdown | State::ClusterShutdown) {
            return state;
        }
        self.notify.notified().await;
        self.state()
    }

    /// Get a sync follower guard
    #[must_use]
    #[inline]
    pub fn sync_follower_guard(&self) -> SyncFollowerGuard {
        self.cluster_shutdown_tracker.sync_follower_task_count_inc();
        SyncFollowerGuard {
            tracker: Arc::clone(&self.cluster_shutdown_tracker),
        }
    }

    /// Mark mpmc channel shutdown
    #[inline]
    pub fn mark_mpmc_channel_shutdown(&self) {
        self.cluster_shutdown_tracker.mark_mpmc_channel_shutdown();
    }
}

/// Sync follower guard, used to track sync follower task count
#[derive(Debug)]
pub struct SyncFollowerGuard {
    /// Cluster shutdown tracker
    tracker: Arc<ClusterShutdownTracker>,
}

impl Drop for SyncFollowerGuard {
    #[inline]
    fn drop(&mut self) {
        self.tracker.sync_follower_task_count_dec();
    }
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_inner_shutdown() {
        let tm = TaskManager::new();
        let (record_tx, mut record_rx) = mpsc::unbounded_channel();
        for name in TaskName::iter() {
            let record_tx = record_tx.clone();
            tm.spawn(name, move |listener| async move {
                listener.wait().await;
                record_tx.send(name).unwrap();
            });
        }
        drop(record_tx);
        tokio::time::sleep(Duration::from_secs(1)).await;
        TaskManager::inner_shutdown(Arc::clone(&tm.tasks), Arc::clone(&tm.state)).await;
        let mut shutdown_order = vec![];
        while let Some(name) = record_rx.recv().await {
            shutdown_order.push(name);
        }
        for (from, to) in ALL_EDGES {
            let from_index = shutdown_order
                .iter()
                .position(|n| *n == from)
                .unwrap_or_else(|| unreachable!("task {:?} should exist", from));
            let to_index = shutdown_order
                .iter()
                .position(|n| *n == to)
                .unwrap_or_else(|| unreachable!("task {:?} should exist", to));
            assert!(
                from_index < to_index,
                "{from:?} should shutdown before {to:?}"
            );
        }
    }
}
