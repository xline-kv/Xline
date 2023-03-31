#![allow(
    clippy::wildcard_enum_match_arm,
    clippy::match_wildcard_for_single_variants
)] // wildcard actually is more clear in this module
#![allow(clippy::integer_arithmetic)] // u64 is large enough

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::oneshot;
use tracing::{debug, error};

use super::{CEEvent, CEEventTx};
use crate::{
    cmd::{Command, ProposeId},
    snapshot::{Snapshot, SnapshotMeta},
    LogIndex,
};

/// CE task
pub(in crate::server) struct Task<C> {
    /// Corresponding vertex id
    vid: u64,
    /// Task type
    inner: TaskType<C>,
}

/// Task Type
pub(in crate::server) enum TaskType<C> {
    /// Execute a cmd
    SpecExe(Arc<C>),
    /// After sync a cmd
    AS(Arc<C>, LogIndex),
    /// Reset the CE
    Reset(Option<Snapshot>, Option<oneshot::Sender<()>>),
    /// Snapshot
    Snapshot(Option<(oneshot::Sender<Snapshot>, SnapshotMeta)>),
}

impl<C> Task<C> {
    /// Get inner task
    pub(super) fn inner(&mut self) -> &mut TaskType<C> {
        &mut self.inner
    }
}

/// Vertex
#[derive(Debug)]
struct Vertex<C> {
    /// Successor cmds that arrive later with keys that conflict this cmd
    successors: HashSet<u64>,
    /// Number of predecessor cmds that arrive earlier with keys that conflict this cmd
    predecessor_cnt: u64,
    /// Vertex inner
    inner: VertexInner<C>,
}

impl<C: Command> Vertex<C> {
    /// Whether two vertex conflict each other
    fn is_conflict(&self, other: &Vertex<C>) -> bool {
        #[allow(clippy::pattern_type_mismatch)]
        // it seems it's impossible to get away with this lint
        match (&self.inner, &other.inner) {
            (VertexInner::Cmd { cmd: cmd1, .. }, VertexInner::Cmd { cmd: cmd2, .. }) => {
                cmd1.is_conflict(cmd2.as_ref())
            }
            _ => true,
        }
    }
}

/// Vertex inner
#[derive(Debug)]
enum VertexInner<C> {
    /// A cmd vertex
    Cmd {
        /// Cmd
        cmd: Arc<C>,
        /// Execution state
        exe_st: ExeState,
        /// After sync state
        as_st: AsState,
    },
    /// A reset vertex
    Reset {
        /// The snapshot
        snapshot: Option<Snapshot>,
        /// Finish tx
        finish_tx: Option<oneshot::Sender<()>>,
        /// Reset state
        st: OnceState,
    },
    /// A snapshot vertex
    Snapshot {
        /// The sender
        tx: Option<(oneshot::Sender<Snapshot>, SnapshotMeta)>,
        /// Snapshot state
        st: OnceState,
    },
}

/// Execute state of a cmd
#[derive(Debug, Clone, Copy)]
enum ExeState {
    /// Is ready to execute
    ExecuteReady,
    /// Executing
    Executing,
    /// Has been executed, and the result
    Executed(bool),
}

/// After sync state of a cmd
#[derive(Debug, Clone, Copy)]
enum AsState {
    /// Not Synced yet
    NotSynced,
    /// Is ready to do after sync
    AfterSyncReady(LogIndex),
    /// Is doing after syncing
    AfterSyncing,
    /// Has been after synced
    AfterSynced,
}

/// State of a vertex that only has one task
#[derive(Debug)]
enum OnceState {
    /// Reset ready
    Ready,
    /// Resetting
    Doing,
    /// Completed
    Completed,
}

/// The filter will block any msg if its predecessors(msgs that arrive earlier and conflict with it) haven't finished process
/// Internally it maintains a dependency graph of conflicting cmds
struct Filter<C> {
    /// Index from `ProposeId` to `vertex`
    cmd_vid: HashMap<ProposeId, u64>,
    /// Conflict graph
    vs: HashMap<u64, Vertex<C>>,
    /// Next vertex id
    next_id: u64,
    /// Send task to users
    filter_tx: flume::Sender<Task<C>>,
}

impl<C: Command> Filter<C> {
    /// Create a new filter that checks conflict in between msgs
    fn new(filter_tx: flume::Sender<Task<C>>) -> Self {
        Self {
            cmd_vid: HashMap::new(),
            vs: HashMap::new(),
            next_id: 0,
            filter_tx,
        }
    }

    /// Next vertex id
    fn next_vertex_id(&mut self) -> u64 {
        let new_vid = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        new_vid
    }

    /// Insert a new vertex to inner graph
    fn insert_new_vertex(&mut self, new_vid: u64, mut new_v: Vertex<C>) {
        for v in self.vs.values_mut() {
            if v.is_conflict(&new_v) {
                assert!(v.successors.insert(new_vid), "cannot insert a vertex twice");
                new_v.predecessor_cnt += 1;
            }
        }
        assert!(
            self.vs.insert(new_vid, new_v).is_none(),
            "cannot insert a vertex twice"
        );
    }

    /// Mark a cmd executed, will release blocked msgs
    fn mark_executed(&mut self, vid: u64, succeeded: bool) {
        let v = self.get_vertex_mut(vid);
        match v.inner {
            VertexInner::Cmd { ref mut exe_st, .. } => {
                debug_assert!(matches!(*exe_st, ExeState::Executing));
                *exe_st = ExeState::Executed(succeeded);
            }
            _ => unreachable!("impossible vertex type, {v:?}"),
        };
        self.update_graph(vid);
    }

    /// Mark a cmd after synced, will release blocked msgs
    fn mark_after_synced(&mut self, vid: u64) {
        let v = self.get_vertex_mut(vid);
        match v.inner {
            VertexInner::Cmd { ref mut as_st, .. } => {
                debug_assert!(matches!(*as_st, AsState::AfterSyncing));
                *as_st = AsState::AfterSynced;
            }
            _ => unreachable!("impossible vertex type, {v:?}"),
        };
        self.update_graph(vid);
    }

    /// Mark a vertex reset
    fn mark_reset(&mut self, vid: u64) {
        let v = self.get_vertex_mut(vid);
        match v.inner {
            VertexInner::Reset {
                ref snapshot,
                ref finish_tx,
                ref mut st,
            } => {
                debug_assert!(snapshot.is_none(), "snapshot is not taken");
                debug_assert!(finish_tx.is_none(), "snapshot finish tx is not taken");
                debug_assert!(matches!(*st, OnceState::Doing));
                *st = OnceState::Completed;
            }
            _ => unreachable!("impossible vertex type, {v:?}"),
        }
        self.update_graph(vid);
    }

    /// Mark a vertex snapshot finished
    fn mark_snapshot_finished(&mut self, vid: u64) {
        let Some(v) = self.vs.get_mut(&vid) else { return; };
        match v.inner {
            VertexInner::Snapshot { ref tx, ref mut st } => {
                debug_assert!(tx.is_none(), "snapshot tx is not taken");
                debug_assert!(matches!(*st, OnceState::Doing));
                *st = OnceState::Completed;
            }
            _ => unreachable!("impossible vertex type"),
        }
        self.update_graph(vid);
    }

    /// Update a graph after a vertex has been updated
    fn update_graph(&mut self, vid: u64) {
        let vertex_finished = self.update_vertex(vid);
        if vertex_finished {
            #[allow(clippy::expect_used)]
            let v = self
                .vs
                .remove(&vid)
                .expect("no such vertex in conflict graph");
            if let VertexInner::Cmd { ref cmd, .. } = v.inner {
                assert!(self.cmd_vid.remove(cmd.id()).is_some(), "no such cmd");
            }
            self.update_successors(&v);
        }
    }

    /// Update a vertex's successors
    fn update_successors(&mut self, v: &Vertex<C>) {
        for successor_id in v.successors.iter().copied() {
            let successor = self.get_vertex_mut(successor_id);
            successor.predecessor_cnt -= 1;
            assert!(
                !self.update_vertex(successor_id),
                "successor can't have finished before predecessor"
            );
        }
    }

    /// Update the vertex, see if it can progress
    /// Return true if it can be removed
    fn update_vertex(&mut self, vid: u64) -> bool {
        let v = self.get_vertex_mut(vid);
        if v.predecessor_cnt != 0 {
            return false;
        }
        match v.inner {
            VertexInner::Cmd {
                ref cmd,
                ref mut exe_st,
                ref mut as_st,
            } => match (*exe_st, *as_st) {
                (ExeState::ExecuteReady, AsState::NotSynced | AsState::AfterSyncReady(_)) => {
                    *exe_st = ExeState::Executing;
                    let task = Task {
                        vid,
                        inner: TaskType::SpecExe(Arc::clone(cmd)),
                    };
                    if let Err(e) = self.filter_tx.send(task) {
                        error!("failed to send task through filter, {e}");
                    }
                    false
                }
                (ExeState::Executed(true), AsState::AfterSyncReady(index)) => {
                    *as_st = AsState::AfterSyncing;
                    let task = Task {
                        vid,
                        inner: TaskType::AS(Arc::clone(cmd), index),
                    };
                    if let Err(e) = self.filter_tx.send(task) {
                        error!("failed to send task through filter, {e}");
                    }
                    false
                }
                (ExeState::Executed(true), AsState::AfterSynced)
                | (ExeState::Executed(false), AsState::AfterSyncReady(_)) => true,
                (ExeState::Executing | ExeState::Executed(_), AsState::NotSynced)
                | (ExeState::Executing, AsState::AfterSyncReady(_) | AsState::AfterSyncing)
                | (ExeState::Executed(true), AsState::AfterSyncing) => false,
                (exe_st, as_st) => {
                    unreachable!("no such cmd state can be reached: {exe_st:?}, {as_st:?}")
                }
            },
            VertexInner::Reset {
                ref mut snapshot,
                ref mut finish_tx,
                ref mut st,
            } => match *st {
                OnceState::Ready => {
                    let task = Task {
                        vid,
                        inner: TaskType::Reset(snapshot.take(), finish_tx.take()),
                    };
                    *st = OnceState::Doing;
                    if let Err(e) = self.filter_tx.send(task) {
                        error!("failed to send task through filter, {e}");
                    }
                    false
                }
                OnceState::Doing => false,
                OnceState::Completed => true,
            },
            VertexInner::Snapshot {
                ref mut tx,
                ref mut st,
            } => match *st {
                OnceState::Ready => {
                    let task = Task {
                        vid,
                        inner: TaskType::Snapshot(tx.take()),
                    };
                    *st = OnceState::Doing;
                    if let Err(e) = self.filter_tx.send(task) {
                        error!("failed to send task through filter, {e}");
                    }
                    false
                }
                OnceState::Doing => false,
                OnceState::Completed => true,
            },
        }
    }

    /// Get vertex from id
    fn get_vertex_mut(&mut self, vid: u64) -> &mut Vertex<C> {
        #[allow(clippy::expect_used)]
        self.vs
            .get_mut(&vid)
            .expect("no such vertex in conflict graph")
    }

    /// Handle event
    fn handle_event(&mut self, event: CEEvent<C>) {
        debug!("new ce event: {event:?}");
        let vid = match event {
            CEEvent::SpecExeReady(cmd) => {
                let new_vid = self.next_vertex_id();
                assert!(
                    self.cmd_vid.insert(cmd.id().clone(), new_vid).is_none(),
                    "cannot insert a cmd twice"
                );
                let new_v = Vertex {
                    successors: HashSet::new(),
                    predecessor_cnt: 0,
                    inner: VertexInner::Cmd {
                        cmd,
                        exe_st: ExeState::ExecuteReady,
                        as_st: AsState::NotSynced,
                    },
                };
                self.insert_new_vertex(new_vid, new_v);
                new_vid
            }
            CEEvent::ASReady(cmd, index) => {
                if let Some(vid) = self.cmd_vid.get(cmd.id()).copied() {
                    let v = self.get_vertex_mut(vid);
                    match v.inner {
                        VertexInner::Cmd { ref mut as_st, .. } => {
                            debug_assert!(matches!(*as_st, AsState::NotSynced));
                            *as_st = AsState::AfterSyncReady(index);
                        }
                        _ => unreachable!("impossible vertex type"),
                    }
                    vid
                } else {
                    let new_vid = self.next_vertex_id();
                    assert!(
                        self.cmd_vid.insert(cmd.id().clone(), new_vid).is_none(),
                        "cannot insert a cmd twice"
                    );
                    let new_v = Vertex {
                        successors: HashSet::new(),
                        predecessor_cnt: 0,
                        inner: VertexInner::Cmd {
                            cmd,
                            exe_st: ExeState::ExecuteReady,
                            as_st: AsState::AfterSyncReady(index),
                        },
                    };
                    self.insert_new_vertex(new_vid, new_v);
                    new_vid
                }
            }
            CEEvent::Reset(snapshot, finish_tx) => {
                // since a reset is needed, all other vertexes doesn't matter anymore, so delete them all
                self.cmd_vid.clear();
                self.vs.clear();

                let new_vid = self.next_vertex_id();
                let new_v = Vertex {
                    successors: HashSet::new(),
                    predecessor_cnt: 0,
                    inner: VertexInner::Reset {
                        snapshot,
                        finish_tx: Some(finish_tx),
                        st: OnceState::Ready,
                    },
                };
                self.insert_new_vertex(new_vid, new_v);
                new_vid
            }
            CEEvent::Snapshot(tx, meta) => {
                let new_vid = self.next_vertex_id();
                let new_v = Vertex {
                    successors: HashSet::new(),
                    predecessor_cnt: 0,
                    inner: VertexInner::Snapshot {
                        tx: Some((tx, meta)),
                        st: OnceState::Ready,
                    },
                };
                self.insert_new_vertex(new_vid, new_v);
                new_vid
            }
        };
        self.update_graph(vid);
    }
}

/// Create conflict checked channel. The channel guarantees there will be no conflicted msgs received by multiple receivers at the same time.
// Message flow:
// send_tx -> filter_rx -> filter -> filter_tx -> recv_rx -> done_tx -> done_rx
#[allow(clippy::type_complexity)] // it's clear
pub(in crate::server) fn channel<C: 'static + Command>() -> (
    CEEventTx<C>,
    flume::Receiver<Task<C>>,
    flume::Sender<(Task<C>, bool)>,
) {
    // recv from user, insert it into filter
    let (send_tx, filter_rx) = flume::unbounded();
    // recv from filter, pass the msg to user
    let (filter_tx, recv_rx) = flume::unbounded();
    // recv from user to mark a msg done
    let (done_tx, done_rx) = flume::unbounded::<(Task<C>, bool)>();
    let _ig = tokio::spawn(async move {
        let mut filter = Filter::new(filter_tx);
        #[allow(clippy::integer_arithmetic, clippy::pattern_type_mismatch)]
        // tokio internal triggers
        loop {
            tokio::select! {
                biased; // cleanup filter first so that the buffer in filter can be kept as small as possible
                Ok((task, succeeded)) = done_rx.recv_async() => {
                    match task.inner {
                        TaskType::SpecExe(_) => filter.mark_executed(task.vid, succeeded),
                        TaskType::AS(_, _) => filter.mark_after_synced(task.vid),
                        TaskType::Reset(_, _) => filter.mark_reset(task.vid),
                        TaskType::Snapshot(_) => filter.mark_snapshot_finished(task.vid),
                    }
                },
                Ok(event) = filter_rx.recv_async() => {
                    filter.handle_event(event);
                },
                else => {
                    error!("mpmc channel stopped unexpectedly");
                    return;
                }
            }
        }
    });
    (CEEventTx(send_tx), recv_rx, done_tx)
}
