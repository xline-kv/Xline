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

use self::cart::Cart;
use super::{CEEvent, CEEventTx};
use crate::{
    cmd::{Command, ProposeId},
    snapshot::{Snapshot, SnapshotMeta},
    LogIndex,
};

/// Cart
mod cart {
    /// Cart is a utility that acts as a temporary container.
    /// It is usually filled by the provider and consumed by the customer.
    /// This is useful when we are sure that the provider will fill the cart and the cart will be consumed by the customer
    /// so that we don't need to check whether there is something in the `Option`.
    #[derive(Debug)]
    pub(super) struct Cart<T>(Option<T>);

    impl<T> Cart<T> {
        /// New cart with object
        pub(super) fn new(object: T) -> Self {
            Self(Some(object))
        }
        /// Take the object. Panic if its inner has already been taken.
        pub(super) fn take(&mut self) -> T {
            #[allow(clippy::expect_used)]
            self.0.take().expect("the cart is empty")
        }
        /// Check whether the object is taken
        pub(super) fn is_taken(&self) -> bool {
            self.0.is_none()
        }
    }
}

/// CE task
pub(in crate::server) struct Task<C> {
    /// Corresponding vertex id
    vid: u64,
    /// Task type
    inner: Cart<TaskType<C>>,
}

/// Task Type
pub(in crate::server) enum TaskType<C> {
    /// Execute a cmd
    SpecExe(Arc<C>),
    /// After sync a cmd
    AS(Arc<C>, LogIndex),
    /// Reset the CE
    Reset(Option<Snapshot>, oneshot::Sender<()>),
    /// Snapshot
    Snapshot(SnapshotMeta, oneshot::Sender<Snapshot>),
}

impl<C> Task<C> {
    /// Get inner task
    pub(super) fn take(&mut self) -> TaskType<C> {
        self.inner.take()
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
        /// The snapshot and finish notifier
        inner: Cart<(Box<Option<Snapshot>>, oneshot::Sender<()>)>, // use `Box` to avoid enum members with large size
        /// Reset state
        st: OnceState,
    },
    /// A snapshot vertex
    Snapshot {
        /// The sender
        inner: Cart<(SnapshotMeta, oneshot::Sender<Snapshot>)>,
        /// Snapshot state
        st: OnceState,
    },
}

/// Execute state of a cmd
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExeState {
    /// Is ready to execute
    ExecuteReady,
    /// Executing
    Executing,
    /// Has been executed, and the result
    Executed(bool),
}

/// After sync state of a cmd
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, PartialEq, Eq)]
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
    /// Send task to `as_worker`
    as_tx: flume::Sender<Task<C>>,
}

impl<C: Command> Filter<C> {
    /// Create a new filter that checks conflict in between msgs
    fn new(filter_tx: flume::Sender<Task<C>>, as_tx: flume::Sender<Task<C>>) -> Self {
        Self {
            cmd_vid: HashMap::new(),
            vs: HashMap::new(),
            next_id: 0,
            filter_tx,
            as_tx,
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

    /// Progress a vertex
    fn progress(&mut self, vid: u64, succeeded: bool) {
        let v = self.get_vertex_mut(vid);
        match v.inner {
            VertexInner::Cmd {
                ref mut exe_st,
                ref mut as_st,
                ..
            } => {
                if *exe_st == ExeState::Executing && *as_st != AsState::AfterSyncing {
                    *exe_st = ExeState::Executed(succeeded);
                } else if *as_st == AsState::AfterSyncing {
                    *as_st = AsState::AfterSynced;
                } else {
                    unreachable!("cmd is neither being executed nor being after synced, exe_st: {exe_st:?}, as_st: {as_st:?}")
                }
            }
            VertexInner::Reset {
                ref inner,
                ref mut st,
            } => {
                if *st == OnceState::Doing {
                    debug_assert!(inner.is_taken(), "snapshot and tx is not taken by the user");
                    *st = OnceState::Completed;
                } else {
                    unreachable!("reset is not ongoing when it is marked done, reset state: {st:?}")
                }
            }
            VertexInner::Snapshot {
                ref inner,
                ref mut st,
            } => {
                if *st == OnceState::Doing {
                    debug_assert!(
                        inner.is_taken(),
                        "snapshot meta and tx is not taken by the user"
                    );
                    *st = OnceState::Completed;
                } else {
                    unreachable!(
                        "snapshot is not ongoing when it is marked done, reset state: {st:?}"
                    )
                }
            }
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
                        inner: Cart::new(TaskType::SpecExe(Arc::clone(cmd))),
                    };
                    if let Err(e) = self.filter_tx.send(task) {
                        error!("failed to send task through filter, {e}");
                    }
                    false
                }
                (ExeState::Executed(_), AsState::AfterSyncReady(index)) => {
                    *as_st = AsState::AfterSyncing;
                    let task = Task {
                        vid,
                        inner: Cart::new(TaskType::AS(Arc::clone(cmd), index)),
                    };
                    if let Err(e) = self.as_tx.send(task) {
                        error!("failed to send task through filter, {e}");
                    }
                    false
                }
                (ExeState::Executed(_), AsState::AfterSynced) => true,
                (ExeState::Executing | ExeState::Executed(_), AsState::NotSynced)
                | (ExeState::Executing, AsState::AfterSyncReady(_) | AsState::AfterSyncing)
                | (ExeState::Executed(true), AsState::AfterSyncing) => false,
                (exe_st, as_st) => {
                    unreachable!("no such exe and as state can be reached: {exe_st:?}, {as_st:?}")
                }
            },
            VertexInner::Reset {
                ref mut inner,
                ref mut st,
            } => match *st {
                OnceState::Ready => {
                    let (snapshot, tx) = inner.take();
                    let task = Task {
                        vid,
                        inner: Cart::new(TaskType::Reset(*snapshot, tx)),
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
                ref mut inner,
                ref mut st,
            } => match *st {
                OnceState::Ready => {
                    let (meta, tx) = inner.take();
                    let task = Task {
                        vid,
                        inner: Cart::new(TaskType::Snapshot(meta, tx)),
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
                        inner: Cart::new((Box::new(snapshot), finish_tx)),
                        st: OnceState::Ready,
                    },
                };
                self.insert_new_vertex(new_vid, new_v);
                new_vid
            }
            CEEvent::Snapshot(meta, tx) => {
                let new_vid = self.next_vertex_id();
                let new_v = Vertex {
                    successors: HashSet::new(),
                    predecessor_cnt: 0,
                    inner: VertexInner::Snapshot {
                        inner: Cart::new((meta, tx)),
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
/// The user should use the `CEEventTx` to send events for command executor.
/// The events will be automatically processed and corresponding ce tasks will be generated and sent through the task receiver.
/// After the task is finished, the user should notify the channel by the done notifier.
// Message flow:
// send_tx -> filter_rx -> filter -> filter_tx -> recv_rx -> done_tx -> done_rx
#[allow(clippy::type_complexity)] // it's clear
pub(in crate::server) fn channel<C: 'static + Command>() -> (
    CEEventTx<C>,
    flume::Receiver<Task<C>>,
    flume::Receiver<Task<C>>,
    flume::Sender<(Task<C>, bool)>,
) {
    // recv from user, insert it into filter
    let (send_tx, filter_rx) = flume::unbounded();
    // recv from filter, pass the msg to user
    let (filter_tx, recv_rx) = flume::unbounded();
    // recv from filter, pass the msg to user
    let (as_tx, as_rx) = flume::unbounded();
    // recv from user to mark a msg done
    let (done_tx, done_rx) = flume::unbounded::<(Task<C>, bool)>();
    let _ig = tokio::spawn(async move {
        let mut filter = Filter::new(filter_tx, as_tx);
        #[allow(clippy::integer_arithmetic, clippy::pattern_type_mismatch)]
        // tokio internal triggers
        loop {
            tokio::select! {
                biased; // cleanup filter first so that the buffer in filter can be kept as small as possible
                Ok((task, succeeded)) = done_rx.recv_async() => {
                    filter.progress(task.vid, succeeded);
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
    (CEEventTx(send_tx), recv_rx, as_rx, done_tx)
}
