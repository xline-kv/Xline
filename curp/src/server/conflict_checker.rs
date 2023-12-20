#![allow(dead_code)]

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use curp_external_api::cmd::Command;
use dashmap::{DashMap, DashSet};
use tokio::sync::Notify;

use crate::{
    log_entry::{EntryData, LogEntry},
    rpc::ProposeId,
};

/// The conflict checker
#[derive(Debug)]
pub(crate) struct ConflictChecker<C: Command> {
    /// Index from `ProposeId` to `vertex`
    cmd_vid: Arc<DashMap<ProposeId, u64>>,
    /// Conflict graph
    vs: Arc<DashMap<u64, Vertex<C>>>,
    /// Next vertex id
    next_id: AtomicU64,
}

/// The vertex type stored in conflict checker
#[derive(Debug)]
pub(crate) enum VertexType<C: Command> {
    /// A entry vertex
    Entry(Arc<LogEntry<C>>),
    /// A reset vertex
    Reset,
    /// A snapshot vertex
    Snapshot,
}

/// Waiting state for `Registration`
pub(crate) struct Waiting;
/// Ready state for `Registration`
pub(crate) struct Ready;
/// A registration for entry in the conflict checker
pub(crate) struct Registration<C: Command, S> {
    /// The vertex id
    vid: u64,
    /// Notifier
    notify: Option<Arc<Notify>>,
    /// Conflict graph
    vs: Arc<DashMap<u64, Vertex<C>>>,
    /// Index from `ProposeId` to `vertex`
    cmd_vid: Arc<DashMap<ProposeId, u64>>,
    /// The current state
    state: PhantomData<S>,
}

/// Vertex
#[derive(Debug)]
struct Vertex<C: Command> {
    /// Successor cmds that arrive later with keys that conflict this cmd
    successors: DashSet<u64>,
    /// Number of predecessor cmds that arrive earlier with keys that conflict this cmd
    predecessor_cnt: AtomicU64,
    /// Vertex inner
    inner: VertexType<C>,
    /// Notifier
    notify: Arc<Notify>,
}

impl<C: Command> ConflictChecker<C> {
    /// Creates a new `ConflictChecker`
    pub(crate) fn new() -> Self {
        Self {
            cmd_vid: Arc::new(DashMap::new()),
            vs: Arc::new(DashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }

    /// Registers a vertex to the checker
    pub(crate) fn register(&self, vertex: VertexType<C>) -> Registration<C, Waiting> {
        let new_vid = self.next_vertex_id();
        match vertex {
            VertexType::Entry(ref e) => {
                // Already registered in speculative execution
                if let Some(entry) = self.cmd_vid.get(&e.propose_id) {
                    return self.new_register(*entry.value(), None);
                }
                let _ignore = self.cmd_vid.insert(e.propose_id, new_vid);
            }
            VertexType::Reset => {
                // since a reset is needed, all other vertices doesn't matter anymore, so delete them all
                self.cmd_vid.clear();
                self.vs.clear();
            }
            VertexType::Snapshot => {}
        }

        let notify = Arc::new(Notify::new());
        let new_v = Vertex::new(vertex, Arc::clone(&notify));
        self.insert_vertex(new_vid, new_v);
        self.new_register(new_vid, Some(notify))
    }

    /// Creates a new register
    fn new_register(&self, new_vid: u64, notify: Option<Arc<Notify>>) -> Registration<C, Waiting> {
        Registration {
            vid: new_vid,
            notify,
            vs: Arc::clone(&self.vs),
            cmd_vid: Arc::clone(&self.cmd_vid),
            state: PhantomData,
        }
    }

    /// Next vertex id
    fn next_vertex_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Insert a new vertex to inner graph
    fn insert_vertex(&self, new_vid: u64, new_v: Vertex<C>) {
        for entry in self.vs.iter() {
            let v = entry.value();
            if v.is_conflict(&new_v) {
                assert!(v.successors.insert(new_vid), "cannot insert a vertex twice");
                let _ignore = new_v.predecessor_cnt.fetch_add(1, Ordering::Relaxed);
            }
        }
        if new_v.predecessor_cnt.load(Ordering::Relaxed) == 0 {
            new_v.notify.notify_one();
        }
        assert!(
            self.vs.insert(new_vid, new_v).is_none(),
            "cannot insert a vertex twice"
        );
    }
}

impl<C: Command> Vertex<C> {
    /// Creates a new `Vertex`
    fn new(entry: VertexType<C>, notify: Arc<Notify>) -> Self {
        Self {
            successors: DashSet::new(),
            predecessor_cnt: AtomicU64::new(0),
            inner: entry,
            notify,
        }
    }

    /// Whether two vertex conflict each other
    fn is_conflict(&self, other: &Vertex<C>) -> bool {
        #[allow(clippy::pattern_type_mismatch)]
        // it seems it's impossible to get away with this lint
        match (&self.inner, &other.inner) {
            (VertexType::Entry(entry1), VertexType::Entry(entry2)) => {
                let EntryData::Command(ref cmd1) = entry1.entry_data else {
                    return true;
                };
                let EntryData::Command(ref cmd2) = entry2.entry_data else {
                    return true;
                };
                cmd1.is_conflict(cmd2)
            }
            _ => true,
        }
    }
}

impl<C: Command> Registration<C, Waiting> {
    /// Waits for the resolution of the conflicts
    pub(crate) async fn wait(self) -> Registration<C, Ready> {
        if let Some(ref notify) = self.notify {
            notify.notified().await;
        }
        Registration {
            vid: self.vid,
            notify: self.notify,
            vs: self.vs,
            cmd_vid: self.cmd_vid,
            state: PhantomData,
        }
    }
}

impl<C: Command> Registration<C, Ready> {
    /// Finishes a vertex
    pub(crate) fn finish(self) {
        let (_, v) = self
            .vs
            .remove(&self.vid)
            .unwrap_or_else(|| unreachable!("no such vertex in conflict graph"));
        if let VertexType::Entry(ref e) = v.inner {
            assert!(self.cmd_vid.remove(&e.propose_id).is_some(), "no such cmd");
        }
        self.update_successors(&v);
    }

    /// Update a vertex's successors
    fn update_successors(self, v: &Vertex<C>) {
        for successor_id in v.successors.iter().map(|t| *t.key()) {
            let entry = self
                .vs
                .get(&successor_id)
                .unwrap_or_else(|| unreachable!("no such vertex in conflict graph"));
            let successor = entry.value();
            if successor.predecessor_cnt.fetch_sub(1, Ordering::Relaxed) == 1 {
                successor.notify.notify_one();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use curp_test_utils::test_cmd::TestCommand;
    use futures::future::poll_immediate;

    use super::*;

    #[tokio::test]
    async fn conflict_checking_for_pairs_should_work() {
        let mut gen = EntryGen::default();
        let entry_pairs_conflict = [
            (gen.log_entry(vec![0]), gen.log_entry(vec![0])),
            (gen.log_entry(vec![0]), VertexType::Snapshot),
            (VertexType::Snapshot, VertexType::Snapshot),
            (VertexType::Reset, gen.log_entry(vec![0])),
            (VertexType::Reset, VertexType::Snapshot),
        ];
        let conflict_checker = ConflictChecker::new();

        for (entry1, entry2) in entry_pairs_conflict {
            let reg1 = conflict_checker.register(entry1);
            let reg1 = poll_immediate(reg1.wait()).await.unwrap();
            let reg2 = conflict_checker.register(entry2);
            let mut fut2 = Box::pin(reg2.wait());
            assert!(poll_immediate(&mut fut2).await.is_none());
            reg1.finish();
            let reg2 = poll_immediate(fut2).await.unwrap();
            reg2.finish();
        }
    }

    #[tokio::test]
    async fn conflict_checking_for_graph_should_work() {
        let mut gen = EntryGen::default();
        let conflict_checker = ConflictChecker::new();

        let log_entries = vec![
            gen.log_entry(vec![1, 11]),
            gen.log_entry(vec![2, 22]),
            gen.log_entry(vec![3, 1, 2]),
            gen.log_entry(vec![4, 11]),
            gen.log_entry(vec![5, 3, 4]),
        ];
        let edges = [(0, 2), (0, 3), (1, 2), (2, 4), (3, 4)];

        let mut regs: Vec<_> = log_entries
            .into_iter()
            .map(|e| Some(conflict_checker.register(e)))
            .collect();

        // Run a toposort
        let mut graph = vec![vec![]; 5];
        let mut degree = vec![0; 5];
        for (x, y) in edges {
            graph[x].push(y);
            degree[y] += 1;
        }
        let mut q = VecDeque::new();
        for i in 0..5 {
            if degree[i] == 0 {
                q.push_back(i);
            }
        }
        while !q.is_empty() {
            let front = q.pop_front().unwrap();
            let wait_fut = regs[front].take().unwrap().wait();
            let reg = poll_immediate(wait_fut).await.unwrap();
            reg.finish();
            for v in graph[front].iter().copied() {
                degree[v] -= 1;
                if degree[v] == 0 {
                    q.push_back(v);
                }
            }
        }
    }

    #[derive(Default)]
    struct EntryGen {
        seq: u64,
    }

    impl EntryGen {
        fn log_entry(&mut self, keys: Vec<u32>) -> VertexType<TestCommand> {
            let cmd = Arc::new(TestCommand::new_put(keys, 0));
            self.seq += 1;
            VertexType::Entry(Arc::new(LogEntry::new(
                0,
                0,
                ProposeId(0, self.seq),
                EntryData::Command(cmd),
            )))
        }
    }
}
