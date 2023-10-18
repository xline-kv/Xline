use std::{
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use clippy_utilities::OverflowArithmetic;
use curp::cmd::ProposeId;
use event_listener::Event;
use parking_lot::Mutex;

/// Waiter for index
#[derive(Debug)]
pub(crate) struct IndexBarrier {
    /// Inner
    inner: Mutex<IndexBarrierInner>,
}

impl IndexBarrier {
    /// Create a new index barrier
    pub(crate) fn new() -> Self {
        IndexBarrier {
            inner: Mutex::new(IndexBarrierInner {
                next: 1,
                indices: BinaryHeap::new(),
                barriers: HashMap::new(),
                latest_rev: 1,
            }),
        }
    }

    /// Wait for the index until it is triggered.
    pub(crate) async fn wait(&self, index: u64) -> i64 {
        if index == 0 {
            return 0;
        }
        let (listener, revision) = {
            let mut inner_l = self.inner.lock();
            if inner_l.next > index {
                return inner_l.latest_rev;
            }
            let Trigger {
                ref event,
                ref revision,
            } = *inner_l
                .barriers
                .entry(index)
                .or_insert_with(Trigger::default);
            (event.listen(), Arc::clone(revision))
        };
        listener.await;
        revision.load(Ordering::SeqCst)
    }

    /// Trigger all barriers whose index is less than or equal to the given index.
    pub(crate) fn trigger(&self, index: u64, rev: i64) {
        let mut inner_l = self.inner.lock();
        inner_l.indices.push(IndexRevision {
            index,
            revision: rev,
        });
        while inner_l
            .indices
            .peek()
            .map_or(false, |i| i.index.eq(&inner_l.next))
        {
            let next = inner_l.next;
            let IndexRevision { revision, .. } = inner_l
                .indices
                .pop()
                .unwrap_or_else(|| unreachable!("IndexRevision should be Some"));
            inner_l.latest_rev = revision;
            if let Some(trigger) = inner_l.barriers.remove(&next) {
                trigger.revision.store(revision, Ordering::SeqCst);
                trigger.event.notify(usize::MAX);
            }
            inner_l.next = next.overflow_add(1);
        }
    }
}

/// Inner of index barrier.
#[derive(Debug)]
struct IndexBarrierInner {
    /// The next index that haven't been triggered
    next: u64,
    /// Store all indices that larger than `next`
    indices: BinaryHeap<IndexRevision>,
    /// Events
    barriers: HashMap<u64, Trigger>,
    /// latest revision of the last triggered log index
    latest_rev: i64,
}

/// Barrier for id
#[derive(Debug)]
pub(crate) struct IdBarrier {
    /// Barriers of id
    barriers: Mutex<HashMap<ProposeId, Trigger>>,
}

impl IdBarrier {
    /// Create a new id barrier
    pub(crate) fn new() -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
        }
    }

    /// Wait for the id until it is triggered.
    pub(crate) async fn wait(&self, id: ProposeId) -> i64 {
        let (listener, revision) = {
            let mut barriers_l = self.barriers.lock();
            let Trigger {
                ref event,
                ref revision,
            } = *barriers_l.entry(id).or_insert_with(Trigger::default);
            (event.listen(), Arc::clone(revision))
        };
        listener.await;
        revision.load(Ordering::SeqCst)
    }

    /// Trigger the barrier of the given id.
    pub(crate) fn trigger(&self, id: ProposeId, rev: i64) {
        if let Some(trigger) = self.barriers.lock().remove(&id) {
            let Trigger { event, revision } = trigger;
            revision.store(rev, Ordering::SeqCst);
            event.notify(usize::MAX);
        }
    }
}

/// Index and revision pair type
#[derive(Debug, PartialEq, Eq)]
struct IndexRevision {
    /// The log index
    index: u64,
    /// The revision correspond to the index
    revision: i64,
}

impl PartialOrd for IndexRevision {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.index.partial_cmp(&self.index)
    }
}

impl Ord for IndexRevision {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.index.cmp(&self.index)
    }
}

/// The event trigger with revision
#[derive(Debug, Default)]
struct Trigger {
    /// The event
    event: Event,
    /// Revision passed between trigger task and wait task
    revision: Arc<AtomicI64>,
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use futures::future::join_all;
    use test_macros::abort_on_panic;
    use tokio::time::{sleep, timeout};

    use super::*;

    #[tokio::test]
    #[abort_on_panic]
    async fn test_id_barrier() {
        let id_barrier = Arc::new(IdBarrier::new());
        let barriers = (0..5)
            .map(|i| {
                let id_barrier = Arc::clone(&id_barrier);
                tokio::spawn(async move {
                    id_barrier.wait(ProposeId(i, i)).await;
                })
            })
            .collect::<Vec<_>>();
        sleep(Duration::from_millis(10)).await;
        for i in 0..5 {
            id_barrier.trigger(ProposeId(i, i), 0);
        }
        timeout(Duration::from_millis(100), join_all(barriers))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_index_barrier() {
        let index_barrier = Arc::new(IndexBarrier::new());
        let (done_tx, done_rx) = flume::bounded(5);
        let barriers = (1..=5)
            .map(|i| {
                let index_barrier = Arc::clone(&index_barrier);
                let done_tx_c = done_tx.clone();
                tokio::spawn(async move {
                    let rev = index_barrier.wait(i).await;
                    done_tx_c.send(rev).unwrap();
                })
            })
            .collect::<Vec<_>>();

        index_barrier.trigger(2, 2);
        index_barrier.trigger(3, 3);
        sleep(Duration::from_millis(100)).await;
        assert!(done_rx.try_recv().is_err());
        index_barrier.trigger(1, 1);
        sleep(Duration::from_millis(100)).await;
        assert_eq!(done_rx.try_recv().unwrap(), 1);
        assert_eq!(done_rx.try_recv().unwrap(), 2);
        assert_eq!(done_rx.try_recv().unwrap(), 3);
        index_barrier.trigger(4, 4);
        index_barrier.trigger(5, 5);

        assert_eq!(
            timeout(Duration::from_millis(100), index_barrier.wait(3))
                .await
                .unwrap(),
            5
        );

        timeout(Duration::from_millis(100), join_all(barriers))
            .await
            .unwrap();
    }
}
