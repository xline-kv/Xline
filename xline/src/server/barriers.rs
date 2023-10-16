use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
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
            }),
        }
    }

    /// Wait for the index until it is triggered.
    pub(crate) async fn wait(&self, index: u64) {
        let listener = {
            let mut inner_l = self.inner.lock();
            if inner_l.next > index {
                return;
            }
            inner_l
                .barriers
                .entry(index)
                .or_insert_with(Event::new)
                .listen()
        };
        listener.await;
    }

    /// Trigger all barriers whose index is less than or equal to the given index.
    pub(crate) fn trigger(&self, index: u64) {
        let mut inner_l = self.inner.lock();
        inner_l.indices.push(Reverse(index));
        while inner_l
            .indices
            .peek()
            .map_or(false, |i| i.0.eq(&inner_l.next))
        {
            let next = inner_l.next;
            let _ignore = inner_l.indices.pop();
            if let Some(event) = inner_l.barriers.remove(&next) {
                event.notify(usize::MAX);
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
    indices: BinaryHeap<Reverse<u64>>,
    /// Events
    barriers: HashMap<u64, Event>,
}

/// Barrier for id
#[derive(Debug)]
pub(crate) struct IdBarrier {
    /// Barriers of id
    barriers: Mutex<HashMap<ProposeId, Event>>,
}

impl IdBarrier {
    /// Create a new id barrier
    pub(crate) fn new() -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
        }
    }

    /// Wait for the id until it is triggered.
    pub(crate) async fn wait(&self, id: ProposeId) {
        let listener = self
            .barriers
            .lock()
            .entry(id)
            .or_insert_with(Event::new)
            .listen();
        listener.await;
    }

    /// Trigger the barrier of the given id.
    pub(crate) fn trigger(&self, id: ProposeId) {
        if let Some(event) = self.barriers.lock().remove(&id) {
            event.notify(usize::MAX);
        }
    }
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
            id_barrier.trigger(ProposeId(i, i));
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
                    index_barrier.wait(i).await;
                    done_tx_c.send(i).unwrap();
                })
            })
            .collect::<Vec<_>>();

        index_barrier.trigger(2);
        index_barrier.trigger(3);
        sleep(Duration::from_millis(100)).await;
        assert!(done_rx.try_recv().is_err());
        index_barrier.trigger(1);
        sleep(Duration::from_millis(100)).await;
        assert_eq!(done_rx.try_recv().unwrap(), 1);
        assert_eq!(done_rx.try_recv().unwrap(), 2);
        assert_eq!(done_rx.try_recv().unwrap(), 3);
        index_barrier.trigger(4);
        index_barrier.trigger(5);

        timeout(Duration::from_millis(100), index_barrier.wait(3))
            .await
            .unwrap();

        timeout(Duration::from_millis(100), join_all(barriers))
            .await
            .unwrap();
    }
}
