use std::collections::{BTreeMap, HashMap};

use clippy_utilities::OverflowArithmetic;
use curp::{InflightId, LogIndex};
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
                last_trigger_index: 0,
                barriers: BTreeMap::new(),
            }),
        }
    }

    /// Wait for the index until it is triggered.
    pub(crate) async fn wait(&self, index: LogIndex) {
        let listener = {
            let mut inner_l = self.inner.lock();
            if inner_l.last_trigger_index >= index {
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
    pub(crate) fn trigger(&self, index: LogIndex) {
        let mut inner_l = self.inner.lock();
        if inner_l.last_trigger_index < index {
            inner_l.last_trigger_index = index;
        }
        let mut split_barriers = inner_l.barriers.split_off(&(index.overflow_add(1)));
        std::mem::swap(&mut inner_l.barriers, &mut split_barriers);
        for (_, barrier) in split_barriers {
            barrier.notify(usize::MAX);
        }
    }
}

/// Inner of index barrier.
#[derive(Debug)]
struct IndexBarrierInner {
    /// The last index that the barrier has triggered.
    last_trigger_index: LogIndex,
    /// Barrier of index.
    barriers: BTreeMap<LogIndex, Event>,
}

/// Barrier for id
#[derive(Debug)]
pub(crate) struct IdBarrier {
    /// Barriers of id
    barriers: Mutex<HashMap<InflightId, Event>>,
}

impl IdBarrier {
    /// Create a new id barrier
    pub(crate) fn new() -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
        }
    }

    /// Wait for the id until it is triggered.
    pub(crate) async fn wait(&self, id: InflightId) {
        let listener = self
            .barriers
            .lock()
            .entry(id)
            .or_insert_with(Event::new)
            .listen();
        listener.await;
    }

    /// Trigger the barrier of the given inflight id.
    pub(crate) fn trigger(&self, id: InflightId) {
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
                    id_barrier.wait(i).await;
                })
            })
            .collect::<Vec<_>>();
        sleep(Duration::from_millis(10)).await;
        for i in 0..5 {
            id_barrier.trigger(i);
        }
        timeout(Duration::from_millis(100), join_all(barriers))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_index_barrier() {
        let index_barrier = Arc::new(IndexBarrier::new());
        let barriers = (0..5).map(|i| {
            let id_barrier = Arc::clone(&index_barrier);
            tokio::spawn(async move {
                id_barrier.wait(i).await;
            })
        });
        index_barrier.trigger(5);

        timeout(Duration::from_millis(100), index_barrier.wait(3))
            .await
            .unwrap();

        timeout(Duration::from_millis(100), join_all(barriers))
            .await
            .unwrap();
    }
}
