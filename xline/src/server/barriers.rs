use std::collections::{BTreeMap, HashMap};

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
    /// Create a new index waiter
    pub(crate) fn new() -> Self {
        IndexBarrier {
            inner: Mutex::new(IndexBarrierInner {
                last_trigger_index: 0,
                waiters: BTreeMap::new(),
            }),
        }
    }

    /// Wait for the index until it is triggered.
    pub(crate) async fn wait(&self, index: u64) {
        let listener = {
            let mut inner_l = self.inner.lock();
            if inner_l.last_trigger_index >= index {
                return;
            }
            inner_l
                .waiters
                .entry(index)
                .or_insert_with(Event::new)
                .listen()
        };
        listener.await;
    }

    /// Trigger all waiters whose index is less than or equal to the given index.
    pub(crate) fn trigger(&self, index: u64) {
        let mut inner_l = self.inner.lock();
        inner_l.last_trigger_index = index;
        let mut split_waiters = inner_l.waiters.split_off(&(index.overflow_add(1)));
        std::mem::swap(&mut inner_l.waiters, &mut split_waiters);
        for (_, waiter) in split_waiters {
            waiter.notify(usize::MAX);
        }
    }
}

/// Inner of index waiter.
#[derive(Debug)]
struct IndexBarrierInner {
    /// The last index that the waiter has triggered.
    last_trigger_index: u64,
    /// Waiters of index.
    waiters: BTreeMap<u64, Event>,
}

/// Waiter for id
#[derive(Debug)]
pub(crate) struct IdBarrier {
    /// Waiters of id
    waiters: Mutex<HashMap<ProposeId, Event>>,
}

impl IdBarrier {
    /// Create a new id waiter
    pub(crate) fn new() -> Self {
        Self {
            waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Wait for the id until it is triggered.
    pub(crate) async fn wait(&self, id: ProposeId) {
        let listener = self
            .waiters
            .lock()
            .entry(id)
            .or_insert_with(Event::new)
            .listen();
        listener.await;
    }

    /// Trigger the waiter of the given id.
    pub(crate) fn trigger(&self, id: &ProposeId) {
        if let Some(event) = self.waiters.lock().remove(id) {
            event.notify(usize::MAX);
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use futures::future::join_all;
    use tokio::time::{sleep, timeout};

    use super::*;

    #[tokio::test]
    async fn test_id_waiter() {
        let id_waiter = Arc::new(IdBarrier::new());
        let waiters = (0..5)
            .map(|i| {
                let id_waiter = Arc::clone(&id_waiter);
                tokio::spawn(async move {
                    id_waiter.wait(ProposeId::new(i.to_string())).await;
                })
            })
            .collect::<Vec<_>>();
        sleep(Duration::from_millis(10)).await;
        for i in 0..5 {
            id_waiter.trigger(&ProposeId::new(i.to_string()));
        }
        timeout(Duration::from_millis(100), join_all(waiters))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_index_waiter() {
        let index_waiter = Arc::new(IndexBarrier::new());
        let waiters = (0..5).map(|i| {
            let id_waiter = Arc::clone(&index_waiter);
            tokio::spawn(async move {
                id_waiter.wait(i).await;
            })
        });
        index_waiter.trigger(5);

        timeout(Duration::from_millis(100), index_waiter.wait(3))
            .await
            .unwrap();

        timeout(Duration::from_millis(100), join_all(waiters))
            .await
            .unwrap();
    }
}
