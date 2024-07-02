use std::{collections::HashMap, hash::Hash};

use event_listener::Event;
use futures::{stream::FuturesOrdered, Future, FutureExt, StreamExt};
use parking_lot::Mutex;

/// Barrier for id
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Default)]
pub struct IdBarrier<Id> {
    /// Barriers of id
    barriers: Mutex<HashMap<Id, Event>>,
}

impl<Id> IdBarrier<Id> {
    /// Create a new id barrier
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
        }
    }
}

impl<Id> IdBarrier<Id>
where
    Id: Eq + Hash,
{
    /// Wait for the id until it is triggered.
    #[inline]
    pub async fn wait(&self, id: Id) {
        let listener = self.barriers.lock().entry(id).or_default().listen();
        listener.await;
    }

    /// Wait for a collection of ids.
    #[inline]
    pub fn wait_all(&self, ids: Vec<Id>) -> impl Future<Output = ()> + Send {
        let mut barriers_l = self.barriers.lock();
        let listeners: FuturesOrdered<_> = ids
            .into_iter()
            .map(|id| barriers_l.entry(id).or_default().listen())
            .collect();
        listeners.collect::<Vec<_>>().map(|_| ())
    }

    /// Trigger the barrier of the given inflight id.
    #[inline]
    pub fn trigger(&self, id: &Id) {
        if let Some(event) = self.barriers.lock().remove(id) {
            let _ignore = event.notify(usize::MAX);
        }
    }
}
