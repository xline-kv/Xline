use std::{collections::BTreeMap, future::Future};

use clippy_utilities::OverflowArithmetic;
use event_listener::Event;
use parking_lot::Mutex;

/// A Index trait that can be used as the index of `IndexBarrier`.
pub trait Index: Copy + Clone + Default + Ord + std::fmt::Debug {
    /// Get the next index.
    fn next(&self) -> Self;
}

/// Waiter for index
#[derive(Debug)]
pub struct IndexBarrier<Idx> {
    /// Inner
    inner: Mutex<Inner<Idx>>,
}

impl<Idx> IndexBarrier<Idx>
where
    Idx: Index,
{
    /// Create a new index barrier
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait for the index until it is triggered.
    #[inline]
    pub fn wait(&self, index: Idx) -> Box<dyn Future<Output = ()> + Send + Sync + 'static> {
        let mut inner_l = self.inner.lock();
        if inner_l.last_trigger_index >= index {
            return Box::new(futures::future::ready(()));
        }
        Box::new(inner_l.barriers.entry(index).or_default().listen())
    }

    /// Trigger all barriers whose index is less than or equal to the given
    /// index.
    #[inline]
    pub fn trigger(&self, index: Idx) {
        let mut inner_l = self.inner.lock();
        if inner_l.last_trigger_index < index {
            inner_l.last_trigger_index = index;
        }
        let mut split_barriers = inner_l.barriers.split_off(&(index.next()));
        std::mem::swap(&mut inner_l.barriers, &mut split_barriers);
        for (_, barrier) in split_barriers {
            let _ignore = barrier.notify(usize::MAX);
        }
    }
}

impl<Idx> Default for IndexBarrier<Idx>
where
    Idx: Index,
{
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Inner of index barrier.
#[derive(Default, Debug)]
struct Inner<Idx> {
    /// The last index that the barrier has triggered.
    last_trigger_index: Idx,
    /// Barrier of index.
    barriers: BTreeMap<Idx, Event>,
}

impl Index for u64 {
    fn next(&self) -> Self {
        self.overflow_add(1)
    }
}
