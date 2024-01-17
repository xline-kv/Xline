use std::{
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

use clippy_utilities::OverflowArithmetic;
use tokio::sync::RwLock;
use tracing::{info, warn};
use utils::shutdown;

use super::{Compactable, Compactor};
use crate::revision_number::RevisionNumberGenerator;

/// check for the need of compaction every 5 minutes
const CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Revision auto compactor
#[derive(Debug)]
pub(crate) struct RevisionCompactor<C: Compactable> {
    /// `is_leader` indicates whether the current node is a leader or not.
    is_leader: AtomicBool,
    /// curp client
    compactable: RwLock<Option<C>>,
    /// revision getter
    revision_getter: Arc<RevisionNumberGenerator>,
    /// shutdown listener
    shutdown_listener: shutdown::Listener,
    /// revision retention
    retention: i64,
}

impl<C: Compactable> RevisionCompactor<C> {
    /// Creates a new revision compactor
    pub(super) fn new_arc(
        is_leader: bool,
        revision_getter: Arc<RevisionNumberGenerator>,
        shutdown_listener: shutdown::Listener,
        retention: i64,
    ) -> Arc<Self> {
        Arc::new(Self {
            is_leader: AtomicBool::new(is_leader),
            compactable: RwLock::new(None),
            revision_getter,
            shutdown_listener,
            retention,
        })
    }

    /// perform auto compaction logic
    async fn do_compact(&self, last_revision: Option<i64>) -> Option<i64> {
        if !self.is_leader.load(Relaxed) {
            return None;
        }

        let target_revision = self.revision_getter.get().overflow_sub(self.retention);
        if target_revision <= 0 || Some(target_revision) <= last_revision {
            return None;
        }

        let now = Instant::now();
        info!(
            "starting auto revision compaction, revision = {}, retention = {}",
            target_revision, self.retention
        );

        let Some(ref compactable) = *self.compactable.read().await else {
            return None;
        };

        match compactable.compact(target_revision).await {
            Ok(rev) => {
                info!(
                    "completed auto revision compaction, request revision = {}, target revision = {}, retention = {}, took {:?}",
                    target_revision,
                    rev,
                    self.retention,
                    now.elapsed().as_secs()
                );
                Some(rev)
            }
            Err(err) => {
                warn!(
                    "failed auto revision compaction, revision = {}, retention = {}, result: {}",
                    target_revision, self.retention, err
                );
                None
            }
        }
    }
}

#[async_trait::async_trait]
impl<C: Compactable> Compactor<C> for RevisionCompactor<C> {
    fn pause(&self) {
        self.is_leader.store(false, Relaxed);
    }

    fn resume(&self) {
        self.is_leader.store(true, Relaxed);
    }

    #[allow(clippy::arithmetic_side_effects)]
    async fn run(&self) {
        let mut last_revision = None;
        let mut ticker = tokio::time::interval(CHECK_INTERVAL);
        let mut shutdown_listener = self.shutdown_listener.clone();
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(last_compacted_rev) = self.do_compact(last_revision).await {
                        last_revision = Some(last_compacted_rev);
                    }
                }
                _ = shutdown_listener.wait_self_shutdown() => {
                    break;
                }
            }
        }
    }

    async fn set_compactable(&self, compactable: C) {
        *self.compactable.write().await = Some(compactable);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::compact::MockCompactable;

    #[tokio::test]
    async fn revision_compactor_should_work_in_normal_path() {
        let mut compactable = MockCompactable::new();
        compactable.expect_compact().times(3).returning(Ok);
        let (_shutdown_trigger, shutdown_listener) = shutdown::channel();
        let revision_gen = Arc::new(RevisionNumberGenerator::new(110));
        let revision_compactor =
            RevisionCompactor::new_arc(true, Arc::clone(&revision_gen), shutdown_listener, 100);
        revision_compactor.set_compactable(compactable).await;
        // auto_compactor works successfully
        assert_eq!(revision_compactor.do_compact(None).await, Some(10));
        revision_gen.next(); // current revision: 111
        assert_eq!(revision_compactor.do_compact(Some(10)).await, Some(11));
        revision_compactor.pause();
        revision_gen.next(); // current revision 112
        assert!(revision_compactor.do_compact(Some(11)).await.is_none());
        revision_gen.next(); // current revision 113
        assert!(revision_compactor.do_compact(Some(11)).await.is_none());
        revision_compactor.resume();
        assert_eq!(revision_compactor.do_compact(Some(11)).await, Some(13));
        // auto compactor should skip those revisions which have been auto compacted.
        assert!(revision_compactor.do_compact(Some(13)).await.is_none());
    }
}
