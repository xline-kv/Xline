use std::{
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

use clippy_utilities::OverflowArithmetic;
use tracing::{info, warn};
use utils::shutdown;

use super::{Compactable, Compactor};
use crate::{revision_number::RevisionNumberGenerator, storage::ExecuteError};

/// check for the need of compaction every 5 minutes
const CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Revision auto compactor
#[derive(Debug)]
pub(crate) struct RevisionCompactor<C: Compactable> {
    /// `is_leader` indicates whether the current node is a leader or not.
    is_leader: AtomicBool,
    /// curp client
    client: Arc<C>,
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
        client: Arc<C>,
        revision_getter: Arc<RevisionNumberGenerator>,
        shutdown_listener: shutdown::Listener,
        retention: i64,
    ) -> Arc<Self> {
        Arc::new(Self {
            is_leader: AtomicBool::new(is_leader),
            client,
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
        if let Err(e) = self.client.compact(target_revision).await {
            if let ExecuteError::RevisionCompacted(_rev, compacted_rev) = e {
                info!(
                    "required revision {} has been compacted, the current compacted revision is {},  retention = {:?}",
                    target_revision,
                    compacted_rev,
                    self.retention,
                );
                Some(compacted_rev)
            } else {
                warn!(
                    "failed auto revision compaction, revision = {}, retention = {}, error: {:?}",
                    target_revision, self.retention, e
                );
                None
            }
        } else {
            info!(
                "completed auto revision compaction, revision = {}, retention = {}, took {:?}",
                target_revision,
                self.retention,
                now.elapsed().as_secs()
            );
            Some(target_revision)
        }
    }
}

#[async_trait::async_trait]
impl<C: Compactable> Compactor for RevisionCompactor<C> {
    fn pause(&self) {
        self.is_leader.store(false, Relaxed);
    }

    fn resume(&self) {
        self.is_leader.store(true, Relaxed);
    }

    #[allow(clippy::integer_arithmetic)]
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
                // To ensure that each iteration invokes the same `shutdown_trigger` and keeps
                // events losing due to the cancellation of `shutdown_trigger` at bay.
                _ = shutdown_listener.wait_self_shutdown() => {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::compact::MockCompactable;

    #[tokio::test]
    async fn revision_compactor_should_work_in_normal_path() {
        let mut compactable = MockCompactable::new();
        compactable.expect_compact().times(3).returning(|_| Ok(()));
        let (_shutdown_trigger, shutdown_listener) = shutdown::channel();
        let revision_gen = Arc::new(RevisionNumberGenerator::new(110));
        let revision_compactor = RevisionCompactor::new_arc(
            true,
            Arc::new(compactable),
            Arc::clone(&revision_gen),
            shutdown_listener,
            100,
        );
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
