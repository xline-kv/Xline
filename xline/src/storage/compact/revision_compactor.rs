use std::{
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

use clippy_utilities::OverflowArithmetic;
use event_listener::Event;
use tracing::{info, warn};

use super::{Compactable, Compactor};
use crate::revision_number::RevisionNumberGenerator;

/// check for the need of compaction every 5 minutes
const CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Revision auto compactor
#[derive(Debug)]
pub(crate) struct RevisionCompactor {
    /// `is_leader` indicates whether the current node is a leader or not.
    is_leader: AtomicBool,
    /// curp client
    client: Arc<dyn Compactable>,
    /// revision getter
    revision_getter: Arc<RevisionNumberGenerator>,
    /// shutdown trigger
    shutdown_trigger: Arc<Event>,
    /// revision retention
    retention: i64,
}

impl RevisionCompactor{
    /// Creates a new revision compactor
    pub(super) fn new_arc(
        is_leader: bool,
        client: Arc<dyn Compactable + Send + Sync>,
        revision_getter: Arc<RevisionNumberGenerator>,
        shutdown_trigger: Arc<Event>,
        retention: i64,
    ) -> Arc<Self> {
        Arc::new(Self {
            is_leader: AtomicBool::new(is_leader),
            client,
            revision_getter,
            shutdown_trigger,
            retention,
        })
    }
}

#[async_trait::async_trait]
impl Compactor for RevisionCompactor {
    fn pause(&self) {
        self.is_leader.store(false, Relaxed);
    }

    fn resume(&self) {
        self.is_leader.store(true, Relaxed);
    }

    #[allow(clippy::integer_arithmetic)]
    async fn run(&self) {
        let prev = 0;
        let shutdown_trigger = self.shutdown_trigger.listen();
        let mut ticker = tokio::time::interval(CHECK_INTERVAL);
        tokio::pin!(shutdown_trigger);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !self.is_leader.load(Relaxed) {
                        continue;
                    }
                }
                // To ensure that each iteration invokes the same `shutdown_trigger` and keeps
                // events losing due to the cancellation of `shutdown_trigger` at bay.
                _ = &mut shutdown_trigger => {
                    break;
                }
            }

            let target_revision = self.revision_getter.get().overflow_sub(self.retention);
            if target_revision <= 0 || target_revision == prev {
                continue;
            }

            let now = Instant::now();
            info!(
                "starting auto revision compaction, revision = {}, retention = {}",
                target_revision, self.retention
            );
            // TODO: add more error processing logic
            if let Err(e) = self.client.compact(target_revision).await {
                warn!(
                    "failed auto revision compaction, revision = {}, retention = {}, error: {:?}",
                    target_revision, self.retention, e
                );
            } else {
                info!(
                    "completed auto revision compaction, revision = {}, retention = {}, took {:?}",
                    target_revision,
                    self.retention,
                    now.elapsed().as_secs()
                );
            }
        }
    }
}
