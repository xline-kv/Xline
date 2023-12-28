use std::{
    cmp::Ordering,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

use clippy_utilities::OverflowArithmetic;
use tokio::sync::RwLock;
use tracing::{info, warn};
use utils::task_manager::Listener;

use super::{Compactable, Compactor};
use crate::revision_number::RevisionNumberGenerator;

/// `RevisionWindow` is a ring buffer used to store periodically sampled revision.
struct RevisionWindow {
    /// inner ring buffer
    ring_buf: Vec<i64>,
    /// head index of the ring buffer
    cursor: usize,
    /// sample total amount
    retention: usize,
}

impl RevisionWindow {
    /// Create a new `RevisionWindow`
    fn new(retention: usize) -> Self {
        Self {
            ring_buf: vec![0; retention],
            cursor: retention.overflow_sub(1),
            retention,
        }
    }

    /// Store the revision into the inner ring buffer
    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
    fn sample(&mut self, revision: i64) {
        self.cursor = (self.cursor + 1) % self.retention; // it's ok to do so since cursor will never overflow
        self.ring_buf[self.cursor] = revision;
    }

    /// Retrieve the expired revision that is sampled period ago
    #[allow(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
    fn expired_revision(&self) -> Option<i64> {
        let target = self.ring_buf[(self.cursor + 1) % self.retention];
        if target == 0 {
            None
        } else {
            Some(target)
        }
    }
}

/// Revision auto compactor
#[derive(Debug)]
pub(crate) struct PeriodicCompactor<C: Compactable> {
    /// `is_leader` indicates whether the current node is a leader or not.
    is_leader: AtomicBool,
    /// curp client
    compactable: RwLock<Option<C>>,
    /// revision getter
    revision_getter: Arc<RevisionNumberGenerator>,
    /// compaction period
    period: Duration,
}

impl<C: Compactable> PeriodicCompactor<C> {
    /// Creates a new revision compactor
    pub(super) fn new_arc(
        is_leader: bool,
        revision_getter: Arc<RevisionNumberGenerator>,
        period: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            is_leader: AtomicBool::new(is_leader),
            compactable: RwLock::new(None),
            revision_getter,
            period,
        })
    }

    /// perform auto compaction logic
    async fn do_compact(
        &self,
        last_revision: Option<i64>,
        revision_window: &RevisionWindow,
    ) -> Option<i64> {
        if !self.is_leader.load(Relaxed) {
            return None;
        }
        let target_revision = revision_window.expired_revision();
        if target_revision == last_revision {
            return None;
        }
        let revision =
            target_revision.unwrap_or_else(|| unreachable!("target revision shouldn't be None"));
        let now = Instant::now();
        info!(
            "starting auto periodic compaction, revision = {}, period = {:?}",
            revision, self.period
        );

        let Some(ref compactable) = *self.compactable.read().await else {
            return None;
        };

        match compactable.compact(revision).await {
            Ok(rev) => {
                info!(
                    "completed auto revision compaction, request revision = {}, target revision = {}, period = {:?}, took {:?}",
                    revision,
                    rev,
                    self.period,
                    now.elapsed().as_secs()
                );
                Some(rev)
            }
            Err(err) => {
                warn!(
                    "failed auto revision compaction, revision = {}, period = {:?}, err: {}",
                    revision, self.period, err
                );
                None
            }
        }
    }
}

/// Calculate the sample frequency and the total amount of samples.
fn sample_config(period: Duration) -> (Duration, usize) {
    /// one hour duration
    const ONEHOUR: Duration = Duration::from_secs(3600);
    let base_interval = match period.cmp(&ONEHOUR) {
        Ordering::Less => period,
        Ordering::Equal | Ordering::Greater => ONEHOUR,
    };
    let divisor = 10;
    let check_interval = base_interval
        .checked_div(divisor)
        .unwrap_or_else(|| unreachable!("duration divisor should not be 0"));
    let check_interval_secs = check_interval.as_secs();
    let periodic_secs = period.as_secs();
    let length = periodic_secs
        .overflow_div(check_interval_secs)
        .overflow_add(1);
    let retention =
        usize::try_from(length).unwrap_or_else(|e| panic!("auto compact period is too large: {e}"));
    (check_interval, retention)
}

#[async_trait::async_trait]
impl<C: Compactable> Compactor<C> for PeriodicCompactor<C> {
    #[allow(clippy::arithmetic_side_effects)]
    async fn run(&self, shutdown_listener: Listener) {
        let mut last_revision: Option<i64> = None;
        let (sample_frequency, sample_total) = sample_config(self.period);
        let mut ticker = tokio::time::interval(sample_frequency);
        let mut revision_window = RevisionWindow::new(sample_total);
        loop {
            revision_window.sample(self.revision_getter.get());
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(last_compacted_rev) = self.do_compact(last_revision, &revision_window).await {
                        last_revision = Some(last_compacted_rev);
                    }
                }
                _ = shutdown_listener.wait() => break,
            }
        }
    }

    async fn set_compactable(&self, compactable: C) {
        *self.compactable.write().await = Some(compactable);
    }

    fn pause(&self) {
        self.is_leader.store(false, Relaxed);
    }

    fn resume(&self) {
        self.is_leader.store(true, Relaxed);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::compact::MockCompactable;

    #[test]
    fn revision_window_should_work() {
        let mut rw = RevisionWindow::new(3);
        assert!(rw.expired_revision().is_none());
        rw.sample(1);
        assert!(rw.expired_revision().is_none());
        rw.sample(2);
        assert!(rw.expired_revision().is_none());
        rw.sample(3);
        assert_eq!(rw.expired_revision(), Some(1));
        // retention is 2
        // The first 3 minutes: 1,2,3
        // The second 2 minutes: 3,4
        rw.sample(3);
        rw.sample(4);
        assert_eq!(rw.expired_revision(), Some(3));
    }

    #[test]
    fn sample_config_should_success() {
        // period is 59 minutes, less than one hour
        let (interval, retention) = sample_config(Duration::from_secs(59 * 60));
        assert_eq!(interval, Duration::from_secs(354));
        assert_eq!(retention, 11);

        // period is 60 minutes, equal to one hour
        let (interval, retention) = sample_config(Duration::from_secs(60 * 60));
        assert_eq!(interval, Duration::from_secs(6 * 60));
        assert_eq!(retention, 11);

        // period is 24 hours, lager than one hour
        let (interval, retention) = sample_config(Duration::from_secs(24 * 60 * 60));
        assert_eq!(interval, Duration::from_secs(6 * 60));
        assert_eq!(retention, 241);
    }

    #[tokio::test]
    async fn periodic_compactor_should_work_in_normal_path() {
        let mut revision_window = RevisionWindow::new(11);
        // revision_window: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        for revision in 1..=11 {
            revision_window.sample(revision);
        }
        let mut compactable = MockCompactable::new();
        compactable.expect_compact().times(3).returning(Ok);
        let revision_gen = Arc::new(RevisionNumberGenerator::new(1));
        let periodic_compactor =
            PeriodicCompactor::new_arc(true, revision_gen, Duration::from_secs(10));
        periodic_compactor.set_compactable(compactable).await;
        // auto_compactor works successfully
        assert_eq!(
            periodic_compactor.do_compact(None, &revision_window).await,
            Some(1)
        );
        revision_window.sample(12);
        assert_eq!(
            periodic_compactor
                .do_compact(Some(1), &revision_window)
                .await,
            Some(2)
        );
        periodic_compactor.pause();
        revision_window.sample(13);
        assert!(periodic_compactor
            .do_compact(Some(2), &revision_window)
            .await
            .is_none());
        revision_window.sample(14);
        assert!(periodic_compactor
            .do_compact(Some(2), &revision_window)
            .await
            .is_none());
        periodic_compactor.resume();
        // revision_window: [12, 13, 14, 4, 5, 6, 7, 8, 9, 10, 11]
        assert_eq!(
            periodic_compactor
                .do_compact(Some(3), &revision_window)
                .await,
            Some(4)
        );

        // auto compactor should skip those revisions which have been auto compacted.
        assert!(periodic_compactor
            .do_compact(Some(4), &revision_window)
            .await
            .is_none());
    }
}
