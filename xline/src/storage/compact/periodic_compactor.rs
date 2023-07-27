use std::{
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
    cmp::Ordering,
};

use event_listener::Event;
use tracing::{info, warn};
use clippy_utilities::OverflowArithmetic;

use super::{Compactable, Compactor};
use crate::revision_number::RevisionNumberGenerator;


/// `RevisionWindow` is a ring buffer used to store periodically sampled revision.
struct RevisionWindow {
    /// inner ring buffer
    ring_buf: Vec<i64>,
    /// head index of the ring buffer
    cursor: usize,
    /// sample total amount
    retention: usize
}

impl RevisionWindow {
    /// Create a new `RevisionWindow`
    fn new(retention: usize) -> Self {
        Self {
            ring_buf: Vec::with_capacity(retention),
            cursor: retention.overflow_sub(1),
            retention,
        }
    }

    /// Store the revision into the inner ring buffer
    #[allow(clippy::integer_arithmetic)]
    fn sample(&mut self, revision: i64) {
        self.cursor = (self.cursor + 1) % self.retention;  // it's ok to do so since cursor will never overflow
        match self.ring_buf.len().cmp(&self.retention) {
            Ordering::Less => {self.ring_buf.push(revision)},
            Ordering::Equal => {
                if let Some(element) = self.ring_buf.get_mut(self.cursor){
                    *element = revision;
                } else {
                    unreachable!("ring_buf ({:?}) at {} should not be None", self.ring_buf, self.cursor);
                }
            },
            Ordering::Greater => {unreachable!("the length of RevisionWindow should be less than {}", self.retention)}
        }
    }

    /// Retrieve the expired revision that is sampled period ago
    #[allow(clippy::indexing_slicing, clippy::integer_arithmetic)]
    fn expired_revision(&self) -> Option<i64> {
        debug_assert!(self.ring_buf.len() <= self.retention, "the length of RevisionWindow should be less than {}", self.retention);
        if self.ring_buf.len() < self.retention {
            None
        } else {
            let target = (self.cursor + 1) % self.retention;
            Some(self.ring_buf[target]) // it's ok to do so since ring_buf[target] should not be None.
        }
    }
}

/// Revision auto compactor
#[derive(Debug)]
pub(crate) struct PeriodicCompactor {
    /// `is_leader` indicates whether the current node is a leader or not.
    is_leader: AtomicBool,
    /// curp client
    client: Arc<dyn Compactable>,
    /// revision getter
    revision_getter: Arc<RevisionNumberGenerator>,
    /// shutdown trigger
    shutdown_trigger: Arc<Event>,
    /// compaction period
    period: Duration,
}

impl PeriodicCompactor{
    #[allow(dead_code)]
    /// Creates a new revision compactor
    pub(super) fn new_arc(
        is_leader: bool,
        client: Arc<dyn Compactable + Send + Sync>,
        revision_getter: Arc<RevisionNumberGenerator>,
        shutdown_trigger: Arc<Event>,
        period: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            is_leader: AtomicBool::new(is_leader),
            client,
            revision_getter,
            shutdown_trigger,
            period,
        })
    }
}

/// Calculate the sample frequency and the total amount of samples.
fn sample_config(period: Duration) -> (Duration, usize) {
    let one_hour = Duration::from_secs(60.overflow_mul(60));
    let base_interval = match period.cmp(&one_hour) {
        Ordering::Less => period,
        Ordering::Equal | Ordering::Greater => one_hour,
    };
    let divisor = 10;
    let check_interval = base_interval.checked_div(divisor).unwrap_or_else(|| {unreachable!("duration divisor should not be 0")});
    let check_interval_secs = check_interval.as_secs();
    let periodic_secs = period.as_secs();
    let length = periodic_secs.overflow_div(check_interval_secs).overflow_add(1);
    let retention = usize::try_from(length).unwrap_or_else(|e| {panic!("auto compact period is too large: {e}")});
    (check_interval, retention)
}

#[async_trait::async_trait]
impl Compactor for PeriodicCompactor {
    fn pause(&self) {
        self.is_leader.store(false, Relaxed);
    }

    fn resume(&self) {
        self.is_leader.store(true, Relaxed);
    }

    #[allow(clippy::integer_arithmetic)]
    async fn run(&self) {
        let mut last_revision = None;
        let shutdown_trigger = self.shutdown_trigger.listen();
        let (sample_frequency, sample_total) = sample_config(self.period);
        let mut ticker = tokio::time::interval(sample_frequency);
        let mut revision_window = RevisionWindow::new(sample_total);
        tokio::pin!(shutdown_trigger);
        loop {
            revision_window.sample(self.revision_getter.get());
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

            let target_revision = revision_window.expired_revision();
            if target_revision != last_revision {
                let revision = target_revision.unwrap_or_else(|| {unreachable!("target revision shouldn't be None")});
                let now = Instant::now();
                info!(
                    "starting auto periodic compaction, revision = {}, period = {:?}",
                    revision, self.period
                );
                // TODO: add more error processing logic
                if let Err(e) = self.client.compact(revision).await {
                    warn!(
                        "failed auto revision compaction, revision = {}, period = {:?}, error: {:?}",
                        revision, self.period, e
                    );
                } else {
                    info!(
                        "completed auto revision compaction, revision = {}, period = {:?}, took {:?}",
                        revision,
                        self.period,
                        now.elapsed().as_secs()
                    );
                    last_revision = Some(revision);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn revision_window_should_work() {
        let mut rw = RevisionWindow::new(3);
        assert!(rw.expired_revision().is_none());
        rw.sample(0);
        assert!(rw.expired_revision().is_none());
        rw.sample(1);
        assert!(rw.expired_revision().is_none());
        rw.sample(2);
        assert_eq!(rw.expired_revision(), Some(0));
        // retention is 2
        // The first 3 minutes: 0,1,2
        // The second 2 minutes: 3,4
        rw.sample(3);
        rw.sample(4);
        assert_eq!(rw.expired_revision(), Some(2));
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
}
