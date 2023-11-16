use std::sync::{Arc, OnceLock};

use clippy_utilities::NumericCast;
use curp_external_api::{cmd::Command, role_change::RoleChange};
use opentelemetry::{
    global::meter_with_version,
    metrics::{Counter, Histogram, Meter, MetricsError, ObservableGauge},
    KeyValue,
};

use super::raw_curp::RawCurp;

/// Global metrics for curp server
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Meter for metrics
static METRICS_METER: OnceLock<Meter> = OnceLock::new();

/// Get the curp metrics
pub(super) fn get() -> &'static Metrics {
    METRICS.get_or_init(|| Metrics::new(meter()))
}

/// Get the curp metrics meter
fn meter() -> &'static Meter {
    METRICS_METER.get_or_init(|| {
        meter_with_version(
            env!("CARGO_PKG_NAME"),
            Some(env!("CARGO_PKG_VERSION")),
            Some(env!("CARGO_PKG_REPOSITORY")),
            Some(vec![KeyValue::new("component", "curp")]),
        )
    })
}

/// All metrics exported from curp
/// Borrowed from etcd
#[derive(Debug)]
pub(super) struct Metrics {
    /// The number of leader changes seen.
    pub(super) leader_changes: Counter<u64>,
    /// The total number of failed learner promotions (likely learner not ready) while this member is leader.
    pub(super) learner_promote_failed: Counter<u64>,
    /// The total number of successful learner promotions while this member is leader.
    pub(super) learner_promote_succeed: Counter<u64>,
    /// The total number of leader heartbeat send failures (likely overloaded from slow disk).
    pub(super) heartbeat_send_failures: Counter<u64>,
    /// 1 if the server is applying the incoming snapshot. 0 if none.
    pub(super) apply_snapshot_in_progress: ObservableGauge<u64>,
    /// The total number of consensus proposals committed.
    pub(super) proposals_committed: ObservableGauge<u64>,
    /// The total number of failed proposals seen.
    pub(super) proposals_failed: Counter<u64>,
    /// The total number of consensus proposals applied.
    pub(super) proposals_applied: ObservableGauge<u64>,
    /// The current number of pending proposals to commit.
    pub(super) proposals_pending: ObservableGauge<u64>,
    /// The total latency distributions of save called by install_snapshot.
    pub(super) snapshot_install_total_duration_seconds: Histogram<u64>,
    // /// The total number of bytes send to peers.
    // pub(super) peer_sent_bytes_total: Counter<u64>,
    // /// The total number of bytes received from peers.
    // pub(super) peer_received_bytes_total: Counter<u64>,
    // /// The total number of send failures to peers.
    // pub(super) peer_sent_failures_total: Counter<u64>
    // /// The total number of receive failures from peers.
    // pub(super) peer_received_failures_total: Counter<u64>
    // /// The round-trip-time histogram between peers.
    // pub(super) peer_round_trip_time_seconds: Histogram<u64>
}

impl Metrics {
    /// Create a new `CurpMetrics` from meter
    fn new(meter: &Meter) -> Self {
        Self {
            leader_changes: meter
                .u64_counter("leader_changes")
                .with_description("The number of leader changes seen.")
                .init(),
            learner_promote_failed: meter
                .u64_counter("learner_promote_failed")
                .with_description("The total number of failed learner promotions (likely learner not ready) while this member is leader.")
                .init(),
            learner_promote_succeed: meter
                .u64_counter("learner_promote_succeed")
                .with_description("The total number of successful learner promotions while this member is leader.")
                .init(),
            heartbeat_send_failures: meter
                .u64_counter("heartbeat_send_failures")
                .with_description("The total number of leader heartbeat send failures (likely overloaded from slow disk).")
                .init(),
            apply_snapshot_in_progress: meter
                .u64_observable_gauge("apply_snapshot_in_progress")
                .with_description("1 if the server is applying the incoming snapshot. 0 if none.")
                .init(),
            proposals_committed: meter
                .u64_observable_gauge("proposals_committed")
                .with_description("The total number of consensus proposals committed.")
                .init(),
            proposals_applied: meter
                .u64_observable_gauge("proposals_applied")
                .with_description("The total number of consensus proposals applied.")
                .init(),
            proposals_pending: meter
                .u64_observable_gauge("proposals_pending")
                .with_description("The current number of pending proposals to commit.")
                .init(),
            proposals_failed: meter
                .u64_counter("proposals_failed")
                .with_description("The total number of failed proposals seen.")
                .init(),
            snapshot_install_total_duration_seconds: meter
                .u64_histogram("snapshot_install_total_duration_seconds")
                .with_description("The total latency distributions of save called by install_snapshot.")
                .init(),
        }
    }

    /// Register observable instruments
    pub(super) fn register_callback<C: Command + 'static, RC: RoleChange + 'static>(
        curp: Arc<RawCurp<C, RC>>,
    ) -> Result<(), MetricsError> {
        let meter = meter();
        let (
            has_leader,
            is_leader,
            is_learner,
            server_id,
            sp_total,
        ) = (
            meter
                .u64_observable_gauge("has_leader")
                .with_description("Whether or not a leader exists. 1 is existence, 0 is not.")
                .init(),
            meter
                .u64_observable_gauge("is_leader")
                .with_description("Whether or not this member is a leader. 1 if is, 0 otherwise.")
                .init(),
            meter
                .u64_observable_gauge("is_learner")
                .with_description("Whether or not this member is a learner. 1 if is, 0 otherwise.")
                .init(),
            meter
                .u64_observable_gauge("server_id")
                .with_description("Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID.")
                .init(),
            meter
                .u64_observable_gauge("sp_total")
                .with_description("The speculative pool size of this server")
                .init(),
        );

        _ = meter.register_callback(
            &[
                has_leader.as_any(),
                is_leader.as_any(),
                is_learner.as_any(),
                server_id.as_any(),
                sp_total.as_any(),
            ],
            move |observer| {
                let (leader_id, _, leader) = curp.leader();
                observer.observe_u64(&has_leader, leader_id.map_or(0, |_| 1), &[]);
                observer.observe_u64(&is_leader, u64::from(leader), &[]);

                let learner = curp.cluster().self_member().is_learner();
                let id = curp.cluster().self_id();
                observer.observe_u64(&is_learner, u64::from(learner), &[]);
                observer.observe_u64(&server_id, id, &[]);

                let sp_size = curp.spec_pool().lock().pool.len();
                observer.observe_u64(&sp_total, sp_size.numeric_cast(), &[]);
            },
        )?;

        Ok(())
    }
}
