use std::sync::{Arc, OnceLock};

use clippy_utilities::NumericCast;
use curp_external_api::{cmd::Command, role_change::RoleChange};
use opentelemetry::{
    global::meter_with_version,
    metrics::{Counter, Meter, MetricsError, ObservableGauge},
    KeyValue,
};

use super::raw_curp::RawCurp;

/// Global metrics for curp server
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Meter for metrics
static METRICS_METER: OnceLock<Meter> = OnceLock::new();

/// Get the curp metrics
pub(super) fn get() -> &'static Metrics {
    METRICS.get_or_init(|| {        
        Metrics::new(get_meter())
    })
}

/// Get the curp metrics meter
pub(super) fn get_meter() -> &'static Meter {
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
    // /// The total number of pending read indexes not in sync with leader's or timed out read index requests.
    // pub(super) slow_read_index: Counter<u64>,
    // /// The total number of failed read indexes seen.
    // pub(super) read_index_failed: Counter<u64>,
    // /// The total number of expired leases.
    // pub(super) lease_expired: Counter<u64>,
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
            // slow_read_index: meter
            //     .u64_counter("slow_read_index")
            //     .with_description("The total number of pending read indexes not in sync with leader's or timed out read index requests.")
            //     .init(),
            // read_index_failed: meter
            //     .u64_counter("read_index_failed")
            //     .with_description("The total number of failed read indexes seen.")
            //     .init(),
            // lease_expired: meter
            //     .u64_counter("lease_expired")
            //     .with_description("The total number of expired leases.")
            //     .init(),
        }
    }

    /// Register observable instruments
    pub(super) fn register_callback<C: Command + 'static, RC: RoleChange + 'static>(
        meter: &Meter,
        curp: Arc<RawCurp<C, RC>>,
    ) -> Result<(), MetricsError> {
        let (
            has_leader,
            is_leader,
            is_learner,
            current_version,
            current_rust_version,
            server_id,
            fd_used,
            fd_limit,
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
                .u64_observable_gauge("current_version")
                .with_description("Which version is running. 1 for 'server_version' label with current version.")
                .init(),
            meter
                .u64_observable_gauge("current_rust_version")
                .with_description("Which Rust version server is running with. 1 for 'server_rust_version' label with current version.")
                .init(),
            meter
                .u64_observable_gauge("server_id")
                .with_description("Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID.")
                .init(),
            meter
                .u64_observable_gauge("fd_used")
                .with_description("The number of used file descriptors.")
                .init(),
            meter
                .u64_observable_gauge("fd_limit")
                .with_description("The file descriptor limit.")
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

        _ = meter.register_callback(
            &[current_version.as_any(), current_rust_version.as_any()],
            move |observer| {
                let crate_version: &str = env!("CARGO_PKG_VERSION");
                let rust_version: &str = env!("CARGO_PKG_RUST_VERSION");
                observer.observe_u64(
                    &current_version,
                    1,
                    &[KeyValue::new("server_version", crate_version)],
                );
                observer.observe_u64(
                    &current_rust_version,
                    1,
                    &[KeyValue::new("server_rust_version", rust_version)],
                );
            },
        )?;

        _ = meter.register_callback(&[fd_used.as_any(), fd_limit.as_any()], move |observer| {
            #[allow(unsafe_code)] // we need this
            #[allow(clippy::multiple_unsafe_ops_per_block)] // Is it bad?
            let (used, limit) = unsafe {
                let mut rlimit = nix::libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                let _ig = nix::libc::getrlimit(nix::libc::RLIMIT_NOFILE, &mut rlimit);
                (nix::libc::getdtablesize(), rlimit.rlim_cur)
            };

            observer.observe_u64(&fd_used, used.numeric_cast(), &[]);
            observer.observe_u64(&fd_limit, limit, &[]);
        })?;

        Ok(())
    }
}
