use std::sync::Arc;

use clippy_utilities::NumericCast;
use curp_external_api::{cmd::Command, role_change::RoleChange};
use opentelemetry::metrics::{Counter, Histogram, MetricsError, ObservableGauge};
use utils::define_metrics;

use super::raw_curp::RawCurp;

define_metrics! {
    "curp_server",
    leader_changes: Counter<u64> = meter()
        .u64_counter("leader_changes")
        .with_description("The number of leader changes seen.")
        .init(),
    learner_promote_failed: Counter<u64> = meter()
        .u64_counter("learner_promote_failed")
        .with_description("The total number of failed learner promotions (likely learner not ready) while this member is leader.")
        .init(),
    learner_promote_succeed: Counter<u64> = meter()
        .u64_counter("learner_promote_succeed")
        .with_description("The total number of successful learner promotions while this member is leader.")
        .init(),
    heartbeat_send_failures: Counter<u64> = meter()
        .u64_counter("heartbeat_send_failures")
        .with_description("The total number of leader heartbeat send failures (likely overloaded from slow disk).")
        .init(),
    apply_snapshot_in_progress: ObservableGauge<u64> = meter()
        .u64_observable_gauge("apply_snapshot_in_progress")
        .with_description("1 if the server is applying the incoming snapshot. 0 if none.")
        .init(),
    proposals_committed: ObservableGauge<u64> = meter()
        .u64_observable_gauge("proposals_committed")
        .with_description("The total number of consensus proposals committed.")
        .init(),
    proposals_failed: Counter<u64> = meter()
        .u64_counter("proposals_failed")
        .with_description("The total number of failed proposals seen.")
        .init(),
    proposals_applied: ObservableGauge<u64> = meter()
        .u64_observable_gauge("proposals_applied")
        .with_description("The total number of consensus proposals applied.")
        .init(),
    proposals_pending: ObservableGauge<u64> = meter()
        .u64_observable_gauge("proposals_pending")
        .with_description("The current number of pending proposals to commit.")
        .init(),
    snapshot_install_total_duration_seconds: Histogram<u64> = meter()
        .u64_histogram("snapshot_install_total_duration_seconds")
        .with_description("The total latency distributions of save called by install_snapshot.")
        .init(),
    client_id_revokes: Counter<u64> = meter()
        .u64_counter("client_id_renews")
        .with_description("The total number of client id revokes times.")
        .init()
}

impl Metrics {
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
            online_clients,
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
            meter
                .u64_observable_gauge("online_clients")
                .with_description("The online client ids count of this server if it is the leader")
                .init(),
        );

        _ = meter.register_callback(
            &[
                has_leader.as_any(),
                is_leader.as_any(),
                is_learner.as_any(),
                server_id.as_any(),
                sp_total.as_any(),
                online_clients.as_any(),
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

                let client_ids = curp.lease_manager().read().expiry_queue.len();
                observer.observe_u64(&online_clients, client_ids.numeric_cast(), &[]);
            },
        )?;

        Ok(())
    }
}
