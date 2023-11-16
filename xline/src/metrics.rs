use std::sync::OnceLock;

use clippy_utilities::Cast;
use opentelemetry::{
    metrics::{Counter, Meter, MetricsError},
    KeyValue,
};

/// Global metrics for curp server
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Meter for metrics
static METRICS_METER: OnceLock<Meter> = OnceLock::new();

/// Get the metrics
pub(super) fn get() -> &'static Metrics {
    METRICS.get_or_init(|| Metrics::new(meter()))
}

/// Get the metrics meter
fn meter() -> &'static Meter {
    METRICS_METER.get_or_init(|| {
        opentelemetry::global::meter_with_version(
            env!("CARGO_PKG_NAME"),
            Some(env!("CARGO_PKG_VERSION")),
            Some(env!("CARGO_PKG_REPOSITORY")),
            Some(vec![KeyValue::new("component", "xline")]),
        )
    })
}

/// Xline metrics
#[derive(Debug)]
pub(super) struct Metrics {
    /// The total number of pending read indexes not in sync with leader's or timed out read index requests.
    pub(super) slow_read_indexes_total: Counter<u64>,
    /// The total number of failed read indexes seen.
    pub(super) read_indexes_failed_total: Counter<u64>,
    /// The total number of expired leases.
    pub(super) lease_expired_total: Counter<u64>,
}

impl Metrics {
    /// Create xline metrics
    fn new(meter: &Meter) -> Self {
        Self {
            slow_read_indexes_total: meter
                .u64_counter("slow_read_index")
                .with_description("The total number of pending read indexes not in sync with leader's or timed out read index requests.")
                .init(),
            read_indexes_failed_total: meter
                .u64_counter("read_index_failed")
                .with_description("The total number of failed read indexes seen.")
                .init(),
            lease_expired_total: meter
                .u64_counter("lease_expired")
                .with_description("The total number of expired leases.")
                .init(),
        }
    }

    /// Register metrics
    pub(super) fn register_callback() -> Result<(), MetricsError> {
        let meter = meter();
        let (fd_used, fd_limit, current_version, current_rust_version) = (
            meter
                .u64_observable_gauge("fd_used")
                .with_description("The number of used file descriptors.")
                .init(),
            meter
                .u64_observable_gauge("fd_limit")
                .with_description("The file descriptor limit.")
                .init(),
            meter
                .u64_observable_gauge("current_version")
                .with_description("Which version is running. 1 for 'server_version' label with current version.")
                .init(),
            meter
                .u64_observable_gauge("current_rust_version")
                .with_description("Which Rust version server is running with. 1 for 'server_rust_version' label with current version.")
                .init(),
        );

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

            observer.observe_u64(&fd_used, used.cast(), &[]);
            observer.observe_u64(&fd_limit, limit, &[]);
        })?;

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

        Ok(())
    }
}
