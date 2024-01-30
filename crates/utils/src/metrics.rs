/// Define some metrics
#[macro_export]
#[allow(clippy::module_name_repetitions)] // exported macros
macro_rules! define_metrics {
    ($com:expr, $($field:ident : $ki:ty = $init:expr),*) => {

        pub(crate) struct Metrics {
            $(pub(crate) $field: $ki),*,
        }

        impl Metrics {
            fn new() -> Self {
                Self {
                    $($field: $init),*
                }
            }
        }

        /// Global metrics for curp server
        static METRICS: std::sync::OnceLock<Metrics> = std::sync::OnceLock::new();

        /// Meter for metrics
        static METRICS_METER: std::sync::OnceLock<opentelemetry::metrics::Meter> = std::sync::OnceLock::new();

        /// Get the curp metrics
        pub(crate) fn get() -> &'static Metrics {
            METRICS.get_or_init(|| Metrics::new())
        }

        /// Get the curp metrics meter
        fn meter() -> &'static opentelemetry::metrics::Meter {
            METRICS_METER.get_or_init(|| {
                opentelemetry::global::meter_with_version(
                    env!("CARGO_PKG_NAME"),
                    Some(env!("CARGO_PKG_VERSION")),
                    Some(env!("CARGO_PKG_REPOSITORY")),
                    Some(vec![opentelemetry::KeyValue::new("component", $com)]),
                )
            })
        }
    };
}
