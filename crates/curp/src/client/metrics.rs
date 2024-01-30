use opentelemetry::metrics::Counter;
use utils::define_metrics;

define_metrics! {
    "curp_client",
    client_retry_count: Counter<u64> = meter()
        .u64_counter("client_retry_count")
        .with_description("The total number of retries when the client propose to the cluster.")
        .init(),
    client_fast_path_count: Counter<u64> = meter()
        .u64_counter("client_fast_path_count")
        .with_description("The total number of fast path when the client propose to the cluster.")
        .init(),
    client_slow_path_count: Counter<u64> = meter()
        .u64_counter("client_slow_path_count")
        .with_description("The total number of slow path when the client propose to the cluster.")
        .init(),
    client_fast_path_fallback_slow_path_count: Counter<u64> = meter()
        .u64_counter("client_fast_path_fallback_slow_path_count")
        .with_description("The total number of fast path fallbacks into slow path when the client propose to the cluster.")
        .init()
}
