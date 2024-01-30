use opentelemetry::metrics::{Counter, Histogram};
use utils::define_metrics;

define_metrics! {
    "curp_p2p",
    peer_sent_bytes_total: Counter<u64> = meter()
        .u64_counter("peer_sent_bytes_total")
        .with_description("The total number of bytes send to peers.")
        .init(),
    peer_sent_failures_total: Counter<u64> = meter()
        .u64_counter("peer_sent_failures_total")
        .with_description("The total number of send failures to peers.")
        .init(),
    peer_round_trip_time_seconds: Histogram<u64> = meter()
        .u64_histogram("peer_round_trip_time_seconds")
        .with_description("The round-trip-time histogram between peers.")
        .init()
}
