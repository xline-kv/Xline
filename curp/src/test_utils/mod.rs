#![allow(
    clippy::all,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::integer_arithmetic,
    clippy::str_to_string,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::shadow_unrelated,
    clippy::pedantic,
    dead_code,
    unused_results
)]

use std::time::Duration;

use madsim::rand::{distributions::Alphanumeric, thread_rng, Rng};

pub(crate) mod test_cmd;

pub(crate) async fn sleep_millis(n: u64) {
    tokio::time::sleep(Duration::from_millis(n)).await;
}

pub(crate) async fn sleep_secs(n: u64) {
    tokio::time::sleep(Duration::from_secs(n)).await;
}

pub(crate) fn random_id() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}
