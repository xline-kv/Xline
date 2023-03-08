#![allow(dead_code, unused)]

use std::{env, io, time::Duration};

use madsim::rand::{distributions::Alphanumeric, thread_rng, Rng};
use thiserror::Error;
use tracing_subscriber::fmt::time::{uptime, OffsetTime, Uptime};

pub mod curp_group;
pub mod test_cmd;

pub fn init_logger() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "curp,server");
    }
    tracing_subscriber::fmt()
        .with_timer(uptime())
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

pub async fn sleep_millis(n: u64) {
    tokio::time::sleep(Duration::from_millis(n)).await;
}

pub async fn sleep_secs(n: u64) {
    tokio::time::sleep(Duration::from_secs(n)).await;
}

pub(crate) fn random_id() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}
