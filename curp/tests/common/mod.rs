#![allow(dead_code, unused)]

use std::{
    io,
    {env, time::Duration},
};

use thiserror::Error;

pub mod curp_group;
pub mod test_cmd;

pub fn init_logger() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "curp,server");
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

pub async fn sleep_millis(n: u64) {
    tokio::time::sleep(Duration::from_millis(n)).await;
}

pub async fn sleep_secs(n: u64) {
    tokio::time::sleep(Duration::from_secs(n)).await;
}
