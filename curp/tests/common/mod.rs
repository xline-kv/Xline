#![allow(dead_code, unused)]

use std::{
    env, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use curp::leader_change::LeaderChange;
use madsim::rand::{distributions::Alphanumeric, thread_rng, Rng};
use parking_lot::RwLock;
use thiserror::Error;
use tracing_subscriber::fmt::time::{uptime, OffsetTime, Uptime};
use utils::parking_lot_lock::RwLockMap;

pub mod curp_group;
pub mod test_cmd;

pub const TEST_TABLE: &str = "test";
pub const REVISION_TABLE: &str = "revision";

#[derive(Default, Debug)]
pub struct TestLeaderChange {
    inner: Arc<RwLock<LeaderInfo>>,
}

#[derive(Default, Debug)]
pub struct LeaderInfo {
    pub leader_id: Option<String>,
    pub is_leader: bool,
}

impl TestLeaderChange {
    pub(super) fn get_inner(&self) -> Arc<RwLock<LeaderInfo>> {
        Arc::clone(&self.inner)
    }
}

impl LeaderChange for TestLeaderChange {
    fn on_follower(&self) {
        self.inner.map_write(|mut inner| inner.is_leader = false);
    }

    fn on_leader(&self) {
        self.inner.map_write(|mut inner| inner.is_leader = true);
    }
}

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
