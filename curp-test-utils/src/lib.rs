#![allow(dead_code, unused)]

use curp_external_api::role_change::RoleChange;
use madsim::rand::{distributions::Alphanumeric, thread_rng, Rng};
use parking_lot::RwLock;
use std::{
    env, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tracing_subscriber::fmt::time::{uptime, OffsetTime, Uptime};
use utils::parking_lot_lock::RwLockMap;

pub mod test_cmd;

pub const TEST_TABLE: &str = "test";
pub const REVISION_TABLE: &str = "revision";

#[derive(Default, Debug)]
pub struct TestRoleChange {
    pub inner: Arc<TestRoleChangeInner>,
}

#[derive(Default, Debug)]
pub struct TestRoleChangeInner {
    is_leader: AtomicBool,
}

impl TestRoleChange {
    pub fn get_inner_arc(&self) -> Arc<TestRoleChangeInner> {
        Arc::clone(&self.inner)
    }
}

impl RoleChange for TestRoleChange {
    fn on_calibrate(&self) {
        self.inner.is_leader.store(false, Ordering::Relaxed);
    }

    fn on_election_win(&self) {
        self.inner.is_leader.store(true, Ordering::Relaxed);
    }
}

impl TestRoleChangeInner {
    pub fn get_is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
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

pub fn mock_role_change() -> TestRoleChange {
    TestRoleChange::default()
}

pub async fn sleep_millis(n: u64) {
    tokio::time::sleep(Duration::from_millis(n)).await;
}

pub async fn sleep_secs(n: u64) {
    tokio::time::sleep(Duration::from_secs(n)).await;
}

pub fn random_id() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}
