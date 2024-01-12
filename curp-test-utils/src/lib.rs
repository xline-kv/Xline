use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use curp_external_api::role_change::RoleChange;
use tracing_subscriber::fmt::time::uptime;

pub mod test_cmd;

pub const TEST_TABLE: &str = "test";
pub const TEST_CLIENT_ID: u64 = 12345;
pub const REVISION_TABLE: &str = "revision";
pub const META_TABLE: &str = "meta";

#[derive(Default, Debug, Clone)]
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
        env::set_var("RUST_LOG", "curp=debug,xline=debug");
    }
    _ = tracing_subscriber::fmt()
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
