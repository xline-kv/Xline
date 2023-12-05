use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use curp_test_utils::test_cmd::TestCommand;
use rand::{random, thread_rng, Rng};
use tracing::info;

use crate::server::storage::wal::tests::FrameGenerator;

use super::*;

const LAZYFS_FIFO_PATH: &str = "/tmp/faults.fifo";

const LAZYFS_MOUTN_POINT: &str = "/tmp/lazyfs.mnt";

const TEST_SEGMENT_SIZE: u64 = 512 * 1024;

struct LazyFSController {}

impl LazyFSController {
    async fn clear_cache() {
        Self::write_fifo("lazyfs::clear-cache").await;
    }

    async fn checkpoint() {
        Self::write_fifo("lazyfs::cache-checkpoint").await;
    }

    async fn usage() {
        Self::write_fifo("lazyfs::display-cache-usage").await;
    }

    async fn report() {
        Self::write_fifo("lazyfs::unsynced-data-report").await;
    }

    async fn write_fifo(data: &str) {
        tokio::fs::write(LAZYFS_FIFO_PATH, format!("{data}\n"))
            .await
            .unwrap();
    }
}

async fn start_sending(
    mut wal_storage: WALStorage<TestCommand>,
    mut frame_gen: FrameGenerator,
    shutdown_flag: Arc<AtomicBool>,
) -> Vec<LogEntry<TestCommand>> {
    let mut frames_all = vec![];
    let mut interval = tokio::time::interval(Duration::from_millis(1));
    loop {
        if shutdown_flag.load(Ordering::Relaxed) {
            break;
        }
        interval.tick().await;
        let batch_size = {
            let mut rng = thread_rng();
            rng.gen_range(1..10)
        };
        let frames = frame_gen.take(batch_size);
        frames_all.extend(frames.clone());
        wal_storage.send_sync(frames).await.unwrap();
    }

    frames_all
        .into_iter()
        .map(|f| {
            let DataFrame::Entry(entry) = f else { unreachable!() };
            entry
        })
        .collect()
}

async fn start_failure_injection(failure_duration: Duration, shutdown_flag: Arc<AtomicBool>) {
    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(failure_duration).await;
        info!("Clearing LazyFS cache");
        LazyFSController::clear_cache().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lazyfs_test() {
    _ = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let mut wal_dir = PathBuf::from(LAZYFS_MOUTN_POINT);
    wal_dir.push("wal_data");
    fs::remove_dir_all(&wal_dir);
    fs::create_dir(&wal_dir);
    let dir = fs::File::open(&wal_dir).unwrap();
    dir.sync_all().unwrap();

    let config = WALConfig::new(wal_dir).with_max_segment_size(TEST_SEGMENT_SIZE);
    let (wal_storage, _logs) = WALStorage::<TestCommand>::new_or_recover(config.clone())
        .await
        .unwrap();
    let frame_gen = FrameGenerator::new(TEST_SEGMENT_SIZE);

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let send_handle = tokio::spawn(start_sending(
        wal_storage,
        frame_gen,
        Arc::clone(&shutdown_flag),
    ));
    let failure_handle = tokio::spawn(start_failure_injection(
        Duration::from_secs(1),
        Arc::clone(&shutdown_flag),
    ));

    tokio::time::sleep(Duration::from_secs(10)).await;
    shutdown_flag.store(true, Ordering::Relaxed);
    let logs_all = send_handle.await.unwrap();
    info!("entries sent: {}", logs_all.len());

    let (wal_storage, logs_recovered) = WALStorage::<TestCommand>::new_or_recover(config)
        .await
        .unwrap();

    info!("{} logs recovered", logs_recovered.len());

    assert!(
        logs_all
            .into_iter()
            .zip(logs_recovered.into_iter())
            .all(|(x, y)| x == y),
        "logs not match"
    );
}
