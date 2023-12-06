mod power_failure;

mod silent_corruption;

use std::{
    fs, iter,
    path::{Path, PathBuf},
    pin::Pin,
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use curp_test_utils::test_cmd::TestCommand;
use futures::FutureExt;
use rand::{random, thread_rng, Rng};
use tokio::time::interval;
use tracing::info;

use crate::server::storage::wal::tests::EntryGenerator;

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

struct BitFlip {
    path: PathBuf,
}

impl BitFlip {
    fn offset(&self, offset: usize) {
        self.spawn_bitflip(vec!["offset".to_owned(), format!("{offset}")]);
    }

    fn random(&self) {
        self.spawn_bitflip(vec![]);
    }

    fn spawn_bitflip(&self, args: Vec<String>) {
        let mut c = Command::new("bitflip")
            .args(
                args.into_iter()
                    .chain(iter::once(self.path.as_path().to_string_lossy().into())),
            )
            .spawn()
            .unwrap();
        c.wait().unwrap();
    }
}

async fn start_sending(
    mut wal_storage: WALStorage<TestCommand>,
    mut all_entries: Vec<LogEntry<TestCommand>>,
    mut entry_gen: Arc<EntryGenerator>,
    shutdown_flag: Arc<AtomicBool>,
    log_persistent_interval: Duration,
    head_truncation_interval: Option<Duration>,
    tail_truncation_interval: Option<Duration>,
) -> (Vec<LogEntry<TestCommand>>, LogIndex) {
    // The minimum index that guaranteed to be persistent
    let mut min_persistent_index = 0;
    let mut log_persistent_interval = interval(log_persistent_interval);
    let mut head_truncation_interval = head_truncation_interval.map(interval);
    let mut tail_truncation_interval = tail_truncation_interval.map(interval);

    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            break;
        }
        let head_truncation_fut: Pin<Box<dyn Future<Output = ()> + Send>> =
            match head_truncation_interval {
                Some(ref mut i) => Box::pin(i.tick().map(|_| ())),
                None => Box::pin(futures::future::pending::<()>()),
            };
        let tail_truncation_fut: Pin<Box<dyn Future<Output = ()> + Send>> =
            match tail_truncation_interval {
                Some(ref mut i) => Box::pin(i.tick().map(|_| ())),
                None => Box::pin(futures::future::pending::<()>()),
            };

        tokio::select! {
            _ = log_persistent_interval.tick() => {
                let batch_size = {
                    let mut rng = thread_rng();
                    rng.gen_range(1..10)
                };
                let entries = entry_gen.take(batch_size);
                all_entries.extend(entries.clone());
                wal_storage.send_sync(entries.clone().into_iter().map(DataFrame::Entry).collect()).await.unwrap();

                // Here's a checkpoint:
                // The send_sync successfully completed means that the logs are guaranteed
                // to be persistented if there's no silent corruption

                if !shutdown_flag.load(Ordering::SeqCst) {
                    let Some(entry) = entries.last() else {
                        unreachable!()
                    };
                    min_persistent_index = entry.index;
                }
            }
            _ = head_truncation_fut => {
                let current_index = entry_gen.current_index();
                if current_index > 1000 {
                    info!("event: head truncation");
                    let truncate_index = current_index - 1000;
                    wal_storage.truncate_head(truncate_index).await.unwrap();
                }
            }
            _ = tail_truncation_fut => {
                let current_index = entry_gen.current_index();
                if current_index > 10 {
                    info!("event: tail truncation");
                    let truncate_index = current_index - 10;
                    wal_storage.truncate_tail(truncate_index).await.unwrap();
                    entry_gen.reset_next_index_to(truncate_index + 1);
                    min_persistent_index = truncate_index;
                    all_entries.truncate(all_entries.len() - 10);
                }
            }

        }
    }

    (all_entries, min_persistent_index)
}

async fn start_faults_injection(failure_duration: Duration, shutdown_flag: Arc<AtomicBool>) {
    let mut interval = interval(failure_duration);
    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            break;
        }
        interval.tick().await;
        info!("fault: clearing LazyFS cache");
        LazyFSController::clear_cache().await;
    }
}

async fn start_wal(
    wal_dir: impl AsRef<Path>,
) -> (WALStorage<TestCommand>, Vec<LogEntry<TestCommand>>) {
    let config = WALConfig::new(wal_dir).with_max_segment_size(TEST_SEGMENT_SIZE);
    WALStorage::<TestCommand>::new_or_recover(config.clone())
        .await
        .unwrap()
}

fn init_wal_dir() -> PathBuf {
    let mut wal_dir = PathBuf::from(LAZYFS_MOUTN_POINT);
    wal_dir.push("wal_data");
    // fs::remove_dir_all(&wal_dir);
    // fs::create_dir(&wal_dir);
    let dir = fs::File::open(&wal_dir).unwrap();
    dir.sync_all().unwrap();
    wal_dir
}

fn init_logger() {
    _ = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

fn logs_expect(expected: &Vec<LogEntry<TestCommand>>, recovered: &Vec<LogEntry<TestCommand>>) {
    if recovered.len() == 0 {
        return;
    }
    let start_index = recovered.first().unwrap().index;
    let expected = expected.into_iter().skip_while(|e| e.index < start_index);
    for (e, r) in expected.zip(recovered.into_iter()) {
        assert_eq!(e, r, "recovered logs not match with expected logs");
    }
}
