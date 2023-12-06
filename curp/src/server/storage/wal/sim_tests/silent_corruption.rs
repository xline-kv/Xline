//* Silent corruption faults means that the any part of the logs can be corrupted
//* We test that if all logs recovered are consecutive and contains valid data
use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn log_recovery_on_lazyfs_silent_corruption() {
    init_logger();
    let dir = init_wal_dir();
    let (wal_storage, _logs) = start_wal(&dir).await;
    let frame_gen = Arc::new(EntryGenerator::new(TEST_SEGMENT_SIZE));
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let send_handle = tokio::spawn(start_sending(
        wal_storage,
        vec![],
        frame_gen,
        Arc::clone(&shutdown_flag),
        Duration::from_millis(1),
        None,
        Some(Duration::from_millis(200)),
    ));
    let failure_handle = tokio::spawn(start_faults_injection(
        Duration::from_secs(1),
        Arc::clone(&shutdown_flag),
    ));

    tokio::time::sleep(Duration::from_secs(10)).await;
    shutdown_flag.store(true, Ordering::Relaxed);

    let (logs_all, _index) = send_handle.await.unwrap();

    let (wal_storage, logs_recovered) = start_wal(&dir).await;

    info!("logs all: {}", logs_all.len());
    info!("logs recovered: {}", logs_recovered.len());
    logs_expect(&logs_all, &logs_recovered);
}
