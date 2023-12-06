//* Power failure fault means that only the last unsynced data might be lost.
//* This is to test if all the checkpoints are valid.

use super::*;

#[test]
fn wal_wont_lost_persistent_index_on_power_failure() {
    init_logger();
    let dir = init_wal_dir();

    let test_count = std::env::var("WAL_TEST_COUNT").map_or(1, |c| c.parse().unwrap());
    let entry_gen = Arc::new(EntryGenerator::new(TEST_SEGMENT_SIZE));
    let mut logs_all = vec![];
    let mut min_persistent_index = 0;
    for _ in 0..test_count {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let dir_c = dir.clone();
        let entry_gen_c = Arc::clone(&entry_gen);
        (logs_all, min_persistent_index) = rt.block_on(async move {
            let (mut wal_storage, logs) = start_wal(&dir_c).await;
            info!("log len :{}", logs.len());
            let recovered_index = logs.last().map(|e| e.index).unwrap_or(0);
            info!("min persistent index: {min_persistent_index}");
            info!("last recoverd index: {}", recovered_index);
            assert!(
                recovered_index >= min_persistent_index,
                "recovered log index should be larger or equal to min persistent index"
            );
            logs_expect(&logs_all, &logs);
            entry_gen_c.reset_next_index_to(recovered_index + 1);
            wal_storage
                .send_sync(vec![DataFrame::Entry(entry_gen_c.next())])
                .await;
            panic!("");

            let shutdown_flag = Arc::new(AtomicBool::new(false));

            let send_handle = tokio::spawn(start_sending(
                wal_storage,
                logs_all,
                Arc::clone(&entry_gen_c),
                Arc::clone(&shutdown_flag),
                Duration::from_millis(1),
                None,
                None,
            ));

            tokio::time::sleep(Duration::from_secs(2)).await;
            shutdown_flag.store(true, Ordering::SeqCst);
            //            LazyFSController::clear_cache().await;
            let (logs_current, min_persistent_index) = send_handle.await.unwrap();
            info!(
                "max logs index sent: {}",
                logs_current.last().unwrap().index
            );
            (logs_current, min_persistent_index)
        });
    }
}
