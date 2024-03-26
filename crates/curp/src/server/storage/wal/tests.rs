use std::{fs, path::Path, sync::Arc};

use bytes::BytesMut;
use curp_test_utils::test_cmd::TestCommand;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio_util::codec::Encoder;

use crate::{
    log_entry::{EntryData, LogEntry},
    rpc::ProposeId,
    server::storage::wal::{
        codec::DataFrameOwned, test_util::EntryGenerator, util::get_file_paths_with_ext,
    },
};

use super::*;

const TEST_SEGMENT_SIZE: u64 = 512;

#[test]
fn simple_append_and_recovery_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    test_follow_up_append_recovery(wal_test_path.path(), 100);
}

#[test]
fn log_head_truncation_is_ok() {
    for num_entries in 1..40 {
        for truncate_at in 1..=num_entries {
            let wal_test_path = tempfile::tempdir().unwrap();
            test_head_truncate_at(wal_test_path.path(), num_entries, truncate_at as u64);
            test_follow_up_append_recovery(wal_test_path.path(), 10);
        }
    }
}

#[test]
fn log_tail_truncation_is_ok() {
    for num_entries in 1..40 {
        for truncate_at in 1..=num_entries {
            let wal_test_path = tempfile::tempdir().unwrap();
            test_tail_truncate_at(wal_test_path.path(), num_entries, truncate_at as u64);
            test_follow_up_append_recovery(wal_test_path.path(), 10);
        }
    }
}

/// Checks if the segment files are deleted
fn test_head_truncate_at(wal_test_path: &Path, num_entries: usize, truncate_at: LogIndex) {
    let get_num_segments = || {
        get_file_paths_with_ext(&wal_test_path, ".wal")
            .unwrap()
            .len()
    };

    let config = WALConfig::new(&wal_test_path).with_max_segment_size(TEST_SEGMENT_SIZE);
    let mut storage = WALStorage::<TestCommand>::new(config.clone()).unwrap();
    let _logs = storage.recover().unwrap();

    let mut entry_gen = EntryGenerator::new(TEST_SEGMENT_SIZE);
    let num_entries_per_segment = entry_gen.num_entries_per_segment();

    for frame in entry_gen
        .take(num_entries)
        .into_iter()
        .map(DataFrameOwned::Entry)
    {
        storage.send_sync(vec![frame.get_ref()]).unwrap();
    }

    // If the wal segment is full after pushing a log, the storage will allocate
    // a new segment immediately.
    let num_segments = num_entries / num_entries_per_segment + 1;
    assert_eq!(num_segments, get_num_segments());

    storage.truncate_head(truncate_at).unwrap();

    let num_segments_truncated = ((truncate_at as usize + num_entries_per_segment - 1)
        / num_entries_per_segment)
        .saturating_sub(1);
    assert_eq!(num_segments - num_segments_truncated, get_num_segments());
}

fn test_tail_truncate_at(wal_test_path: &Path, num_entries: usize, truncate_at: LogIndex) {
    assert!(num_entries as u64 >= truncate_at);
    let config = WALConfig::new(&wal_test_path).with_max_segment_size(TEST_SEGMENT_SIZE);
    let mut storage = WALStorage::<TestCommand>::new(config.clone()).unwrap();
    let _logs = storage.recover().unwrap();

    let mut entry_gen = EntryGenerator::new(TEST_SEGMENT_SIZE);
    for frame in entry_gen
        .take(num_entries)
        .into_iter()
        .map(DataFrameOwned::Entry)
    {
        storage.send_sync(vec![frame.get_ref()]).unwrap();
    }

    storage.truncate_tail(truncate_at);
    let next_entry =
        LogEntry::<TestCommand>::new(truncate_at + 1, 1, ProposeId(1, 3), EntryData::Empty);
    storage
        .send_sync(vec![DataFrameOwned::Entry(next_entry.clone()).get_ref()])
        .unwrap();

    drop(storage);

    let mut storage = WALStorage::<TestCommand>::new(config.clone()).unwrap();
    let logs = storage.recover().unwrap();

    assert_eq!(
        logs.len() as u64,
        truncate_at + 1,
        "failed to recover all logs"
    );

    assert_eq!(*logs.last().unwrap(), next_entry);
}

/// Test if the append and recovery are ok after some event
fn test_follow_up_append_recovery(wal_test_path: &Path, to_append: usize) {
    let config = WALConfig::new(&wal_test_path).with_max_segment_size(TEST_SEGMENT_SIZE);
    let mut storage = WALStorage::<TestCommand>::new(config.clone()).unwrap();
    let logs_initial = storage.recover().unwrap();

    let next_log_index = logs_initial.last().map_or(0, |e| e.index) + 1;

    let mut entry_gen = EntryGenerator::new(TEST_SEGMENT_SIZE);
    entry_gen.skip(next_log_index as usize - 1);
    let frames = entry_gen
        .take(to_append)
        .into_iter()
        .map(DataFrameOwned::Entry);

    for frame in frames.clone() {
        storage.send_sync(vec![frame.get_ref()]).unwrap();
    }

    drop(storage);

    let mut storage = WALStorage::<TestCommand>::new(config.clone()).unwrap();
    let logs = storage.recover().unwrap();

    assert_eq!(
        logs.len(),
        logs_initial.len() + to_append,
        "failed to recover all logs"
    );

    assert!(
        logs.into_iter()
            .skip(logs_initial.len())
            .zip(frames)
            .all(|(x, y)| DataFrameOwned::Entry(x) == y),
        "log entries mismatched"
    );
}
