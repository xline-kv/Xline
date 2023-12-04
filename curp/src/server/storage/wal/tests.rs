use std::path::Path;

use bytes::BytesMut;
use curp_external_api::cmd::ProposeId;
use curp_test_utils::test_cmd::TestCommand;
use tempfile::TempDir;
use tokio_util::codec::Encoder;

use crate::{
    log_entry::{EntryData, LogEntry},
    server::storage::wal::{codec::DataFrame, util::get_file_paths_with_ext},
};

use super::*;

const TEST_SEGMENT_SIZE: u64 = 512;

#[tokio::test(flavor = "multi_thread")]
async fn simple_append_and_recovery_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    test_follow_up_append_recovery(wal_test_path.path(), 100);
}

#[tokio::test(flavor = "multi_thread")]
async fn log_head_truncation_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    for num_entries in 1..100 {
        for truncate_at in 1..=num_entries {
            test_head_truncate_at(wal_test_path.path(), num_entries, truncate_at as u64);
            test_follow_up_append_recovery(wal_test_path.path(), 10);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn log_tail_truncation_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    for num_entries in 1..100 {
        for truncate_at in 1..=num_entries {
            test_tail_truncate_at(wal_test_path.path(), num_entries, truncate_at as u64);
            test_follow_up_append_recovery(wal_test_path.path(), 10);
        }
    }
}

/// Checks if the segment files are deleted
async fn test_head_truncate_at(wal_test_path: &Path, num_entries: usize, truncate_at: LogIndex) {
    let get_num_segments = || {
        get_file_paths_with_ext(&wal_test_path, ".wal")
            .unwrap()
            .len()
    };

    let num_entries_per_segment = FrameGenerator::num_entries_per_segment();

    let config = WALConfig::new(&wal_test_path).with_max_segment_size(TEST_SEGMENT_SIZE);
    let (mut storage, _logs) = WALStorage::<TestCommand>::new_or_recover(config.clone())
        .await
        .unwrap();

    let mut frame_gen = FrameGenerator::new();
    for frame in frame_gen.take(num_entries) {
        storage.send_sync(vec![frame]).await.unwrap();
    }

    let num_segments = (num_entries + num_entries_per_segment - 1) / num_entries_per_segment;
    assert_eq!(num_segments, get_num_segments());

    storage.truncate_head(truncate_at).await.unwrap();

    let num_entries_truncated = num_entries - truncate_at as usize;
    let num_segments_truncated =
        (num_entries_truncated + num_entries_per_segment - 1) / num_entries_per_segment;
    assert_eq!(num_segments_truncated, get_num_segments());
}

async fn test_tail_truncate_at(wal_test_path: &Path, num_entries: usize, truncate_at: LogIndex) {
    assert!(num_entries as u64 >= truncate_at);
    let config = WALConfig::new(&wal_test_path).with_max_segment_size(TEST_SEGMENT_SIZE);
    let (mut storage, _logs) = WALStorage::new_or_recover(config.clone()).await.unwrap();

    let mut frame_gen = FrameGenerator::new();
    for frame in frame_gen.take(num_entries) {
        storage.send_sync(vec![frame]).await.unwrap();
    }

    storage.truncate_tail(truncate_at).await;
    let next_entry =
        LogEntry::<TestCommand>::new(truncate_at + 1, 1, EntryData::Empty(ProposeId(1, 3)));
    storage
        .send_sync(vec![DataFrame::Entry(next_entry.clone())])
        .await
        .unwrap();

    drop(storage);

    let (_storage, logs) = WALStorage::<TestCommand>::new_or_recover(config)
        .await
        .unwrap();

    assert_eq!(
        logs.len() as u64,
        truncate_at + 1,
        "failed to recover all logs"
    );

    assert_eq!(*logs.last().unwrap(), next_entry);
}

/// Test if the append and recovery are ok after some event
async fn test_follow_up_append_recovery(wal_test_path: &Path, to_append: usize) {
    let config = WALConfig::new(&wal_test_path).with_max_segment_size(TEST_SEGMENT_SIZE);
    let (mut storage, logs_initial) = WALStorage::<TestCommand>::new_or_recover(config.clone())
        .await
        .unwrap();

    let next_log_index = logs_initial.last().map_or(0, |e| e.index) + 1;

    let mut frame_gen = FrameGenerator::new();
    frame_gen.skip(logs_initial.len());
    let frames = frame_gen.take(to_append);

    for frame in frames.clone() {
        storage.send_sync(vec![frame]).await.unwrap();
    }

    drop(storage);

    let (_storage, logs) = WALStorage::<TestCommand>::new_or_recover(config)
        .await
        .unwrap();

    assert_eq!(
        logs.len(),
        logs_initial.len() + to_append,
        "failed to recover all logs"
    );

    assert!(
        logs.into_iter()
            .skip(logs_initial.len())
            .zip(frames)
            .all(|(x, y)| DataFrame::Entry(x) == y),
        "log entries mismatched"
    );
}

#[derive(Clone)]
struct FrameGenerator {
    next_index: u64,
}

impl FrameGenerator {
    fn new() -> Self {
        Self { next_index: 1 }
    }

    fn skip(&mut self, num_index: usize) {
        self.next_index += num_index as u64;
    }

    fn next(&mut self) -> DataFrame<TestCommand> {
        let entry =
            LogEntry::<TestCommand>::new(self.next_index, 1, EntryData::Empty(ProposeId(1, 2)));
        self.next_index += 1;

        DataFrame::Entry(entry)
    }

    fn take(&mut self, num: usize) -> Vec<DataFrame<TestCommand>> {
        (0..num).map(|_| self.next()).collect()
    }

    fn num_entries_per_segment() -> usize {
        let header_size = 32;
        let sample_entry = LogEntry::<TestCommand>::new(1, 1, EntryData::Empty(ProposeId(1, 2)));
        let mut wal_codec = WAL::<TestCommand>::new();
        let mut buf = BytesMut::new();
        wal_codec.encode(vec![DataFrame::Entry(sample_entry)], &mut buf);
        let entry_size = buf.len();
        (TEST_SEGMENT_SIZE as usize - header_size + entry_size - 1) / entry_size
    }
}
