use std::{collections::HashSet, path::Path, sync::Arc};

use curp_test_utils::test_cmd::TestCommand;

use crate::rpc::{PoolEntry, ProposeId};

use super::{config::WALConfig, PoolWALOps, SpeculativePoolWAL};

#[test]
fn wal_insert_should_work() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let wal = init_wal(&tmp_dir, &[]);

    let mut gen = EntryGenerator::new();
    for i in 0..50 {
        wal.insert(gen.take(i)).unwrap();
    }
}

#[test]
fn wal_insert_remove_should_work() {
    const NUM_ENTRIES: usize = 1000;
    const INSERT_CHUNK_SIZE: usize = 10;
    const REMOVE_CHUNK_SIZE: usize = 5;
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut gen = EntryGenerator::new();
    let wal = init_wal(&tmp_dir, &[]);

    let entries = gen.take(NUM_ENTRIES);
    for chunk in entries.chunks_exact(INSERT_CHUNK_SIZE) {
        wal.insert(chunk.to_vec()).unwrap();
    }
    for chunk in entries.chunks_exact(REMOVE_CHUNK_SIZE) {
        wal.remove(chunk.into_iter().map(|e| e.id).collect())
            .unwrap();
    }
}

#[test]
fn wal_insert_remove_in_different_thread_is_ok() {
    const NUM_ENTRIES: usize = 1000;
    let tmp_dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(init_wal(&tmp_dir, &[]));
    let wal_c = Arc::clone(&wal);

    let handle = std::thread::spawn(move || {
        let mut gen = EntryGenerator::new();
        for e in gen.take(NUM_ENTRIES) {
            wal.insert(vec![e]).unwrap();
        }
    });

    let mut gen = EntryGenerator::new();
    for e in gen.take(NUM_ENTRIES) {
        wal_c.remove(vec![e.id]).unwrap();
    }
    handle.join().unwrap();
}

#[test]
fn wal_insert_recover_is_ok() {
    const NUM_ENTRIES: usize = 1000;
    const INSERT_CHUNK_SIZE: usize = 10;
    let temp_dir = tempfile::tempdir().unwrap();
    let mut gen = EntryGenerator::new();
    let wal = init_wal(&temp_dir, &[]);

    let entries = gen.take(NUM_ENTRIES);
    for chunk in entries.chunks_exact(INSERT_CHUNK_SIZE) {
        wal.insert(chunk.to_vec()).unwrap();
    }
    drop(wal);
    let _wal = init_wal(&temp_dir, &entries);
}

#[test]
fn wal_should_recover_commute_cmds() {
    const NUM_ENTRIES: usize = 1000;
    const INSERT_CHUNK_SIZE: usize = 10;
    let temp_dir = tempfile::tempdir().unwrap();
    let wal = init_wal(&temp_dir, &[]);
    let cmds = vec![
        TestCommand::new_put(vec![0], 0),
        TestCommand::new_put(vec![1], 0),
        TestCommand::new_put(vec![2], 0),
        TestCommand::new_put(vec![3], 0),
        TestCommand::new_put(vec![4], 0),
        TestCommand::new_put(vec![1], 0),
        TestCommand::new_put(vec![2], 0),
    ];
    let entries: Vec<_> = cmds
        .into_iter()
        .enumerate()
        .map(|(i, c)| PoolEntry::new(ProposeId(i as u64, 0), Arc::new(c)))
        .collect();
    for e in entries.clone() {
        wal.insert(vec![e]).unwrap();
    }
    drop(wal);
    let _wal = init_wal(&temp_dir, &entries[3..7]);
}

#[test]
fn wal_insert_remove_then_recover_is_ok() {
    const NUM_ENTRIES: usize = 1000;
    const INSERT_CHUNK_SIZE: usize = 10;
    const REMOVE_CHUNK_SIZE: usize = 5;
    let temp_dir = tempfile::tempdir().unwrap();
    let mut gen = EntryGenerator::new();
    let wal = init_wal(&temp_dir, &[]);

    let entries = gen.take(NUM_ENTRIES);
    for chunk in entries.chunks_exact(INSERT_CHUNK_SIZE) {
        wal.insert(chunk.to_vec()).unwrap();
    }
    for chunk in entries[0..200].chunks_exact(REMOVE_CHUNK_SIZE) {
        wal.remove(chunk.into_iter().map(|e| e.id).collect())
            .unwrap();
    }
    drop(wal);
    let _wal = init_wal(&temp_dir, &entries[200..]);
}

#[test]
fn wal_gc_is_ok() {
    const NUM_ENTRIES: usize = 1000;
    const INSERT_CHUNK_SIZE: usize = 10;
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut gen = EntryGenerator::new();
    let wal = init_wal(&tmp_dir, &[]);

    let entries = gen.take(NUM_ENTRIES);
    for chunk in entries.chunks_exact(INSERT_CHUNK_SIZE) {
        wal.insert(chunk.to_vec()).unwrap();
    }
    let ids = wal.insert_segment_ids();

    let to_gc: HashSet<_> = ids.iter().take(10).flatten().collect();
    let num = to_gc.len();
    wal.gc(|id| to_gc.contains(id)).unwrap();
    drop(wal);
    let _wal = init_wal(&tmp_dir, &entries[num..]);
}

fn init_wal(
    dir: impl AsRef<Path>,
    expect: &[PoolEntry<TestCommand>],
) -> SpeculativePoolWAL<TestCommand> {
    let config = WALConfig::new(dir)
        .with_max_insert_segment_size(512)
        .with_max_remove_segment_size(32);
    let wal = SpeculativePoolWAL::<TestCommand>::new(config).unwrap();
    let mut recovered = wal.recover().unwrap();
    recovered.sort_unstable();
    let mut expect_sorted = expect.to_vec();
    expect_sorted.sort_unstable();
    assert_eq!(
        &recovered,
        &expect_sorted,
        "recovered: {}, expect: {}",
        recovered.len(),
        expect.len()
    );
    wal
}

struct EntryGenerator {
    next: u32,
}

impl EntryGenerator {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> PoolEntry<TestCommand> {
        self.next += 1;
        let cmd = TestCommand::new_put(vec![self.next], 0);
        PoolEntry::new(ProposeId(self.next as u64, 1), Arc::new(cmd))
    }

    fn take(&mut self, n: usize) -> Vec<PoolEntry<TestCommand>> {
        std::iter::repeat_with(|| self.next()).take(n).collect()
    }
}

impl Eq for PoolEntry<TestCommand> {}

impl PartialOrd for PoolEntry<TestCommand> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl Ord for PoolEntry<TestCommand> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}
