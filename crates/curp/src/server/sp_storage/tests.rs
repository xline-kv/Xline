use std::sync::Arc;

use curp_test_utils::test_cmd::TestCommand;

use crate::rpc::{PoolEntry, ProposeId};

use super::{config::WALConfig, PoolWALOps, SpeculativePoolWAL};

#[test]
fn it_works() {
    let dir = tempfile::tempdir().unwrap();
    let config = WALConfig::new(dir)
        .with_max_insert_segment_size(512)
        .with_max_remove_segment_size(32);
    let wal = SpeculativePoolWAL::<TestCommand>::new(config).unwrap();
    let cmd = TestCommand::new_put(vec![1], 0);
    let entry = PoolEntry::new(ProposeId(0, 1), Arc::new(cmd));
    wal.insert(vec![entry]).unwrap();
}
