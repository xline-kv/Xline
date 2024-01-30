use std::{collections::HashSet, time::Duration};

use utils::{parking_lot_lock::MutexMap, task_manager::Listener};

use super::spec_pool::SpecPoolRef;
use crate::{cmd::Command, rpc::ProposeId, server::cmd_board::CmdBoardRef};

/// Cleanup spec pool
pub(super) async fn gc_spec_pool<C: Command>(
    sp: SpecPoolRef<C>,
    interval: Duration,
    shutdown_listener: Listener,
) {
    let mut last_check: HashSet<ProposeId> =
        sp.map_lock(|sp_l| sp_l.pool.keys().copied().collect());
    #[allow(clippy::arithmetic_side_effects)] // introduced by tokio select
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown_listener.wait() => break,
        }
        let mut sp = sp.lock();
        sp.pool.retain(|k, _v| !last_check.contains(k));

        last_check = sp.pool.keys().copied().collect();
    }
}

/// Cleanup cmd board
pub(super) async fn gc_cmd_board<C: Command>(
    cmd_board: CmdBoardRef<C>,
    interval: Duration,
    shutdown_listener: Listener,
) {
    let mut last_check_len_er = 0;
    let mut last_check_len_asr = 0;
    let mut last_check_len_sync = 0;
    let mut last_check_len_conf = 0;
    #[allow(clippy::arithmetic_side_effects)] // introduced by tokio select
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown_listener.wait() => break,
        }
        let mut board = cmd_board.write();

        // last_check_len_xxx should always be smaller than board.xxx_.len(), the check is just for precaution

        if last_check_len_er <= board.er_buffer.len() {
            let new_er_buffer = board.er_buffer.split_off(last_check_len_er);
            board.er_buffer = new_er_buffer;
            last_check_len_er = board.er_buffer.len();
        }

        if last_check_len_asr <= board.asr_buffer.len() {
            let new_asr_buffer = board.asr_buffer.split_off(last_check_len_asr);
            board.asr_buffer = new_asr_buffer;
            last_check_len_asr = board.asr_buffer.len();
        }

        if last_check_len_sync <= board.sync.len() {
            let new_sync = board.sync.split_off(last_check_len_sync);
            board.sync = new_sync;
            last_check_len_sync = board.sync.len();
        }

        if last_check_len_conf <= board.conf_buffer.len() {
            let new_conf = board.conf_buffer.split_off(last_check_len_conf);
            board.conf_buffer = new_conf;
            last_check_len_conf = board.conf_buffer.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use curp_test_utils::{
        sleep_secs,
        test_cmd::{TestCommand, TestCommandResult},
    };
    use parking_lot::{Mutex, RwLock};
    use test_macros::abort_on_panic;
    use utils::task_manager::{tasks::TaskName, TaskManager};

    use super::*;
    use crate::{
        rpc::{PoolEntry, ProposeId},
        server::{
            cmd_board::{CmdBoardRef, CommandBoard},
            gc::gc_cmd_board,
            spec_pool::{SpecPoolRef, SpeculativePool},
        },
    };

    #[tokio::test]
    #[abort_on_panic]
    async fn cmd_board_gc_test() {
        let task_manager = TaskManager::new();
        let board: CmdBoardRef<TestCommand> = Arc::new(RwLock::new(CommandBoard::new()));
        task_manager.spawn(TaskName::GcCmdBoard, |n| {
            gc_cmd_board(Arc::clone(&board), Duration::from_millis(500), n)
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId(1, 1), Ok(TestCommandResult::default()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId(2, 2), Ok(TestCommandResult::default()));
        board
            .write()
            .asr_buffer
            .insert(ProposeId(1, 1), Ok(0.into()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .asr_buffer
            .insert(ProposeId(2, 2), Ok(0.into()));

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId(3, 3), Ok(TestCommandResult::default()));
        board
            .write()
            .asr_buffer
            .insert(ProposeId(3, 3), Ok(0.into()));

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let board = board.write();
        assert_eq!(board.er_buffer.len(), 1);
        assert_eq!(*board.er_buffer.get_index(0).unwrap().0, ProposeId(3, 3));
        assert_eq!(board.asr_buffer.len(), 1);
        assert_eq!(*board.asr_buffer.get_index(0).unwrap().0, ProposeId(3, 3));
        task_manager.shutdown(true).await;
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn spec_gc_test() {
        let task_manager = TaskManager::new();
        let spec: SpecPoolRef<TestCommand> = Arc::new(Mutex::new(SpeculativePool::new()));
        task_manager.spawn(TaskName::GcSpecPool, |n| {
            gc_spec_pool(Arc::clone(&spec), Duration::from_millis(500), n)
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd1 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(ProposeId(0, 1), PoolEntry::new(ProposeId(0, 1), cmd1));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd2 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(ProposeId(0, 2), PoolEntry::new(ProposeId(0, 2), cmd2));

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        let cmd3 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(ProposeId(0, 3), PoolEntry::new(ProposeId(0, 3), cmd3));

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let spec = spec.lock();
        assert_eq!(spec.pool.len(), 1);
        assert!(spec.pool.contains_key(&ProposeId(0, 3)));
        task_manager.shutdown(true).await;
    }

    // To verify #206 is fixed
    #[tokio::test]
    #[abort_on_panic]
    async fn spec_gc_will_not_panic() {
        let task_manager = TaskManager::new();
        let spec: SpecPoolRef<TestCommand> = Arc::new(Mutex::new(SpeculativePool::new()));

        let cmd1 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(ProposeId(0, 1), PoolEntry::new(ProposeId(0, 1), cmd1));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd2 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(ProposeId(0, 2), PoolEntry::new(ProposeId(0, 2), cmd2));

        let cmd3 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(ProposeId(0, 2), PoolEntry::new(ProposeId(0, 3), cmd3));

        task_manager.spawn(TaskName::GcSpecPool, |n| {
            gc_spec_pool(Arc::clone(&spec), Duration::from_millis(500), n)
        });

        spec.lock().remove(&ProposeId(0, 2));

        sleep_secs(1).await;
        task_manager.shutdown(true).await;
    }
}
