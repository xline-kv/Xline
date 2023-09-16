use std::{collections::HashSet, time::Duration};

use utils::{parking_lot_lock::MutexMap, shutdown};

use super::spec_pool::SpecPoolRef;
use crate::{
    cmd::{Command, ProposeId},
    server::cmd_board::CmdBoardRef,
};

/// Run background GC tasks for Curp server
pub(super) fn run_gc_tasks<C: Command + 'static>(
    cmd_board: CmdBoardRef<C>,
    spec: SpecPoolRef<C>,
    gc_interval: Duration,
    mut shutdown_listener: shutdown::Listener,
) {
    let spec_pool_gc = tokio::spawn(gc_spec_pool(spec, gc_interval));
    let cmd_board_gc = tokio::spawn(gc_cmd_board(cmd_board, gc_interval));
    let _ig = tokio::spawn(async move {
        shutdown_listener.wait_self_shutdown().await;
        spec_pool_gc.abort();
        cmd_board_gc.abort();
    });
}

/// Cleanup spec pool
async fn gc_spec_pool<C: Command + 'static>(sp: SpecPoolRef<C>, interval: Duration) {
    let mut last_check: HashSet<ProposeId> =
        sp.map_lock(|sp_l| sp_l.pool.keys().cloned().collect());
    loop {
        tokio::time::sleep(interval).await;
        let mut sp = sp.lock();
        sp.pool.retain(|k, _v| !last_check.contains(k));

        last_check = sp.pool.keys().cloned().collect();
    }
}

/// Cleanup cmd board
async fn gc_cmd_board<C: Command + 'static>(cmd_board: CmdBoardRef<C>, interval: Duration) {
    let mut last_check_len_er = 0;
    let mut last_check_len_asr = 0;
    let mut last_check_len_sync = 0;
    loop {
        tokio::time::sleep(interval).await;
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

    use super::*;
    use crate::{
        cmd::ProposeId,
        server::{
            cmd_board::{CmdBoardRef, CommandBoard},
            gc::gc_cmd_board,
            spec_pool::{SpecPoolRef, SpeculativePool},
        },
    };

    #[tokio::test]
    #[abort_on_panic]
    async fn cmd_board_gc_test() {
        let board: CmdBoardRef<TestCommand> = Arc::new(RwLock::new(CommandBoard::new()));
        tokio::spawn(gc_cmd_board(Arc::clone(&board), Duration::from_millis(500)));

        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId::from("1"), Ok(TestCommandResult::default()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId::from("2"), Ok(TestCommandResult::default()));
        board
            .write()
            .asr_buffer
            .insert(ProposeId::from("1"), Ok(0.into()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .asr_buffer
            .insert(ProposeId::from("2"), Ok(0.into()));

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId::from("3"), Ok(TestCommandResult::default()));
        board
            .write()
            .asr_buffer
            .insert(ProposeId::from("3"), Ok(0.into()));

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let board = board.write();
        assert_eq!(board.er_buffer.len(), 1);
        assert_eq!(board.er_buffer.get_index(0).unwrap().0, "3");
        assert_eq!(board.asr_buffer.len(), 1);
        assert_eq!(board.asr_buffer.get_index(0).unwrap().0, "3");
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn spec_gc_test() {
        let spec: SpecPoolRef<TestCommand> = Arc::new(Mutex::new(SpeculativePool::new()));
        tokio::spawn(gc_spec_pool(Arc::clone(&spec), Duration::from_millis(500)));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd1 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(cmd1.id().clone(), Arc::clone(&cmd1));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd2 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(cmd2.id().clone(), Arc::clone(&cmd2));

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        let cmd3 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(cmd3.id().clone(), Arc::clone(&cmd3));

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let spec = spec.lock();
        assert_eq!(spec.pool.len(), 1);
        assert!(spec.pool.contains_key(cmd3.id()));
    }

    // To verify #206 is fixed
    #[tokio::test]
    #[abort_on_panic]
    async fn spec_gc_will_not_panic() {
        let spec: SpecPoolRef<TestCommand> = Arc::new(Mutex::new(SpeculativePool::new()));

        let cmd1 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(cmd1.id().clone(), Arc::clone(&cmd1));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd2 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(cmd2.id().clone(), Arc::clone(&cmd2));

        let cmd3 = Arc::new(TestCommand::default());
        spec.lock()
            .pool
            .insert(cmd2.id().clone(), Arc::clone(&cmd3));

        tokio::spawn(gc_spec_pool(Arc::clone(&spec), Duration::from_millis(500)));

        spec.lock().remove(cmd2.id());

        sleep_secs(1).await;
    }
}
