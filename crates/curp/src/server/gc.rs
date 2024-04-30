use std::time::Duration;

use utils::task_manager::Listener;

use crate::{cmd::Command, rpc::ProposeId, server::cmd_board::CmdBoardRef};

use super::lease_manager::LeaseManagerRef;

// TODO: Speculative pool GC

/// Cleanup cmd board
pub(super) async fn gc_cmd_board<C: Command>(
    cmd_board: CmdBoardRef<C>,
    lease_mamanger: LeaseManagerRef,
    interval: Duration,
    shutdown_listener: Listener,
) {
    #[allow(clippy::arithmetic_side_effects, clippy::ignored_unit_patterns)]
    // introduced by tokio select
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown_listener.wait() => break,
        }
        let mut board = cmd_board.write();
        let expired_er_ids: Vec<_> = board
            .er_buffer
            .keys()
            .copied()
            .filter(|ProposeId(client_id, _)| !lease_mamanger.read().check_alive(*client_id))
            .collect();
        for id in expired_er_ids {
            let _ignore = board.er_buffer.swap_remove(&id);
        }
        let expired_asr_ids: Vec<_> = board
            .asr_buffer
            .keys()
            .copied()
            .filter(|ProposeId(client_id, _)| !lease_mamanger.read().check_alive(*client_id))
            .collect();
        for id in expired_asr_ids {
            let _ignore = board.asr_buffer.swap_remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use curp_test_utils::test_cmd::{TestCommand, TestCommandResult};
    use parking_lot::RwLock;
    use test_macros::abort_on_panic;
    use utils::task_manager::{tasks::TaskName, TaskManager};

    use crate::{
        rpc::ProposeId,
        server::{
            cmd_board::{CmdBoardRef, CommandBoard},
            gc::gc_cmd_board,
            lease_manager::LeaseManager,
        },
    };

    #[tokio::test]
    #[abort_on_panic]
    async fn cmd_board_gc_test() {
        let task_manager = TaskManager::new();
        let board: CmdBoardRef<TestCommand> = Arc::new(RwLock::new(CommandBoard::new()));
        let lease_manager = Arc::new(RwLock::new(LeaseManager::new()));
        task_manager.spawn(TaskName::GcCmdBoard, |n| {
            gc_cmd_board(
                Arc::clone(&board),
                lease_manager,
                Duration::from_millis(500),
                n,
            )
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
}
