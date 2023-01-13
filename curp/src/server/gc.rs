use std::time::Duration;

use crate::{cmd::Command, server::cmd_board::CmdBoardRef};

/// How often cmd board should
const CMD_BOARD_GC_INTERVAL: Duration = Duration::from_secs(20);

/// Run background GC tasks for Curp server
pub(super) fn run_gc_tasks<C: Command + 'static>(cmd_board: CmdBoardRef<C>) {
    let _cmd_board_gc = tokio::spawn(gc_cmd_board(cmd_board, CMD_BOARD_GC_INTERVAL));
}

/// Cleanup cmd board
async fn gc_cmd_board<C: Command + 'static>(cmd_board: CmdBoardRef<C>, interval: Duration) {
    let mut last_check_len_er = 0;
    let mut last_check_len_asr = 0;
    loop {
        tokio::time::sleep(interval).await;
        let mut board = cmd_board.write();

        let new_er_buffer = board.er_buffer.split_off(last_check_len_er);
        board.er_buffer = new_er_buffer;
        last_check_len_er = board.er_buffer.len();

        let new_asr_buffer = board.asr_buffer.split_off(last_check_len_asr);
        board.asr_buffer = new_asr_buffer;
        last_check_len_asr = board.asr_buffer.len();
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use parking_lot::RwLock;

    use crate::{
        cmd::ProposeId,
        server::{
            cmd_board::{CmdBoardRef, CommandBoard},
            gc::gc_cmd_board,
        },
        test_utils::test_cmd::TestCommand,
    };

    #[allow(unused_results, clippy::unwrap_used)]
    #[tokio::test]
    async fn cmd_board_gc_test() {
        let board: CmdBoardRef<TestCommand> = Arc::new(RwLock::new(CommandBoard::new()));
        tokio::spawn(gc_cmd_board(Arc::clone(&board), Duration::from_millis(500)));

        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId::new("1".to_owned()), Ok(vec![]));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId::new("2".to_owned()), Ok(vec![]));
        board
            .write()
            .asr_buffer
            .insert(ProposeId::new("1".to_owned()), Ok(0));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .asr_buffer
            .insert(ProposeId::new("2".to_owned()), Ok(0));

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId::new("3".to_owned()), Ok(vec![]));
        board
            .write()
            .asr_buffer
            .insert(ProposeId::new("3".to_owned()), Ok(0));

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let board = board.write();
        assert_eq!(board.er_buffer.len(), 1);
        assert_eq!(
            board.er_buffer.get_index(0).unwrap().0,
            &ProposeId::new("3".to_owned())
        );
        assert_eq!(board.asr_buffer.len(), 1);
        assert_eq!(
            board.asr_buffer.get_index(0).unwrap().0,
            &ProposeId::new("3".to_owned())
        );
    }
}
