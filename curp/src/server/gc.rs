use std::{sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::time::Instant;

use super::spec_pool::SpeculativePool;
use crate::{cmd::Command, server::cmd_board::CommandBoard};

/// How often spec should GC
const SPEC_GC_INTERVAL: Duration = Duration::from_secs(10);

/// How often cmd board should
const CMD_BOARD_GC_INTERVAL: Duration = Duration::from_secs(20);

/// Run background GC tasks for Curp server
pub(super) fn run_gc_tasks<C: Command + 'static>(
    spec: Arc<Mutex<SpeculativePool<C>>>,
    cmd_board: Arc<Mutex<CommandBoard>>,
) {
    let _spec_gc_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(SPEC_GC_INTERVAL).await;
            spec.lock().gc();
        }
    });

    let _cmd_board_gc = tokio::spawn(gc_cmd_board(cmd_board, CMD_BOARD_GC_INTERVAL));
}

impl<C: Command + 'static> SpeculativePool<C> {
    /// Speculative pool GC
    pub(super) fn gc(&mut self) {
        let now = Instant::now();
        self.ready.retain(|_, time| now - *time >= SPEC_GC_INTERVAL);
    }
}

/// Cleanup cmd board
async fn gc_cmd_board(cmd_board: Arc<Mutex<CommandBoard>>, interval: Duration) {
    let mut last_check_len = 0;
    loop {
        tokio::time::sleep(interval).await;
        let mut board = cmd_board.lock();
        let new_board = board.cmd_states.split_off(last_check_len);
        board.cmd_states = new_board;
        last_check_len = board.cmd_states.len();
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use parking_lot::Mutex;

    use crate::{
        cmd::ProposeId,
        server::{
            cmd_board::{CmdState, CommandBoard},
            gc::gc_cmd_board,
        },
    };

    #[allow(unused_results, clippy::unwrap_used)]
    #[tokio::test]
    async fn cmd_board_gc_test() {
        let board = Arc::new(Mutex::new(CommandBoard::new()));
        tokio::spawn(gc_cmd_board(Arc::clone(&board), Duration::from_millis(500)));

        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .lock()
            .cmd_states
            .insert(ProposeId::new("1".to_owned()), CmdState::EarlyArrive);
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .lock()
            .cmd_states
            .insert(ProposeId::new("2".to_owned()), CmdState::EarlyArrive);

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        board
            .lock()
            .cmd_states
            .insert(ProposeId::new("3".to_owned()), CmdState::EarlyArrive);

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let board = board.lock();
        assert_eq!(board.cmd_states.len(), 1);
        assert_eq!(
            board.cmd_states.get_index(0).unwrap().0,
            &ProposeId::new("3".to_owned())
        );
    }
}
