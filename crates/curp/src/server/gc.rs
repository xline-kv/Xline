use std::time::Duration;

use utils::task_manager::Listener;

use crate::{cmd::Command, rpc::ProposeId, server::cmd_board::CmdBoardRef};

use super::{conflict::spec_pool_new::SpeculativePoolRef, lease_manager::LeaseManagerRef};

/// Garbage collects relevant objects when the client lease expires
pub(super) async fn gc_client_lease<C: Command>(
    lease_mamanger: LeaseManagerRef,
    cmd_board: CmdBoardRef,
    sp: SpeculativePoolRef<C>,
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

        let mut lm_w = lease_mamanger.write();
        let board = cmd_board.read();
        let mut sp_l = sp.lock();
        let expired_ids = lm_w.gc_expired();

        let mut expired_propose_ids = Vec::new();
        for id in expired_ids {
            if let Some(tracker) = board.trackers.get(&id) {
                let incompleted_nums = tracker.all_incompleted();
                expired_propose_ids
                    .extend(incompleted_nums.into_iter().map(|num| ProposeId(id, num)));
            }
        }
        for id in &expired_propose_ids {
            sp_l.remove_by_id(id);
        }
    }
}
