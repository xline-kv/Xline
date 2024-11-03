use curp_external_api::{cmd::Command, role_change::RoleChange};

use crate::{
    rpc::{CurpError, ProposeId},
    server::cmd_board::CommandBoard,
};

use super::RawCurp;

impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Process deduplication and acknowledge the `first_incomplete` for this
    /// client id
    pub(crate) fn deduplicate(
        &self,
        ProposeId(client_id, seq_num): ProposeId,
        first_incomplete: Option<u64>,
    ) -> Result<(), CurpError> {
        // deduplication
        if self.ctx.lm.read().check_alive(client_id) {
            let mut cb_w = self.ctx.cb.write();
            let tracker = cb_w.tracker(client_id);
            if tracker.only_record(seq_num) {
                // TODO: obtain the previous ER from cmd_board and packed into
                // CurpError::Duplicated as an entry.
                return Err(CurpError::duplicated());
            }
            if let Some(first_incomplete) = first_incomplete {
                let before = tracker.first_incomplete();
                if tracker.must_advance_to(first_incomplete) {
                    for seq_num_ack in before..first_incomplete {
                        Self::ack(ProposeId(client_id, seq_num_ack), &mut cb_w);
                    }
                }
            }
        } else {
            self.ctx.cb.write().client_expired(client_id);
            return Err(CurpError::expired_client_id());
        }
        Ok(())
    }

    /// Acknowledge the propose id and GC it's cmd board result
    fn ack(id: ProposeId, cb: &mut CommandBoard<C>) {
        let _ignore_er = cb.er_buffer.swap_remove(&id);
        let _ignore_asr = cb.asr_buffer.swap_remove(&id);
        let _ignore_conf = cb.conf_buffer.swap_remove(&id);
    }
}
