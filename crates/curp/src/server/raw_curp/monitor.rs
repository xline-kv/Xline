use std::collections::BTreeMap;

use curp_external_api::{cmd::Command, role_change::RoleChange, LogIndex};
use tokio::sync::broadcast;

use super::RawCurp;

impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Adds new nodes to monitor
    pub(crate) fn register_monitoring<Ids: IntoIterator<Item = u64>>(
        &self,
        node_ids: Ids,
    ) -> BTreeMap<u64, broadcast::Receiver<(LogIndex, LogIndex)>> {
        /// Max number of receivers
        const MAX_RECEIVERS: usize = 1024;
        let mut monitoring_w = self.ctx.monitoring.write();
        node_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    monitoring_w
                        .entry(id)
                        .or_insert_with(|| broadcast::channel(MAX_RECEIVERS).0)
                        .subscribe(),
                )
            })
            .collect()
    }
}
