//! `exe` stands for execution
//! `as` stands for after sync

use std::sync::Arc;

use curp_external_api::cmd::{AfterSyncCmd, AfterSyncOk};
use tokio::sync::oneshot;
use tracing::{debug, error, info};

use super::{curp_node::AfterSyncEntry, raw_curp::RawCurp};
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    response::ResponseSender,
    role_change::RoleChange,
    rpc::{PoolEntry, ProposeResponse, SyncedResponse},
    snapshot::{Snapshot, SnapshotMeta},
};

/// Removes an entry from sp and ucp
fn remove_from_sp_ucp<C, RC, E, I>(curp: &RawCurp<C, RC>, entries: I)
where
    C: Command,
    RC: RoleChange,
    E: AsRef<LogEntry<C>>,
    I: IntoIterator<Item = E>,
{
    let (mut sp, mut ucp) = (curp.spec_pool().lock(), curp.uncommitted_pool().lock());
    for entry in entries {
        let entry = entry.as_ref();
        if let EntryData::Command(ref c) = entry.entry_data {
            let pool_entry = PoolEntry::new(entry.propose_id, Arc::clone(c));
            sp.remove(&pool_entry);
            ucp.remove(&pool_entry);
        };
    }
}

/// ER and ASR
type ErAsr<C> = (<C as Command>::ER, Option<<C as Command>::ASR>);

/// Cmd worker execute handler
pub(super) fn execute<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    entry: &LogEntry<C>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> Result<ErAsr<C>, <C as Command>::Error> {
    let EntryData::Command(ref cmd) = entry.entry_data else {
        unreachable!("should not speculative execute {:?}", entry.entry_data);
    };
    let result = if cmd.is_read_only() {
        ce.execute_ro(cmd).map(|(er, asr)| (er, Some(asr)))
    } else {
        ce.execute(cmd).map(|er| (er, None))
    };
    debug!(
        "{} cmd({}) is speculatively executed, exe status: {}",
        curp.id(),
        entry.propose_id,
        result.is_ok(),
    );
    result
}

/// After sync cmd entries
#[allow(clippy::pattern_type_mismatch)] // Can't be fixed
fn after_sync_cmds<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    cmd_entries: &[AfterSyncEntry<C>],
    ce: &CE,
    curp: &RawCurp<C, RC>,
) {
    if cmd_entries.is_empty() {
        return;
    }
    info!("after sync: {cmd_entries:?}");
    let resp_txs = cmd_entries
        .iter()
        .map(|(_, tx)| tx.as_ref().map(AsRef::as_ref));
    let highest_index = cmd_entries
        .last()
        .map_or_else(|| unreachable!(), |(entry, _)| entry.index);
    let cmds: Vec<_> = cmd_entries
        .iter()
        .map(|(entry, tx)| {
            let EntryData::Command(ref cmd) = entry.entry_data else {
                unreachable!("only allows command entry");
            };
            AfterSyncCmd::new(
                cmd.as_ref(),
                // If the response sender is absent, it indicates that a new leader
                // has been elected, and the entry has been recovered from the log
                // or the speculative pool. In such cases, these entries needs to
                // be re-executed.
                tx.as_ref().map_or(true, |t| t.is_conflict()),
            )
        })
        .collect();
    let results = ce.after_sync(cmds, Some(highest_index));

    send_as_results(results.into_iter(), resp_txs);

    for (entry, _) in cmd_entries {
        curp.trigger(&entry.propose_id);
        ce.trigger(entry.inflight_id());
    }
    remove_from_sp_ucp(curp, cmd_entries.iter().map(|(e, _)| e));
}

/// Send cmd results to clients
fn send_as_results<'a, C, R, S>(results: R, txs: S)
where
    C: Command,
    R: Iterator<Item = Result<AfterSyncOk<C>, C::Error>>,
    S: Iterator<Item = Option<&'a ResponseSender>>,
{
    for (result, tx_opt) in results.zip(txs) {
        match result {
            Ok(r) => {
                let (asr, er_opt) = r.into_parts();
                let _ignore_er = tx_opt.as_ref().zip(er_opt.as_ref()).map(|(tx, er)| {
                    // In after sync result, `sp_version` could be safely ignored (set to 0) as the
                    // command has successfully replicated to the majority of nodes
                    tx.send_propose(ProposeResponse::new_result::<C>(&Ok(er.clone()), true, 0));
                });
                let _ignore_asr = tx_opt
                    .as_ref()
                    .map(|tx| tx.send_synced(SyncedResponse::new_result::<C>(&Ok(asr.clone()))));
            }
            Err(e) => {
                let _ignore = tx_opt.as_ref().map(|tx| tx.send_err::<C>(e.clone()));
            }
        }
    }
}

/// After sync entries other than cmd
fn after_sync_others<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    others: Vec<AfterSyncEntry<C>>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) {
    let id = curp.id();
    let cb = curp.cmd_board();
    #[allow(clippy::pattern_type_mismatch)] // Can't be fixed
    for (entry, resp_tx) in others {
        match (&entry.entry_data, resp_tx) {
            (EntryData::Shutdown, _) => {
                curp.task_manager().cluster_shutdown();
                if curp.is_leader() {
                    curp.task_manager().mark_leader_notified();
                }
                if let Err(e) = ce.set_last_applied(entry.index) {
                    error!("failed to set last_applied, {e}");
                }
                cb.write().notify_shutdown();
            }
            // The no-op command has been applied to state machine
            (EntryData::Empty, _) => curp.set_no_op_applied(),
            (EntryData::Member(_), _) => {}
            (EntryData::SpecPoolReplication(r), _) => {
                if let Err(err) = curp.gc_spec_pool(r.ids(), r.version()) {
                    error!("failed to gc spec pool: {err:?}");
                }
            }

            _ => unreachable!(),
        }
        ce.trigger(entry.inflight_id());
        curp.trigger(&entry.propose_id);
        debug!("{id} cmd({}) after sync is called", entry.propose_id);
    }
}

/// Cmd worker after sync handler
pub(super) async fn after_sync<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    entries: Vec<AfterSyncEntry<C>>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) {
    #[allow(clippy::pattern_type_mismatch)] // Can't be fixed
    let (cmd_entries, others): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|(entry, _)| matches!(entry.entry_data, EntryData::Command(_)));
    after_sync_cmds(&cmd_entries, ce, curp);
    after_sync_others(others, ce, curp);
}

/// Cmd worker reset handler
pub(super) async fn worker_reset<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    snapshot: Option<Snapshot>,
    finish_tx: oneshot::Sender<()>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    let id = curp.id();
    if let Some(snapshot) = snapshot {
        let meta = snapshot.meta;
        #[allow(clippy::expect_used)] // only in debug
        if let Err(e) = ce
            .reset(Some((snapshot.into_inner(), meta.last_included_index)))
            .await
        {
            error!("reset failed, {e}");
        } else {
            debug_assert_eq!(
                ce.last_applied()
                    .expect("failed to get last_applied from ce"),
                meta.last_included_index,
                "inconsistent last_applied"
            );
            debug!("{id}'s command executor has been reset by a snapshot");
            curp.reset_by_snapshot(meta);
        }
    } else {
        if let Err(e) = ce.reset(None).await {
            error!("reset failed, {e}");
        }
        debug!("{id}'s command executor has been restored to the initial state");
    }
    let _ig = finish_tx.send(());
    true
}

/// Cmd worker snapshot handler
pub(super) async fn worker_snapshot<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    meta: SnapshotMeta,
    tx: oneshot::Sender<Snapshot>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    match ce.snapshot().await {
        Ok(snapshot) => {
            debug_assert!(
                ce.last_applied()
                    .is_ok_and(|last_applied| last_applied <= meta.last_included_index),
                " the `last_as` should always be less than or equal to the `last_exe`"
            ); // sanity check
            let snapshot = Snapshot::new(meta, snapshot);
            debug!("{} takes a snapshot, {snapshot:?}", curp.id());
            if tx.send(snapshot).is_err() {
                error!("snapshot oneshot closed");
            }
            true
        }
        Err(e) => {
            error!("snapshot failed, {e}");
            false
        }
    }
}
