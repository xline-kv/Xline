//! `exe` stands for execution
//! `as` stands for after sync

use std::sync::Arc;

use curp_external_api::cmd::{AfterSyncCmd, AfterSyncOk};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use super::{curp_node::AfterSyncEntry, raw_curp::RawCurp};
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    response::ResponseSender,
    role_change::RoleChange,
    rpc::{ConfChangeType, PoolEntry, ProposeId, ProposeResponse, SyncedResponse},
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
    let cb = curp.cmd_board();
    let id = curp.id();
    let EntryData::Command(ref cmd) = entry.entry_data else {
        unreachable!("should not speculative execute {:?}", entry.entry_data);
    };
    if cmd.is_read_only() {
        ce.execute_ro(cmd).map(|(er, asr)| (er, Some(asr)))
    } else {
        let er = ce.execute(cmd);
        let mut cb_w = cb.write();
        cb_w.insert_er(entry.propose_id, er.clone());
        debug!(
            "{id} cmd({}) is speculatively executed, exe status: {}",
            entry.propose_id,
            er.is_ok(),
        );
        er.map(|e| (e, None))
    }
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
    let propose_ids = cmd_entries.iter().map(|(e, _)| e.propose_id);

    let results = ce.after_sync(cmds, Some(highest_index));

    send_results(curp, results.into_iter(), resp_txs, propose_ids);

    for (entry, _) in cmd_entries {
        curp.trigger(&entry.propose_id);
        ce.trigger(entry.inflight_id());
    }
    remove_from_sp_ucp(curp, cmd_entries.iter().map(|(e, _)| e));
}

/// Send cmd results to clients
fn send_results<'a, C, RC, R, S, P>(curp: &RawCurp<C, RC>, results: R, txs: S, propose_ids: P)
where
    C: Command,
    RC: RoleChange,
    R: Iterator<Item = Result<AfterSyncOk<C>, C::Error>>,
    S: Iterator<Item = Option<&'a ResponseSender>>,
    P: Iterator<Item = ProposeId>,
{
    let cb = curp.cmd_board();
    let mut cb_w = cb.write();

    for ((result, tx_opt), id) in results.zip(txs).zip(propose_ids) {
        match result {
            Ok(r) => {
                let (asr, er_opt) = r.into_parts();
                let _ignore_er = tx_opt.as_ref().zip(er_opt.as_ref()).map(|(tx, er)| {
                    tx.send_propose(ProposeResponse::new_result::<C>(&Ok(er.clone()), true));
                });
                let _ignore = er_opt.map(|er| cb_w.insert_er(id, Ok(er)));
                let _ignore_asr = tx_opt
                    .as_ref()
                    .map(|tx| tx.send_synced(SyncedResponse::new_result::<C>(&Ok(asr.clone()))));
                cb_w.insert_asr(id, Ok(asr));
            }
            Err(e) => {
                let _ignore = tx_opt.as_ref().map(|tx| tx.send_err::<C>(e.clone()));
                cb_w.insert_asr(id, Err(e));
            }
        }
    }
}

/// After sync entries other than cmd
async fn after_sync_others<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
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
            (EntryData::ConfChange(ref conf_change), _) => {
                if let Err(e) = ce.set_last_applied(entry.index) {
                    error!("failed to set last_applied, {e}");
                    return;
                }
                let change = conf_change.first().unwrap_or_else(|| {
                    unreachable!("conf change should always have at least one change")
                });
                let shutdown_self =
                    change.change_type() == ConfChangeType::Remove && change.node_id == id;
                cb.write().insert_conf(entry.propose_id);
                remove_from_sp_ucp(curp, Some(&entry));
                if shutdown_self {
                    if let Some(maybe_new_leader) = curp.pick_new_leader() {
                        info!(
                            "the old leader {} will shutdown, try to move leadership to {}",
                            id, maybe_new_leader
                        );
                        if curp
                            .handle_move_leader(maybe_new_leader)
                            .unwrap_or_default()
                        {
                            if let Err(e) = curp
                                .connects()
                                .get(&maybe_new_leader)
                                .unwrap_or_else(|| {
                                    unreachable!("connect to {} should exist", maybe_new_leader)
                                })
                                .try_become_leader_now(curp.cfg().wait_synced_timeout)
                                .await
                            {
                                warn!(
                                    "{} send try become leader now to {} failed: {:?}",
                                    curp.id(),
                                    maybe_new_leader,
                                    e
                                );
                            };
                        }
                    } else {
                        info!(
                        "the old leader {} will shutdown, but no other node can be the leader now",
                        id
                    );
                    }
                    curp.task_manager().shutdown(false).await;
                }
            }
            (EntryData::SetNodeState(node_id, ref name, ref client_urls), _) => {
                info!("setting node state: {node_id}, urls: {:?}", client_urls);
                if let Err(e) = ce.set_last_applied(entry.index) {
                    error!("failed to set last_applied, {e}");
                    return;
                }
                curp.cluster()
                    .set_node_state(*node_id, name.clone(), client_urls.clone());
            }
            // The no-op command has been applied to state machine
            (EntryData::Empty, _) => curp.set_no_op_applied(),
            _ => unreachable!(),
        }
        ce.trigger(entry.inflight_id());
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
    after_sync_others(others, ce, curp).await;
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
