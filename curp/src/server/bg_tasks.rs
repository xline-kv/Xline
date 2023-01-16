use std::{collections::HashMap, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use madsim::rand::{thread_rng, Rng};
use tokio::{sync::mpsc, task::JoinHandle, time::MissedTickBehavior};
use tracing::{debug, error, info, warn};

use super::{
    raw_curp::{AppendEntries, RawCurp, TickAction, Vote},
    SyncMessage,
};
use crate::{
    cmd::Command,
    log::LogEntry,
    message::ServerId,
    rpc::{
        self,
        connect::{Connect, ConnectInterface},
        AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse,
    },
    TxFilter,
};

/// Connects
type Connects<Conn> = HashMap<ServerId, Arc<Conn>>;

/// Run background tasks
#[allow(clippy::too_many_arguments)] // we call this function once, it's ok
pub(super) async fn run_bg_tasks<C: Command + 'static>(
    curp: Arc<RawCurp<C>>,
    others: HashMap<ServerId, String>,
    sync_rx: flume::Receiver<SyncMessage<C>>,
    shutdown_trigger: Arc<Event>,
    tx_filter: Option<Box<dyn TxFilter>>,
) {
    // establish connection with other servers
    let connects: Connects<Connect> = rpc::connect(others, tx_filter).await;

    // carry the index of the log that needs to be replicated on every follower
    let (ae_trigger, ae_trigger_rx) = mpsc::unbounded_channel();

    // carry id of a server that needs to be calibrated by the leader
    let (calibrate_tx, calibrate_rx) = mpsc::unbounded_channel();

    let bg_tick_handle = tokio::spawn(bg_tick(
        Arc::clone(&curp),
        connects.clone(),
        calibrate_tx.clone(),
    ));
    let bg_ae_handle = tokio::spawn(bg_append_entries(
        Arc::clone(&curp),
        connects.clone(),
        ae_trigger_rx,
        calibrate_tx,
    ));
    let bg_calibrate_handle = tokio::spawn(bg_calibrate(
        Arc::clone(&curp),
        connects.clone(),
        calibrate_rx,
    ));
    let bg_get_sync_cmds_handle =
        tokio::spawn(bg_get_sync_cmds(Arc::clone(&curp), sync_rx, ae_trigger));

    shutdown_trigger.listen().await;
    bg_tick_handle.abort();
    bg_ae_handle.abort();
    bg_get_sync_cmds_handle.abort();
    bg_calibrate_handle.abort();
    info!("all background task stopped");
}

/// Do periodical task
async fn bg_tick<C: Command + 'static, Conn: ConnectInterface>(
    curp: Arc<RawCurp<C>>,
    connects: Connects<Conn>,
    calibrate_tx: mpsc::UnboundedSender<ServerId>,
) {
    let (heartbeat_interval, rpc_timeout) = (
        *curp.timeout().heartbeat_interval(),
        *curp.timeout().rpc_timeout(),
    );
    // wait for some random time before tick starts to minimize vote split possibility
    let rand = thread_rng()
        .gen_range(0..heartbeat_interval.as_millis())
        .numeric_cast();
    tokio::time::sleep(Duration::from_millis(rand)).await;

    let mut ticker = tokio::time::interval(heartbeat_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    #[allow(clippy::integer_arithmetic, unused_attributes)] // tokio internal triggered
    loop {
        let _now = ticker.tick().await;
        let action = curp.tick();
        match action {
            TickAction::Heartbeat(hbs) => {
                let resps = bcast_heartbeats(&connects, hbs, rpc_timeout);
                handle_heartbeat_responses(curp.as_ref(), resps, &calibrate_tx).await;
            }
            TickAction::Votes(votes) => {
                let resps = bcast_votes(&connects, votes, rpc_timeout);
                handle_vote_responses(curp.as_ref(), resps).await;
            }
            TickAction::Nothing => {}
        }
    }
}

/// Fetch commands need to be synced and add them to the log
async fn bg_get_sync_cmds<C: Command + 'static>(
    curp: Arc<RawCurp<C>>,
    sync_rx: flume::Receiver<SyncMessage<C>>,
    ae_tx: mpsc::UnboundedSender<usize>,
) {
    loop {
        let (term, cmd) = match sync_rx.recv_async().await {
            Ok(msg) => msg.inner(),
            Err(_) => {
                return;
            }
        };

        let index = curp.push_log_entry(LogEntry::new(term, &[cmd]));
        if let Err(e) = ae_tx.send(index) {
            error!("ae_trigger failed: {}", e);
        }

        debug!("{} received new log[{index}]", curp.id(),);
    }
}

/// Background `append_entries`, only works for the leader
async fn bg_append_entries<C: Command + 'static, Conn: ConnectInterface>(
    curp: Arc<RawCurp<C>>,
    connects: HashMap<ServerId, Arc<Conn>>,
    mut ae_rx: mpsc::UnboundedReceiver<usize>,
    calibrate_tx: mpsc::UnboundedSender<ServerId>,
) {
    while let Some(i) = ae_rx.recv().await {
        let req = {
            let ae = curp.append_entries_single(i);
            let req = AppendEntriesRequest::new(
                ae.term,
                ae.leader_id,
                ae.prev_log_index,
                ae.prev_log_term,
                ae.entries,
                ae.leader_commit,
            );
            match req {
                Err(e) => {
                    error!("can't serialize log entries, {e}");
                    continue;
                }
                Ok(req) => req,
            }
        };

        curp.opt_out_hb();

        // send append_entries to each server in parallel
        for connect in connects.values() {
            let _handle = tokio::spawn(send_log_until_succeed(
                Arc::clone(&curp),
                Arc::clone(connect),
                calibrate_tx.clone(),
                i,
                req.clone(),
            ));
        }
    }
}

/// Send `append_entries` containing a single log to a server
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn send_log_until_succeed<C: Command + 'static, Conn: ConnectInterface>(
    curp: Arc<RawCurp<C>>,
    connect: Arc<Conn>,
    calibrate_tx: mpsc::UnboundedSender<ServerId>,
    i: usize,
    req: AppendEntriesRequest,
) {
    let (rpc_timeout, retry_timeout) = (
        *curp.timeout().rpc_timeout(),
        *curp.timeout().retry_timeout(),
    );
    // send log[i] until succeed
    loop {
        let resp = connect.append_entries(req.clone(), rpc_timeout).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {e}");
            }
            Ok(resp) => {
                let resp = resp.into_inner();

                let result = curp.handle_append_entries_resp(
                    connect.id(),
                    Some(i),
                    resp.term,
                    resp.success,
                    resp.hint_index.numeric_cast(),
                );

                match result {
                    Ok(true) | Err(()) => return,
                    Ok(false) => {
                        if let Err(e) = calibrate_tx.send(connect.id().clone()) {
                            warn!("can't send calibrate task, {e}");
                        }
                    }
                }
            }
        }
        // wait for some time until next retry
        tokio::time::sleep(retry_timeout).await;
    }
}

/// Leader broadcast heartbeats
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
fn bcast_heartbeats<C: 'static + Command, Conn: ConnectInterface>(
    connects: &Connects<Conn>,
    hbs: HashMap<ServerId, AppendEntries<C>>,
    rpc_timeout: Duration,
) -> impl Stream<Item = (ServerId, AppendEntriesResponse)> {
    hbs.into_iter()
        .map(|(id, hb)| {
            let connect = connects
                .get(&id)
                .cloned()
                .unwrap_or_else(|| unreachable!("no server {id}'s connect"));
            let req = AppendEntriesRequest::new_heartbeat(
                hb.term,
                hb.leader_id,
                hb.prev_log_index,
                hb.prev_log_term,
                hb.leader_commit,
            );
            async move {
                let resp = connect.append_entries(req, rpc_timeout).await;
                (id, resp)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|(id, resp)| async move {
            match resp {
                Err(e) => {
                    warn!("heartbeat to {} failed, {e}", id);
                    None
                }
                Ok(resp) => Some((id, resp.into_inner())),
            }
        })
}

/// Candidate broadcasts vote
fn bcast_votes<Conn: ConnectInterface>(
    connects: &Connects<Conn>,
    votes: HashMap<ServerId, Vote>,
    rpc_timeout: Duration,
) -> impl Stream<Item = (ServerId, VoteResponse)> {
    votes
        .into_iter()
        .map(|(id, vote)| {
            let connect = connects
                .get(&id)
                .cloned()
                .unwrap_or_else(|| unreachable!("no server {id}'s connect"));
            let req = VoteRequest::new(
                vote.term,
                vote.candidate_id,
                vote.last_log_index,
                vote.last_log_term,
            );
            async move {
                let resp = connect.vote(req, rpc_timeout).await;
                (id, resp)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|(id, resp)| async move {
            match resp {
                Err(e) => {
                    warn!("request vote from {id} failed, {e}");
                    None
                }
                Ok(resp) => Some((id, resp.into_inner())),
            }
        })
}

/// Handle heartbeat responses
async fn handle_heartbeat_responses<C, S>(
    curp: &RawCurp<C>,
    resps: S,
    calibrate_tx: &mpsc::UnboundedSender<ServerId>,
) where
    C: Command + 'static,
    S: Stream<Item = (ServerId, AppendEntriesResponse)>,
{
    pin_mut!(resps);
    while let Some((id, resp)) = resps.next().await {
        let result = curp.handle_append_entries_resp(
            &id,
            None,
            resp.term,
            resp.success,
            resp.hint_index.numeric_cast(),
        );
        match result {
            Ok(true) => {}
            Ok(false) => {
                if let Err(e) = calibrate_tx.send(id) {
                    warn!("can't send calibrate task, {e}");
                }
            }
            Err(()) => return,
        }
    }
}

/// Candidate handles vote responses
async fn handle_vote_responses<C, S>(curp: &RawCurp<C>, resps: S)
where
    C: Command + 'static,
    S: Stream<Item = (ServerId, VoteResponse)>,
{
    pin_mut!(resps);
    while let Some((id, resp)) = resps.next().await {
        // collect follower spec pool
        let follower_spec_pool = match resp.spec_pool() {
            Err(e) => {
                error!("can't deserialize spec_pool from vote response, {e}");
                continue;
            }
            Ok(spec_pool) => spec_pool.into_iter().map(|cmd| Arc::new(cmd)).collect(),
        };
        let result = curp.handle_vote_resp(&id, resp.term, resp.vote_granted, follower_spec_pool);
        match result {
            Ok(false) => {}
            Ok(true) | Err(()) => return,
        }
    }
}

/// Background leader calibrate followers
async fn bg_calibrate<C: Command + 'static, Conn: ConnectInterface>(
    curp: Arc<RawCurp<C>>,
    connects: Connects<Conn>,
    mut calibrate_rx: mpsc::UnboundedReceiver<ServerId>,
) {
    let mut handlers: HashMap<ServerId, JoinHandle<()>> = HashMap::new();
    while let Some(follower_id) = calibrate_rx.recv().await {
        if handlers
            .get(&follower_id)
            .map_or(false, |hd| !hd.is_finished())
        {
            continue;
        }
        let connect = connects
            .get(&follower_id)
            .cloned()
            .unwrap_or_else(|| unreachable!("no server {follower_id}'s connect"));
        let hd = tokio::spawn(leader_calibrates_follower(Arc::clone(&curp), connect));
        let _prev_hd = handlers.insert(follower_id, hd);
    }
}

/// Leader calibrates a follower
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn leader_calibrates_follower<C: Command + 'static, Conn: ConnectInterface>(
    curp: Arc<RawCurp<C>>,
    connect: Arc<Conn>,
) {
    debug!("{} starts calibrating follower {}", curp.id(), connect.id());
    let (rpc_timeout, retry_timeout) = (
        *curp.timeout().rpc_timeout(),
        *curp.timeout().retry_timeout(),
    );
    loop {
        // send append entry
        let ae = curp.append_entries(connect.id());
        let last_sent_index = ae.prev_log_index + ae.entries.len();
        let req = match AppendEntriesRequest::new(
            ae.term,
            ae.leader_id,
            ae.prev_log_index,
            ae.prev_log_term,
            ae.entries,
            ae.leader_commit,
        ) {
            Err(e) => {
                error!("unable to serialize append entries request: {}", e);
                return;
            }
            Ok(req) => req,
        };

        let resp = connect.append_entries(req, rpc_timeout).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {}", e);
            }
            Ok(resp) => {
                let resp = resp.into_inner();

                let result = curp.handle_append_entries_resp(
                    connect.id(),
                    Some(last_sent_index),
                    resp.term,
                    resp.success,
                    resp.hint_index.numeric_cast(),
                );

                match result {
                    Ok(true) => {
                        debug!(
                            "{} successfully calibrates follower {}",
                            curp.id(),
                            connect.id()
                        );
                        return;
                    }
                    Ok(false) => {}
                    Err(()) => {
                        return;
                    }
                }
            }
        };
        tokio::time::sleep(retry_timeout).await;
    }
}

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;
    use utils::config::default_retry_timeout;

    use super::*;
    use crate::{
        error::ProposeError,
        rpc::{connect::MockConnectInterface, AppendEntriesRequest},
        server::cmd_worker::MockCmdExeSenderInterface,
        test_utils::{sleep_millis, test_cmd::TestCommand},
    };

    /*************** tests for send_log_until_succeed **************/

    #[traced_test]
    #[tokio::test]
    async fn logs_will_be_resent() {
        let curp = Arc::new(RawCurp::new_test(
            3,
            MockCmdExeSenderInterface::<TestCommand>::default(),
        ));

        let mut mock_connect = MockConnectInterface::default();
        mock_connect
            .expect_append_entries()
            .times(3..)
            .returning(|_, _| Err(ProposeError::RpcStatus("timeout".to_owned())));
        mock_connect.expect_id().return_const("S1".to_owned());
        let (tx, _rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                "S0".to_owned(),
                0,
                0,
                vec![LogEntry::new(1, &[Arc::new(TestCommand::default())])],
                0,
            )
            .unwrap();
            send_log_until_succeed(curp, Arc::new(mock_connect), tx, 1, req).await;
        });
        sleep_millis(default_retry_timeout().as_millis() as u64 * 4).await;
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[traced_test]
    #[tokio::test]
    async fn send_log_will_stop_after_new_election() {
        let curp = {
            let mut exe_tx = MockCmdExeSenderInterface::<TestCommand>::default();
            exe_tx.expect_send_reset().returning(|| ());
            Arc::new(RawCurp::new_test(3, exe_tx))
        };

        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_append_entries().returning(|_, _| {
            Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                2, 0,
            )))
        });
        mock_connect.expect_id().return_const("S1".to_owned());
        let (tx, _rx) = mpsc::unbounded_channel();

        let curp_c = Arc::clone(&curp);
        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                "S0".to_owned(),
                0,
                0,
                vec![LogEntry::new(1, &[Arc::new(TestCommand::default())])],
                0,
            )
            .unwrap();
            send_log_until_succeed(curp, Arc::new(mock_connect), tx, 1, req).await;
        });

        assert!(handle.await.is_ok());
        assert_eq!(curp_c.term(), 2);
    }

    #[traced_test]
    #[tokio::test]
    async fn send_log_will_succeed_and_commit() {
        let curp = {
            let mut exe_tx = MockCmdExeSenderInterface::<TestCommand>::default();
            exe_tx.expect_send_reset().returning(|| ());
            exe_tx.expect_send_after_sync().returning(|_, _| ());
            Arc::new(RawCurp::new_test(3, exe_tx))
        };
        let cmd = Arc::new(TestCommand::default());
        curp.push_log_entry(LogEntry::new(0, &[Arc::clone(&cmd)]));
        let (tx, _rx) = mpsc::unbounded_channel();

        let mut mock_connect = MockConnectInterface::default();
        mock_connect
            .expect_append_entries()
            .returning(|_, _| Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))));
        mock_connect.expect_id().return_const("S1".to_owned());
        let curp_c = Arc::clone(&curp);
        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                "S0".to_owned(),
                0,
                0,
                vec![LogEntry::new(1, &[cmd])],
                0,
            )
            .unwrap();
            send_log_until_succeed(curp, Arc::new(mock_connect), tx, 1, req).await;
        });

        assert!(handle.await.is_ok());
        assert_eq!(curp_c.term(), 0);
        assert_eq!(curp_c.commit_index(), 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn leader_will_calibrate_follower_until_succeed() {
        let curp = {
            let mut exe_tx = MockCmdExeSenderInterface::<TestCommand>::default();
            exe_tx.expect_send_after_sync().returning(|_, _| ());
            Arc::new(RawCurp::new_test(3, exe_tx))
        };
        let cmd1 = Arc::new(TestCommand::default());
        curp.push_log_entry(LogEntry::new(0, &[Arc::clone(&cmd1)]));
        let cmd2 = Arc::new(TestCommand::default());
        curp.push_log_entry(LogEntry::new(0, &[Arc::clone(&cmd2)]));

        let mut mock_connect = MockConnectInterface::default();
        let mut call_cnt = 0;
        mock_connect
            .expect_append_entries()
            .times(2)
            .returning(move |_, _| {
                call_cnt += 1;
                match call_cnt {
                    1 => Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                        0, 1,
                    ))),
                    2 => Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))),
                    _ => panic!("should not be called more than 2 times"),
                }
            });
        mock_connect.expect_id().return_const("S1".to_owned());

        leader_calibrates_follower(curp, Arc::new(mock_connect)).await;
    }
}
