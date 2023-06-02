use std::time::Instant;

use tokio::{sync::oneshot, time::sleep};
use tracing_test::traced_test;
use utils::config::{
    default_candidate_timeout_ticks, default_follower_timeout_ticks, default_heartbeat_interval,
};

use super::*;
use crate::{
    server::{
        cmd_board::CommandBoard,
        cmd_worker::{CEEventTxApi, MockCEEventTxApi},
        raw_curp::UncommittedPool,
        spec_pool::SpeculativePool,
    },
    test_utils::{mock_role_change, test_cmd::TestCommand},
    LogIndex,
};

// Hooks for tests
impl<C: 'static + Command, RC: RoleChange + 'static> RawCurp<C, RC> {
    fn role(&self) -> Role {
        self.st.read().role
    }

    pub(crate) fn commit_index(&self) -> LogIndex {
        self.log.read().commit_index
    }

    pub(crate) fn new_test<Tx: CEEventTxApi<C>>(n: u64, exe_tx: Tx, role_change: RC) -> Self {
        let others: HashSet<ServerId> = (1..n).map(|i| format!("S{i}")).collect();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let sync_events = others
            .iter()
            .map(|id| (id.clone(), Arc::new(Event::new())))
            .collect();

        Self::new(
            "S0".to_owned(),
            others,
            true,
            cmd_board,
            spec_pool,
            uncommitted_pool,
            Arc::new(CurpConfig::default()),
            Arc::new(exe_tx),
            sync_events,
            log_tx,
            role_change,
        )
    }

    /// Add a new cmd to the log, will return log entry index
    pub(crate) fn push_cmd(&self, cmd: Arc<C>) -> LogIndex {
        let st_r = self.st.read();
        let mut log_w = self.log.write();
        log_w.push_cmd(st_r.term, cmd).unwrap()
    }
}

/*************** tests for propose **************/
#[traced_test]
#[test]
fn leader_handle_propose_will_succeed() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx.expect_send_sp_exe().returning(|_, _| {});
        RawCurp::new_test(3, exe_tx, mock_role_change())
    };
    let cmd = Arc::new(TestCommand::default());
    let ((leader_id, term), result) = curp.handle_propose(cmd);
    assert_eq!(leader_id, Some(curp.id().clone()));
    assert_eq!(term, 0);
    assert!(matches!(result, Ok(true)));
}

#[traced_test]
#[test]
fn leader_handle_propose_will_reject_conflicted() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx.expect_send_sp_exe().returning(|_, _| {});
        RawCurp::new_test(3, exe_tx, mock_role_change())
    };

    let cmd1 = Arc::new(TestCommand::new_put(vec![1], 0));
    let ((leader_id, term), result) = curp.handle_propose(cmd1);
    assert_eq!(leader_id, Some(curp.id().clone()));
    assert_eq!(term, 0);
    assert!(matches!(result, Ok(true)));

    let cmd2 = Arc::new(TestCommand::new_put(vec![1, 2], 1));
    let ((leader_id, term), result) = curp.handle_propose(cmd2);
    assert_eq!(leader_id, Some(curp.id().clone()));
    assert_eq!(term, 0);
    assert!(matches!(result, Err(ProposeError::KeyConflict)));

    // leader will also reject cmds that conflict un-synced cmds
    let cmd3 = Arc::new(TestCommand::new_put(vec![2], 1));
    let ((leader_id, term), result) = curp.handle_propose(cmd3);
    assert_eq!(leader_id, Some(curp.id().clone()));
    assert_eq!(term, 0);
    assert!(matches!(result, Err(ProposeError::KeyConflict)));
}

#[traced_test]
#[test]
fn leader_handle_propose_will_reject_duplicated() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx.expect_send_sp_exe().returning(|_, _| {});
        RawCurp::new_test(3, exe_tx, mock_role_change())
    };
    let cmd = Arc::new(TestCommand::default());
    let ((leader_id, term), result) = curp.handle_propose(Arc::clone(&cmd));
    assert_eq!(leader_id, Some(curp.id().clone()));
    assert_eq!(term, 0);
    assert!(matches!(result, Ok(true)));

    let ((leader_id, term), result) = curp.handle_propose(cmd);
    assert_eq!(leader_id, Some(curp.id().clone()));
    assert_eq!(term, 0);
    assert!(matches!(result, Err(ProposeError::Duplicated)));
}

#[traced_test]
#[test]
fn follower_handle_propose_will_succeed() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);
    let cmd = Arc::new(TestCommand::new_get(vec![1]));
    let ((leader_id, term), result) = curp.handle_propose(cmd);
    assert_eq!(leader_id, None);
    assert_eq!(term, 1);
    assert!(matches!(result, Ok(false)));
}

#[traced_test]
#[test]
fn follower_handle_propose_will_reject_conflicted() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let cmd1 = Arc::new(TestCommand::new_get(vec![1]));
    let ((leader_id, term), result) = curp.handle_propose(cmd1);
    assert_eq!(leader_id, None);
    assert_eq!(term, 1);
    assert!(matches!(result, Ok(false)));

    let cmd2 = Arc::new(TestCommand::new_get(vec![1]));
    let ((leader_id, term), result) = curp.handle_propose(cmd2);
    assert_eq!(leader_id, None);
    assert_eq!(term, 1);
    assert!(matches!(result, Err(ProposeError::KeyConflict)));
}

/*************** tests for append_entries(heartbeat) **************/

#[traced_test]
#[test]
fn heartbeat_will_calibrate_term() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        RawCurp::new_test(3, exe_tx, mock_role_change())
    };

    let result = curp.handle_append_entries_resp(&"S1".to_owned(), None, 1, false, 1);
    assert!(result.is_err());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 1);
    assert_eq!(st_r.role, Role::Follower);
}

#[traced_test]
#[test]
fn heartbeat_will_calibrate_next_index() {
    let curp = RawCurp::new_test(
        3,
        MockCEEventTxApi::<TestCommand>::default(),
        mock_role_change(),
    );

    let result = curp.handle_append_entries_resp(&"S1".to_owned(), None, 0, false, 1);
    assert_eq!(result, Ok(false));

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 0);
    assert_eq!(curp.lst.get_next_index(&"S1".to_string()), 1);
}

#[traced_test]
#[test]
fn handle_ae_will_calibrate_term() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let result = curp.handle_append_entries(2, "S2".to_owned(), 0, 0, vec![], 0);
    assert!(result.is_ok());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 2);
    assert_eq!(st_r.role, Role::Follower);
    assert_eq!(st_r.leader_id, Some("S2".to_owned()));
}

#[traced_test]
#[test]
fn handle_ae_will_set_leader_id() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let result = curp.handle_append_entries(1, "S2".to_owned(), 0, 0, vec![], 0);
    assert!(result.is_ok());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 1);
    assert_eq!(st_r.role, Role::Follower);
    assert_eq!(st_r.leader_id, Some("S2".to_owned()));
}

#[traced_test]
#[test]
fn handle_ae_will_reject_wrong_term() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let result = curp.handle_append_entries(0, "S2".to_owned(), 0, 0, vec![], 0);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().0, 1);
}

#[traced_test]
#[test]
fn handle_ae_will_reject_wrong_log() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let result = curp.handle_append_entries(
        1,
        "S2".to_owned(),
        1,
        1,
        vec![LogEntry::new(2, 1, Arc::new(TestCommand::default()))],
        0,
    );
    assert_eq!(result, Err((1, 1)));
}

/*************** tests for election **************/

#[traced_test]
#[tokio::test]
async fn follower_will_not_start_election_when_heartbeats_are_received() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let curp_c = Arc::clone(&curp);
    let handle = tokio::spawn(async move {
        loop {
            sleep(default_heartbeat_interval()).await;
            let action = curp_c.tick_election();
            assert!(matches!(action, None));
        }
    });

    for _ in 0..default_follower_timeout_ticks() * 5 {
        sleep(default_heartbeat_interval()).await;
        curp.reset_election_tick();
    }

    assert!(!handle.is_finished());
    handle.abort();
}

#[traced_test]
#[tokio::test]
async fn follower_or_candidate_will_start_election_if_timeout() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    let start = Instant::now();
    let mut follower_election = None;
    loop {
        sleep(default_heartbeat_interval()).await;
        let role = curp.role();
        let action = curp.tick_election();
        if matches!(action, Some(_)) && role == Role::Follower {
            let now = Instant::now();
            let dur = now - start;
            assert!(dur >= default_heartbeat_interval() * default_follower_timeout_ticks() as u32);
            assert!(
                dur <= default_heartbeat_interval() * default_follower_timeout_ticks() as u32 * 2
                    + default_heartbeat_interval() // plus another interval to tolerate deviation
            );
            follower_election = Some(now);
        }
        if matches!(action, Some(_)) && role == Role::Candidate {
            let prev = follower_election.unwrap();
            let now = Instant::now();

            let dur = now - prev;
            assert!(dur >= default_heartbeat_interval() * default_candidate_timeout_ticks() as u32);
            assert!(
                dur <= default_heartbeat_interval() * default_candidate_timeout_ticks() as u32 * 2
                    + default_heartbeat_interval() // plus another interval to tolerate deviation
            );
            return;
        }
    }
}

#[traced_test]
#[test]
fn handle_vote_will_calibrate_term() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };

    let result = curp.handle_vote(1, "S1".to_owned(), 0, 0).unwrap();
    assert_eq!(result.0, 1);

    assert_eq!(curp.term(), 1);
    assert_eq!(curp.role(), Role::Follower);
}

#[traced_test]
#[test]
fn handle_vote_will_reject_smaller_term() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 2);

    let result = curp.handle_vote(1, "S1".to_owned(), 0, 0);
    assert_eq!(result, Err(2));
}

#[traced_test]
#[test]
fn handle_vote_will_reject_outdated_candidate() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    let result = curp.handle_append_entries(
        1,
        "S2".to_owned(),
        0,
        0,
        vec![LogEntry::new(1, 1, Arc::new(TestCommand::default()))],
        0,
    );
    assert!(result.is_ok());

    let result = curp.handle_vote(3, "S1".to_owned(), 0, 0);
    assert_eq!(result, Err(3));
}

#[traced_test]
#[test]
fn candidate_will_become_leader_after_election_succeeds() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    // tick till election starts
    while curp.role() != Role::Candidate {
        let _ig = curp.tick_election();
    }

    let result = curp
        .handle_vote_resp(&"S1".to_owned(), 2, true, vec![])
        .unwrap();
    assert!(result);
    assert_eq!(curp.role(), Role::Leader);

    let result = curp.handle_vote_resp(&"S2".to_owned(), 2, true, vec![]);
    assert!(result.is_err());
    assert_eq!(curp.role(), Role::Leader);
}

#[traced_test]
#[test]
fn vote_will_calibrate_candidate_term() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    // tick till election starts
    while curp.role() != Role::Candidate {
        let _ig = curp.tick_election();
    }

    let result = curp.handle_vote_resp(&"S1".to_owned(), 3, false, vec![]);
    assert!(result.is_err());

    let st_r = curp.st.read();
    assert_eq!(st_r.term, 3);
    assert_eq!(st_r.role, Role::Follower);
}

/*************** tests for recovery **************/

#[traced_test]
#[test]
fn recover_from_spec_pools_will_pick_the_correct_cmds() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(5, exe_tx, mock_role_change()))
    };
    curp.update_to_term_and_become_follower(&mut *curp.st.write(), 1);

    // cmd1 has already been committed
    let cmd0 = Arc::new(TestCommand::new_put(vec![1], 1));
    // cmd2 has been speculatively successfully but not committed yet
    let cmd1 = Arc::new(TestCommand::new_put(vec![2], 1));
    // cmd3 has been speculatively successfully by the leader but not stored by the superquorum of the followers
    let cmd2 = Arc::new(TestCommand::new_put(vec![3], 1));
    curp.push_cmd(Arc::clone(&cmd0));
    curp.log.map_write(|mut log_w| log_w.commit_index = 1);

    let spec_pools = HashMap::from([
        ("S0".to_owned(), vec![Arc::clone(&cmd1), Arc::clone(&cmd2)]),
        ("S1".to_owned(), vec![Arc::clone(&cmd1)]),
        ("S2".to_owned(), vec![Arc::clone(&cmd1)]),
        ("S3".to_owned(), vec![Arc::clone(&cmd1)]),
        ("S4".to_owned(), vec![]),
    ]);

    curp.recover_from_spec_pools(&mut *curp.st.write(), &mut *curp.log.write(), &spec_pools);

    curp.log.map_read(|log_r| {
        assert_eq!(log_r[1].cmd.id(), cmd0.id());
        assert_eq!(log_r[2].cmd.id(), cmd1.id());
        assert_eq!(log_r.last_log_index(), 2);
    });
}

/*************** tests for other small functions **************/

#[traced_test]
#[test]
fn quorum() {
    let curp = {
        let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
        exe_tx
            .expect_send_reset()
            .returning(|_| oneshot::channel().1);
        Arc::new(RawCurp::new_test(5, exe_tx, mock_role_change()))
    };
    assert_eq!(curp.quorum(), 3);
    assert_eq!(curp.recover_quorum(), 2);
}
