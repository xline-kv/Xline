use std::time::Duration;

use curp::{client::ReadState, cmd::Command};
use curp_test_utils::{init_logger, sleep_millis, test_cmd::TestCommand};
use test_macros::abort_on_panic;
use utils::config::ClientTimeout;

use crate::common::curp_group::CurpGroup;

mod common;

#[tokio::test]
#[abort_on_panic]
async fn read_state() {
    init_logger();
    let group = CurpGroup::new(3).await;
    let put_client = group.new_client(ClientTimeout::default()).await;
    let put_cmd = TestCommand::new_put(vec![0], 0).set_exe_dur(Duration::from_millis(100));
    let put_id = put_cmd.id().clone();
    tokio::spawn(async move {
        assert_eq!(
            put_client.propose(put_cmd, true).await.unwrap().0,
            (vec![], vec![])
        );
    });
    let get_client = group.new_client(ClientTimeout::default()).await;
    let res = get_client
        .fetch_read_state(&TestCommand::new_get(vec![0]))
        .await
        .unwrap();
    if let ReadState::Ids(v) = res {
        assert_eq!(v, vec![put_id])
    } else {
        unreachable!(
            "expected result should be ReadState::Ids({:?}), but received {:?}",
            vec![put_id],
            res
        );
    }

    sleep_millis(100).await;

    let res = get_client
        .fetch_read_state(&TestCommand::new_get(vec![0]))
        .await
        .unwrap();
    if let ReadState::CommitIndex(index) = res {
        assert_eq!(index, 1);
    } else {
        unreachable!(
            "expected result should be ReadState::CommitIndex({:?}), but received {:?}",
            1, res
        );
    }
    group.stop();
}
