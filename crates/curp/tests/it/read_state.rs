use std::time::Duration;

use curp::{client::ClientApi, rpc::ReadState};
use curp_test_utils::{
    init_logger, sleep_millis,
    test_cmd::{TestCommand, TestCommandResult},
};
use test_macros::abort_on_panic;

use crate::common::curp_group::CurpGroup;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn read_state() {
    init_logger();
    let group = CurpGroup::new(3).await;
    let put_client = group.new_client().await;
    let put_cmd = TestCommand::new_put(vec![0], 0).set_exe_dur(Duration::from_millis(100));
    tokio::spawn(async move {
        assert_eq!(
            put_client
                .propose(&put_cmd, None, true)
                .await
                .unwrap()
                .unwrap()
                .0,
            TestCommandResult::default(),
        );
    });
    sleep_millis(10).await;
    let get_client = group.new_client().await;
    let res = get_client
        .fetch_read_state(&TestCommand::new_get(vec![0]))
        .await
        .unwrap();
    if let ReadState::Ids(v) = res {
        assert_eq!(v.inflight_ids.len(), 1);
    } else {
        unreachable!(
            "expected result should be ReadState::Ids(v) where len(v) = 1, but received {:?}",
            res
        );
    }

    sleep_millis(500).await;

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
}
