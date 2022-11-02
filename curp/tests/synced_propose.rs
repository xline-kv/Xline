use curp::cmd::ProposeId;

use crate::common::{create_servers_client, TestCommand, TestCommandResult, TestCommandType};

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn synced_propose() {
    tracing_subscriber::fmt::init();
    let (mut exe_rx, mut after_sync_rx, client) = create_servers_client().await;
    let result = client
        .propose_indexed(TestCommand::new(
            ProposeId::new("id1".to_owned()),
            TestCommandType::Get,
            vec!["A".to_owned()],
            None,
        ))
        .await;

    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        (TestCommandResult::GetResult("".to_owned()), 1) // log[0] is a fake one
    );

    for _ in 0..3 {
        let (t, key) = exe_rx.recv().await.unwrap();
        assert_eq!(t, TestCommandType::Get);
        assert_eq!(key, "A".to_owned());
    }

    for _ in 0..3 {
        let (t, key) = after_sync_rx.recv().await.unwrap();
        assert_eq!(t, TestCommandType::Get);
        assert_eq!(key, "A".to_owned());
    }
}
