use std::time::Duration;

use curp_test_utils::init_logger;
use madsim::time::sleep;
use simulation::xline_group::{SimEtcdClient, XlineGroup};
use xline_client::types::{
    cluster::{MemberAddRequest, MemberListRequest},
    kv::{CompactionRequest, PutRequest},
    watch::WatchRequest,
};

// TODO: Add more tests if needed

#[madsim::test]
async fn basic_put() {
    init_logger();
    let group = XlineGroup::new(3).await;
    let client = group.client().await;
    let res = client.put(PutRequest::new("key", "value")).await;
    assert!(res.is_ok());
}

#[madsim::test]
async fn watch_compacted_revision_should_receive_canceled_response() {
    init_logger();
    let group = XlineGroup::new(3).await;
    let watch_addr = group.get_node("S2").client_url.clone();

    let client = SimEtcdClient::new(watch_addr, group.client_handle.clone()).await;

    for i in 1..=6 {
        let result = client
            .put(PutRequest::new("key", format!("value{}", i)))
            .await;
        assert!(result.is_ok());
    }

    let result = client
        .compact(CompactionRequest::new(5).with_physical())
        .await;
    assert!(result.is_ok());

    let (_, mut watch_stream) = client
        .watch(WatchRequest::new("key").with_start_revision(4))
        .await
        .unwrap();
    let r = watch_stream.message().await.unwrap().unwrap();
    assert!(r.canceled);
}

#[madsim::test]
async fn xline_members_restore() {
    init_logger();
    let mut group = XlineGroup::new(3).await;
    let node = group.get_node("S1");
    let addr = node.client_url.clone();
    let client = SimEtcdClient::new(addr, group.client_handle.clone()).await;

    let res = client
        .member_add(MemberAddRequest::new(
            vec!["http://192.168.1.4:12345".to_owned()],
            true,
        ))
        .await
        .unwrap();
    assert_eq!(res.members.len(), 4);
    let members = client
        .member_list(MemberListRequest::new(false))
        .await
        .unwrap();
    assert_eq!(members.members.len(), 4);
    group.crash("S1").await;
    sleep(Duration::from_secs(10)).await;

    group.restart("S1").await;
    sleep(Duration::from_secs(10)).await;
    let members = client
        .member_list(MemberListRequest::new(false))
        .await
        .unwrap();
    assert_eq!(members.members.len(), 4);
}
