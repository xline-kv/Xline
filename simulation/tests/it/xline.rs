use curp_test_utils::init_logger;
use simulation::xline_group::{SimEtcdClient, XlineGroup};
use xline_client::types::{
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
    let watch_addr = group.get_node_by_name("S2").addr.clone();

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
