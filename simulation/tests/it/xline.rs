use curp_test_utils::init_logger;
use simulation::xline_group::XlineGroup;
use xline_client::types::kv::PutRequest;

#[madsim::test]
async fn basic_put() {
    init_logger();
    let group = XlineGroup::new(3).await;
    let client = group.client().await;
    let res = client.put(PutRequest::new("key", "value")).await;
    assert!(res.is_ok());
}
