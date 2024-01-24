use xline_client::{
    error::Result,
    types::lease::{
        LeaseGrantRequest, LeaseKeepAliveRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest,
    },
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
async fn grant_revoke_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let mut client = client.lease_client();

    let resp = client.grant(LeaseGrantRequest::new(123)).await?;
    assert_eq!(resp.ttl, 123);
    let id = resp.id;
    client.revoke(LeaseRevokeRequest::new(id)).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_alive_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let mut client = client.lease_client();

    let resp = client.grant(LeaseGrantRequest::new(60)).await?;
    assert_eq!(resp.ttl, 60);
    let id = resp.id;

    let (mut keeper, mut stream) = client.keep_alive(LeaseKeepAliveRequest::new(id)).await?;
    keeper.keep_alive()?;

    let resp = stream.message().await?.unwrap();
    assert_eq!(resp.id, keeper.id());
    assert_eq!(resp.ttl, 60);

    client.revoke(LeaseRevokeRequest::new(id)).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn time_to_live_ttl_is_consistent_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let mut client = client.lease_client();

    let lease_id = 200;
    let resp = client
        .grant(LeaseGrantRequest::new(60).with_id(lease_id))
        .await?;
    assert_eq!(resp.ttl, 60);
    assert_eq!(resp.id, lease_id);

    let resp = client
        .time_to_live(LeaseTimeToLiveRequest::new(lease_id))
        .await?;
    assert_eq!(resp.id, lease_id);
    assert_eq!(resp.granted_ttl, 60);

    client.revoke(LeaseRevokeRequest::new(lease_id)).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn leases_should_include_granted_in_normal_path() -> Result<()> {
    let lease1 = 100;
    let lease2 = 101;
    let lease3 = 102;

    let (_cluster, client) = get_cluster_client().await.unwrap();
    let mut client = client.lease_client();

    let resp = client
        .grant(LeaseGrantRequest::new(60).with_id(lease1))
        .await?;
    assert_eq!(resp.ttl, 60);
    assert_eq!(resp.id, lease1);

    let resp = client
        .grant(LeaseGrantRequest::new(60).with_id(lease2))
        .await?;
    assert_eq!(resp.ttl, 60);
    assert_eq!(resp.id, lease2);

    let resp = client
        .grant(LeaseGrantRequest::new(60).with_id(lease3))
        .await?;
    assert_eq!(resp.ttl, 60);
    assert_eq!(resp.id, lease3);

    let resp = client.leases().await?;
    let leases: Vec<_> = resp.leases.iter().map(|status| status.id).collect();
    assert!(leases.contains(&lease1));
    assert!(leases.contains(&lease2));
    assert!(leases.contains(&lease3));

    client.revoke(LeaseRevokeRequest::new(lease1)).await?;
    client.revoke(LeaseRevokeRequest::new(lease2)).await?;
    client.revoke(LeaseRevokeRequest::new(lease3)).await?;

    Ok(())
}
