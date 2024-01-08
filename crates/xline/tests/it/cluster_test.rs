use std::{error::Error, time::Duration};

use etcd_client::ConnectOptions;
use test_macros::abort_on_panic;
use tokio::{net::TcpListener, time::sleep};
use utils::certs;
use xline_client::{
    types::{
        cluster::{MemberAddRequest, MemberListRequest, MemberRemoveRequest, MemberUpdateRequest},
        kv::PutRequest,
    },
    Client, ClientOptions,
};
use xline_test_utils::Cluster;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn xline_remove_node() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(5).await;
    cluster.start().await;
    let mut cluster_client = Client::connect(cluster.addrs(), ClientOptions::default())
        .await?
        .cluster_client();
    let list_res = cluster_client
        .member_list(MemberListRequest::new(false))
        .await?;
    assert_eq!(list_res.members.len(), 5);
    let remove_id = list_res.members[0].id;
    let remove_req = MemberRemoveRequest::new(remove_id);
    let remove_res = cluster_client.member_remove(remove_req).await?;
    assert_eq!(remove_res.members.len(), 4);
    assert!(remove_res.members.iter().all(|m| m.id != remove_id));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn xline_add_node() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = Client::connect(cluster.addrs(), ClientOptions::default()).await?;
    let mut cluster_client = client.cluster_client();
    let kv_client = client.kv_client();
    _ = kv_client.put(PutRequest::new("key", "value")).await?;
    let new_node_listener = TcpListener::bind("0.0.0.0:0").await?;
    let new_node_addrs = vec![new_node_listener.local_addr()?.to_string()];
    let add_req = MemberAddRequest::new(new_node_addrs.clone(), false);
    let add_res = cluster_client.member_add(add_req).await?;
    assert_eq!(add_res.members.len(), 4);
    cluster.run_node(new_node_listener).await;
    let tls_config = etcd_client::TlsOptions::new()
        .ca_certificate(etcd_client::Certificate::from_pem(certs::ca_cert()));
    let mut etcd_client = etcd_client::Client::connect(
        &new_node_addrs,
        Some(ConnectOptions::default().with_tls(tls_config)),
    )
    .await?;
    let res = etcd_client.get("key", None).await?;
    assert_eq!(res.kvs().get(0).unwrap().value(), b"value");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn xline_update_node() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let mut cluster_client = cluster.client().await.cluster_client();
    let old_list_res = cluster_client
        .member_list(MemberListRequest::new(false))
        .await?;
    assert_eq!(old_list_res.members.len(), 3);
    let update_id = old_list_res.members[0].id;
    let port = old_list_res.members[0]
        .peer_ur_ls
        .first()
        .unwrap()
        .split(':')
        .last()
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let update_req =
        MemberUpdateRequest::new(update_id, vec![format!("http://localhost:{}", port)]);
    let update_res = cluster_client.member_update(update_req).await?;
    assert_eq!(update_res.members.len(), 3);
    sleep(Duration::from_secs(3)).await;
    let new_list_res = cluster_client
        .member_list(MemberListRequest::new(false))
        .await?;
    assert_eq!(new_list_res.members.len(), 3);
    let old_addr = &old_list_res
        .members
        .iter()
        .find(|m| m.id == update_id)
        .unwrap()
        .peer_ur_ls;
    let new_addr = &new_list_res
        .members
        .iter()
        .find(|m| m.id == update_id)
        .unwrap()
        .peer_ur_ls;
    assert_ne!(old_addr, new_addr);

    Ok(())
}
