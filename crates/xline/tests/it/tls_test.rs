use std::{fs, iter, path::PathBuf};

use etcd_client::ConnectOptions;
use test_macros::abort_on_panic;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use utils::config::prelude::{
    AuthConfig, ClusterConfig, CompactConfig, LogConfig, MetricsConfig, StorageConfig, TlsConfig,
    TraceConfig, XlineServerConfig,
};
use xline_test_utils::{enable_auth, set_user, Cluster};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_basic_tls() {
    let mut cluster = Cluster::new_with_configs(basic_tls_configs(3)).await;
    cluster.start().await;

    let client = cluster
        .client_with_tls_config(basic_tls_client_config())
        .await;
    let res = client.kv_client().put("foo", "bar", None).await;
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_mtls() {
    let mut cluster = Cluster::new_with_configs(mtls_configs(3)).await;
    cluster.start().await;

    let client = cluster
        .client_with_tls_config(mtls_client_config("root"))
        .await;
    let res = client.kv_client().put("foo", "bar", None).await;
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_certificate_authenticate() {
    let mut cluster = Cluster::new_with_configs(mtls_configs(3)).await;
    cluster.start().await;

    let root_client = cluster
        .client_with_tls_config(mtls_client_config("root"))
        .await;
    enable_auth(&root_client).await.unwrap();

    let addr = cluster.get_client_url(0);
    let mut etcd_u2_client = etcd_client::Client::connect(
        [addr],
        Some(ConnectOptions::new().with_tls(mtls_client_config("u2"))),
    )
    .await
    .unwrap();
    let res = etcd_u2_client.put("foa", "bar", None).await;
    assert!(res.is_err());
    let u1_client = cluster
        .client_with_tls_config(mtls_client_config("u1"))
        .await;
    let res = u1_client.kv_client().put("foo", "bar", None).await;
    assert!(res.is_err());

    set_user(&root_client, "u1", "123", "r1", b"foo", &[])
        .await
        .unwrap();
    set_user(&root_client, "u2", "123", "r2", b"foa", &[])
        .await
        .unwrap();

    let res = etcd_u2_client.put("foa", "bar", None).await;
    assert!(res.is_ok());
    let res = u1_client.kv_client().put("foo", "bar", None).await;
    assert!(res.is_ok());
}

fn configs_with_tls_config(size: usize, tls_config: TlsConfig) -> Vec<XlineServerConfig> {
    iter::repeat(tls_config)
        .map(|tls_config| {
            XlineServerConfig::builder()
                .cluster(ClusterConfig::default())
                .storage(StorageConfig::default())
                .log(LogConfig::default())
                .trace(TraceConfig::default())
                .auth(AuthConfig::default())
                .compact(CompactConfig::default())
                .tls(tls_config)
                .metrics(MetricsConfig::default())
                .build()
        })
        .take(size)
        .collect()
}

fn basic_tls_client_config() -> ClientTlsConfig {
    ClientTlsConfig::default().ca_certificate(Certificate::from_pem(
        fs::read("../../fixtures/ca.crt").unwrap(),
    ))
}

fn basic_tls_configs(size: usize) -> Vec<XlineServerConfig> {
    configs_with_tls_config(
        size,
        TlsConfig::builder()
            .peer_ca_cert_path(None)
            .peer_cert_path(Some(PathBuf::from("../../fixtures/server.crt")))
            .peer_key_path(Some(PathBuf::from("../../fixtures/server.key")))
            .client_ca_cert_path(Some(PathBuf::from("../../fixtures/ca.crt")))
            .client_cert_path(None)
            .client_key_path(None)
            .build(),
    )
}

fn mtls_client_config(name: &str) -> ClientTlsConfig {
    ClientTlsConfig::default()
        .ca_certificate(Certificate::from_pem(
            fs::read("../../fixtures/ca.crt").unwrap(),
        ))
        .identity(Identity::from_pem(
            fs::read(format!("../../fixtures/{name}_client.crt")).unwrap(),
            fs::read(format!("../../fixtures/{name}_client.key")).unwrap(),
        ))
}

fn mtls_configs(size: usize) -> Vec<XlineServerConfig> {
    configs_with_tls_config(
        size,
        TlsConfig::builder()
            .peer_ca_cert_path(Some(PathBuf::from("../../fixtures/ca.crt")))
            .peer_cert_path(Some(PathBuf::from("../../fixtures/server.crt")))
            .peer_key_path(Some(PathBuf::from("../../fixtures/server.key")))
            .client_ca_cert_path(Some(PathBuf::from("../../fixtures/ca.crt")))
            .client_cert_path(Some(PathBuf::from("../../fixtures/root_client.crt")))
            .client_key_path(Some(PathBuf::from("../../fixtures/root_client.key")))
            .build(),
    )
}
