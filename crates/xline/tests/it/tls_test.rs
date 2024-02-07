use std::{fs, iter, path::PathBuf};

use test_macros::abort_on_panic;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use utils::config::{
    AuthConfig, ClusterConfig, CompactConfig, LogConfig, StorageConfig, TlsConfig, TraceConfig,
    XlineServerConfig,
};
use xline_client::types::kv::PutRequest;
use xline_test_utils::Cluster;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_basic_tls() {
    let mut cluster = Cluster::new_with_configs(basic_tls_configs(3)).await;
    cluster.set_client_tls_config(basic_tls_client_config());
    cluster.start().await;

    let client = cluster.client().await;
    let res = client.kv_client().put(PutRequest::new("foo", "bar")).await;
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_mtls() {
    let mut cluster = Cluster::new_with_configs(mtls_configs(3)).await;
    cluster.set_client_tls_config(mtls_client_config());
    cluster.start().await;

    let client = cluster.client().await;
    let res = client.kv_client().put(PutRequest::new("foo", "bar")).await;
    assert!(res.is_ok());
}

fn basic_tls_client_config() -> ClientTlsConfig {
    ClientTlsConfig::default().ca_certificate(Certificate::from_pem(
        fs::read("../../fixtures/ca.crt").unwrap(),
    ))
}

fn mtls_client_config() -> ClientTlsConfig {
    ClientTlsConfig::default()
        .ca_certificate(Certificate::from_pem(
            fs::read("../../fixtures/ca.crt").unwrap(),
        ))
        .identity(Identity::from_pem(
            fs::read("../../fixtures/client.crt").unwrap(),
            fs::read("../../fixtures/client.key").unwrap(),
        ))
}

fn configs_with_tls_config(size: usize, tls_config: TlsConfig) -> Vec<XlineServerConfig> {
    iter::repeat(tls_config)
        .map(|tls_config| {
            XlineServerConfig::new(
                ClusterConfig::default(),
                StorageConfig::default(),
                LogConfig::default(),
                TraceConfig::default(),
                AuthConfig::default(),
                CompactConfig::default(),
                tls_config,
            )
        })
        .take(size)
        .collect()
}

fn basic_tls_configs(size: usize) -> Vec<XlineServerConfig> {
    configs_with_tls_config(
        size,
        TlsConfig::new(
            None,
            Some(PathBuf::from("../../fixtures/server.crt")),
            Some(PathBuf::from("../../fixtures/server.key")),
            Some(PathBuf::from("../../fixtures/ca.crt")),
            None,
            None,
        ),
    )
}

fn mtls_configs(size: usize) -> Vec<XlineServerConfig> {
    configs_with_tls_config(
        size,
        TlsConfig::new(
            Some(PathBuf::from("../../fixtures/ca.crt")),
            Some(PathBuf::from("../../fixtures/server.crt")),
            Some(PathBuf::from("../../fixtures/server.key")),
            Some(PathBuf::from("../../fixtures/ca.crt")),
            Some(PathBuf::from("../../fixtures/client.crt")),
            Some(PathBuf::from("../../fixtures/client.key")),
        ),
    )
}
