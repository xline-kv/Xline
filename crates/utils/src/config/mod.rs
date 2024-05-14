/// Xline auth configuration module
pub mod auth_config;
/// Curp client module
pub mod client_config;
/// Cluster configuration module
pub mod cluster_config;
/// Compaction configuration module
pub mod compact_config;
/// Curp server module
pub mod curp_config;
/// Engine Configuration module
pub mod engine_config;
/// Log configuration module
pub mod log_config;
/// Xline metrics configuration module
pub mod metrics_config;
/// Xline server module
pub mod server_config;
/// Storage Configuration module
pub mod storage_config;
/// Xline tls configuration module
pub mod tls_config;
/// Xline tracing configuration module
pub mod trace_config;

use getset::Getters;
use serde::Deserialize;

use crate::config::cluster_config::ClusterConfig;
use crate::config::compact_config::CompactConfig;
use crate::config::log_config::LogConfig;
use crate::config::metrics_config::MetricsConfig;
use crate::config::storage_config::StorageConfig;
use crate::config::trace_config::TraceConfig;

use crate::config::{auth_config::AuthConfig, tls_config::TlsConfig};

/// Xline server configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
pub struct XlineServerConfig {
    /// cluster configuration object
    #[getset(get = "pub")]
    cluster: ClusterConfig,
    /// xline storage configuration object
    #[getset(get = "pub")]
    storage: StorageConfig,
    /// log configuration object
    #[getset(get = "pub")]
    log: LogConfig,
    /// trace configuration object
    #[getset(get = "pub")]
    trace: TraceConfig,
    /// auth configuration object
    #[getset(get = "pub")]
    auth: AuthConfig,
    /// compactor configuration object
    #[getset(get = "pub")]
    compact: CompactConfig,
    /// tls configuration object
    #[getset(get = "pub")]
    tls: TlsConfig,
    /// Metrics config
    #[getset(get = "pub")]
    #[serde(default = "MetricsConfig::default")]
    metrics: MetricsConfig,
}

impl XlineServerConfig {
    /// Generates a new `XlineServerConfig` object
    #[must_use]
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster: ClusterConfig,
        storage: StorageConfig,
        log: LogConfig,
        trace: TraceConfig,
        auth: AuthConfig,
        compact: CompactConfig,
        tls: TlsConfig,
        metrics: MetricsConfig,
    ) -> Self {
        Self {
            cluster,
            storage,
            log,
            trace,
            auth,
            compact,
            tls,
            metrics,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{
            client_config::{
                default_client_wait_synced_timeout, default_fixed_backoff, default_propose_timeout,
                default_retry_count, ClientConfig,
            },
            compact_config::AutoCompactConfig,
            curp_config::CurpConfigBuilder,
            engine_config::EngineConfig,
            server_config::ServerTimeout,
            storage_config::default_quota,
        },
        InitialClusterState, LevelConfig, MetricsPushProtocol, RotationConfig,
    };

    use super::*;
    use std::{collections::HashMap, path::PathBuf, time::Duration};

    #[allow(clippy::too_many_lines)] // just a testcase, not too bad
    #[test]
    fn test_xline_server_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            r#"[cluster]
            name = 'node1'
            is_leader = true
            initial_cluster_state = 'new'
            peer_listen_urls = ['127.0.0.1:2380']
            peer_advertise_urls = ['127.0.0.1:2380']
            client_listen_urls = ['127.0.0.1:2379']
            client_advertise_urls = ['127.0.0.1:2379']

            [cluster.server_timeout]
            range_retry_timeout = '3s'
            compact_timeout = '5s'
            sync_victims_interval = '20ms'
            watch_progress_notify_interval = '1s'

            [cluster.peers]
            node1 = ['127.0.0.1:2378', '127.0.0.1:2379']
            node2 = ['127.0.0.1:2380']
            node3 = ['127.0.0.1:2381']

            [cluster.curp_config]
            heartbeat_interval = '200ms'
            wait_synced_timeout = '100ms'
            rpc_timeout = '100ms'
            retry_timeout = '100ms'

            [cluster.client_config]
            initial_retry_timeout = '5s'
            max_retry_timeout = '50s'

            [storage]
            engine = { type = 'memory'}

            [compact]
            compact_batch_size = 123
            compact_sleep_interval = '5ms'

            [compact.auto_compact_config]
            mode = 'periodic'
            retention = '10h'

            [log]
            path = '/var/log/xline'
            rotation = 'daily'
            level = 'info'

            [trace]
            jaeger_online = false
            jaeger_offline = false
            jaeger_output_dir = './jaeger_jsons'
            jaeger_level = 'info'

            [auth]
            auth_public_key = './public_key.pem'
            auth_private_key = './private_key.pem'

            [tls]
            server_cert_path = './cert.pem'
            server_key_path = './key.pem'
            client_ca_cert_path = './ca.pem'

            [metrics]
            enable = true
            port = 9100
            path = "/metrics"
            push = true
            push_endpoint = 'http://some-endpoint.com:4396'
            push_protocol = 'http'
            "#,
        )
        .unwrap();

        let curp_config = CurpConfigBuilder::default()
            .heartbeat_interval(Duration::from_millis(200))
            .wait_synced_timeout(Duration::from_millis(100))
            .rpc_timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        let client_config = ClientConfig::new(
            default_client_wait_synced_timeout(),
            default_propose_timeout(),
            Duration::from_secs(5),
            Duration::from_secs(50),
            default_retry_count(),
            default_fixed_backoff(),
            default_client_id_keep_alive_interval(),
        );

        let server_timeout = ServerTimeout::new(
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_millis(20),
            Duration::from_secs(1),
        );

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                HashMap::from_iter([
                    (
                        "node1".to_owned(),
                        vec!["127.0.0.1:2378".to_owned(), "127.0.0.1:2379".to_owned()]
                    ),
                    ("node2".to_owned(), vec!["127.0.0.1:2380".to_owned()]),
                    ("node3".to_owned(), vec!["127.0.0.1:2381".to_owned()]),
                ]),
                true,
                curp_config,
                client_config,
                server_timeout,
                InitialClusterState::New
            )
        );

        assert_eq!(
            config.storage,
            StorageConfig::new(EngineConfig::Memory, default_quota())
        );

        assert_eq!(
            config.log,
            LogConfig::new(
                PathBuf::from("/var/log/xline"),
                RotationConfig::Daily,
                LevelConfig::INFO
            )
        );
        assert_eq!(
            config.trace,
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::INFO
            )
        );

        assert_eq!(
            config.compact,
            CompactConfig::new(
                123,
                Duration::from_millis(5),
                Some(AutoCompactConfig::Periodic(Duration::from_secs(
                    10 * 60 * 60
                )))
            )
        );

        assert_eq!(
            config.auth,
            AuthConfig {
                auth_private_key: Some(PathBuf::from("./private_key.pem")),
                auth_public_key: Some(PathBuf::from("./public_key.pem")),
            }
        );

        assert_eq!(
            config.tls,
            TlsConfig {
                server_cert_path: Some(PathBuf::from("./cert.pem")),
                server_key_path: Some(PathBuf::from("./key.pem")),
                client_ca_cert_path: Some(PathBuf::from("./ca.pem")),
                ..Default::default()
            }
        );

        assert_eq!(
            config.metrics,
            MetricsConfig {
                enable: true,
                port: 9100,
                path: "/metrics".to_owned(),
                push: true,
                push_endpoint: "http://some-endpoint.com:4396".to_owned(),
                push_protocol: MetricsPushProtocol::HTTP,
            },
        );
    }

    #[test]
    fn test_xline_server_default_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            "[cluster]
                name = 'node1'
                is_leader = true
                peer_listen_urls = ['127.0.0.1:2380']
                peer_advertise_urls = ['127.0.0.1:2380']
                client_listen_urls = ['127.0.0.1:2379']
                client_advertise_urls = ['127.0.0.1:2379']

                [cluster.peers]
                node1 = ['127.0.0.1:2379']
                node2 = ['127.0.0.1:2380']
                node3 = ['127.0.0.1:2381']

                [cluster.storage]

                [log]
                path = '/var/log/xline'

                [storage]
                engine = { type = 'rocksdb', data_dir = '/usr/local/xline/data-dir' }

                [compact]

                [trace]
                jaeger_online = false
                jaeger_offline = false
                jaeger_output_dir = './jaeger_jsons'
                jaeger_level = 'info'

                [auth]

                [tls]
                ",
        )
        .unwrap();

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                HashMap::from([
                    ("node1".to_owned(), vec!["127.0.0.1:2379".to_owned()]),
                    ("node2".to_owned(), vec!["127.0.0.1:2380".to_owned()]),
                    ("node3".to_owned(), vec!["127.0.0.1:2381".to_owned()]),
                ]),
                true,
                CurpConfigBuilder::default().build().unwrap(),
                ClientConfig::default(),
                ServerTimeout::default(),
                InitialClusterState::default()
            )
        );

        if let EngineConfig::RocksDB(path) = config.storage.engine {
            assert_eq!(path, PathBuf::from("/usr/local/xline/data-dir"));
        } else {
            unreachable!();
        }

        assert_eq!(
            config.log,
            LogConfig::new(
                PathBuf::from("/var/log/xline"),
                RotationConfig::Daily,
                LevelConfig::INFO
            )
        );
        assert_eq!(
            config.trace,
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::INFO
            )
        );
        assert_eq!(config.compact, CompactConfig::default());
        assert_eq!(config.auth, AuthConfig::default());
        assert_eq!(config.tls, TlsConfig::default());
        assert_eq!(config.metrics, MetricsConfig::default());
    }

    #[test]
    fn test_auto_revision_compactor_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            "[cluster]
                name = 'node1'
                is_leader = true
                peer_listen_urls = ['127.0.0.1:2380']
                peer_advertise_urls = ['127.0.0.1:2380']
                client_listen_urls = ['127.0.0.1:2379']
                client_advertise_urls = ['127.0.0.1:2379']

                [cluster.peers]
                node1 = ['127.0.0.1:2379']
                node2 = ['127.0.0.1:2380']
                node3 = ['127.0.0.1:2381']

                [cluster.storage]

                [log]
                path = '/var/log/xline'

                [storage]
                engine = { type = 'memory' }

                [compact]

                [compact.auto_compact_config]
                mode = 'revision'
                retention = 10000

                [trace]
                jaeger_online = false
                jaeger_offline = false
                jaeger_output_dir = './jaeger_jsons'
                jaeger_level = 'info'

                [auth]

                [tls]
                ",
        )
        .unwrap();

        assert_eq!(
            config.compact,
            CompactConfig {
                auto_compact_config: Some(AutoCompactConfig::Revision(10000)),
                ..Default::default()
            }
        );
    }
}
