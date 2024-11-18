/// Xline auth configuration module
pub mod auth;
/// Curp client module
pub mod client;
/// Cluster configuration module
pub mod cluster;
/// Compaction configuration module
pub mod compact;
/// Curp server module
pub mod curp;
/// Engine Configuration module
pub mod engine;
/// Log configuration module
pub mod log;
/// Xline metrics configuration module
pub mod metrics;
/// Prelude module
pub mod prelude;
/// Xline server configuration
pub mod server;
/// Storage Configuration module
pub mod storage;
/// Xline tls configuration module
pub mod tls;
/// Xline tracing configuration module
pub mod trace;

/// `Duration` deserialization formatter
pub mod duration_format {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer};

    use crate::parse_duration;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)] //  the false positive case blocks us
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

/// batch size deserialization formatter
pub mod bytes_format {
    use serde::{Deserialize, Deserializer};

    use crate::parse_batch_bytes;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)] //  the false positive case blocks us
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_batch_bytes(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, time::Duration};

    use super::prelude::*;

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
            peer_cert_path = './cert.pem'
            peer_key_path = './key.pem'
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

        let server_timeout = XlineServerTimeout::new(
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_millis(20),
            Duration::from_secs(1),
        );

        assert_eq!(
            *config.cluster(),
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
            *config.storage(),
            StorageConfig::new(EngineConfig::Memory, default_quota())
        );

        assert_eq!(
            *config.log(),
            LogConfig::new(
                Some(PathBuf::from("/var/log/xline")),
                RotationConfig::Daily,
                LevelConfig::INFO
            )
        );
        assert_eq!(
            *config.trace(),
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::INFO
            )
        );

        assert_eq!(
            *config.compact(),
            CompactConfig::new(
                123,
                Duration::from_millis(5),
                Some(AutoCompactConfig::Periodic(Duration::from_secs(
                    10 * 60 * 60
                )))
            )
        );

        assert_eq!(
            *config.auth(),
            AuthConfig::new(
                Some(PathBuf::from("./public_key.pem")),
                Some(PathBuf::from("./private_key.pem"))
            )
        );

        assert_eq!(
            *config.tls(),
            TlsConfig {
                peer_cert_path: Some(PathBuf::from("./cert.pem")),
                peer_key_path: Some(PathBuf::from("./key.pem")),
                client_ca_cert_path: Some(PathBuf::from("./ca.pem")),
                ..Default::default()
            }
        );

        assert_eq!(
            *config.metrics(),
            MetricsConfig::new(
                true,
                9100,
                "/metrics".to_owned(),
                true,
                "http://some-endpoint.com:4396".to_owned(),
                PushProtocol::HTTP
            ),
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
            *config.cluster(),
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
                XlineServerTimeout::default(),
                InitialClusterState::default()
            )
        );

        if let EngineConfig::RocksDB(path) = config.storage().engine().clone() {
            assert_eq!(path, PathBuf::from("/usr/local/xline/data-dir"));
        } else {
            unreachable!();
        }

        assert_eq!(
            *config.log(),
            LogConfig::new(
                Some(PathBuf::from("/var/log/xline")),
                RotationConfig::Never,
                LevelConfig::INFO
            )
        );
        assert_eq!(
            *config.trace(),
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::INFO
            )
        );
        assert_eq!(*config.compact(), CompactConfig::default());
        assert_eq!(*config.auth(), AuthConfig::default());
        assert_eq!(*config.tls(), TlsConfig::default());
        assert_eq!(*config.metrics(), MetricsConfig::default());
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
            *config.compact(),
            CompactConfig::new(
                default_compact_batch_size(),
                default_compact_sleep_interval(),
                Some(AutoCompactConfig::Revision(10000))
            )
        );
    }
}
