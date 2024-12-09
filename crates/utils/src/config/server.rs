use std::time::Duration;

use getset::Getters;
use serde::Deserialize;

use super::prelude::{
    default_compact_timeout, default_range_retry_timeout, default_sync_victims_interval,
    default_watch_progress_notify_interval, AuthConfig, ClusterConfig, CompactConfig, LogConfig,
    MetricsConfig, StorageConfig, TlsConfig, TraceConfig,
};

use super::duration_format;

/// Xline server configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Default, Getters)]
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
    pub fn builder() -> Builder {
        Builder::default()
    }
}

/// Builder for `XlineServerConfig`
#[derive(Debug, Default)]
pub struct Builder {
    /// cluster configuration object
    cluster: Option<ClusterConfig>,
    /// xline storage configuration object
    storage: Option<StorageConfig>,
    /// log configuration object
    log: Option<LogConfig>,
    /// trace configuration object
    trace: Option<TraceConfig>,
    /// auth configuration object
    auth: Option<AuthConfig>,
    /// compactor configuration object
    compact: Option<CompactConfig>,
    /// tls configuration object
    tls: Option<TlsConfig>,
    /// Metrics config
    metrics: Option<MetricsConfig>,
}

impl Builder {
    /// set the cluster object
    #[must_use]
    #[inline]
    pub fn cluster(mut self, cluster: ClusterConfig) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// set the storage object
    #[must_use]
    #[inline]
    pub fn storage(mut self, storage: StorageConfig) -> Self {
        self.storage = Some(storage);
        self
    }

    /// set the log object
    #[must_use]
    #[inline]
    pub fn log(mut self, log: LogConfig) -> Self {
        self.log = Some(log);
        self
    }

    /// set the trace object
    #[must_use]
    #[inline]
    pub fn trace(mut self, trace: TraceConfig) -> Self {
        self.trace = Some(trace);
        self
    }

    /// set the trace object
    #[must_use]
    #[inline]
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    /// set the compact object
    #[must_use]
    #[inline]
    pub fn compact(mut self, compact: CompactConfig) -> Self {
        self.compact = Some(compact);
        self
    }

    /// set the compact object
    #[must_use]
    #[inline]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// set the compact object
    #[must_use]
    #[inline]
    pub fn metrics(mut self, metrics: MetricsConfig) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build the `XlineServerConfig`
    #[must_use]
    #[inline]
    pub fn build(self) -> XlineServerConfig {
        XlineServerConfig {
            cluster: self.cluster.unwrap_or_default(),
            storage: self.storage.unwrap_or_default(),
            log: self.log.unwrap_or_default(),
            trace: self.trace.unwrap_or_default(),
            auth: self.auth.unwrap_or_default(),
            compact: self.compact.unwrap_or_default(),
            tls: self.tls.unwrap_or_default(),
            metrics: self.metrics.unwrap_or_default(),
        }
    }
}

/// Xline server settings
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct XlineServerTimeout {
    /// Range request retry timeout settings
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_range_retry_timeout")]
    range_retry_timeout: Duration,
    /// Range request retry timeout settings
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_compact_timeout")]
    compact_timeout: Duration,
    /// Sync victims interval
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_sync_victims_interval")]
    sync_victims_interval: Duration,
    /// Watch progress notify interval settings
    #[getset(get = "pub")]
    #[serde(
        with = "duration_format",
        default = "default_watch_progress_notify_interval"
    )]
    watch_progress_notify_interval: Duration,
}

impl XlineServerTimeout {
    /// Create a builder for `XlineServerTimeout`
    #[must_use]
    #[inline]
    pub fn builder() -> XlineServerTimeoutBuilder {
        XlineServerTimeoutBuilder::default()
    }
}

impl Default for XlineServerTimeout {
    #[inline]
    fn default() -> Self {
        Self {
            range_retry_timeout: default_range_retry_timeout(),
            compact_timeout: default_compact_timeout(),
            sync_victims_interval: default_sync_victims_interval(),
            watch_progress_notify_interval: default_watch_progress_notify_interval(),
        }
    }
}

/// Builder for `XlineServerTimeout`
#[derive(Debug, Default, Clone, Copy)]
pub struct XlineServerTimeoutBuilder {
    /// Range request retry timeout settings
    range_retry_timeout: Option<Duration>,
    /// Range request retry timeout settings
    compact_timeout: Option<Duration>,
    /// Sync victims interval
    sync_victims_interval: Option<Duration>,
    /// Watch progress notify interval settings
    watch_progress_notify_interval: Option<Duration>,
}

impl XlineServerTimeoutBuilder {
    /// Set the range retry timeout
    #[must_use]
    #[inline]
    pub fn range_retry_timeout(mut self, timeout: Duration) -> Self {
        self.range_retry_timeout = Some(timeout);
        self
    }

    /// Set the compact timeout
    #[must_use]
    #[inline]
    pub fn compact_timeout(mut self, timeout: Duration) -> Self {
        self.compact_timeout = Some(timeout);
        self
    }

    /// Set the sync victims interval
    #[must_use]
    #[inline]
    pub fn sync_victims_interval(mut self, interval: Duration) -> Self {
        self.sync_victims_interval = Some(interval);
        self
    }

    /// Set the watch progress notify interval
    #[must_use]
    #[inline]
    pub fn watch_progress_notify_interval(mut self, interval: Duration) -> Self {
        self.watch_progress_notify_interval = Some(interval);
        self
    }

    /// Build the `XlineServerTimeout` instance
    #[must_use]
    #[inline]
    pub fn build(self) -> XlineServerTimeout {
        XlineServerTimeout {
            range_retry_timeout: self.range_retry_timeout.unwrap_or_default(),
            compact_timeout: self.compact_timeout.unwrap_or_default(),
            sync_victims_interval: self.sync_victims_interval.unwrap_or_default(),
            watch_progress_notify_interval: self.watch_progress_notify_interval.unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, time::Duration};

    use crate::config::prelude::*;

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

        let client_config = ClientConfig::builder()
            .wait_synced_timeout(default_client_wait_synced_timeout())
            .propose_timeout(default_propose_timeout())
            .initial_retry_timeout(Duration::from_secs(5))
            .max_retry_timeout(Duration::from_secs(50))
            .retry_count(default_retry_count())
            .fixed_backoff(default_fixed_backoff())
            .keep_alive_interval(default_client_id_keep_alive_interval())
            .build();

        let server_timeout = XlineServerTimeout::builder()
            .range_retry_timeout(Duration::from_secs(3))
            .compact_timeout(Duration::from_secs(5))
            .sync_victims_interval(Duration::from_millis(20))
            .watch_progress_notify_interval(Duration::from_secs(1))
            .build();

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
            StorageConfig::builder()
                .engine(EngineConfig::Memory)
                .quota(default_quota())
                .build()
        );

        assert_eq!(
            *config.log(),
            LogConfig::builder()
                .path(Some(PathBuf::from("/var/log/xline")))
                .rotation(RotationConfig::Daily)
                .level(LevelConfig::INFO)
                .build()
        );
        assert_eq!(
            *config.trace(),
            TraceConfig::builder()
                .jaeger_online(false)
                .jaeger_offline(false)
                .jaeger_output_dir(PathBuf::from("./jaeger_jsons"))
                .jaeger_level(LevelConfig::INFO)
                .build()
        );

        assert_eq!(
            *config.compact(),
            CompactConfig::builder()
                .compact_batch_size(123)
                .compact_sleep_interval(Duration::from_millis(5))
                .auto_compact_config(Some(AutoCompactConfig::Periodic(Duration::from_secs(
                    10 * 60 * 60
                ))))
                .build()
        );

        assert_eq!(
            *config.auth(),
            AuthConfig::builder()
                .auth_public_key(Some(PathBuf::from("./public_key.pem")))
                .auth_private_key(Some(PathBuf::from("./private_key.pem")))
                .build()
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
            MetricsConfig::builder()
                .enable(true)
                .port(9100)
                .path("/metrics".to_owned())
                .push(true)
                .push_endpoint("http://some-endpoint.com:4396".to_owned())
                .push_protocol(PushProtocol::HTTP)
                .build(),
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
            LogConfig::builder()
                .path(Some(PathBuf::from("/var/log/xline")))
                .rotation(RotationConfig::Never)
                .level(LevelConfig::INFO)
                .build()
        );
        assert_eq!(
            *config.trace(),
            TraceConfig::builder()
                .jaeger_online(false)
                .jaeger_offline(false)
                .jaeger_output_dir(PathBuf::from("./jaeger_jsons"))
                .jaeger_level(LevelConfig::INFO)
                .build()
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
            CompactConfig::builder()
                .compact_batch_size(default_compact_batch_size())
                .compact_sleep_interval(default_compact_sleep_interval())
                .auto_compact_config(Some(AutoCompactConfig::Revision(10000)))
                .build()
        );
    }
}
