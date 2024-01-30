use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{metrics::MeterProvider, runtime::Tokio};
use tracing::info;
use utils::config::{MetricsConfig, MetricsPushProtocol};

/// Start metrics server
/// # Errors
/// Return error if init failed
#[inline]
pub fn init_metrics(config: &MetricsConfig) -> anyhow::Result<()> {
    if !config.enable() {
        return Ok(());
    }
    if *config.push() {
        info!(
            "enable metrics push mode, protocol {}",
            config.push_protocol()
        );

        // push mode
        let _ig = match *config.push_protocol() {
            MetricsPushProtocol::HTTP => opentelemetry_otlp::new_pipeline()
                .metrics(Tokio)
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .http()
                        .with_endpoint(config.push_endpoint()),
                )
                .build(),
            MetricsPushProtocol::GRPC => opentelemetry_otlp::new_pipeline()
                .metrics(Tokio)
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(config.push_endpoint()),
                )
                .build(),
            _ => unreachable!("only 'http' or 'gRPC' will be accepted"),
        }?;
        return Ok(());
    }
    // pull mode
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(prometheus::default_registry().clone())
        .build()?;
    let provider = MeterProvider::builder().with_reader(exporter).build();
    global::set_meter_provider(provider);

    let addr = format!("0.0.0.0:{}", config.port())
        .parse()
        .unwrap_or_else(|_| {
            unreachable!("local address 0.0.0.0:{} should be parsed", config.port())
        });
    info!("metrics server start on {addr:?}");
    let app = axum::Router::new().route(config.path(), axum::routing::any(metrics));
    let _ig = tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
    });

    Ok(())
}

/// Metrics handler
#[allow(clippy::unused_async)] // required by axum
async fn metrics() -> Result<String, hyper::StatusCode> {
    let encoder = prometheus::TextEncoder::new();
    let metrics_families = prometheus::gather();
    encoder
        .encode_to_string(&metrics_families)
        .map_err(|_e| hyper::StatusCode::INTERNAL_SERVER_ERROR)
}
