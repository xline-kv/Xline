use anyhow::Result;
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use opentelemetry_sdk::runtime::Tokio;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt::format, layer::SubscriberExt, util::SubscriberInitExt, Layer};
use utils::config::{file_appender, LogConfig, TraceConfig};

/// init tracing subscriber
/// # Errors
/// Return error if init failed
#[inline]
pub fn init_subscriber(
    name: &str,
    log_config: &LogConfig,
    trace_config: &TraceConfig,
) -> Result<Option<WorkerGuard>> {
    let mut guard = None;
    let log_file_layer = log_config.path().as_ref().map(|log_path| {
        let file_appender = file_appender(*log_config.rotation(), log_path, name);
        // `WorkerGuard` should be assigned in the `main` function or whatever the entrypoint of the program is.
        let (non_blocking, guard_inner) = tracing_appender::non_blocking(file_appender);
        guard = Some(guard_inner);
        tracing_subscriber::fmt::layer()
            .event_format(format().compact())
            .with_writer(non_blocking)
            .with_ansi(false)
            .with_filter(*log_config.level())
    });

    let jaeger_level = *trace_config.jaeger_level();
    let jaeger_online_layer = trace_config
        .jaeger_online()
        .then(|| {
            opentelemetry_jaeger::new_agent_pipeline()
                .with_service_name(name)
                .install_batch(Tokio)
                .ok()
        })
        .flatten()
        .map(|tracer| {
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(jaeger_level)
        });
    let jaeger_offline_layer = trace_config.jaeger_offline().then(|| {
        tracing_opentelemetry::layer().with_tracer(
            JaegerJsonExporter::new(
                trace_config.jaeger_output_dir().clone(),
                name.to_owned(),
                name.to_owned(),
                Tokio,
            )
            .install_batch(),
        )
    });

    let jaeger_fmt_layer = tracing_subscriber::fmt::layer()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    tracing_subscriber::registry()
        .with(log_file_layer)
        .with(jaeger_fmt_layer)
        .with(jaeger_online_layer)
        .with(jaeger_offline_layer)
        .try_init()?;
    Ok(guard)
}
