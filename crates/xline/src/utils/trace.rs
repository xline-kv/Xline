use anyhow::Result;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use opentelemetry_sdk::runtime::Tokio;
use tracing::warn;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{fmt::format, Layer};
use utils::config::{file_appender, LogConfig, RotationConfig, TraceConfig};

/// Return a Box trait from the config
fn generate_writer(name: &str, log_config: &LogConfig) -> Box<dyn std::io::Write + Send> {
    if let Some(ref file_path) = *log_config.path() {
        Box::new(file_appender(*log_config.rotation(), file_path, name))
    } else {
        if matches!(*log_config.rotation(), RotationConfig::Never) {
            warn!("The log is output to the terminal, so the rotation parameter is ignored.");
        }
        Box::new(std::io::stdout())
    }
}

/// init tracing subscriber
/// # Errors
/// Return error if init failed
#[inline]
pub fn init_subscriber(
    name: &str,
    log_config: &LogConfig,
    trace_config: &TraceConfig,
) -> Result<Option<WorkerGuard>> {
    let jaeger_level = *trace_config.jaeger_level();
    let jaeger_online_layer = trace_config
        .jaeger_online()
        .then(|| {
            let otlp_exporter = opentelemetry_otlp::new_exporter().tonic();
            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(otlp_exporter)
                .install_batch(Tokio)
                .map(|provider| {
                    let _prev = global::set_tracer_provider(provider.clone());
                    provider.tracer("xline")
                })
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
    let writer = generate_writer(name, log_config);
    let (non_blocking, guard) = tracing_appender::non_blocking(writer);
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::default().add_directive((*log_config.level()).into()));
    let log_layer = tracing_subscriber::fmt::layer()
        .event_format(format().compact())
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_filter(filter);

    tracing_subscriber::registry()
        .with(jaeger_fmt_layer)
        .with(jaeger_online_layer)
        .with(jaeger_offline_layer)
        .with(log_layer)
        .try_init()?;
    anyhow::Ok(Some(guard))
}
