use std::io;

use anyhow::Result;
use benchmark::{Benchmark, CommandRunner};
use clap::Parser;
use tracing::info;
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    prelude::*,
    Layer,
};

fn tracing_init(stdout: bool) {
    let option_layer = if stdout {
        Some(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(io::stdout)
                .with_filter(Targets::new().with_target("benchmark", LevelFilter::INFO)),
        )
    } else {
        None
    };
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .with_filter(Targets::new().with_target("benchmark", LevelFilter::DEBUG));
    tracing_subscriber::registry()
        .with(layer)
        .with(option_layer)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Benchmark = Benchmark::parse();
    tracing_init(args.stdout);

    let mut runner = CommandRunner::new(args);
    let stats = runner.run().await?;

    let summary = stats.summary();
    let histogram = stats.histogram();

    info!("{}", summary);
    info!("{}", histogram);

    Ok(())
}
