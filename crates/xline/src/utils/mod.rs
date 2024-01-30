/// Xline command line arguments
mod args;
/// Xline tracing init
mod trace;

pub use args::{parse_config, ServerArgs};
pub use trace::init_subscriber;
