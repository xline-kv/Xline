/// Xline command line arguments
mod args;
/// Xline tracing init
mod init_trace;

pub use args::{parse_config, ServerArgs};
pub use init_trace::init_subscriber;
