/// Command to be executed
pub(crate) mod command;
/// Xline kv server
mod kv_server;
/// Xline lease server
mod lease_server;
/// Xline lock server
mod lock_server;
/// Xline watch server
mod watch_server;
/// Xline server
pub(crate) mod xline_server;

pub use self::xline_server::XlineServer;
