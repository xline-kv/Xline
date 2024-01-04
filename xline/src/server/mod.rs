/// Xline auth server
mod auth_server;
/// Barriers for range requests
mod barriers;
/// Cluster server
mod cluster_server;
/// Command to be executed
pub(crate) mod command;
/// Xline kv server
mod kv_server;
/// Xline lease server
mod lease_server;
/// Xline lock server
mod lock_server;
/// Xline maintenance client
mod maintenance;
/// Xline watch server
mod watch_server;
/// Xline server
mod xline_server;

pub(crate) use self::maintenance::MAINTENANCE_SNAPSHOT_CHUNK_SIZE;
pub use self::xline_server::XlineServer;

use curp::client_new::ClientApi;
use xlineapi::command::Command;

/// The curp client trait object
/// TODO: use `type CurpClient = impl ClientApi<...>` when `type_alias_impl_trait` stabilized
type CurpClient = dyn ClientApi<Error = tonic::Status, Cmd = Command> + Sync + Send + 'static;
