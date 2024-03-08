/// Storage for alarm
pub(crate) mod alarm_store;
/// Storage for Auth
pub(crate) mod auth_store;
/// Compact module
pub(super) mod compact;
/// Database module
pub mod db;
/// Index module
pub(crate) mod index;
/// Storage for KV
pub(crate) mod kv_store;
/// KV watcher module
pub(crate) mod kvwatcher;
/// Storage for lease
pub(crate) mod lease_store;
/// Revision module
pub(crate) mod revision;
/// Persistent storage abstraction
pub(crate) mod storage_api;

pub use self::revision::Revision;
pub(crate) use self::{
    alarm_store::AlarmStore, auth_store::AuthStore, kv_store::KvStore, lease_store::LeaseStore,
};
