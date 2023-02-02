/// Storage for Auth
pub(crate) mod auth_store;
/// Database module
pub(crate) mod db;
/// Index module
pub(crate) mod index;
/// Storage for KV
pub(crate) mod kv_store;
/// KV watcher module
pub(crate) mod kvwatcher;
/// Storage for lease
pub(crate) mod lease_store;
/// Memory storage module
pub(crate) mod memory;
/// Request context module
pub(crate) mod req_ctx;
/// Revision module
pub(crate) mod revision;
/// Persistent storage abstraction
pub(crate) mod storage_api;

pub(crate) use self::{
    auth_store::AuthStore, kv_store::KvStore, lease_store::LeaseStore, storage_api::StorageApi,
};
