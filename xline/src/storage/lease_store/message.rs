use tokio::sync::oneshot;

use super::Lease;
use crate::storage::ExecuteError;

#[derive(Debug)]
/// Messages from other store
pub(crate) enum LeaseMessage {
    /// Attach message
    Attach(oneshot::Sender<Result<(), ExecuteError>>, i64, Vec<u8>),
    /// Detach message
    Detach(oneshot::Sender<Result<(), ExecuteError>>, i64, Vec<u8>),
    /// Get lease message
    GetLease(oneshot::Sender<i64>, Vec<u8>),
    /// Look up message
    LookUp(oneshot::Sender<Option<Lease>>, i64),
}

impl LeaseMessage {
    /// Attach key to lease
    pub(crate) fn attach(
        lease_id: i64,
        key: impl Into<Vec<u8>>,
    ) -> (Self, oneshot::Receiver<Result<(), ExecuteError>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Attach(tx, lease_id, key.into()), rx)
    }

    /// Detach key from lease
    pub(crate) fn detach(
        lease_id: i64,
        key: impl Into<Vec<u8>>,
    ) -> (Self, oneshot::Receiver<Result<(), ExecuteError>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Detach(tx, lease_id, key.into()), rx)
    }

    /// Get lease id by given key
    pub(crate) fn get_lease(key: impl Into<Vec<u8>>) -> (Self, oneshot::Receiver<i64>) {
        let (tx, rx) = oneshot::channel();
        (Self::GetLease(tx, key.into()), rx)
    }

    /// Get lease by id
    pub(crate) fn look_up(lease_id: i64) -> (Self, oneshot::Receiver<Option<Lease>>) {
        let (tx, rx) = oneshot::channel();
        (Self::LookUp(tx, lease_id), rx)
    }
}
