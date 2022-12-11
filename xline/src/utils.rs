use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Apply a closure on a rwlock after getting the guard
pub(crate) trait RwLockMap<T, R> {
    /// Map a closure to a read mutex
    fn map_read<READ>(&self, f: READ) -> R
    where
        READ: FnOnce(RwLockReadGuard<'_, T>) -> R;

    /// Map a closure to a write mutex
    fn map_write<WRITE>(&self, f: WRITE) -> R
    where
        WRITE: FnOnce(RwLockWriteGuard<'_, T>) -> R;
}

impl<T, R> RwLockMap<T, R> for RwLock<T> {
    fn map_read<READ>(&self, f: READ) -> R
    where
        READ: FnOnce(RwLockReadGuard<'_, T>) -> R,
    {
        let read_guard = self.read();
        f(read_guard)
    }

    fn map_write<WRITE>(&self, f: WRITE) -> R
    where
        WRITE: FnOnce(RwLockWriteGuard<'_, T>) -> R,
    {
        let write_guard = self.write();
        f(write_guard)
    }
}

/// update revision of response
#[macro_export]
macro_rules! update_revision {
    ($res:expr, $sync_res:expr) => {
        if let Some(sync_res) = $sync_res {
            let revision = sync_res.revision();
            if let Some(mut header) = $res.header.as_mut() {
                header.revision = revision;
            }
        }
    };
}
