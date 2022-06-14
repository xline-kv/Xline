use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Apply a closure on a mutex after getting the guard
pub(crate) trait MutexMap<T, R> {
    /// Map a closure to a mutex
    fn map_lock<F>(&self, f: F) -> R
    where
        F: FnOnce(MutexGuard<'_, T>) -> R;
}

impl<T, R> MutexMap<T, R> for Mutex<T> {
    fn map_lock<F>(&self, f: F) -> R
    where
        F: FnOnce(MutexGuard<'_, T>) -> R,
    {
        let lock = self.lock();
        f(lock)
    }
}

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
