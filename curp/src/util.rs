use parking_lot::{Mutex, MutexGuard};

/// Apply a closure on a mutex after getting the guard
pub(crate) trait MutexMap<F, T, R> {
    /// Map a closure to a mutex
    fn map(&self, f: F) -> R
    where
        F: FnOnce(MutexGuard<'_, T>) -> R;
}

impl<F, T, R> MutexMap<F, T, R> for Mutex<T> {
    fn map(&self, f: F) -> R
    where
        F: FnOnce(MutexGuard<'_, T>) -> R,
    {
        let lock = self.lock();
        f(lock)
    }
}
