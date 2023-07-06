use async_trait::async_trait;
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Apply a closure on a mutex after getting the guard
#[async_trait]
pub trait MutexMap<T, R> {
    /// Map a closure to a mutex in an async context
    async fn map_lock<F>(&self, f: F) -> R
    where
        F: FnOnce(MutexGuard<'_, T>) -> R + Send;
}

#[async_trait]
impl<T, R> MutexMap<T, R> for Mutex<T>
where
    T: Send,
{
    #[inline]
    async fn map_lock<F>(&self, f: F) -> R
    where
        F: FnOnce(MutexGuard<'_, T>) -> R + Send,
    {
        let lock = self.lock().await;
        f(lock)
    }
}

/// Apply a closure on a rwlock after getting the guard
#[async_trait]
pub trait RwLockMap<T, R> {
    /// Map a closure to a read mutex
    async fn map_read<READ>(&self, f: READ) -> R
    where
        READ: FnOnce(RwLockReadGuard<'_, T>) -> R + Send;

    /// Map a closure to a write mutex
    async fn map_write<WRITE>(&self, f: WRITE) -> R
    where
        WRITE: FnOnce(RwLockWriteGuard<'_, T>) -> R + Send;
}

#[async_trait]
impl<T, R> RwLockMap<T, R> for RwLock<T>
where
    T: Send + Sync,
{
    #[inline]
    async fn map_read<READ>(&self, f: READ) -> R
    where
        READ: FnOnce(RwLockReadGuard<'_, T>) -> R + Send,
    {
        let read_guard = self.read().await;
        f(read_guard)
    }

    #[inline]
    async fn map_write<WRITE>(&self, f: WRITE) -> R
    where
        WRITE: FnOnce(RwLockWriteGuard<'_, T>) -> R + Send,
    {
        let write_guard = self.write().await;
        f(write_guard)
    }
}

#[cfg(test)]
mod test {
    use test_macros::abort_on_panic;

    use super::*;

    #[tokio::test]
    #[abort_on_panic]
    async fn mutex_map_works() {
        let mu = Mutex::new(1);
        mu.map_lock(|mut g| {
            *g = 3;
        })
        .await;
        let val = mu.map_lock(|g| *g).await;
        assert_eq!(val, 3);
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn rwlock_map_works() {
        let mu = RwLock::new(1);
        mu.map_write(|mut g| {
            *g = 3;
        })
        .await;
        let val = mu.map_read(|g| *g).await;
        assert_eq!(val, 3);
    }
}
