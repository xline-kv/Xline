use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Apply a closure on a mutex after getting the guard
pub trait MutexMap<T, R> {
    /// Map a closure to a mutex
    ///
    /// # Errors
    ///
    /// This function will return an error if the mutex lock fails.
    fn map_lock<F>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce(MutexGuard<'_, T>) -> R;
}

impl<T, R> MutexMap<T, R> for Mutex<T> {
    #[inline]
    fn map_lock<F>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce(MutexGuard<'_, T>) -> R,
    {
        let lock = self.lock().map_err(|e| format!("{e:?}"))?;
        Ok(f(lock))
    }
}

/// Apply a closure on a rwlock after getting the guard
pub trait RwLockMap<T, R> {
    /// Map a closure to a read mutex
    ///
    /// # Errors
    ///
    /// This function will return an error if the mutex lock fails.
    fn map_read<READ>(&self, f: READ) -> Result<R, String>
    where
        READ: FnOnce(RwLockReadGuard<'_, T>) -> R;

    /// Map a closure to a write mutex
    ///
    /// # Errors
    ///
    /// This function will return an error if the mutex lock fails.
    fn map_write<WRITE>(&self, f: WRITE) -> Result<R, String>
    where
        WRITE: FnOnce(RwLockWriteGuard<'_, T>) -> R;
}

impl<T, R> RwLockMap<T, R> for RwLock<T> {
    #[inline]
    fn map_read<READ>(&self, f: READ) -> Result<R, String>
    where
        READ: FnOnce(RwLockReadGuard<'_, T>) -> R,
    {
        let read_guard = self.read().map_err(|e| format!("{e:?}"))?;
        Ok(f(read_guard))
    }

    #[inline]
    fn map_write<WRITE>(&self, f: WRITE) -> Result<R, String>
    where
        WRITE: FnOnce(RwLockWriteGuard<'_, T>) -> R,
    {
        let write_guard = self.write().map_err(|e| format!("{e:?}"))?;
        Ok(f(write_guard))
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use super::*;

    #[test]
    fn mutex_map_works() -> Result<(), Box<dyn Error>> {
        let mu = Mutex::new(1);
        mu.map_lock(|mut g| {
            *g = 3;
        })?;
        let val = mu.map_lock(|g| *g)?;
        assert_eq!(val, 3);
        Ok(())
    }

    #[test]
    fn rwlock_map_works() -> Result<(), Box<dyn Error>> {
        let mu = RwLock::new(1);
        mu.map_write(|mut g| {
            *g = 3;
        })?;
        let val = mu.map_read(|g| *g)?;
        assert_eq!(val, 3);
        Ok(())
    }
}
