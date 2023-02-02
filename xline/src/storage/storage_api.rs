/// persistent storage abstraction
pub trait StorageApi: Send + Sync + 'static + std::fmt::Debug {
    /// Error type returned by storage
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get value by key from storage
    ///
    /// if key found, return `Ok(Some(value))`
    ///
    /// if key not found, return `Ok(None)`
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Get values by keys from storage
    ///
    /// same as `get`, but get multiple keys at once
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn batch_get(&self, keys: &[impl AsRef<[u8]>]) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    /// Insert key-value pair into storage
    ///
    /// if key already exists, return `Ok(Some(old_value))`
    ///
    /// if key not exists, return `Ok(None)`
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn insert(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Delete key from storage
    ///
    /// if key already exists, return `Ok(Some(old_value))`
    ///
    /// if key not exists, return `Ok(None)`
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn remove(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error>;
}
