use std::{collections::HashMap, convert::Infallible};

use super::storage_api::StorageApi;

/// default storage implementation
#[derive(Debug, Default, Clone)]
pub struct Memory(HashMap<Vec<u8>, Vec<u8>>);

impl Memory {
    /// New `Memory`
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl StorageApi for Memory {
    type Error = Infallible;

    #[inline]
    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.0.get(key.as_ref()).cloned())
    }

    #[inline]
    fn batch_get(&self, keys: &[impl AsRef<[u8]>]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        Ok(keys
            .iter()
            .map(|key| self.0.get(key.as_ref()).cloned())
            .collect())
    }

    #[inline]
    fn insert(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.0.insert(key.into(), value.into()))
    }

    #[inline]
    fn remove(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.0.remove(key.as_ref()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_memory() {
        let mut storage = Memory::new();
        assert_eq!(storage.get("key"), Ok(None));
        assert_eq!(storage.insert("key", "value"), Ok(None));
        assert_eq!(
            storage.get("key"),
            Ok(Some("value".to_owned().into_bytes()))
        );
        assert_eq!(storage.insert("key", "value2"), Ok(Some("value".into())));
        assert_eq!(
            storage.get("key"),
            Ok(Some("value2".to_owned().into_bytes()))
        );
        assert_eq!(storage.batch_get(&["key"]), Ok(vec![Some("value2".into())]));
        assert_eq!(storage.remove("key"), Ok(Some("value2".into())));
        assert_eq!(storage.get("key"), Ok(None));
    }
}
