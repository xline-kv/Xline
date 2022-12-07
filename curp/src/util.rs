use opentelemetry::propagation::{Extractor, Injector};

/// Struct for extract data from `MetadataMap`
pub(crate) struct ExtractMap<'a>(pub(crate) &'a tonic::metadata::MetadataMap);

impl Extractor for ExtractMap<'_> {
    /// Get a value for a key from the `MetadataMap`.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the `MetadataMap`.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

/// Struct for inject data to `MetadataMap`
pub(crate) struct InjectMap<'a>(pub(crate) &'a mut tonic::metadata::MetadataMap);

impl Injector for InjectMap<'_> {
    /// Set a key and value in the `MetadataMap`.  Does nothing if the key or value are not valid inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = tonic::metadata::MetadataValue::try_from(&value) {
                let _option = self.0.insert(key, val);
            }
        }
    }
}
