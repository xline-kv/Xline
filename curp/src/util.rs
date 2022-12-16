use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

/// Struct for extract data from `MetadataMap`
struct ExtractMap<'a>(&'a tonic::metadata::MetadataMap);

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

/// Function for extract data from some struct
pub(crate) trait Extract {
    /// extract span context from self and set as parent context
    fn extract_span(&self);
}

impl Extract for tonic::metadata::MetadataMap {
    fn extract_span(&self) {
        let parent_ctx = global::get_text_map_propagator(|prop| prop.extract(&ExtractMap(self)));
        let span = Span::current();
        span.set_parent(parent_ctx);
    }
}

/// Struct for inject data to `MetadataMap`
struct InjectMap<'a>(&'a mut tonic::metadata::MetadataMap);

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

/// Function for extract data from some struct
pub(crate) trait Inject {
    /// Inject span context into self
    fn inject_span(&mut self, span: &Span);
    /// Inject span context into self

    fn inject_current(&mut self) {
        let curr_span = Span::current();
        self.inject_span(&curr_span);
    }
}

impl Inject for tonic::metadata::MetadataMap {
    fn inject_span(&mut self, span: &Span) {
        let ctx = span.context();
        global::get_text_map_propagator(|prop| {
            prop.inject_context(&ctx, &mut InjectMap(self));
        });
    }
}
