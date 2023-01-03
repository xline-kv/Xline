use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

/// Function for inject data to some struct
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
