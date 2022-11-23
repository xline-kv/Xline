use crate::cmd::Command;
use crate::server::SpeculativePool;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// How often spec should GC
const SPEC_GC_INTERVAL: Duration = Duration::from_secs(10);

/// Run background GC tasks for Curp server
pub(crate) fn run_gc_tasks<C: Command + 'static>(spec: Arc<Mutex<SpeculativePool<C>>>) {
    let _spec_gc_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(SPEC_GC_INTERVAL).await;
            spec.lock().gc();
        }
    });
}

impl<C: Command + 'static> SpeculativePool<C> {
    /// Speculative pool GC
    pub(crate) fn gc(&mut self) {
        let now = Instant::now();
        self.ready.retain(|_, time| now - *time >= SPEC_GC_INTERVAL);
    }
}
