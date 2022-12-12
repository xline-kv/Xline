use std::{sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::cmd::Command;

use super::spec_pool::SpeculativePool;

/// How often spec should GC
const SPEC_GC_INTERVAL: Duration = Duration::from_secs(10);

/// Run background GC tasks for Curp server
pub(super) fn run_gc_tasks<C: Command + 'static>(spec: Arc<Mutex<SpeculativePool<C>>>) {
    let _spec_gc_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(SPEC_GC_INTERVAL).await;
            spec.lock().gc();
        }
    });
}

impl<C: Command + 'static> SpeculativePool<C> {
    /// Speculative pool GC
    pub(super) fn gc(&mut self) {
        let now = Instant::now();
        self.ready.retain(|_, time| now - *time >= SPEC_GC_INTERVAL);
    }
}
