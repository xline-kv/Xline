/// Propose id guard, used to ensure the sequence of propose id is recorded.
struct ProposeIdGuard<'a> {
    /// The propose id
    propose_id: ProposeId,
    /// The tracker
    tracker: &'a RwLock<Tracker>,
}

impl Deref for ProposeIdGuard<'_> {
    type Target = ProposeId;

    fn deref(&self) -> &Self::Target {
        &self.propose_id
    }
}

impl<'a> ProposeIdGuard<'a> {
    /// Create a new propose id guard
    fn new(tracker: &'a RwLock<Tracker>, propose_id: ProposeId) -> Self {
        Self {
            propose_id,
            tracker,
        }
    }
}

impl Drop for ProposeIdGuard<'_> {
    fn drop(&mut self) {
        let _ig = self.tracker.write().record(self.propose_id.1);
    }
}

/// Command tracker
#[derive(Debug, Default)]
struct CmdTracker {
    /// Last sent sequence number
    last_sent_seq: AtomicU64,
    /// Request tracker
    tracker: RwLock<Tracker>,
}

impl CmdTracker {
    /// New a seq num and record it
    fn new_seq_num(&self) -> u64 {
        self.last_sent_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Generate a unique propose id during the retry process.
    fn gen_propose_id(&self, client_id: u64) -> ProposeIdGuard<'_> {
        let seq_num = self.new_seq_num();
        ProposeIdGuard::new(&self.tracker, ProposeId(client_id, seq_num))
    }

    /// Generate a unique propose id during the retry process.
    fn first_incomplete(&self) -> u64 {
        self.tracker.read().first_incomplete()
    }
}
