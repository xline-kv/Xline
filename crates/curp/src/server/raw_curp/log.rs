#![allow(clippy::arithmetic_side_effects)] // u64 is large enough and won't overflow

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    ops::Range,
    sync::Arc,
    vec,
};

use bincode::serialized_size;
use clippy_utilities::{NumericCast, OverflowArithmetic};
use itertools::Itertools;
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    cmd::Command,
    log_entry::{EntryData, LogEntry},
    rpc::ProposeId,
    server::metrics,
    snapshot::SnapshotMeta,
    LogIndex,
};

/// Curp logs
/// There exists a fake log entry 0 whose term equals 0
/// For the leader, there should never be a gap between snapshot and entries
pub(super) struct Log<C: Command> {
    /// Log entries, should be persisted
    /// Note that the logical index in `LogEntry` is different from physical index
    entries: LogEntryVecDeque<C>,
    /// The last log index that has been compacted
    pub(super) base_index: LogIndex,
    /// The last log term that has been compacted
    pub(super) base_term: u64,
    /// Index of highest log entry known to be committed
    pub(super) commit_index: LogIndex,
    /// Index of highest log entry sent to after sync. `last_as` should always be less than or equal to `last_exe`.
    pub(super) last_as: LogIndex,
    /// Index of highest log entry sent to speculatively exe. `last_exe` should always be greater than or equal to `last_as`.
    pub(super) last_exe: LogIndex,
    /// Contexts of fallback log entries
    pub(super) fallback_contexts: HashMap<LogIndex, FallbackContext<C>>,
    /// Tx to send log entries to persist task
    log_tx: mpsc::UnboundedSender<Arc<LogEntry<C>>>,
    /// Entries to keep in memory
    entries_cap: usize,
}

/// Context of fallback conf change entry
pub(super) struct FallbackContext<C: Command> {
    /// The origin entry
    pub(super) origin_entry: Arc<LogEntry<C>>,
    /// The addresses of the old config
    pub(super) addrs: Vec<String>,
    /// The name of the old config
    pub(super) name: String,
    /// Whether the old config is a learner
    pub(super) is_learner: bool,
}

impl<C: Command> FallbackContext<C> {
    /// Create a new fallback context
    pub(super) fn new(
        origin_entry: Arc<LogEntry<C>>,
        addrs: Vec<String>,
        name: String,
        is_learner: bool,
    ) -> Self {
        Self {
            origin_entry,
            addrs,
            name,
            is_learner,
        }
    }
}

/// That's a struct to store log entries and calculate batch of log
#[derive(Debug)]
struct LogEntryVecDeque<C: Command> {
    /// A VecDeque to store log entries, it will be serialized and persisted
    entries: VecDeque<Arc<LogEntry<C>>>,
    /// The sum of serialized size of previous log entries
    /// batch_index[i+1] = batch_index[i] + size(entries[i])
    batch_index: VecDeque<u64>,
    /// Batch size limit
    batch_limit: u64,
}

impl<C: Command> LogEntryVecDeque<C> {
    /// return a log entries with cap
    fn new(cap: usize, batch_limit: u64) -> Self {
        let mut batch_index = VecDeque::with_capacity(cap.overflow_add(1));
        batch_index.push_back(0);
        Self {
            entries: VecDeque::with_capacity(cap),
            batch_index,
            batch_limit,
        }
    }

    /// Shortens the log entries, keeping the first `len` elements and dropping
    /// the rest.
    /// `batch_index` will keep len+1 elem
    fn truncate(&mut self, len: usize) {
        self.entries.truncate(len);
        self.batch_index.truncate(len.overflow_add(1));
    }

    /// push a log entry into the back of queue
    fn push_back(&mut self, entry: Arc<LogEntry<C>>) -> Result<(), bincode::Error> {
        let entry_size = serialized_size(&entry)?;

        self.entries.push_back(entry);
        let Some(&pre_entries_size) = self.batch_index.back() else {
            unreachable!("batch_index cannot be None")
        };
        self.batch_index
            .push_back(pre_entries_size.overflow_add(entry_size));
        Ok(())
    }

    /// pop a log entry from the front of queue
    fn pop_front(&mut self) -> Option<Arc<LogEntry<C>>> {
        if self.entries.front().is_some() {
            _ = self.batch_index.pop_front();
            self.entries.pop_front()
        } else {
            None
        }
    }

    /// restore log entries from Vec
    fn restore(&mut self, entries: Vec<LogEntry<C>>) {
        let mut batch_index = VecDeque::with_capacity(entries.capacity());
        batch_index.push_back(0);
        for entry in &entries {
            #[allow(clippy::expect_used)]
            let entry_size =
                serialized_size(entry).expect("log entry {entry:?} cannot be serialized");
            if let Some(cur_size) = batch_index.back() {
                batch_index.push_back(cur_size.overflow_add(entry_size));
            }
        }

        self.entries = entries.into_iter().map(Arc::new).collect();
        self.batch_index = batch_index;
    }

    /// clear whole log entries
    fn clear(&mut self) {
        self.entries.clear();
        self.batch_index.clear();
        self.batch_index.push_back(0);
    }

    /// Get the range [left, right) of the log entry, whose size should be equal or smaller than `batch_limit`
    fn get_range_by_batch(&self, left: usize) -> Range<usize> {
        #[allow(clippy::indexing_slicing)]
        let target = self.batch_index[left].overflow_add(self.batch_limit);
        // remove the fake index 0 in `batch_index`
        match self.batch_index.binary_search(&target) {
            Ok(right) => left..right,
            Err(right) => left..right - 1,
        }
    }

    /// Get a range of log entry
    fn get_from(&self, left: usize) -> Vec<Arc<LogEntry<C>>> {
        let range = self.get_range_by_batch(left);
        self.entries.range(range).cloned().collect_vec()
    }

    /// check whether the log entry range [li,..) exceeds the batch limit or not
    fn has_next_batch(&self, left: usize) -> bool {
        if let (Some(&cur_size), Some(&last_size)) =
            (self.batch_index.get(left), self.batch_index.back())
        {
            let target_size = cur_size.overflow_add(self.batch_limit);
            target_size <= last_size
        } else {
            false
        }
    }
}

impl<C: Command> std::ops::Deref for LogEntryVecDeque<C> {
    type Target = VecDeque<Arc<LogEntry<C>>>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<C: Command> Debug for Log<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Log")
            .field("entries", &self.entries)
            .field("base_index", &self.base_index)
            .field("base_term", &self.base_term)
            .field("commit_index", &self.commit_index)
            .field("last_as", &self.last_as)
            .field("last_exe", &self.last_exe)
            .finish()
    }
}

/// Conf change entries type
type ConfChangeEntries<C> = Vec<Arc<LogEntry<C>>>;
/// Fallback indexes type
type FallbackIndexes = HashSet<LogIndex>;

impl<C: Command> Log<C> {
    /// Create a new log
    pub(super) fn new(
        log_tx: mpsc::UnboundedSender<Arc<LogEntry<C>>>,
        batch_limit: u64,
        entries_cap: usize,
    ) -> Self {
        Self {
            entries: LogEntryVecDeque::new(entries_cap, batch_limit),
            commit_index: 0,
            base_index: 0,
            base_term: 0,
            last_as: 0,
            last_exe: 0,
            log_tx,
            entries_cap,
            fallback_contexts: HashMap::new(),
        }
    }

    /// Get last log index
    pub(super) fn last_log_index(&self) -> LogIndex {
        self.entries
            .back()
            .map_or(self.base_index, |entry| entry.index)
    }

    /// Get last log term
    pub(super) fn last_log_term(&self) -> u64 {
        self.entries
            .back()
            .map_or(self.base_term, |entry| entry.term)
    }

    /// Transform logical index to physical index of `self.entries`
    fn li_to_pi(&self, i: LogIndex) -> usize {
        assert!(
            i > self.base_index,
            "can't access the log entry whose index is not bigger than {}, might have been compacted",
            self.base_index
        );
        (i - self.base_index - 1).numeric_cast()
    }

    /// Get log entry
    pub(super) fn get(&self, i: LogIndex) -> Option<&Arc<LogEntry<C>>> {
        (i > self.base_index)
            .then(|| self.entries.get(self.li_to_pi(i)))
            .flatten()
    }

    /// Try to append log entries, hand back the entries if they can't be appended
    /// and return conf change entries if any
    #[allow(clippy::unwrap_in_result)]
    pub(super) fn try_append_entries(
        &mut self,
        entries: Vec<LogEntry<C>>,
        prev_log_index: LogIndex,
        prev_log_term: u64,
    ) -> Result<(ConfChangeEntries<C>, FallbackIndexes), Vec<LogEntry<C>>> {
        let mut conf_changes = vec![];
        let mut need_fallback_indexes = HashSet::new();
        // check if entries can be appended
        if self.get(prev_log_index).map_or_else(
            || (self.base_index, self.base_term) != (prev_log_index, prev_log_term),
            |entry| entry.term != prev_log_term,
        ) {
            return Err(entries);
        }
        // append log entries, will erase inconsistencies
        // Find the index of the first entry that needs to be truncated
        let mut pi = self.li_to_pi(prev_log_index + 1);
        for entry in &entries {
            if self
                .entries
                .get(pi)
                .map_or(true, |old_entry| old_entry.term != entry.term)
            {
                break;
            }
            pi += 1;
        }
        // Record entries that need to be fallback in the truncated entries
        for e in self.entries.range(pi..) {
            if matches!(e.entry_data, EntryData::ConfChange(_)) {
                let _ig = need_fallback_indexes.insert(e.index);
            }
        }
        // Truncate entries
        self.entries.truncate(pi);
        // Push the remaining entries and record the conf change entries
        for entry in entries
            .into_iter()
            .skip(pi - self.li_to_pi(prev_log_index + 1))
            .map(Arc::new)
        {
            if matches!(entry.entry_data, EntryData::ConfChange(_)) {
                conf_changes.push(Arc::clone(&entry));
            }
            #[allow(clippy::expect_used)] // It's safe to expect here.
            self.entries
                .push_back(Arc::clone(&entry))
                .expect("log entry {entry:?} cannot be serialized");

            self.send_persist(entry);
        }

        Ok((conf_changes, need_fallback_indexes))
    }

    /// Send log entries to persist task
    pub(super) fn send_persist(&self, entry: Arc<LogEntry<C>>) {
        if let Err(err) = self.log_tx.send(entry) {
            error!("failed to send log to persist, {err}");
        }
    }

    /// Check if the candidate's log is up-to-date
    pub(super) fn log_up_to_date(&self, last_log_term: u64, last_log_index: LogIndex) -> bool {
        if last_log_term == self.last_log_term() {
            // if the last log entry has the same term, grant vote if candidate has a longer log
            last_log_index >= self.last_log_index()
        } else {
            // otherwise grant vote only if the candidate has higher log term
            last_log_term > self.last_log_term()
        }
    }

    /// Push a log entry into the end of log
    pub(super) fn push(
        &mut self,
        term: u64,
        propose_id: ProposeId,
        entry: impl Into<EntryData<C>>,
    ) -> Result<Arc<LogEntry<C>>, bincode::Error> {
        let index = self.last_log_index() + 1;
        let entry = Arc::new(LogEntry::new(index, term, propose_id, entry));
        self.entries.push_back(Arc::clone(&entry))?;
        self.send_persist(Arc::clone(&entry));
        Ok(entry)
    }

    /// check whether the log entry range [li,..) exceeds the batch limit or not
    pub(super) fn has_next_batch(&self, li: u64) -> bool {
        let idx = self.li_to_pi(li);
        self.entries.has_next_batch(idx)
    }

    /// Get a range of log entry
    pub(super) fn get_from(&self, li: LogIndex) -> Vec<Arc<LogEntry<C>>> {
        let left_bound = self.li_to_pi(li);
        self.entries.get_from(left_bound)
    }

    /// Get existing cmd ids
    pub(super) fn get_cmd_ids(&self) -> HashSet<ProposeId> {
        self.entries.iter().map(|entry| entry.propose_id).collect()
    }

    /// Get previous log entry's term and index
    pub(super) fn get_prev_entry_info(&self, i: LogIndex) -> (LogIndex, u64) {
        assert!(i > 0, "log[0] has no previous log");
        if self.base_index == i - 1 {
            (self.base_index, self.base_term)
        } else {
            let entry = self.get(i - 1).unwrap_or_else(|| {
                unreachable!("get log[{}] when base_index is {}", i - 1, self.base_index)
            });
            (entry.index, entry.term)
        }
    }

    /// Reset log base by snapshot
    pub(super) fn reset_by_snapshot_meta(&mut self, meta: SnapshotMeta) {
        self.base_index = meta.last_included_index;
        self.base_term = meta.last_included_term;
        self.last_as = meta.last_included_index;
        self.last_exe = meta.last_included_index;
        self.commit_index = meta.last_included_index;
        self.entries.clear();
    }

    /// Restore log entries, provided entries must be in order
    pub(super) fn restore_entries(&mut self, entries: Vec<LogEntry<C>>) {
        // restore batch index
        self.entries.restore(entries);
        self.compact();
    }

    /// Compact log
    pub(super) fn compact(&mut self) {
        let Some(first_entry) = self.entries.front() else {
            return;
        };
        if self.last_as <= first_entry.index {
            return;
        }
        let compact_from = if self.last_as - first_entry.index >= self.entries_cap.numeric_cast() {
            self.last_as - self.entries_cap.numeric_cast::<u64>()
        } else {
            return;
        };
        while self
            .entries
            .front()
            .map_or(false, |e| e.index <= compact_from)
        {
            match self.entries.pop_front() {
                Some(entry) => {
                    self.base_index = entry.index;
                    self.base_term = entry.term;
                }
                None => return,
            }
        }
    }

    /// Commit to log index and return the fallback contexts
    pub(super) fn commit_to(&mut self, commit_index: LogIndex) {
        assert!(
            commit_index >= self.commit_index,
            "commit_index {} is smaller than current commit_index {}",
            commit_index,
            self.commit_index
        );
        self.commit_index = commit_index;
        self.fallback_contexts.retain(|&idx, c| {
            if idx > self.commit_index {
                return true;
            }
            if c.is_learner {
                metrics::get().learner_promote_succeed.add(1, &[]);
            }
            false
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::repeat, ops::Index, sync::Arc};

    use curp_test_utils::test_cmd::TestCommand;
    use utils::config::{default_batch_max_size, default_log_entries_cap};

    use super::*;

    // impl index for test is handy
    impl<C: Command> Index<usize> for Log<C> {
        type Output = LogEntry<C>;

        fn index(&self, i: usize) -> &Self::Output {
            let pi = self.li_to_pi(i.numeric_cast());
            &self.entries[pi]
        }
    }

    fn set_batch_limit(log: &mut Log<TestCommand>, batch_limit: u64) {
        log.entries.batch_limit = batch_limit;
    }

    #[test]
    fn test_log_up_to_date() {
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut log =
            Log::<TestCommand>::new(log_tx, default_batch_max_size(), default_log_entries_cap());
        let result = log.try_append_entries(
            vec![
                LogEntry::new(1, 1, ProposeId(0, 0), Arc::new(TestCommand::default())),
                LogEntry::new(2, 1, ProposeId(0, 1), Arc::new(TestCommand::default())),
            ],
            0,
            0,
        );
        assert!(result.is_ok());

        assert!(log.log_up_to_date(1, 2));
        assert!(log.log_up_to_date(1, 3));
        assert!(log.log_up_to_date(2, 3));
        assert!(!log.log_up_to_date(1, 1));
        assert!(!log.log_up_to_date(0, 0));
    }

    #[test]
    fn try_append_entries_will_remove_inconsistencies() {
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut log =
            Log::<TestCommand>::new(log_tx, default_batch_max_size(), default_log_entries_cap());
        let result = log.try_append_entries(
            vec![
                LogEntry::new(1, 1, ProposeId(0, 1), Arc::new(TestCommand::default())),
                LogEntry::new(2, 1, ProposeId(0, 2), Arc::new(TestCommand::default())),
                LogEntry::new(3, 1, ProposeId(0, 3), Arc::new(TestCommand::default())),
            ],
            0,
            0,
        );
        assert!(result.is_ok());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(2, 2, ProposeId(0, 4), Arc::new(TestCommand::default())),
                LogEntry::new(3, 2, ProposeId(0, 5), Arc::new(TestCommand::default())),
            ],
            1,
            1,
        );
        assert!(result.is_ok());
        assert_eq!(log[3].term, 2);
        assert_eq!(log[2].term, 2);
    }

    #[test]
    fn try_append_entries_will_not_append() {
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut log =
            Log::<TestCommand>::new(log_tx, default_batch_max_size(), default_log_entries_cap());
        let result = log.try_append_entries(
            vec![LogEntry::new(
                1,
                1,
                ProposeId(0, 0),
                Arc::new(TestCommand::default()),
            )],
            0,
            0,
        );
        assert!(result.is_ok());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(4, 2, ProposeId(0, 1), Arc::new(TestCommand::default())),
                LogEntry::new(5, 2, ProposeId(0, 2), Arc::new(TestCommand::default())),
            ],
            3,
            1,
        );
        assert!(result.is_err());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(2, 2, ProposeId(0, 3), Arc::new(TestCommand::default())),
                LogEntry::new(3, 2, ProposeId(0, 4), Arc::new(TestCommand::default())),
            ],
            1,
            2,
        );
        assert!(result.is_err());
    }

    #[test]
    fn get_from_should_success() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut log =
            Log::<TestCommand>::new(tx, default_batch_max_size(), default_log_entries_cap());

        // Note: this test must use the same test command to ensure the size of the entry is fixed
        let test_cmd = Arc::new(TestCommand::default());
        let _res = repeat(Arc::clone(&test_cmd))
            .take(10)
            .enumerate()
            .map(|(idx, cmd)| log.push(1, ProposeId(0, idx.numeric_cast()), cmd).unwrap())
            .collect::<Vec<_>>();
        let log_entry_size = log.entries.batch_index[1];

        set_batch_limit(&mut log, 3 * log_entry_size - 1);
        let bound_1 = log.entries.get_range_by_batch(3);
        assert_eq!(
            bound_1,
            3..5,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.entries.batch_index,
            log.entries.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(8));
        assert!(!log.has_next_batch(9));

        set_batch_limit(&mut log, 4 * log_entry_size);
        let bound_2 = log.entries.get_range_by_batch(3);
        assert_eq!(
            bound_2,
            3..7,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.entries.batch_index,
            log.entries.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(7));
        assert!(!log.has_next_batch(8));

        set_batch_limit(&mut log, 5 * log_entry_size + 2);
        let bound_3 = log.entries.get_range_by_batch(3);
        assert_eq!(
            bound_3,
            3..8,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.entries.batch_index,
            log.entries.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(5));
        assert!(!log.has_next_batch(6));

        set_batch_limit(&mut log, 100 * log_entry_size);
        let bound_4 = log.entries.get_range_by_batch(3);
        assert_eq!(
            bound_4,
            3..10,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.entries.batch_index,
            log.entries.batch_limit,
            log_entry_size
        );
        assert!(!log.has_next_batch(1));
        assert!(!log.has_next_batch(5));

        set_batch_limit(&mut log, log_entry_size - 1);
        let bound_5 = log.entries.get_range_by_batch(3);
        assert_eq!(
            bound_5,
            3..3,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.entries.batch_index,
            log.entries.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(10));
    }

    #[test]
    fn recover_log_should_success() {
        // Note: this test must use the same test command to ensure the size of the entry is fixed
        let test_cmd = Arc::new(TestCommand::default());
        let entries = repeat(Arc::clone(&test_cmd))
            .enumerate()
            .take(10)
            .map(|(idx, cmd)| {
                LogEntry::new(
                    (idx + 1).numeric_cast(),
                    1,
                    ProposeId(0, idx.numeric_cast()),
                    cmd,
                )
            })
            .collect::<Vec<LogEntry<TestCommand>>>();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut log =
            Log::<TestCommand>::new(tx, default_batch_max_size(), default_log_entries_cap());

        log.restore_entries(entries);
        assert_eq!(log.entries.len(), 10);
        assert_eq!(log.entries.batch_index.len(), 11);
        assert_eq!(log.entries.batch_index[0], 0);
        let entry_size = log.entries.batch_index[1];

        log.entries
            .batch_index
            .iter()
            .enumerate()
            .for_each(|(idx, &size)| {
                assert_eq!(
                    size,
                    entry_size * idx.numeric_cast::<u64>(),
                    "batch_index = {:?}, batch = {}, entry_size = {}",
                    log.entries.batch_index,
                    log.entries.batch_limit,
                    entry_size
                );
            });
    }

    #[test]
    fn compact_test() {
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut log = Log::<TestCommand>::new(log_tx, default_batch_max_size(), 10);

        for i in 0..30 {
            log.push(0, ProposeId(0, i), Arc::new(TestCommand::default()))
                .unwrap();
        }
        log.last_as = 22;
        log.last_exe = 22;
        log.compact();
        assert_eq!(log.base_index, 12);
        assert_eq!(log.entries.front().unwrap().index, 13);
        assert_eq!(log.entries.batch_index.len(), 19);
    }
}
