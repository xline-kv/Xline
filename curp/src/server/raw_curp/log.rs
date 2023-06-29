#![allow(clippy::integer_arithmetic)] // u64 is large enough and won't overflow

use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    ops::Range,
    sync::Arc,
};

use bincode::serialized_size;
use clippy_utilities::{NumericCast, OverflowArithmetic};
use itertools::Itertools;
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    cmd::{Command, ProposeId},
    log_entry::LogEntry,
    snapshot::SnapshotMeta,
    LogIndex,
};

/// Given that the log vector is used to store a large number of log entries, we can specify a suitable capacity to avoid unnecessary allocation.
/// The initial capacity of the log entries
const LOG_INIT_CAPACITY: usize = 8192;

/// Curp logs
/// There exists a fake log entry 0 whose term equals 0
/// For the leader, there should never be a gap between snapshot and entries
pub(super) struct Log<C: Command> {
    /// Log entries, should be persisted
    /// Note that the logical index in `LogEntry` is different from physical index
    entries: VecDeque<LogEntry<C>>,
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
    /// Tx to send log entries to persist task
    log_tx: mpsc::UnboundedSender<LogEntry<C>>,
    /// Prefix sum vector of log entries
    batch_index: VecDeque<u64>,
    /// Batch size limit
    batch_limit: u64,
    /// Entries to keep in memory
    entries_cap: usize,
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

impl<C: 'static + Command> Log<C> {
    /// Create a new log
    pub(super) fn new(
        log_tx: mpsc::UnboundedSender<LogEntry<C>>,
        batch_limit: u64,
        entries_cap: usize,
    ) -> Self {
        let mut batch_index = VecDeque::with_capacity(entries_cap);
        batch_index.push_back(0);
        Self {
            entries: VecDeque::with_capacity(LOG_INIT_CAPACITY),
            commit_index: 0,
            base_index: 0,
            base_term: 0,
            last_as: 0,
            last_exe: 0,
            log_tx,
            batch_index,
            batch_limit,
            entries_cap,
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
            "can't access the log entry whose index is smaller than {}, might have been compacted",
            self.base_index
        );
        (i - self.base_index - 1).numeric_cast()
    }

    /// Get log entry
    pub(super) fn get(&self, i: LogIndex) -> Option<&LogEntry<C>> {
        (i > self.base_index)
            .then(|| self.entries.get(self.li_to_pi(i)))
            .flatten()
    }

    /// Try to append log entries, hand back the entries if they can't be appended
    #[allow(clippy::unwrap_in_result)]
    pub(super) fn try_append_entries(
        &mut self,
        entries: Vec<LogEntry<C>>,
        prev_log_index: LogIndex,
        prev_log_term: u64,
    ) -> Result<(), Vec<LogEntry<C>>> {
        // check if entries can be appended
        if self.get(prev_log_index).map_or_else(
            || (self.base_index, self.base_term) != (prev_log_index, prev_log_term),
            |entry| entry.term != prev_log_term,
        ) {
            return Err(entries);
        }

        // append log entries, will erase inconsistencies
        let mut li = prev_log_index;
        for entry in entries {
            li += 1;
            let pi = self.li_to_pi(li);
            if self
                .entries
                .get(pi)
                .map_or(false, |old_entry| old_entry.term == entry.term)
            {
                continue;
            }

            #[allow(clippy::expect_used)] // It's safe to expect here.
            let entry_size =
                serialized_size(&entry).expect("log entry {entry:?} cannot be serialized");

            self.entries.truncate(pi);
            self.batch_index.truncate(pi.overflow_add(1));

            self.entries.push_back(entry.clone());
            let pre_entry_size = if let Some(&last_entry_size) = self.batch_index.back() {
                last_entry_size
            } else {
                0
            };
            self.batch_index
                .push_back(pre_entry_size.overflow_add(entry_size));
            self.send_persist(entry);
        }

        Ok(())
    }

    /// Send log entries to persist task
    pub(super) fn send_persist(&self, entry: LogEntry<C>) {
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

    /// Pack the cmd into a log entry and push it to the end of the log, return its index
    pub(super) fn push_cmd(&mut self, term: u64, cmd: Arc<C>) -> Result<LogIndex, bincode::Error> {
        assert_eq!(
            self.batch_index.len(),
            self.entries.len() + 1,
            "The batch_index.len {} is not equal to the log entries.len {} + 1",
            self.batch_index.len(),
            self.entries.len()
        );

        let index = self.last_log_index() + 1;
        let entry = LogEntry::new(index, term, cmd);

        let entry_size = serialized_size(&entry)?;
        let pre_entry_size = if let Some(&last_entry_size) = self.batch_index.back() {
            last_entry_size
        } else {
            0
        };
        self.batch_index
            .push_back(pre_entry_size.overflow_add(entry_size));
        self.entries.push_back(entry.clone());
        self.send_persist(entry);
        Ok(self.last_log_index())
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

    /// check whether the log entry range [li,..) exceeds the batch limit or not
    pub(super) fn has_next_batch(&self, li: u64) -> bool {
        let idx = self.li_to_pi(li);
        if let (Some(&cur_size), Some(&last_size)) =
            (self.batch_index.get(idx), self.batch_index.back())
        {
            let target_size = cur_size.overflow_add(self.batch_limit);
            target_size <= last_size
        } else {
            false
        }
    }

    /// Get a range of log entry
    pub(super) fn get_from(&self, li: LogIndex) -> Vec<LogEntry<C>> {
        let left_bound = self.li_to_pi(li);
        let log_range = self.get_range_by_batch(left_bound);
        self.entries.range(log_range).cloned().collect_vec()
    }

    /// Get existing cmd ids
    pub(super) fn get_cmd_ids(&self) -> HashSet<&ProposeId> {
        self.entries.iter().map(|entry| entry.cmd.id()).collect()
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

    /// Restore log entries, provided entires must be in order
    pub(super) fn restore_entries(&mut self, entries: Vec<LogEntry<C>>) {
        // restore batch index
        let mut batch_index = VecDeque::with_capacity(entries.capacity());
        batch_index.push_back(0);
        for entry in &entries {
            #[allow(clippy::expect_used)]
            // it's in the initialization stage, panic doesn't affect much
            let entry_size =
                serialized_size(entry).expect("log entry {entry:?} cannot be serialized");
            if let Some(cur_size) = batch_index.back() {
                batch_index.push_back(cur_size.overflow_add(entry_size));
            }
        }
        self.batch_index = batch_index;

        self.entries = VecDeque::from(entries);
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
        #[allow(clippy::unwrap_used)] // checked
        while self
            .entries
            .front()
            .map_or(false, |e| e.index <= compact_from)
            & self.batch_index.front().is_some()
        {
            let e = self.entries.pop_front().unwrap();
            let _ig_i = self.batch_index.pop_front().unwrap();
            self.base_index = e.index;
            self.base_term = e.term;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::repeat, ops::Index, sync::Arc};

    use utils::config::{default_batch_max_size, default_log_entries_cap};

    use super::*;
    use curp_test_utils::test_cmd::TestCommand;

    // impl index for test is handy
    impl<C: 'static + Command> Index<usize> for Log<C> {
        type Output = LogEntry<C>;

        fn index(&self, i: usize) -> &Self::Output {
            let pi = self.li_to_pi(i.numeric_cast());
            &self.entries[pi]
        }
    }

    fn set_batch_limit(log: &mut Log<TestCommand>, batch_limit: u64) {
        log.batch_limit = batch_limit;
    }

    #[test]
    fn test_log_up_to_date() {
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut log =
            Log::<TestCommand>::new(log_tx, default_batch_max_size(), default_log_entries_cap());
        let result = log.try_append_entries(
            vec![
                LogEntry::new(1, 1, Arc::new(TestCommand::default())),
                LogEntry::new(2, 1, Arc::new(TestCommand::default())),
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
                LogEntry::new(1, 1, Arc::new(TestCommand::default())),
                LogEntry::new(2, 1, Arc::new(TestCommand::default())),
                LogEntry::new(3, 1, Arc::new(TestCommand::default())),
            ],
            0,
            0,
        );
        assert!(result.is_ok());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(2, 2, Arc::new(TestCommand::default())),
                LogEntry::new(3, 2, Arc::new(TestCommand::default())),
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
            vec![LogEntry::new(1, 1, Arc::new(TestCommand::default()))],
            0,
            0,
        );
        assert!(result.is_ok());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(4, 2, Arc::new(TestCommand::default())),
                LogEntry::new(5, 2, Arc::new(TestCommand::default())),
            ],
            3,
            1,
        );
        assert!(result.is_err());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(2, 2, Arc::new(TestCommand::default())),
                LogEntry::new(3, 2, Arc::new(TestCommand::default())),
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
            .map(|cmd| log.push_cmd(1, cmd).unwrap())
            .collect::<Vec<u64>>();
        let log_entry_size = log.batch_index[1];

        set_batch_limit(&mut log, 3 * log_entry_size - 1);
        let bound_1 = log.get_range_by_batch(3);
        assert_eq!(
            bound_1,
            3..5,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.batch_index,
            log.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(8));
        assert!(!log.has_next_batch(9));

        set_batch_limit(&mut log, 4 * log_entry_size);
        let bound_2 = log.get_range_by_batch(3);
        assert_eq!(
            bound_2,
            3..7,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.batch_index,
            log.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(7));
        assert!(!log.has_next_batch(8));

        set_batch_limit(&mut log, 5 * log_entry_size + 2);
        let bound_3 = log.get_range_by_batch(3);
        assert_eq!(
            bound_3,
            3..8,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.batch_index,
            log.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(5));
        assert!(!log.has_next_batch(6));

        set_batch_limit(&mut log, 100 * log_entry_size);
        let bound_4 = log.get_range_by_batch(3);
        assert_eq!(
            bound_4,
            3..10,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.batch_index,
            log.batch_limit,
            log_entry_size
        );
        assert!(!log.has_next_batch(1));
        assert!(!log.has_next_batch(5));

        set_batch_limit(&mut log, log_entry_size - 1);
        let bound_5 = log.get_range_by_batch(3);
        assert_eq!(
            bound_5,
            3..3,
            "batch_index = {:?}, batch = {}, log_entry_size = {}",
            log.batch_index,
            log.batch_limit,
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
            .map(|(idx, cmd)| LogEntry::new((idx + 1).numeric_cast(), 1, cmd))
            .collect::<Vec<LogEntry<TestCommand>>>();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut log =
            Log::<TestCommand>::new(tx, default_batch_max_size(), default_log_entries_cap());

        log.restore_entries(entries);
        assert_eq!(log.entries.len(), 10);
        assert_eq!(log.batch_index.len(), 11);
        assert_eq!(log.batch_index[0], 0);
        let entry_size = log.batch_index[1];

        log.batch_index.iter().enumerate().for_each(|(idx, &size)| {
            assert_eq!(
                size,
                entry_size * idx.numeric_cast::<u64>(),
                "batch_index = {:?}, batch = {}, entry_size = {}",
                log.batch_index,
                log.batch_limit,
                entry_size
            );
        });
    }

    #[test]
    fn compact_test() {
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut log = Log::<TestCommand>::new(log_tx, default_batch_max_size(), 10);

        for _ in 0..30 {
            log.push_cmd(0, Arc::new(TestCommand::default())).unwrap();
        }
        log.last_as = 22;
        log.last_exe = 22;
        log.compact();
        assert_eq!(log.base_index, 12);
        assert_eq!(log.entries.front().unwrap().index, 13);
        assert_eq!(log.batch_index.len(), 19);
    }
}
