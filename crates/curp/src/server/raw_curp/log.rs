#![allow(clippy::arithmetic_side_effects)] // u64 is large enough and won't overflow

use std::{
    cmp::{min, Ordering},
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    ops::{Bound, Range, RangeBounds, RangeInclusive},
    sync::Arc,
    vec,
};

use clippy_utilities::NumericCast;
use itertools::Itertools;
use tracing::warn;

use crate::{
    cmd::Command,
    log_entry::{EntryData, LogEntry},
    rpc::ProposeId,
    server::metrics,
    snapshot::SnapshotMeta,
    LogIndex,
};

/// A `LogEntry` wrapper
#[derive(Debug, Clone)]
struct Entry<C: Command> {
    /// The inner `LogEntry`
    inner: Arc<LogEntry<C>>,
    /// The serialized size of the inner `LogEntry`
    size: u64,
}

/// Enum representing a range of values in the log.
#[derive(Debug, Eq, PartialEq)]
enum LogRange<T> {
    /// Represents a range using the `Range` type.
    Range(Range<T>),
    /// Represents a range using the `RangeInclusive` type.
    RangeInclusive(RangeInclusive<T>),
}

impl<T> RangeBounds<T> for LogRange<T> {
    fn start_bound(&self) -> Bound<&T> {
        match *self {
            Self::Range(ref range) => range.start_bound(),
            Self::RangeInclusive(ref range) => range.start_bound(),
        }
    }

    fn end_bound(&self) -> Bound<&T> {
        match *self {
            Self::Range(ref range) => range.end_bound(),
            Self::RangeInclusive(ref range) => range.end_bound(),
        }
    }
}

impl<T> From<Range<T>> for LogRange<T> {
    fn from(range: Range<T>) -> Self {
        Self::Range(range)
    }
}

impl<T> From<RangeInclusive<T>> for LogRange<T> {
    fn from(range: RangeInclusive<T>) -> Self {
        Self::RangeInclusive(range)
    }
}

/// ```text
/// Curp logs
///
/// There exists a fake log entry 0 whose term equals 0
///
/// For the leader, there should never be a gap between snapshot and entries
///
/// Examples:
/// This example will describe the relationship among `batch_end`, `batch_limit`, `first_idx_in_cur_batch` and `cur_batch_size`
/// if `batch_limit` = 11 and entry sizes is vec![1,5,6,2,3,4,2], then the relationship between entry size and `batch_end` looks like:
/// -------------------------------------------
/// `entry_size[i]`| 1 | 5 | 6 | 2 | 3 | 4 | 2 |
/// ---------------+---------------------------+
/// i              | 0 | 1 | 2 | 3 | 4 | 5 | 6 |
/// ---------------+---+---+---+---+---+---+---+
/// `batch_end[i]` | 1 | 2 | 4 | 5 | 0 | 0 | 0 |
/// -------------------------------------------
///                                  ↑
///                       `first_idx_in_cur_batch`
/// (0, `batch_end[0]`) = (0,1), which means the `entries[0..=1]` is a valid batch whose size is 1+5=6, less than the `batch_limit`
/// (1, `batch_end[1]`) = (1,2), which means the `entries[1..=2]` is a valid batch whose size is 5+6=11, equal to the `batch_limit`
/// ...
/// (`first_idx_in_cur_batch`, `batch_end[first_idx_in_cur_batch]`) = (4, 0), which means the `entries[4..]` is a valid batch (aka. current batch) whose size (aka. `cur_batch_size`) is 3+4+2=9, less than the `batch_limit`
/// ```
pub(super) struct Log<C: Command> {
    /// Log entries, should be persisted
    /// A VecDeque to store log entries, it will be serialized and persisted
    /// Note that the logical index in `LogEntry` is different from physical index
    entries: VecDeque<Entry<C>>,
    /// Each element `batch_end[i]` represents the right inclusive bound of a log batch whose size is less than or equal to `batch_limit`
    /// if you want to fetch a batch which begins at the index `i`, you can fetch it directly by `i..=batch_end[i]`
    batch_end: VecDeque<LogIndex>,
    /// Batch size limit
    batch_limit: u64,
    /// the first entry idx of the current batch
    first_idx_in_cur_batch: usize,
    /// the current batch size
    cur_batch_size: u64,
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

impl<C: Command> Log<C> {
    /// Shortens the log entries, keeping the first `len` elements and dropping
    /// the rest.
    /// `batch_end` will keep len elem
    fn truncate(&mut self, len: usize) {
        self.entries.truncate(len);
        self.batch_end.truncate(len);
        let last_index = if len == 0 { return } else { len - 1 };
        self.first_idx_in_cur_batch = min(self.first_idx_in_cur_batch, len);
        #[allow(clippy::indexing_slicing)]
        // it's safe since we check `self.first_idx_in_cur_batch` at the very first beginning
        loop {
            if self.first_idx_in_cur_batch == 0 {
                break;
            }
            let end = self.li_to_pi(self.batch_end[self.first_idx_in_cur_batch - 1]);
            match end.cmp(&last_index) {
                // All the `batch_end[i]` larger than `len - 1` should be reset to zero
                Ordering::Greater => {
                    self.batch_end[self.first_idx_in_cur_batch - 1] = 0;
                    self.first_idx_in_cur_batch -= 1;
                }
                Ordering::Equal => {
                    // when the `end == last_index`, it means that we should compare the sum of `get_range_by_batch(self.first_idx_in_cur_batch - 1)` with `batch_limit`
                    // Less: indicates that it should be a part of the current batch so we should update the relevant element in `batch_end`
                    // Equal: indicates that it shouldn't be a part of the current batch. We terminate this loop when it happens.
                    // Greater: never gonna be happened
                    let real_batch_size: u64 = self
                        .entries
                        .range(self.get_range_by_batch(self.first_idx_in_cur_batch - 1))
                        .map(|entry| entry.size)
                        .sum();
                    if real_batch_size < self.batch_limit {
                        self.batch_end[self.first_idx_in_cur_batch - 1] = 0;
                        self.first_idx_in_cur_batch -= 1;
                    } else {
                        break;
                    }
                }
                Ordering::Less => {
                    break;
                }
            }
        }

        // recalculate the `cur_batch_size`
        self.cur_batch_size = self
            .entries
            .iter()
            .skip(self.first_idx_in_cur_batch)
            .map(|entry| entry.size)
            .sum();
    }

    /// push a log entry into the back of queue
    fn push_back(&mut self, inner: Arc<LogEntry<C>>, size: u64) {
        if size > self.batch_limit {
            warn!("entry_size of an entry > batch_limit, which may be too small.",);
        }

        self.entries.push_back(Entry { inner, size });
        self.batch_end.push_back(0); // placeholder
        self.cur_batch_size += size;

        // it's safe to do so:
        // 1. The `self.first_idx_in_cur_batch` is always less than `self.batch_end.len()`
        // 2. The `self.entries.len()` is always larger than or equal to 1
        // 3. When the condition `self.cur_batch_size > self.batch_limit && entry < self.batch_limit` is met, the `self.entries.len()` is always larger than or equal to 2
        #[allow(clippy::indexing_slicing)]
        // when the `cur_batch_size` >= `batch_limit` is true, we should do the following three things:
        // 1. update the `batch_end[first_idx_in_cur_batch]`
        // 2. remove the size of `entries[first_idx_in_cur_batch]` from `cur_batch_size`
        // 3. increase the `first_idx_in_cur_batch`
        while self.cur_batch_size >= self.batch_limit {
            self.batch_end[self.first_idx_in_cur_batch] =
                if self.cur_batch_size == self.batch_limit || size > self.batch_limit {
                    self.entries[self.entries.len() - 1].inner.index
                } else {
                    self.entries[self.entries.len() - 2].inner.index
                };
            self.cur_batch_size -= self.entries[self.first_idx_in_cur_batch].size;
            self.first_idx_in_cur_batch += 1;
        }
    }

    /// pop a log entry from the front of queue
    fn pop_front(&mut self) -> Option<Arc<LogEntry<C>>> {
        if let Some(entry) = self.entries.pop_front() {
            if self.first_idx_in_cur_batch == 0 {
                self.cur_batch_size -= entry.size;
            } else {
                self.first_idx_in_cur_batch -= 1;
            }
            let _ = self
                .batch_end
                .pop_front()
                .unwrap_or_else(|| unreachable!("The batch_end cannot be empty"));
            Some(entry.inner)
        } else {
            None
        }
    }

    /// restore log entries from Vec
    fn restore(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), bincode::Error> {
        self.batch_end = VecDeque::with_capacity(entries.capacity());
        self.entries = VecDeque::with_capacity(entries.capacity());

        self.cur_batch_size = 0;
        self.first_idx_in_cur_batch = 0;

        for entry in entries {
            let entry = Arc::from(entry);
            let size = bincode::serialized_size(&entry)?;
            self.push_back(entry, size);
        }
        Ok(())
    }

    /// clear whole log entries
    fn clear(&mut self) {
        self.entries.clear();
        self.batch_end.clear();
        self.cur_batch_size = 0;
        self.first_idx_in_cur_batch = 0;
    }

    /// Get the range [left, right) of the log entry, whose size should be equal or smaller than `batch_limit`
    #[allow(clippy::indexing_slicing)] // it's safe to do so since we validate `left` at very begin place
    fn get_range_by_batch(&self, left: usize) -> LogRange<usize> {
        if left >= self.batch_end.len() {
            return LogRange::Range(self.batch_end.len()..self.batch_end.len());
        }

        if self.entries[left].size == self.batch_limit {
            return LogRange::RangeInclusive(left..=left);
        }

        let right = if self.batch_end[left] == 0 {
            self.entries.len() - 1
        } else {
            self.li_to_pi(self.batch_end[left])
        };

        LogRange::RangeInclusive(left..=right)
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

/// Type returned when append success
type AppendSuccess<C> = (Vec<Arc<LogEntry<C>>>, ConfChangeEntries<C>, FallbackIndexes);

impl<C: Command> Log<C> {
    /// Create a new log
    pub(super) fn new(batch_limit: u64, entries_cap: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(entries_cap),
            batch_end: VecDeque::with_capacity(entries_cap),
            batch_limit,
            first_idx_in_cur_batch: 0,
            cur_batch_size: 0,
            commit_index: 0,
            base_index: 0,
            base_term: 0,
            last_as: 0,
            last_exe: 0,
            fallback_contexts: HashMap::new(),
            entries_cap,
        }
    }

    /// Get last log index
    pub(super) fn last_log_index(&self) -> LogIndex {
        self.entries
            .back()
            .map_or(self.base_index, |entry| entry.inner.index)
    }

    /// Get last log term
    pub(super) fn last_log_term(&self) -> u64 {
        self.entries
            .back()
            .map_or(self.base_term, |entry| entry.inner.term)
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
            .then(|| self.entries.get(self.li_to_pi(i)).map(|entry| &entry.inner))
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
    ) -> Result<AppendSuccess<C>, Vec<LogEntry<C>>> {
        let mut to_persist = Vec::with_capacity(entries.len());
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
                .map_or(true, |old_entry| old_entry.inner.term != entry.term)
            {
                break;
            }
            pi += 1;
        }
        // Record entries that need to be fallback in the truncated entries
        for e in self.entries.range(pi..) {
            if matches!(e.inner.entry_data, EntryData::ConfChange(_)) {
                let _ig = need_fallback_indexes.insert(e.inner.index);
            }
        }
        // Truncate entries
        self.truncate(pi);
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
            self.push_back(
                Arc::clone(&entry),
                bincode::serialized_size(&entry).expect("log entry {entry:?} cannot be serialized"),
            );

            to_persist.push(entry);
        }

        Ok((to_persist, conf_changes, need_fallback_indexes))
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
    // FIXME: persistent other log entries
    // TODO: Avoid allocation during locking
    pub(super) fn push(
        &mut self,
        term: u64,
        propose_id: ProposeId,
        entry: impl Into<EntryData<C>>,
    ) -> Arc<LogEntry<C>> {
        let index = self.last_log_index() + 1;
        let entry = Arc::new(LogEntry::new(index, term, propose_id, entry));
        let size = bincode::serialized_size(&entry)
            .unwrap_or_else(|_| unreachable!("bindcode serialization should always succeed"));
        self.push_back(Arc::clone(&entry), size);
        entry
    }

    /// check whether the log entry range [li,..) exceeds the batch limit or not
    #[allow(clippy::indexing_slicing)] // it's safe to do so since the length of `batch_end` is always same as `entry_size`
    pub(super) fn has_next_batch(&self, li: u64) -> bool {
        let left = self.li_to_pi(li);
        if let Some(&batch_end) = self.batch_end.get(left) {
            batch_end != 0 || self.entries[left].size == self.batch_limit
        } else {
            false
        }
    }

    /// Get a range of log entry
    pub(super) fn get_from(&self, li: LogIndex) -> Vec<Arc<LogEntry<C>>> {
        let left = self.li_to_pi(li);
        let range = self.get_range_by_batch(left);
        self.entries
            .range(range)
            .map(|entry| &entry.inner)
            .cloned()
            .collect_vec()
    }

    /// Get existing cmd ids
    pub(super) fn get_cmd_ids(&self) -> HashSet<ProposeId> {
        self.entries
            .iter()
            .map(|entry| entry.inner.propose_id)
            .collect()
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
        self.clear();
    }

    /// Restore log entries, provided entries must be in order
    pub(super) fn restore_entries(
        &mut self,
        entries: Vec<LogEntry<C>>,
    ) -> Result<(), bincode::Error> {
        // restore batch index
        self.restore(entries)?;
        self.compact();
        Ok(())
    }

    /// Compact log
    pub(super) fn compact(&mut self) {
        let Some(first_entry) = self.entries.front() else {
            return;
        };
        if self.last_as <= first_entry.inner.index {
            return;
        }
        let compact_from =
            if self.last_as - first_entry.inner.index >= self.entries_cap.numeric_cast() {
                self.last_as - self.entries_cap.numeric_cast::<u64>()
            } else {
                return;
            };
        while self
            .entries
            .front()
            .map_or(false, |entry| entry.inner.index <= compact_from)
        {
            let res = self.pop_front();
            match res {
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

    #[cfg(test)]
    /// set batch limit and reconstruct `batch_end`
    pub(super) fn set_batch_limit(&mut self, batch_limit: u64) {
        #![allow(clippy::indexing_slicing)]
        self.batch_limit = batch_limit;
        self.cur_batch_size = 0;
        self.first_idx_in_cur_batch = 0;
        self.batch_end.clear();

        let prev_entries = self.entries.clone();
        self.entries.clear();

        let _unused = prev_entries.into_iter().for_each(|entry| {
            let _u = self.push_back(entry.inner, entry.size);
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
            &self.entries[pi].inner
        }
    }

    #[test]
    fn test_log_up_to_date() {
        let mut log = Log::<TestCommand>::new(default_batch_max_size(), default_log_entries_cap());
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
        let mut log = Log::<TestCommand>::new(default_batch_max_size(), default_log_entries_cap());
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
        let mut log = Log::<TestCommand>::new(default_batch_max_size(), default_log_entries_cap());
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
        let mut log = Log::<TestCommand>::new(default_batch_max_size(), default_log_entries_cap());

        // Note: this test must use the same test command to ensure the size of the entry is fixed
        let test_cmd = Arc::new(TestCommand::default());
        let _res = repeat(Arc::clone(&test_cmd))
            .take(10)
            .enumerate()
            .map(|(idx, cmd)| log.push(1, ProposeId(0, idx.numeric_cast()), cmd))
            .collect::<Vec<_>>();
        let log_entry_size = log.entries[0].size;

        log.set_batch_limit(3 * log_entry_size - 1);
        let bound_1 = log.get_range_by_batch(3);
        assert_eq!(
            bound_1,
            LogRange::RangeInclusive(3..=4),
            "batch_end = {:?}, batch = {}, log_entry_size = {}",
            log.batch_end,
            log.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(8));
        assert!(!log.has_next_batch(9));

        log.set_batch_limit(4 * log_entry_size);
        let bound_2 = log.get_range_by_batch(3);
        assert_eq!(
            bound_2,
            LogRange::RangeInclusive(3..=6),
            "batch_end = {:?}, batch = {}, log_entry_size = {}",
            log.batch_end,
            log.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(7));
        assert!(!log.has_next_batch(8));

        log.set_batch_limit(5 * log_entry_size + 2);
        let bound_3 = log.get_range_by_batch(3);
        assert_eq!(
            bound_3,
            LogRange::RangeInclusive(3..=7),
            "batch_end = {:?}, batch = {}, log_entry_size = {}",
            log.batch_end,
            log.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(5));
        assert!(!log.has_next_batch(6));

        log.set_batch_limit(100 * log_entry_size);
        let bound_4 = log.get_range_by_batch(3);
        assert_eq!(
            bound_4,
            LogRange::RangeInclusive(3..=9),
            "batch_end = {:?}, batch = {}, log_entry_size = {}",
            log.batch_end,
            log.batch_limit,
            log_entry_size
        );
        assert!(!log.has_next_batch(1));
        assert!(!log.has_next_batch(5));

        log.set_batch_limit(log_entry_size - 1);
        let bound_5 = log.get_range_by_batch(3);
        assert_eq!(
            bound_5,
            LogRange::RangeInclusive(3..=3),
            "batch_end = {:?}, batch = {}, log_entry_size = {}",
            log.batch_end,
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
            .map(|(idx, cmd)| {
                LogEntry::new(
                    (idx + 1).numeric_cast(),
                    1,
                    ProposeId(0, idx.numeric_cast()),
                    cmd,
                )
            })
            .collect::<Vec<LogEntry<TestCommand>>>();
        let mut log = Log::<TestCommand>::new(default_batch_max_size(), default_log_entries_cap());

        log.restore_entries(entries).unwrap();
        assert_eq!(log.entries.len(), 10);
        assert_eq!(log.batch_end.len(), 10);
    }

    #[test]
    fn compact_test() {
        let mut log = Log::<TestCommand>::new(default_batch_max_size(), 10);

        for i in 0..30 {
            log.push(0, ProposeId(0, i), Arc::new(TestCommand::default()));
        }
        log.last_as = 22;
        log.last_exe = 22;
        log.compact();
        assert_eq!(log.base_index, 12);
        assert_eq!(log.entries.front().unwrap().inner.index, 13);
        assert_eq!(log.batch_end.len(), 18);
        assert!(log.entries.len() == log.batch_end.len());
    }

    #[test]
    fn get_from_should_success_after_compact() {
        let mut log = Log::<TestCommand>::new(default_batch_max_size(), 10);
        for i in 0..30 {
            log.push(0, ProposeId(0, i), Arc::new(TestCommand::default()));
        }
        let log_entry_size = log.entries[0].size;
        log.set_batch_limit(2 * log_entry_size);
        log.last_as = 22;
        log.last_exe = 22;
        log.compact();
        assert_eq!(log.base_index, 12);
        assert_eq!(log.entries.front().unwrap().inner.index, 13);
        assert_eq!(log.batch_end.len(), 18);
        assert_eq!(log.first_idx_in_cur_batch, 17);

        let batch_1 = log.get_range_by_batch(3);
        assert_eq!(
            batch_1,
            LogRange::RangeInclusive(3..=4),
            "batch_end = {:?}, batch = {}, log_entry_size = {}",
            log.batch_end,
            log.batch_limit,
            log_entry_size
        );

        let batch_2 = log.get_range_by_batch(1024);
        assert_eq!(
            batch_2,
            LogRange::Range(18..18),
            "batch_end = {:?}, batch = {}, log_entry_size = {}",
            log.batch_end,
            log.batch_limit,
            log_entry_size
        );
        assert!(log.has_next_batch(15));
    }

    #[test]
    fn batch_info_should_update_correctly_after_truncated() {
        let mut log = Log::<TestCommand>::new(11, 10);
        let mock_entries_sizes = vec![1, 5, 6, 2, 3, 4, 5];
        let test_cmd = Arc::new(TestCommand::default());

        let _entries = repeat(Arc::clone(&test_cmd))
            .take(mock_entries_sizes.len())
            .enumerate()
            .map(|(idx, cmd)| {
                Arc::new(LogEntry::new(
                    (idx + 1).numeric_cast(),
                    1,
                    ProposeId(0, idx.numeric_cast()),
                    cmd,
                ))
            })
            .zip(mock_entries_sizes)
            .map(|(entry, size)| log.push_back(entry, size))
            .collect::<Vec<_>>();

        assert_eq!(log.cur_batch_size, 9);
        assert_eq!(log.first_idx_in_cur_batch, 5);

        // case 1. truncate len > first_idx_in_cur_batch
        // after truncate, the `entries` should be [1, 5, 6, 2, 3, 4], the `batch_end` should be [2, 3, 5, 0, 0, 0]
        log.truncate(6);
        assert_eq!(log.first_idx_in_cur_batch, 3);
        assert_eq!(log.cur_batch_size, 9);
        assert_eq!(log.batch_end, VecDeque::from(vec![2, 3, 5, 0, 0, 0]));

        // case 2. truncate len = first_idx_in_cur_batch
        // after truncate, the `entries` should be [1, 5, 6, 2, 3], the `batch_end` should be [2, 3, 5, 0, 0]
        log.truncate(5);
        assert_eq!(log.first_idx_in_cur_batch, 3);
        assert_eq!(log.cur_batch_size, 5);
        assert_eq!(log.batch_end, VecDeque::from(vec![2, 3, 5, 0, 0]));

        // case 3. truncate len < first_idx_in_cur_batch
        // after truncate, the `entries` should be [1, 5], the `batch_end` should be [0, 0]
        log.truncate(2);
        assert_eq!(log.first_idx_in_cur_batch, 0);
        assert_eq!(log.cur_batch_size, 6);
        assert_eq!(log.batch_end, VecDeque::from(vec![0, 0]));
    }
}
