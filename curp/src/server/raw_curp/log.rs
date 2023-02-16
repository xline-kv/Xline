#![allow(clippy::indexing_slicing)] // use index to access log is safe
#![allow(clippy::integer_arithmetic)] // u64 is large enough and won't overflow

use std::{collections::HashSet, ops::Index, sync::Arc};

use crate::{
    cmd::{Command, ProposeId},
    log_entry::LogEntry,
};

/// Curp logs
/// There exists a fake log entry 0 whose term equals 0
#[derive(Debug)]
pub(super) struct Log<C: Command> {
    /// Log entries, should be persisted
    /// Note that the logical index in `LogEntry` is different from physical index
    entries: Vec<LogEntry<C>>,
    /// Base index in entries, is `1`(if no snapshot) or `last_log_index_in_snapshot + 1`
    base_index: usize,
    /// Index of highest log entry known to be committed
    pub(super) commit_index: usize,
    /// Index of highest log entry applied to state machine
    pub(super) last_applied: usize,
}

impl<C: Command> Index<usize> for Log<C> {
    type Output = LogEntry<C>;

    fn index(&self, i: usize) -> &Self::Output {
        let pi = self.li_to_pi(i);
        &self.entries[pi]
    }
}

impl<C: Command> Log<C> {
    /// Create a new log
    pub(super) fn new() -> Self {
        Self {
            entries: vec![], // a fake log[0]
            commit_index: 0,
            last_applied: 0,
            base_index: 1,
        }
    }

    /// Get last log index
    pub(super) fn last_log_index(&self) -> usize {
        self.entries.last().map_or(0, |entry| entry.index)
    }

    /// Get last log term
    pub(super) fn last_log_term(&self) -> u64 {
        self.entries.last().map_or(0, |entry| entry.term)
    }

    /// Transform logical index to physical index of `self.entries`
    pub(super) fn li_to_pi(&self, i: usize) -> usize {
        assert!(
            i >= self.base_index,
            "can't access the log entry whose index is smaller than {}",
            self.base_index
        );
        i - self.base_index
    }

    /// Try to append log entries, hand back the entries if they can't be appended
    pub(super) fn try_append_entries(
        &mut self,
        entries: Vec<LogEntry<C>>,
        prev_log_index: usize,
        prev_log_term: u64,
    ) -> Result<(), Vec<LogEntry<C>>> {
        // check if entries can be appended
        if prev_log_index != 0
            && self
                .entries
                .get(self.li_to_pi(prev_log_index))
                .map_or(true, |entry| entry.term != prev_log_term)
        {
            return Err(entries);
        }

        // append log entries, will erase inconsistencies
        let mut li = prev_log_index;
        for entry in entries {
            li += 1;
            let pi = self.li_to_pi(li);
            let inconsistent = if let Some(old_entry) = self.entries.get(pi) {
                old_entry.term != entry.term
            } else {
                self.entries.push(entry);
                continue;
            };
            if inconsistent {
                self.entries.truncate(pi);
                self.entries.push(entry);
            }
        }

        Ok(())
    }

    /// Check if the candidate's log is up-to-date
    pub(super) fn log_up_to_date(&self, last_log_term: u64, last_log_index: usize) -> bool {
        if last_log_term == self.last_log_term() {
            // if the last log entry has the same term, grant vote if candidate has a longer log
            last_log_index >= self.last_log_index()
        } else {
            // otherwise grant vote only if the candidate has higher log term
            last_log_term > self.last_log_term()
        }
    }

    /// Pack the cmd into a log entry and push it to the end of the log, return its index
    pub(super) fn push_cmd(&mut self, term: u64, cmd: Arc<C>) -> usize {
        let index = self.last_log_index() + 1;
        let entry = LogEntry::new(index, term, cmd);
        self.entries.push(entry);
        self.last_log_index()
    }

    /// Get a range of log entry
    pub(super) fn get_from(&self, li: usize) -> &[LogEntry<C>] {
        let pi = self.li_to_pi(li);
        &self.entries[pi..]
    }

    /// Get existing cmd ids
    pub(super) fn get_cmd_ids(&self) -> HashSet<&ProposeId> {
        self.entries.iter().map(|entry| entry.cmd.id()).collect()
    }

    /// Get previous log entry's term and index
    pub(super) fn get_prev_entry_info(&self, i: usize) -> (u64, usize) {
        assert!(i > 0);
        if i == 1 {
            (0, 0) // fake log[0]
        } else {
            (self.index(i - 1).term, self.index(i - 1).index)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::test_utils::test_cmd::TestCommand;

    #[test]
    fn test_log_up_to_date() {
        let mut log = Log::<TestCommand>::new();
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
        let mut log = Log::<TestCommand>::new();
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
        let mut log = Log::<TestCommand>::new();
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
}
