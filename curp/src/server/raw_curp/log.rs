use crate::{cmd::Command, log::LogEntry};

/// Curp logs
#[derive(Debug)]
pub(super) struct Log<C: Command> {
    /// Log entries, should be persisted
    pub(super) entries: Vec<LogEntry<C>>,
    /// Index of highest log entry known to be committed
    pub(super) commit_index: usize,
    /// Index of highest log entry applied to state machine
    pub(super) last_applied: usize,
}

impl<C: Command> Log<C> {
    /// Create a new log
    pub(super) fn new() -> Self {
        Self {
            entries: vec![LogEntry::new(0, &[])], // a fake log[0]
            commit_index: 0,
            last_applied: 0,
        }
    }

    /// Get last log index
    pub(super) fn last_log_index(&self) -> usize {
        self.entries.len() - 1
    }

    /// Get last log term
    pub(super) fn last_log_term(&self) -> u64 {
        #[allow(clippy::indexing_slicing)] // there is always a fake log[0]
        self.entries[self.entries.len() - 1].term()
    }

    /// Try to append log entries, hand back the entries if they can't be appended
    pub(super) fn try_append_entries(
        &mut self,
        entries: Vec<LogEntry<C>>,
        prev_log_index: usize,
        prev_log_term: u64,
    ) -> Result<(), Vec<LogEntry<C>>> {
        // check if entries can be appended
        if self
            .entries
            .get(prev_log_index)
            .map_or(true, |entry| entry.term() != prev_log_term)
        {
            return Err(entries);
        }

        // append log entries, will erase inconsistencies
        let mut i = prev_log_index;
        for entry in entries {
            i += 1;
            let inconsistent = if let Some(old_entry) = self.entries.get(i) {
                old_entry.term() != entry.term()
            } else {
                self.entries.push(entry);
                continue;
            };
            if inconsistent {
                self.entries.truncate(i);
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
                LogEntry::new(1, &[Arc::new(TestCommand::default())]),
                LogEntry::new(1, &[Arc::new(TestCommand::default())]),
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
                LogEntry::new(1, &[Arc::new(TestCommand::default())]),
                LogEntry::new(1, &[Arc::new(TestCommand::default())]),
                LogEntry::new(1, &[Arc::new(TestCommand::default())]),
            ],
            0,
            0,
        );
        assert!(result.is_ok());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(2, &[Arc::new(TestCommand::default())]),
                LogEntry::new(2, &[Arc::new(TestCommand::default())]),
            ],
            1,
            1,
        );
        assert!(result.is_ok());
        assert_eq!(log.entries[3].term(), 2);
        assert_eq!(log.entries[2].term(), 2);
    }

    #[test]
    fn try_append_entries_will_not_append() {
        let mut log = Log::<TestCommand>::new();
        let result = log.try_append_entries(
            vec![LogEntry::new(1, &[Arc::new(TestCommand::default())])],
            0,
            0,
        );
        assert!(result.is_ok());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(2, &[Arc::new(TestCommand::default())]),
                LogEntry::new(2, &[Arc::new(TestCommand::default())]),
            ],
            3,
            1,
        );
        assert!(result.is_err());

        let result = log.try_append_entries(
            vec![
                LogEntry::new(2, &[Arc::new(TestCommand::default())]),
                LogEntry::new(2, &[Arc::new(TestCommand::default())]),
            ],
            1,
            2,
        );
        assert!(result.is_err());
    }
}
