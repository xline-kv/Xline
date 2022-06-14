use crate::{cmd::Command, message::TermNum};

/// Log entry status
#[derive(Debug, Clone)]
pub(crate) enum EntryStatus {
    /// The entry has not synced
    #[allow(dead_code)]
    Unsynced,
    /// The entry has been synced to the majority to the
    #[allow(dead_code)]
    Synced,
}

/// Log entry
#[derive(Debug, Clone)]
pub(crate) struct LogEntry<C: Command> {
    /// Term id
    #[allow(dead_code)]
    term: TermNum,
    /// Command
    #[allow(dead_code)]
    cmd: C,
    /// Log entry status
    #[allow(dead_code)]
    status: EntryStatus,
}

impl<C: Command> LogEntry<C> {
    /// Create a new `LogEntry`
    pub(crate) fn new(term: TermNum, cmd: C, status: EntryStatus) -> Self {
        Self { term, cmd, status }
    }

    /// Get term id
    #[allow(dead_code)]
    pub(crate) fn term(&self) -> TermNum {
        self.term
    }

    /// Get command in the entry
    #[allow(dead_code)]
    pub(crate) fn cmd(&self) -> &C {
        &self.cmd
    }

    /// Get status in the entry
    #[allow(dead_code)]
    pub(crate) fn status(&self) -> &EntryStatus {
        &self.status
    }

    /// Set entry status
    #[allow(dead_code)]
    pub(crate) fn set_status(&mut self, status: EntryStatus) {
        self.status = status;
    }
}
