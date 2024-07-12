use std::{collections::VecDeque, io, marker::PhantomData};

use curp_external_api::LogIndex;
use serde::{de::DeserializeOwned, Serialize};

use crate::log_entry::LogEntry;

use super::{codec::DataFrame, config::WALConfig, WALStorageOps};

/// The mock WAL storage
#[derive(Debug)]
pub(crate) struct WALStorage<C> {
    /// Storage
    entries: VecDeque<LogEntry<C>>,
}

impl<C> WALStorage<C> {
    /// Creates a new mock `WALStorage`
    pub(super) fn new() -> WALStorage<C> {
        Self {
            entries: VecDeque::new(),
        }
    }
}

impl<C> WALStorageOps<C> for WALStorage<C>
where
    C: Clone,
{
    fn recover(&mut self) -> io::Result<Vec<LogEntry<C>>> {
        Ok(self.entries.clone().into_iter().collect())
    }

    fn send_sync(&mut self, item: Vec<DataFrame<'_, C>>) -> io::Result<()> {
        for frame in item {
            if let DataFrame::Entry(entry) = frame {
                self.entries.push_back(entry.clone());
            }
        }

        Ok(())
    }

    fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
        while self
            .entries
            .front()
            .is_some_and(|e| e.index <= compact_index)
        {
            let _ignore = self.entries.pop_front();
        }
        Ok(())
    }

    fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()> {
        while self.entries.back().is_some_and(|e| e.index > max_index) {
            let _ignore = self.entries.pop_back();
        }
        Ok(())
    }
}
