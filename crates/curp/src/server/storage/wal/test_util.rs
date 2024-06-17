use bytes::BytesMut;
use curp_external_api::LogIndex;
use curp_test_utils::test_cmd::TestCommand;
use parking_lot::Mutex;

use crate::{
    log_entry::{EntryData, LogEntry},
    rpc::ProposeId,
    server::storage::wal::segment::WAL_HEADER_SIZE,
};

use super::{
    codec::{DataFrameOwned, WAL},
    framed::Encoder,
};

pub(super) struct EntryGenerator {
    inner: Mutex<Inner>,
}

struct Inner {
    next_index: u64,
    segment_size: u64,
    logs_sent: Vec<LogEntry<TestCommand>>,
}

impl EntryGenerator {
    pub(super) fn new(segment_size: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                next_index: 1,
                segment_size,
                logs_sent: Vec::new(),
            }),
        }
    }

    pub(super) fn skip(&self, num_index: usize) {
        let mut this = self.inner.lock();
        this.next_index += num_index as u64;
    }

    pub(super) fn next(&self) -> LogEntry<TestCommand> {
        let mut this = self.inner.lock();
        let entry =
            LogEntry::<TestCommand>::new(this.next_index, 1, ProposeId(1, 2), EntryData::Empty);
        this.logs_sent.push(entry.clone());
        this.next_index += 1;
        entry
    }

    pub(super) fn take(&self, num: usize) -> Vec<LogEntry<TestCommand>> {
        (0..num).map(|_| self.next()).collect()
    }

    pub(super) fn reset_next_index_to(&self, index: LogIndex) {
        let mut this = self.inner.lock();
        this.next_index = index;
        this.logs_sent.truncate(index as usize - 1);
    }

    pub(super) fn current_index(&self) -> LogIndex {
        let this = self.inner.lock();
        this.next_index - 1
    }

    pub(super) fn all_logs(&self) -> Vec<LogEntry<TestCommand>> {
        self.inner.lock().logs_sent.clone()
    }

    pub(super) fn num_entries_per_page() -> usize {
        let page_size = 4096;
        Self::cal_num(page_size)
    }

    pub(super) fn num_entries_per_segment(&self) -> usize {
        let this = self.inner.lock();
        Self::cal_num(this.segment_size as usize)
    }

    fn cal_num(size: usize) -> usize {
        let entry_size = Self::entry_size();
        (size - WAL_HEADER_SIZE + entry_size - 1) / entry_size
    }

    fn entry_size() -> usize {
        let sample_entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let mut wal_codec = WAL::<TestCommand>::new();
        let buf = wal_codec
            .encode(vec![DataFrameOwned::Entry(sample_entry).get_ref()])
            .unwrap();
        buf.len()
    }
}
