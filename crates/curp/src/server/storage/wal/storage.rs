use std::{io, marker::PhantomData, ops::Mul};

use clippy_utilities::OverflowArithmetic;
use curp_external_api::LogIndex;
use futures::{future::join_all, Future, SinkExt, StreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use crate::log_entry::LogEntry;

use super::{
    codec::{DataFrame, DataFrameOwned, WAL},
    config::PersistentConfig,
    error::{CorruptType, WALError},
    pipeline::FilePipeline,
    remover::SegmentRemover,
    segment::WALSegment,
    util::{self, LockedFile},
    WALStorageOps, WAL_FILE_EXT,
};

/// The WAL storage
#[derive(Debug)]
pub(crate) struct WALStorage<C> {
    /// The config of wal files
    config: PersistentConfig,
    /// The pipeline that pre-allocates files
    pipeline: FilePipeline,
    /// WAL segments
    segments: Vec<WALSegment>,
    /// The next segment id
    next_segment_id: u64,
    /// The next log index
    next_log_index: LogIndex,
    /// The phantom data
    _phantom: PhantomData<C>,
}

impl<C> WALStorage<C> {
    /// Creates a new `LogStorage`
    pub(super) fn new(config: PersistentConfig) -> io::Result<WALStorage<C>> {
        if !config.dir.try_exists()? {
            std::fs::create_dir_all(&config.dir);
        }
        let mut pipeline = FilePipeline::new(config.dir.clone(), config.max_segment_size);
        Ok(Self {
            config,
            pipeline,
            segments: vec![],
            next_segment_id: 0,
            next_log_index: 0,
            _phantom: PhantomData,
        })
    }
}

impl<C> WALStorageOps<C> for WALStorage<C>
where
    C: Serialize + DeserializeOwned + std::fmt::Debug,
{
    /// Recover from the given directory if there's any segments
    fn recover(&mut self) -> io::Result<Vec<LogEntry<C>>> {
        /// Number of lines printed around the missing log in debug information
        const NUM_LINES_DEBUG: usize = 3;
        // We try to recover the removal first
        SegmentRemover::recover(&self.config.dir)?;

        let file_paths = util::get_file_paths_with_ext(&self.config.dir, WAL_FILE_EXT)?;
        let lfiles: Vec<_> = file_paths
            .into_iter()
            .map(LockedFile::open_rw)
            .collect::<io::Result<_>>()?;

        let segment_opening = lfiles
            .into_iter()
            .map(|f| WALSegment::open(f, self.config.max_segment_size));

        let mut segments = Self::take_until_io_error(segment_opening)?;
        segments.sort_unstable();
        debug!("Recovered segments: {:?}", segments);

        let logs_iter = segments.iter_mut().map(WALSegment::recover_segment_logs);

        let logs_batches = Self::take_until_io_error(logs_iter)?;
        let mut logs: Vec<_> = logs_batches.into_iter().flatten().collect();

        let pos = Self::highest_valid_pos(&logs[..]);
        if pos != logs.len() {
            let debug_logs: Vec<_> = logs
                .iter()
                .skip(pos.overflow_sub(pos.min(NUM_LINES_DEBUG)))
                .take(NUM_LINES_DEBUG.mul(2))
                .collect();
            error!(
                "WAL corrupted: {}, truncated at position: {pos}, logs around this position: {debug_logs:?}",
                CorruptType::LogNotContinue
            );
            logs.truncate(pos);
        }

        let next_segment_id = segments.last().map_or(0, |s| s.id().overflow_add(1));
        let next_log_index = logs.last().map_or(1, |l| l.index.overflow_add(1));
        self.next_segment_id = next_segment_id;
        self.next_log_index = next_log_index;
        self.segments = segments;

        self.open_new_segment()?;
        info!("WAL successfully recovered");

        Ok(logs)
    }

    #[allow(clippy::pattern_type_mismatch)] // Cannot satisfy both clippy
    fn send_sync(&mut self, item: Vec<DataFrame<'_, C>>) -> io::Result<()> {
        let last_segment = self
            .segments
            .last_mut()
            .unwrap_or_else(|| unreachable!("there should be at least on segment"));
        if let Some(DataFrame::Entry(entry)) = item.last() {
            self.next_log_index = entry.index.overflow_add(1);
        }
        last_segment.write_sync(item, WAL::new())?;

        if last_segment.is_full() {
            self.open_new_segment()?;
        }

        Ok(())
    }

    /// Truncate all the logs whose index is less than or equal to
    /// `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
        if compact_index >= self.next_log_index {
            warn!(
                "head truncation: compact index too large, compact index: {}, storage next index: {}",
                compact_index, self.next_log_index
            );
            return Ok(());
        }

        debug!("performing head truncation on index: {compact_index}");

        let mut to_remove_num = self
            .segments
            .iter()
            .take_while(|s| s.base_index() <= compact_index)
            .count()
            .saturating_sub(1);

        if to_remove_num == 0 {
            return Ok(());
        }

        // The last segment does not need to be removed
        let to_remove: Vec<_> = self.segments.drain(0..to_remove_num).collect();
        SegmentRemover::new_removal(&self.config.dir, to_remove.iter())?;

        Ok(())
    }

    /// Truncate all the logs whose index is greater than `max_index`
    fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()> {
        // segments to truncate
        let segments: Vec<_> = self
            .segments
            .iter_mut()
            .rev()
            .take_while_inclusive::<_>(|s| s.base_index() > max_index)
            .collect();

        for segment in segments {
            segment.seal::<C>(max_index)?;
        }

        let to_remove = self.update_segments();
        SegmentRemover::new_removal(&self.config.dir, to_remove.iter())?;

        self.next_log_index = max_index.overflow_add(1);
        self.open_new_segment()?;

        Ok(())
    }
}

impl<C> WALStorage<C>
where
    C: Serialize + DeserializeOwned + std::fmt::Debug,
{
    /// Opens a new WAL segment
    fn open_new_segment(&mut self) -> io::Result<()> {
        let lfile = self
            .pipeline
            .next()
            .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;

        let segment = WALSegment::create(
            lfile,
            self.next_log_index,
            self.next_segment_id,
            self.config.max_segment_size,
        )?;

        self.segments.push(segment);
        self.next_segment_id = self.next_segment_id.overflow_add(1);

        Ok(())
    }

    /// Removes segments that are no longer needed
    #[allow(clippy::pattern_type_mismatch)] // Cannot satisfy both clippy
    fn update_segments(&mut self) -> Vec<WALSegment> {
        let flags: Vec<_> = self.segments.iter().map(WALSegment::is_redundant).collect();
        let (to_remove, remaining): (Vec<_>, Vec<_>) =
            self.segments.drain(..).zip(flags).partition(|(_, f)| *f);

        self.segments = remaining.into_iter().map(|(s, _)| s).collect();

        to_remove.into_iter().map(|(s, _)| s).collect()
    }

    /// Returns the highest valid position of the log entries,
    /// the logs are continuous before this position
    #[allow(clippy::pattern_type_mismatch)] // can't fix
    fn highest_valid_pos(entries: &[LogEntry<C>]) -> usize {
        let iter = entries.iter();
        iter.clone()
            .zip(iter.skip(1))
            .enumerate()
            .find(|(_, (x, y))| x.index.overflow_add(1) != y.index)
            .map_or(entries.len(), |(i, _)| i)
    }

    /// Iterates until an `io::Error` occurs.
    fn take_until_io_error<T, I>(opening: I) -> io::Result<Vec<T>>
    where
        I: IntoIterator<Item = Result<T, WALError>>,
    {
        let mut ts = vec![];

        for result in opening {
            match result {
                Ok(t) => ts.push(t),
                Err(e) => {
                    let e = e.io_or_corrupt()?;
                    error!("WAL corrupted: {e}");
                }
            }
        }

        Ok(ts)
    }
}

impl<C> Drop for WALStorage<C> {
    fn drop(&mut self) {
        self.pipeline.stop();
    }
}
