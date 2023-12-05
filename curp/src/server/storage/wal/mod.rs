#![allow(unused)] // TODO: remove this until used

/// The WAL codec
mod codec;

/// The config for `WALStorage`
mod config;

/// WAL errors
mod error;

/// File pipeline
mod pipeline;

/// Remover of the segment file
mod remover;

/// WAL segment
mod segment;

/// WAL storage tests
#[cfg(test)]
mod tests;

/// File utils
mod util;

use std::{io, marker::PhantomData};

use clippy_utilities::OverflowArithmetic;
use curp_external_api::LogIndex;
use futures::{future::join_all, Future, SinkExt, StreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use crate::log_entry::LogEntry;

use self::{
    codec::{DataFrame, WAL},
    config::WALConfig,
    error::{CorruptError, WALError},
    pipeline::FilePipeline,
    remover::SegmentRemover,
    segment::{IOState, WALSegment},
    util::LockedFile,
};

/// The magic of the WAL file
const WAL_MAGIC: u32 = 0xd86e_0be2;

/// The current WAL version
const WAL_VERSION: u8 = 0x00;

/// The wal file extension
const WAL_FILE_EXT: &str = ".wal";

/// The WAL storage
struct WALStorage<C> {
    /// The directory to store the log files
    config: WALConfig,
    /// The pipeline that pre-allocates files
    pipeline: FilePipeline,
    /// WAL segments
    segments: Vec<WALSegment>,
    /// Next segment id
    next_segment_id: u64,
    /// Next segment id
    next_log_index: LogIndex,
    /// The phantom data
    _phantom: PhantomData<C>,
}

impl<C> WALStorage<C>
where
    C: Serialize + DeserializeOwned + Unpin + 'static,
{
    /// Recover from the given directory if there's any segments
    /// Otherwise, creates a new `LogStorage`
    pub(super) async fn new_or_recover(config: WALConfig) -> io::Result<(Self, Vec<LogEntry<C>>)> {
        // We try to recover the removal first
        SegmentRemover::recover(&config.dir).await?;

        let mut pipeline = FilePipeline::new(config.dir.clone(), config.max_segment_size);
        let file_paths = util::get_file_paths_with_ext(&config.dir, WAL_FILE_EXT)?;
        let lfiles: Vec<_> = file_paths
            .into_iter()
            .map(LockedFile::open_rw)
            .collect::<io::Result<_>>()?;

        let segment_futs = lfiles
            .into_iter()
            .map(|f| WALSegment::open(f, config.max_segment_size));

        let mut segments = Self::take_until_io_error(segment_futs).await?;
        segments.sort_unstable();
        debug!("Recovered segments: {:?}", segments);

        let logs_fut: Vec<_> = segments
            .iter_mut()
            .map(WALSegment::recover_segment_logs)
            .collect();

        let logs_batches = Self::take_until_io_error(logs_fut).await?;
        let mut logs: Vec<_> = logs_batches.into_iter().flatten().collect();

        let pos = Self::highest_valid_pos(&logs[..]);
        if pos != logs.len() {
            warn!("WAL corrupted: {}", CorruptError::LogNotContinue);
            logs = logs.into_iter().take(pos).collect();
        }

        // If there's no segments to recover, create a new segment
        if segments.is_empty() {
            let lfile = pipeline
                .next()
                .await
                .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;
            segments.push(WALSegment::create(lfile, 1, 0, config.max_segment_size).await?);
        }
        let next_segment_id = segments.last().map_or(0, |s| s.id().overflow_add(1));
        let next_log_index = logs.last().map_or(1, |l| l.index.overflow_add(1));
        debug!(
            "WAL successfully recovered, next_segment_id: {next_segment_id}, next_log_index: {next_log_index}"
        );

        Ok((
            Self {
                config,
                pipeline,
                segments,
                next_segment_id,
                next_log_index,
                _phantom: PhantomData,
            },
            logs,
        ))
    }

    /// Send frames with fsync
    #[allow(clippy::pattern_type_mismatch)] // Cannot satisfy both clippy
    pub(super) async fn send_sync(&mut self, item: Vec<DataFrame<C>>) -> io::Result<()> {
        let last_segment = self
            .segments
            .last_mut()
            .unwrap_or_else(|| unreachable!("there should be at least on segment"));
        let mut framed = Framed::new(last_segment, WAL::<C>::new());
        if let Some(DataFrame::Entry(entry)) = item.last() {
            self.next_log_index = entry.index.overflow_add(1);
        }
        framed.send(item).await?;
        framed.flush().await?;
        framed.get_mut().sync_all().await?;

        if framed.get_ref().is_full() {
            self.open_new_segment().await?;
        }

        Ok(())
    }

    /// Tuncate all the logs whose index is less than or equal to `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    pub(super) async fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
        if compact_index >= self.next_log_index {
            warn!(
                "head truncation: compact index too large, compact index: {}, storage next index: {}",
                compact_index, self.next_log_index
            );
            return Ok(());
        }

        let segments: Vec<_> = self
            .segments
            .iter()
            .take_while(|s| s.base_index() <= compact_index)
            .collect();

        if segments.is_empty() {
            return Ok(());
        }

        // The last segment does not need to be removed
        let to_remove = segments.into_iter().rev().skip(1);
        SegmentRemover::new_removal(&self.config.dir, to_remove).await?;

        Ok(())
    }

    /// Tuncate all the logs whose index is greater than `max_index`
    pub(super) async fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()> {
        // segments to truncate
        let segments = self
            .segments
            .iter_mut()
            .rev()
            .take_while_inclusive::<_>(|s| s.base_index() > max_index);

        for segment in segments {
            segment.seal::<C>(max_index).await;
        }

        let to_remove = self.update_segments();
        SegmentRemover::new_removal(&self.config.dir, to_remove.iter()).await?;

        self.next_log_index = max_index.overflow_add(1);
        self.open_new_segment().await?;

        Ok(())
    }

    /// Opens a new WAL segment
    async fn open_new_segment(&mut self) -> io::Result<()> {
        let lfile = self
            .pipeline
            .next()
            .await
            .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;

        let segment = WALSegment::create(
            lfile,
            self.next_log_index,
            self.next_segment_id,
            self.config.max_segment_size,
        )
        .await?;

        self.segments.push(segment);
        self.next_segment_id = self.next_segment_id.overflow_add(1);

        Ok(())
    }

    /// Removes segments that is no long needed
    #[allow(clippy::pattern_type_mismatch)] // Cannot satisfy both clippy
    fn update_segments(&mut self) -> Vec<WALSegment> {
        let flags: Vec<_> = self.segments.iter().map(WALSegment::is_redundant).collect();
        let (to_remove, remaining): (Vec<_>, Vec<_>) = self
            .segments
            .drain(..)
            .zip(flags.into_iter())
            .partition(|(_, f)| *f);

        self.segments = remaining.into_iter().map(|(s, _)| s).collect();

        to_remove.into_iter().map(|(s, _)| s).collect()
    }

    /// Syncs all flushed segments
    async fn sync_segments(&mut self) -> io::Result<()> {
        let to_sync = self
            .segments
            .iter_mut()
            .filter(|s| matches!(s.io_state(), IOState::Flushed));
        for segment in to_sync {
            segment.sync_all().await?;
        }

        Ok(())
    }

    /// Returns the highest valid position of the log entries,
    /// the logs are continous before this position
    fn highest_valid_pos(entries: &[LogEntry<C>]) -> usize {
        let iter = entries.iter();
        iter.clone()
            .zip(iter.skip(1))
            .enumerate()
            .find(|(_, (x, y))| x.index.overflow_add(1) != y.index)
            .map_or(entries.len(), |(i, _)| i)
    }

    async fn take_until_io_error<T, I>(futs: I) -> io::Result<Vec<T>>
    where
        I: IntoIterator,
        I::Item: Future<Output = Result<T, WALError>>,
    {
        let mut ts = vec![];

        let results: Vec<_> = join_all(futs).await;
        for result in results {
            match result {
                Ok(t) => ts.push(t),
                Err(e) => {
                    let e = e.io_or_corrupt()?;
                    warn!("WAL corrupted: {e}");
                }
            }
        }

        Ok(ts)
    }
}
