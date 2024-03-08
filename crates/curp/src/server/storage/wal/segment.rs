use std::{io, iter, pin::Pin, sync::Arc, task::Poll};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp_external_api::LogIndex;
use futures::{ready, FutureExt, SinkExt};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    fs::File as TokioFile,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use super::{
    codec::{DataFrame, WAL},
    error::{CorruptType, WALError},
    util::{get_checksum, parse_u64, validate_data, LockedFile},
    WAL_FILE_EXT, WAL_MAGIC, WAL_VERSION,
};
use crate::log_entry::LogEntry;

/// The size of wal file header in bytes
const WAL_HEADER_SIZE: usize = 56;

/// A segment of WAL
#[derive(Debug)]
pub(super) struct WALSegment {
    /// The base index of this segment
    base_index: LogIndex,
    /// The id of this segment
    segment_id: u64,
    /// The soft size limit of this segment
    size_limit: u64,
    /// The opened file of this segment
    file: TokioFile,
    /// The file size of the segment
    size: u64,
    /// The highest index of the segment
    seal_index: LogIndex,
    /// The IO state of the file
    io_state: IOState,
}

/// The IO state of the file
#[derive(Clone, Copy, Debug, Default)]
pub(super) enum IOState {
    /// The initial state that haven't written any data or fsynced
    #[default]
    Fsynced,
    /// Already wrote some data, but haven't flushed yet
    Written,
    /// Already flushed, but haven't called fsync yet
    Flushed,
    /// Shutdowned
    Shutdown,
    /// The IO has failed on this file
    Errored,
}

impl WALSegment {
    /// Creates a new `WALSegment`
    pub(super) async fn create(
        tmp_file: LockedFile,
        base_index: LogIndex,
        segment_id: u64,
        size_limit: u64,
    ) -> io::Result<Self> {
        let segment_name = Self::segment_name(segment_id, base_index);
        let mut lfile = tmp_file.rename(segment_name)?;
        let mut tokio_file = lfile.into_async();
        tokio_file
            .write_all(&Self::gen_header(base_index, segment_id))
            .await?;
        tokio_file.flush().await?;
        tokio_file.sync_all().await?;

        Ok(Self {
            base_index,
            segment_id,
            size_limit,
            file: tokio_file,
            size: WAL_HEADER_SIZE.numeric_cast(),
            // For convenience we set it to largest u64 value that represent not sealed
            seal_index: u64::MAX,
            io_state: IOState::default(),
        })
    }

    /// Open an existing WAL segment file
    pub(super) async fn open(mut lfile: LockedFile, size_limit: u64) -> Result<Self, WALError> {
        let mut tokio_file = lfile.into_async();
        let size = tokio_file.metadata().await?.len();
        let mut buf = vec![0; WAL_HEADER_SIZE];
        let _ignore = tokio_file.read_exact(&mut buf).await?;
        let (base_index, segment_id) = Self::parse_header(&buf)?;

        Ok(Self {
            base_index,
            segment_id,
            size_limit,
            file: tokio_file,
            size,
            // Index 0 means the seal_index hasn't been read yet
            seal_index: 0,
            io_state: IOState::default(),
        })
    }

    /// Recover log entries from a `WALSegment`
    pub(super) async fn recover_segment_logs<C>(
        &mut self,
    ) -> Result<impl Iterator<Item = LogEntry<C>>, WALError>
    where
        C: Serialize + DeserializeOwned + 'static,
    {
        let mut self_framed = Framed::new(self, WAL::<C>::new());
        let mut frame_batches = vec![];
        while let Some(result) = self_framed.next().await {
            match result {
                Ok(f) => frame_batches.push(f),
                Err(e) => {
                    /// If the segment file reaches on end, stop reading
                    if matches!(e, WALError::MaybeEnded) {
                        break;
                    }
                    return Err(e);
                }
            }
        }
        // The highest_index of this segment
        let mut highest_index = u64::MAX;
        // We get the last frame batch to check it's type
        if let Some(frames) = frame_batches.last() {
            let frame = frames
                .last()
                .unwrap_or_else(|| unreachable!("a batch should contains at least one frame"));
            if let DataFrame::SealIndex(index) = *frame {
                highest_index = index;
            }
        }

        // Update seal index
        self_framed.get_mut().update_seal_index(highest_index);

        // Get log entries that index is no larger than `highest_index`
        Ok(frame_batches.into_iter().flatten().filter_map(move |f| {
            if let DataFrame::Entry(e) = f {
                (e.index <= highest_index).then_some(e)
            } else {
                None
            }
        }))
    }

    /// Seal the current segment
    ///
    /// After the seal, the log index in this segment should be less than `next_index`
    pub(super) async fn seal<C: Serialize>(&mut self, next_index: LogIndex) -> io::Result<()> {
        let mut framed = Framed::new(self, WAL::<C>::new());
        framed.send(vec![DataFrame::SealIndex(next_index)]).await?;
        framed.flush().await?;
        framed.get_mut().sync_all().await?;
        framed.get_mut().update_seal_index(next_index);
        Ok(())
    }

    /// Syncs the file of this segment
    pub(super) async fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all().await?;
        self.io_state.fsynced();

        Ok(())
    }

    /// Updates the seal index
    pub(super) fn update_seal_index(&mut self, index: LogIndex) {
        self.seal_index = self.seal_index.max(index);
    }

    /// Get the size of the segment
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Checks if the segment is full
    pub(super) fn is_full(&self) -> bool {
        self.size >= self.size_limit
    }

    /// Gets the id of this segment
    pub(super) fn id(&self) -> u64 {
        self.segment_id
    }

    /// Gets the base log index of this segment
    pub(super) fn base_index(&self) -> u64 {
        self.base_index
    }

    /// Checks if the segment is redundant
    ///
    /// The segment is redundant if the `seal_index` is less than the `base_index`
    pub(super) fn is_redundant(&self) -> bool {
        self.seal_index < self.base_index
    }

    /// Gets the io state of this segment
    pub(super) fn io_state(&self) -> IOState {
        self.io_state
    }

    /// Gets the file name of the WAL segment
    pub(super) fn segment_name(segment_id: u64, log_index: u64) -> String {
        format!("{segment_id:016x}-{log_index:016x}{WAL_FILE_EXT}")
    }

    #[allow(clippy::doc_markdown)] // False positive for ASCII graph
    /// Generate the header
    ///
    /// The header layout:
    ///
    /// 0      1      2      3      4      5      6      7      8
    /// |------+------+------+------+------+------+------+------|
    /// | Magic                     | Reserved           | Vsn  |
    /// |------+------+------+------+------+------+------+------|
    /// | BaseIndex                                             |
    /// |------+------+------+------+------+------+------+------|
    /// | SegmentID                                             |
    /// |------+------+------+------+------+------+------+------|
    /// | Checksum (32bytes) ...                                |
    /// |------+------+------+------+------+------+------+------|
    fn gen_header(base_index: LogIndex, segment_id: u64) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend(WAL_MAGIC.to_le_bytes());
        buf.extend(vec![0; 3]);
        buf.push(WAL_VERSION);
        buf.extend(base_index.to_le_bytes());
        buf.extend(segment_id.to_le_bytes());
        buf.extend(get_checksum(&buf));
        buf
    }

    /// Parse the header from the given buffer
    #[allow(
        clippy::unwrap_used, // Unwraps are used to convert slice to const length and is safe
        clippy::arithmetic_side_effects, // Arithmetics cannot overflow
        clippy::indexing_slicing // Index slicings are checked
    )]
    fn parse_header(src: &[u8]) -> Result<(LogIndex, u64), WALError> {
        let mut offset = 0;
        let mut next_field = |len: usize| {
            offset += len;
            &src[(offset - len)..offset]
        };
        let parse_error = Err(WALError::Corrupted(CorruptType::Codec(
            "Segment file header parsing has failed".to_owned(),
        )));
        if src.len() != WAL_HEADER_SIZE
            || next_field(4) != WAL_MAGIC.to_le_bytes()
            || next_field(3) != [0; 3]
            || next_field(1) != [WAL_VERSION]
        {
            return parse_error;
        }
        let base_index = parse_u64(next_field(8));
        let segment_id = parse_u64(next_field(8));
        let checksum = next_field(32);

        if !validate_data(&src[0..24], checksum) {
            return parse_error;
        }

        Ok((base_index, segment_id))
    }
}

impl AsyncWrite for WALSegment {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match ready!(Pin::new(&mut self.file).poll_write(cx, buf)) {
            Ok(len) => {
                self.io_state.written();
                self.size = self.size.overflow_add(len.numeric_cast());
                Poll::Ready(Ok(len))
            }
            Err(e) => {
                self.io_state.errored();
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match ready!(Pin::new(&mut self.file).poll_flush(cx)) {
            Ok(()) => {
                self.io_state.flushed();
                Poll::Ready(Ok(()))
            }
            Err(e) => {
                self.io_state.errored();
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match ready!(Pin::new(&mut self.file).poll_shutdown(cx)) {
            Ok(()) => {
                self.io_state.shutdowned();
                Poll::Ready(Ok(()))
            }
            Err(e) => {
                self.io_state.errored();
                Poll::Ready(Err(e))
            }
        }
    }
}

impl AsyncRead for WALSegment {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl PartialEq for WALSegment {
    fn eq(&self, other: &Self) -> bool {
        self.segment_id.eq(&other.segment_id)
    }
}

impl Eq for WALSegment {}

impl PartialOrd for WALSegment {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.segment_id.partial_cmp(&other.segment_id)
    }
}

impl Ord for WALSegment {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.segment_id.cmp(&other.segment_id)
    }
}

impl IOState {
    /// Mutate the state to `IOState::Written`
    ///
    /// # Panics
    ///
    /// This method panics if the state is not `IOState::Written` or `IOState::Fsynced`
    fn written(&mut self) {
        assert!(
            matches!(*self, IOState::Written | IOState::Fsynced),
            "current state is {self:?}"
        );
        *self = IOState::Written;
    }

    /// Mutate the state to `IOState::Flushed`
    ///
    /// # Panics
    ///
    /// This method panics if the state is not `IOState::Flushed` or `IOState::Written`
    fn flushed(&mut self) {
        assert!(
            matches!(*self, IOState::Flushed | IOState::Written),
            "current state is {self:?}"
        );
        *self = IOState::Flushed;
    }

    /// Mutate the state to `IOState::Written`
    ///
    /// # Panics
    ///
    /// This method panics if the state is not `IOState::Fsynced` or `IOState::Flushed`
    fn fsynced(&mut self) {
        assert!(
            matches!(*self, IOState::Fsynced | IOState::Flushed),
            "current state is {self:?}"
        );
        *self = IOState::Fsynced;
    }

    /// Mutate the state to `IOState::Errored`
    fn errored(&mut self) {
        *self = IOState::Errored;
    }

    /// Mutate the state to `IOState::Shutdowned`
    fn shutdowned(&mut self) {
        *self = IOState::Shutdown;
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use curp_test_utils::test_cmd::TestCommand;

    use super::*;
    use crate::log_entry::EntryData;

    #[tokio::test]
    async fn segment_state_transition_is_correct() {
        let expect_state = |segment: &WALSegment, state: IOState| {
            assert!(
                matches!(segment.io_state(), state),
                "expect {state:?}, current state: {:?}",
                segment.io_state()
            );
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let mut file_path = PathBuf::from(temp_dir.path());
        file_path.push("0.tmp");
        let lfile = LockedFile::open_rw(&file_path).unwrap();
        let mut segment = WALSegment::create(lfile, 1, 0, 512).await.unwrap();

        expect_state(&segment, IOState::Fsynced);

        segment.write_u64(1).await.unwrap();
        expect_state(&segment, IOState::Written);
        segment.write_u64(2).await.unwrap();
        expect_state(&segment, IOState::Written);

        segment.flush().await;
        expect_state(&segment, IOState::Flushed);
        segment.flush().await;
        expect_state(&segment, IOState::Flushed);

        segment.sync_all().await;
        expect_state(&segment, IOState::Fsynced);
        segment.sync_all().await;
        expect_state(&segment, IOState::Fsynced);
    }

    #[test]
    fn gen_parse_header_is_correct() {
        fn corrupt(mut header: Vec<u8>, pos: usize) -> Vec<u8> {
            header[pos] ^= 1;
            header
        }

        for idx in 0..100 {
            for id in 0..100 {
                let header = WALSegment::gen_header(idx, id);
                let (idx_parsed, id_parsed) = WALSegment::parse_header(&header).unwrap();
                assert_eq!(idx, idx_parsed);
                assert_eq!(id, id_parsed);
                for pos in 0..8 * 4 {
                    assert!(WALSegment::parse_header(&corrupt(header.clone(), pos)).is_err());
                }
            }
        }
    }

    #[tokio::test]
    async fn segment_seal_is_ok() {
        const BASE_INDEX: u64 = 17;
        const SEGMENT_ID: u64 = 2;
        const SIZE_LIMIT: u64 = 5;
        let dir = tempfile::tempdir().unwrap();
        let mut tmp_path = dir.path().to_path_buf();
        tmp_path.push("test.tmp");
        let segment_name = WALSegment::segment_name(SEGMENT_ID, BASE_INDEX);
        let mut wal_path = dir.path().to_path_buf();
        wal_path.push(segment_name);
        let file = LockedFile::open_rw(&tmp_path).unwrap();
        let mut segment = WALSegment::create(file, BASE_INDEX, SEGMENT_ID, SIZE_LIMIT)
            .await
            .unwrap();
        segment.seal::<()>(20).await.unwrap();
        segment.seal::<()>(30).await.unwrap();
        segment.seal::<()>(40).await.unwrap();
        drop(segment);

        let file = LockedFile::open_rw(wal_path).unwrap();
        let mut segment = WALSegment::open(file, SIZE_LIMIT).await.unwrap();
        let _ignore = segment.recover_segment_logs::<()>().await.unwrap();
        assert_eq!(segment.seal_index, 40);
    }

    #[tokio::test]
    async fn segment_log_recovery_is_ok() {
        const BASE_INDEX: u64 = 1;
        const SEGMENT_ID: u64 = 1;
        const SIZE_LIMIT: u64 = 5;
        let dir = tempfile::tempdir().unwrap();
        let mut tmp_path = dir.path().to_path_buf();
        tmp_path.push("test.tmp");
        let segment_name = WALSegment::segment_name(SEGMENT_ID, BASE_INDEX);
        let mut wal_path = dir.path().to_path_buf();
        wal_path.push(segment_name);
        let file = LockedFile::open_rw(&tmp_path).unwrap();
        let mut segment = WALSegment::create(file, BASE_INDEX, SEGMENT_ID, SIZE_LIMIT)
            .await
            .unwrap();

        let mut seg_framed = Framed::new(&mut segment, WAL::<TestCommand>::new());
        let frames: Vec<_> = (0..100)
            .map(|i| {
                DataFrame::Entry(LogEntry::new(
                    i,
                    1,
                    crate::rpc::ProposeId(0, 0),
                    EntryData::Command(Arc::new(TestCommand::new_put(vec![i as u32], i as u32))),
                ))
            })
            .collect();
        seg_framed.send(frames.clone()).await.unwrap();
        seg_framed.flush().await.unwrap();
        seg_framed.get_mut().sync_all().await.unwrap();

        drop(segment);

        let file = LockedFile::open_rw(wal_path).unwrap();
        let mut segment = WALSegment::open(file, SIZE_LIMIT).await.unwrap();
        let recovered: Vec<_> = segment
            .recover_segment_logs::<TestCommand>()
            .await
            .unwrap()
            .map(|e| DataFrame::Entry(e))
            .collect();
        assert_eq!(frames, recovered);
    }
}
