use std::{
    fs::File,
    io::{self, Read, Write},
    iter,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp_external_api::LogIndex;
use futures::{ready, FutureExt, SinkExt};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};
use tokio_stream::StreamExt;

use super::{
    codec::{DataFrame, DataFrameOwned, WAL},
    error::{CorruptType, WALError},
    framed::{Decoder, Encoder},
    util::{get_checksum, parse_u64, validate_data, LockedFile},
    WAL_FILE_EXT,
};
use crate::log_entry::LogEntry;

/// The magic of the WAL file
const WAL_MAGIC: u32 = 0xd86e_0be2;

/// The current WAL version
const WAL_VERSION: u8 = 0x00;

/// The size of wal file header in bytes
pub(super) const WAL_HEADER_SIZE: usize = 56;

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
    file: File,
    /// The file size of the segment
    size: u64,
    /// The highest index of the segment
    seal_index: LogIndex,
}

impl WALSegment {
    /// Creates a new `WALSegment`
    pub(super) fn create(
        tmp_file: LockedFile,
        base_index: LogIndex,
        segment_id: u64,
        size_limit: u64,
    ) -> io::Result<Self> {
        let segment_name = Self::segment_name(segment_id, base_index);
        let mut locked_file = tmp_file.rename(segment_name)?;
        let mut file = locked_file.into_std();
        file.write_all(&Self::gen_header(base_index, segment_id))?;
        file.flush()?;
        file.sync_data()?;

        Ok(Self {
            base_index,
            segment_id,
            size_limit,
            file,
            size: WAL_HEADER_SIZE.numeric_cast(),
            // For convenience we set it to largest u64 value that represent not sealed
            seal_index: u64::MAX,
        })
    }

    /// Open an existing WAL segment file
    pub(super) fn open(mut locked_file: LockedFile, size_limit: u64) -> Result<Self, WALError> {
        let mut file = locked_file.into_std();
        let size = file.metadata()?.len();
        let mut buf = vec![0; WAL_HEADER_SIZE];
        file.read_exact(&mut buf)?;
        let (base_index, segment_id) = Self::parse_header(&buf)?;

        Ok(Self {
            base_index,
            segment_id,
            size_limit,
            file,
            size,
            // Index 0 means the seal_index hasn't been read yet
            seal_index: 0,
        })
    }

    /// Recover log entries from a `WALSegment`
    pub(super) fn recover_segment_logs<C>(
        &mut self,
    ) -> Result<impl Iterator<Item = LogEntry<C>>, WALError>
    where
        C: Serialize + DeserializeOwned + std::fmt::Debug,
    {
        let frame_batches = self.read_all(WAL::<C>::new())?;
        let frame_batches_filtered: Vec<_> = frame_batches
            .into_iter()
            .filter(|b| !b.is_empty())
            .collect();
        // The highest_index of this segment
        let mut highest_index = u64::MAX;
        // We get the last frame batch to check it's type
        if let Some(frames) = frame_batches_filtered.last() {
            let frame = frames
                .last()
                .unwrap_or_else(|| unreachable!("a batch should contains at least one frame"));
            if let DataFrameOwned::SealIndex(index) = *frame {
                highest_index = index;
            }
        }

        // Update seal index
        self.update_seal_index(highest_index);

        // Get log entries that index is no larger than `highest_index`
        Ok(frame_batches_filtered
            .into_iter()
            .flatten()
            .filter_map(move |f| {
                if let DataFrameOwned::Entry(e) = f {
                    (e.index <= highest_index).then_some(e)
                } else {
                    None
                }
            }))
    }

    /// Seal the current segment
    ///
    /// After the seal, the log index in this segment should be less than `next_index`
    pub(super) fn seal<C: Serialize>(&mut self, next_index: LogIndex) -> io::Result<()> {
        self.write_sync(vec![DataFrame::SealIndex(next_index)], WAL::<C>::new())?;
        self.update_seal_index(next_index);
        Ok(())
    }

    /// Writes an item to the segment
    pub(super) fn write_sync<U, Item>(&mut self, item: Item, mut encoder: U) -> io::Result<()>
    where
        U: Encoder<Item, Error = io::Error>,
    {
        let encoded_bytes = encoder.encode(item)?;
        self.file.write_all(&encoded_bytes)?;
        self.file.flush()?;
        self.file.sync_data()?;
        self.update_size(encoded_bytes.len().numeric_cast());

        Ok(())
    }

    /// Read all items from the segment
    #[allow(clippy::indexing_slicing)]
    #[allow(clippy::arithmetic_side_effects)] // only used for slice indices
    fn read_all<U, Item>(&mut self, mut decoder: U) -> Result<Vec<Item>, WALError>
    where
        U: Decoder<Item = Item, Error = WALError>,
    {
        let mut buf = Vec::new();
        let _ignore = self.file.read_to_end(&mut buf)?;
        let mut pos = 0;
        let mut entries = Vec::new();
        while pos < buf.len() {
            let (item, n) = match decoder.decode(&buf[pos..]) {
                Ok(d) => d,
                Err(WALError::MaybeEnded) => {
                    if !buf[pos..].iter().all(|b| *b == 0) {
                        return Err(WALError::Corrupted(CorruptType::Codec(
                            "Read zero".to_owned(),
                        )));
                    }
                    return Ok(entries);
                }
                Err(e) => return Err(e),
            };
            entries.push(item);
            pos += n;
            self.update_size(n.numeric_cast());
        }
        Ok(entries)
    }

    /// Updates the size of this segment
    pub(super) fn update_size(&mut self, increment: u64) {
        self.size = self.size.overflow_add(increment);
    }

    /// Updates the seal index
    pub(super) fn update_seal_index(&mut self, index: LogIndex) {
        self.seal_index = index;
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

impl PartialEq for WALSegment {
    fn eq(&self, other: &Self) -> bool {
        self.segment_id.eq(&other.segment_id)
    }
}

impl Eq for WALSegment {}

impl PartialOrd for WALSegment {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WALSegment {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.segment_id.cmp(&other.segment_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use curp_test_utils::test_cmd::TestCommand;

    use super::*;
    use crate::log_entry::EntryData;

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

    #[test]
    fn segment_seal_is_ok() {
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
        let mut segment = WALSegment::create(file, BASE_INDEX, SEGMENT_ID, SIZE_LIMIT).unwrap();
        segment.seal::<()>(20).unwrap();
        segment.seal::<()>(30).unwrap();
        segment.seal::<()>(40).unwrap();
        drop(segment);

        let file = LockedFile::open_rw(wal_path).unwrap();
        let mut segment = WALSegment::open(file, SIZE_LIMIT).unwrap();
        let _ignore = segment.recover_segment_logs::<()>().unwrap();
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
        let mut segment = WALSegment::create(file, BASE_INDEX, SEGMENT_ID, SIZE_LIMIT).unwrap();

        let frames: Vec<_> = (0..100)
            .map(|i| {
                DataFrameOwned::Entry(LogEntry::new(
                    i,
                    1,
                    crate::rpc::ProposeId(0, 0),
                    EntryData::Command(Arc::new(TestCommand::new_put(vec![i as u32], i as u32))),
                ))
            })
            .collect();

        segment.write_sync(
            frames.iter().map(DataFrameOwned::get_ref).collect(),
            WAL::new(),
        );

        drop(segment);

        let file = LockedFile::open_rw(wal_path).unwrap();
        let mut segment = WALSegment::open(file, SIZE_LIMIT).unwrap();
        let recovered: Vec<_> = segment
            .recover_segment_logs::<TestCommand>()
            .unwrap()
            .map(|e| DataFrameOwned::Entry(e))
            .collect();
        assert_eq!(frames, recovered);
    }
}
