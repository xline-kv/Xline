use std::{io, marker::PhantomData};

use clippy_utilities::NumericCast;
use curp_external_api::LogIndex;
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    error::{CorruptType, WALError},
    framed::{Decoder, Encoder},
    util::{get_checksum, validate_data},
};
use crate::log_entry::LogEntry;

/// Invalid frame type
const INVALID: u8 = 0x00;
/// Entry frame type
const ENTRY: u8 = 0x01;
/// Seal frame type
const SEAL: u8 = 0x02;
/// Commit frame type
const COMMIT: u8 = 0x03;

/// Getting the frame type
trait FrameType {
    /// Returns the type of this frame
    fn frame_type(&self) -> u8;
}

/// Encoding of frames
trait FrameEncoder {
    /// Encodes the current frame
    fn encode(&self) -> Vec<u8>;
}

/// The WAL codec
#[allow(clippy::upper_case_acronyms)] // The WAL needs to be all upper cases
#[derive(Debug)]
pub(super) struct WAL<C, H = Sha256> {
    /// Frames stored in decoding
    frames: Vec<DataFrame<C>>,
    /// The hasher state for decoding
    hasher: H,
}

/// Union type of WAL frames
#[derive(Debug)]
enum WALFrame<C> {
    /// Data frame type
    Data(DataFrame<C>),
    /// Commit frame type
    Commit(CommitFrame),
}

/// The data frame
///
/// Contains either a log entry or a seal index
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum DataFrame<C> {
    /// A Frame containing a log entry
    Entry(LogEntry<C>),
    /// A Frame containing the sealed index
    SealIndex(LogIndex),
}

/// The commit frame
///
/// This frames contains a SHA256 checksum of all previous frames since last commit
#[derive(Debug)]
struct CommitFrame {
    /// The SHA256 checksum
    checksum: Vec<u8>,
}

impl<C> WAL<C> {
    /// Creates a new WAL codec
    pub(super) fn new() -> Self {
        Self {
            frames: Vec::new(),
            hasher: Sha256::new(),
        }
    }

    /// Gets encoded length and padding length
    ///
    /// This is used to prevent torn write by forcing 8-bit alignment
    #[allow(unused, clippy::arithmetic_side_effects)] // TODO: 8bit alignment
    fn encode_frame_size(data_len: usize) -> (usize, usize) {
        let mut encoded_len = data_len;
        let pad_len = (8 - data_len % 8) % 8;
        if pad_len != 0 {
            // encode padding info
            encoded_len |= (0x80 | pad_len) << 56;
        }
        (encoded_len, pad_len)
    }
}

impl<C> Encoder<Vec<DataFrame<C>>> for WAL<C>
where
    C: Serialize,
{
    type Error = io::Error;

    /// Encodes a frame
    fn encode(&mut self, frames: Vec<DataFrame<C>>) -> Result<Vec<u8>, Self::Error> {
        let mut frame_data: Vec<_> = frames.into_iter().flat_map(|f| f.encode()).collect();
        let commit_frame = CommitFrame::new_from_data(&frame_data);
        frame_data.extend_from_slice(&commit_frame.encode());

        Ok(frame_data)
    }
}

impl<C> Decoder for WAL<C>
where
    C: Serialize + DeserializeOwned,
{
    type Item = Vec<DataFrame<C>>;

    type Error = WALError;

    #[allow(clippy::arithmetic_side_effects)] // the arithmetic only used as slice indices
    fn decode(&mut self, src: &[u8]) -> Result<(Self::Item, usize), Self::Error> {
        let mut cursor = 0;
        while cursor < src.len() {
            let next = src.get(cursor..).ok_or(WALError::MaybeEnded)?;
            let Some((frame, len)) = WALFrame::<C>::decode(next)? else {
                return Err(WALError::MaybeEnded);
            };
            let decoded_bytes = src.get(cursor..cursor + len).ok_or(WALError::MaybeEnded)?;
            cursor += len;
            match frame {
                WALFrame::Data(data) => {
                    self.frames.push(data);
                    self.hasher.update(decoded_bytes);
                }
                WALFrame::Commit(commit) => {
                    let checksum = self.hasher.clone().finalize();
                    self.hasher.reset();
                    if commit.validate(&checksum) {
                        return Ok((self.frames.drain(..).collect(), cursor));
                    }
                    return Err(WALError::Corrupted(CorruptType::Checksum));
                }
            }
        }
        Err(WALError::MaybeEnded)
    }
}

#[allow(
    clippy::indexing_slicing, // Index slicings are checked
    clippy::arithmetic_side_effects, //  Arithmetics are checked
    clippy::unnecessary_wraps // Use the wraps to make code more consistenct
)]
impl<C> WALFrame<C>
where
    C: DeserializeOwned,
{
    /// Decodes a frame from the buffer
    ///
    /// * The frame header memory layout
    ///
    /// 0      1      2      3      4      5      6      7      8
    /// |------+------+------+------+------+------+------+------|
    /// | Type | Length / Index / Reserved                      |
    /// |------+------+------+------+------+------+------+------|
    ///
    /// * The frame types
    ///
    /// |------------+-------+-------------------------------------------------------|
    /// | Type       | Value | Desc                                                  |
    /// |------------+-------+-------------------------------------------------------|
    /// | Invalid    |  0x00 | Invalid type                                          |
    /// | Entry      |  0x01 | Stores record of CURP log entry                       |
    /// | Seal Index |  0x02 | Stores the highest index of this current sealed frame |
    /// | Commit     |  0x03 | Stores the checksum                                   |
    /// |------------+-------+-------------------------------------------------------|
    fn decode(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        if src.len() < 8 {
            return Ok(None);
        }
        let header: [u8; 8] = src[0..8]
            .try_into()
            .unwrap_or_else(|_| unreachable!("this conversion will always succeed"));
        let frame_type = header[0];
        match frame_type {
            INVALID => Err(WALError::MaybeEnded),
            ENTRY => Self::decode_entry(header, &src[8..]),
            SEAL => Self::decode_seal_index(header),
            COMMIT => Self::decode_commit(&src[8..]),
            _ => Err(WALError::Corrupted(CorruptType::Codec(
                "Unexpected frame type".to_owned(),
            ))),
        }
    }

    /// Decodes an entry frame from source
    fn decode_entry(header: [u8; 8], src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        let len: usize = Self::decode_u64_from_header(header).numeric_cast();
        if src.len() < len {
            return Ok(None);
        }
        let payload = &src[..len];
        let entry: LogEntry<C> = bincode::deserialize(payload)
            .map_err(|e| WALError::Corrupted(CorruptType::Codec(e.to_string())))?;

        Ok(Some((Self::Data(DataFrame::Entry(entry)), 8 + len)))
    }

    /// Decodes an seal index frame from source
    fn decode_seal_index(header: [u8; 8]) -> Result<Option<(Self, usize)>, WALError> {
        let index = Self::decode_u64_from_header(header);

        Ok(Some((Self::Data(DataFrame::SealIndex(index)), 8)))
    }

    /// Decodes a commit frame from source
    fn decode_commit(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        if src.len() < 32 {
            return Ok(None);
        }
        let checksum = src[..32].to_vec();

        Ok(Some((Self::Commit(CommitFrame { checksum }), 8 + 32)))
    }

    /// Gets a u64 from the header
    ///
    /// NOTE: The u64 is encoded using 7 bytes, it can be either a length
    /// or a log index that is smaller than `2^56`
    fn decode_u64_from_header(mut header: [u8; 8]) -> u64 {
        header.rotate_left(1);
        header[7] = 0;
        u64::from_le_bytes(header)
    }
}

impl<C> FrameType for DataFrame<C> {
    fn frame_type(&self) -> u8 {
        match *self {
            DataFrame::Entry(_) => ENTRY,
            DataFrame::SealIndex(_) => SEAL,
        }
    }
}

impl<C> FrameEncoder for DataFrame<C>
where
    C: Serialize,
{
    #[allow(clippy::arithmetic_side_effects)] // The integer shift is safe
    fn encode(&self) -> Vec<u8> {
        match *self {
            DataFrame::Entry(ref entry) => {
                let entry_bytes = bincode::serialize(entry)
                    .unwrap_or_else(|_| unreachable!("serialization should never fail"));
                let len = entry_bytes.len();
                assert_eq!(len >> 56, 0, "log entry length: {len} too large");
                let len_bytes = len.to_le_bytes().into_iter().take(7);
                let header = std::iter::once(self.frame_type()).chain(len_bytes);
                header.chain(entry_bytes).collect()
            }
            DataFrame::SealIndex(index) => {
                assert_eq!(index >> 56, 0, "log index: {index} too large");
                // use the first 7 bytes
                let index_bytes = index.to_le_bytes().into_iter().take(7);
                std::iter::once(self.frame_type())
                    .chain(index_bytes)
                    .collect()
            }
        }
    }
}

impl CommitFrame {
    /// Creates a commit frame of data
    fn new_from_data(data: &[u8]) -> Self {
        Self {
            checksum: get_checksum(data).to_vec(),
        }
    }

    /// Validates the checksum
    fn validate(&self, checksum: &[u8]) -> bool {
        *checksum == self.checksum
    }
}

impl FrameType for CommitFrame {
    fn frame_type(&self) -> u8 {
        COMMIT
    }
}

impl FrameEncoder for CommitFrame {
    fn encode(&self) -> Vec<u8> {
        let header = std::iter::once(self.frame_type()).chain([0u8; 7]);
        header.chain(self.checksum.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use curp_test_utils::test_cmd::TestCommand;
    use futures::SinkExt;
    use tempfile::tempfile;
    use tokio::{
        fs::File as TokioFile,
        io::{AsyncSeekExt, AsyncWriteExt, DuplexStream},
    };
    use tokio_stream::StreamExt;
    use tokio_util::codec::Framed;

    use super::*;
    use crate::{log_entry::EntryData, rpc::ProposeId};

    #[tokio::test]
    async fn frame_encode_decode_is_ok() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrame::Entry(entry.clone());
        let seal_frame = DataFrame::<TestCommand>::SealIndex(1);
        let mut encoded = codec.encode(vec![data_frame]).unwrap();
        encoded.extend_from_slice(&codec.encode(vec![seal_frame]).unwrap());

        let (data_frame_get, len) = codec.decode(&encoded).unwrap();
        let (seal_frame_get, _) = codec.decode(&encoded[len..]).unwrap();
        let DataFrame::Entry(ref entry_get) = data_frame_get[0] else {
            panic!("frame should be type: DataFrame::Entry");
        };
        let DataFrame::SealIndex(ref index) = seal_frame_get[0] else {
            panic!("frame should be type: DataFrame::Entry");
        };

        assert_eq!(*entry_get, entry);
        assert_eq!(*index, 1);
    }

    #[tokio::test]
    async fn frame_zero_write_will_be_detected() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrame::Entry(entry.clone());
        let seal_frame = DataFrame::<TestCommand>::SealIndex(1);
        let mut encoded = codec.encode(vec![data_frame]).unwrap();
        encoded[0] = 0;

        let err = codec.decode(&encoded).unwrap_err();
        assert!(matches!(err, WALError::MaybeEnded), "error {err} not match");
    }

    #[tokio::test]
    async fn frame_corrupt_will_be_detected() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrame::Entry(entry.clone());
        let seal_frame = DataFrame::<TestCommand>::SealIndex(1);
        let mut encoded = codec.encode(vec![data_frame]).unwrap();
        encoded[1] = 0;

        let err = codec.decode(&encoded).unwrap_err();
        assert!(
            matches!(err, WALError::Corrupted(_)),
            "error {err} not match"
        );
    }
}
