use std::{io, marker::PhantomData, sync::Arc};

use clippy_utilities::NumericCast;
use serde::{de::DeserializeOwned, Serialize};
use sha2::{digest::Reset, Digest, Sha256};
use utils::wal::{
    framed::{Decoder, Encoder},
    get_checksum,
};

use crate::rpc::ProposeId;

use super::error::{CorruptType, WALError};

/// Invalid frame type
const INVALID: u8 = 0x00;
/// Entry frame type
const INSERT: u8 = 0x01;
/// Seal frame type
const REMOVE: u8 = 0x02;
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
enum WALFrame<C, H = Sha256> {
    /// Data frame type
    Data(DataFrame<C>),
    /// Commit frame type
    Commit(CommitFrame<H>),
}

/// The data frame
///
/// Contains either a log entry or a seal index
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum DataFrame<C> {
    /// A Frame containing a Insert entry
    Insert { propose_id: ProposeId, cmd: Arc<C> },
    /// A Frame containing the Remove entry
    Remove(ProposeId),
}

/// The commit frame
///
/// This frames contains a SHA256 checksum of all previous frames since last commit
#[derive(Debug)]
struct CommitFrame<H> {
    /// The SHA256 checksum
    checksum: Vec<u8>,
    /// Type of the hasher
    phantom: PhantomData<H>,
}

impl<C> WAL<C> {
    /// Creates a new WAL codec
    pub(super) fn new() -> Self {
        Self {
            frames: Vec::new(),
            hasher: Sha256::new(),
        }
    }
}

impl<C, H> Encoder<Vec<DataFrame<C>>> for WAL<C, H>
where
    C: Serialize,
    H: Digest,
{
    type Error = io::Error;

    /// Encodes a frame
    fn encode(&mut self, frames: Vec<DataFrame<C>>) -> Result<Vec<u8>, Self::Error> {
        let mut frame_data = Vec::new();
        for frame in frames {
            frame_data.extend_from_slice(&frame.encode());
        }
        let commit_frame = CommitFrame::<H>::new_from_data(&frame_data);
        frame_data.extend_from_slice(&commit_frame.encode());

        Ok(frame_data)
    }
}

impl<C, H> Decoder for WAL<C, H>
where
    C: Serialize + DeserializeOwned,
    H: Digest + Reset + Clone,
{
    type Item = Vec<DataFrame<C>>;

    type Error = WALError;

    fn decode(&mut self, src: &[u8]) -> Result<(Self::Item, usize), Self::Error> {
        let mut current = 0;
        loop {
            let Some((frame, len)) = WALFrame::<C>::decode(&src[current..])? else {
                return Err(WALError::UnexpectedEof);
            };
            let decoded_bytes = &src[current..current + len];
            current += len;
            match frame {
                WALFrame::Data(data) => {
                    self.frames.push(data);
                    self.hasher.update(decoded_bytes);
                }
                WALFrame::Commit(commit) => {
                    let checksum = self.hasher.clone().finalize();
                    Digest::reset(&mut self.hasher);
                    if commit.validate(&checksum) {
                        return Ok((self.frames.drain(..).collect(), current));
                    }
                    return Err(WALError::Corrupted(CorruptType::Checksum));
                }
            }
        }
    }
}

/// Encoded size of `ProposeID` in bytes
const PROPOSE_ID_SIZE: usize = 16;

#[allow(
    clippy::indexing_slicing, // Index slicings are checked
    clippy::arithmetic_side_effects, //  Arithmetics are checked
    clippy::unnecessary_wraps // Use the wraps to make code more consistenct
)]
impl<C, H> WALFrame<C, H>
where
    C: DeserializeOwned,
    H: Digest,
{
    /// Decodes a frame from the buffer
    ///
    /// * The frame header memory layout
    ///
    /// 0      1      2      3      4      5      6      7      8
    /// |------+------+------+------+------+------+------+------|
    /// | Type | Data                                           |
    /// |------+------+------+------+------+------+------+------|
    ///
    /// * The frame types
    ///
    /// |------------+-------+-------------------------------------------------------|
    /// | Type       | Value | Desc                                                  |
    /// |------------+-------+-------------------------------------------------------|
    /// | Invalid    |  0x00 | Invalid type                                          |
    /// | Insert     |  0x01 | Inserts a command                                     |
    /// | Remove     |  0x02 | Removes a command                                     |
    /// | Commit     |  0x03 | Stores the checksum                                   |
    /// |------------+-------+-------------------------------------------------------|
    fn decode(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        let frame_type = src[0];
        match frame_type {
            INVALID => Err(WALError::UnexpectedEof),
            INSERT => Self::decode_insert(&src),
            REMOVE => Self::decode_remove(&src),
            COMMIT => Self::decode_commit(&src),
            _ => Err(WALError::Corrupted(CorruptType::Codec(
                "Unexpected frame type".to_owned(),
            ))),
        }
    }

    /// Decodes an entry frame from source
    #[allow(clippy::unwrap_used)]
    fn decode_insert(mut src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        const LEN_SIZE: usize = 8;
        let Some(propose_id) = Self::decode_propose_id(&src) else {
            return Ok(None);
        };
        src = &src[PROPOSE_ID_SIZE..];
        let Ok(len_bytes) = src[..LEN_SIZE].try_into() else {
            return Ok(None);
        };
        src = &src[LEN_SIZE..];
        let len: usize = u64::from_le_bytes(len_bytes).numeric_cast();
        if src.len() < len {
            return Ok(None);
        }
        let payload = &src[..len];
        let cmd: C = bincode::deserialize(payload)
            .map_err(|e| WALError::Corrupted(CorruptType::Codec(e.to_string())))?;

        Ok(Some((
            Self::Data(DataFrame::Insert {
                propose_id,
                cmd: Arc::new(cmd),
            }),
            24 + len,
        )))
    }

    /// Decodes an seal index frame from source
    fn decode_remove(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        Ok(Self::decode_propose_id(&src)
            .map(|id| WALFrame::Data(DataFrame::Remove(id)))
            .map(|frame| (frame, PROPOSE_ID_SIZE)))
    }

    /// Decodes data frame header
    fn decode_propose_id(src: &[u8]) -> Option<ProposeId> {
        if src.len() < PROPOSE_ID_SIZE {
            return None;
        }
        let mut seq_bytes = src[0..8].to_vec();
        seq_bytes.rotate_left(1);
        seq_bytes[7] = 0;
        let seq_num = u64::from_le_bytes(seq_bytes.try_into().unwrap());
        let client_id = u64::from_le_bytes(src[8..16].try_into().unwrap());
        Some(ProposeId(client_id, seq_num))
    }

    /// Decodes a commit frame from source
    fn decode_commit(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        let sum_size = <H as Digest>::output_size();
        Ok(src
            .get(8..8 + sum_size)
            .map(<[u8]>::to_vec)
            .map(|checksum| {
                Self::Commit(CommitFrame {
                    checksum,
                    phantom: PhantomData,
                })
            })
            .map(|frame| (frame, 8 + sum_size)))
    }
}

impl<C> FrameType for DataFrame<C> {
    fn frame_type(&self) -> u8 {
        match *self {
            DataFrame::Insert { .. } => INSERT,
            DataFrame::Remove(_) => REMOVE,
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
            DataFrame::Insert {
                propose_id: ProposeId(client_id, seq_num),
                ref cmd,
            } => {
                assert_eq!(seq_num >> 56, 0, "seq num: {seq_num} too large");
                let entry_bytes = bincode::serialize(&cmd)
                    .unwrap_or_else(|_| unreachable!("serialization should never fail"));
                let len = entry_bytes.len();
                let mut bytes = Vec::with_capacity(3 * 8 + entry_bytes.len());
                bytes.push(self.frame_type());
                bytes.extend_from_slice(&seq_num.to_le_bytes()[..7]);
                bytes.extend_from_slice(&client_id.to_le_bytes());
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(&entry_bytes);
                bytes
            }
            DataFrame::Remove(ProposeId(client_id, seq_num)) => {
                assert_eq!(seq_num >> 56, 0, "seq num: {seq_num} too large");
                let mut bytes = Vec::with_capacity(2 * 8);
                bytes.push(self.frame_type());
                bytes.extend_from_slice(&seq_num.to_le_bytes()[..7]);
                bytes.extend_from_slice(&client_id.to_le_bytes());
                bytes
            }
        }
    }
}

impl<H: Digest> CommitFrame<H> {
    /// Creates a commit frame of data
    fn new_from_data(data: &[u8]) -> Self {
        Self {
            checksum: get_checksum::<H>(data).to_vec(),
            phantom: PhantomData,
        }
    }

    /// Validates the checksum
    fn validate(&self, checksum: &[u8]) -> bool {
        *checksum == self.checksum
    }
}

impl<H> FrameType for CommitFrame<H> {
    fn frame_type(&self) -> u8 {
        COMMIT
    }
}

impl<H> FrameEncoder for CommitFrame<H> {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + self.checksum.len());
        bytes.extend_from_slice(&[0; 8]);
        bytes[0] = self.frame_type();
        bytes.extend_from_slice(&self.checksum);
        bytes
    }
}
