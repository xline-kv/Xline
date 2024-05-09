use std::{
    fs::File,
    io::{self, Read, Write},
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use sha2::Sha256;
use utils::wal::{
    framed::{Decoder, Encoder},
    get_checksum, parse_u64, validate_data, LockedFile,
};

use crate::rpc::ProposeId;

use super::{
    codec::DataFrame,
    error::{CorruptType, WALError},
};

/// The magic of the WAL file
const WAL_MAGIC: u32 = 0xa0ec_41ff;

/// The current WAL version
const WAL_VERSION: u8 = 0x00;

/// The size of wal file header in bytes
const WAL_HEADER_SIZE: usize = 48;

pub trait SegmentAttr {
    /// Segment file extension
    fn ext() -> String;
}

// TODO: merge reusable wal code
struct Segment<T, Codec> {
    /// The opened file of this segment
    file: File,
    /// The id of this segment
    segment_id: u64,
    /// The soft size limit of this segment
    size_limit: u64,
    /// The file size of the segment
    size: u64,
    /// Propose ids of this segment
    propose_ids: Vec<ProposeId>,
    /// Codec of this segment
    codec: Codec,
    /// Type of this Segment
    r#type: T,
}

impl<T, Codec> Segment<T, Codec>
where
    T: SegmentAttr,
{
    /// Creates a new `WALSegment`
    pub(super) fn create(
        tmp_file: LockedFile,
        segment_id: u64,
        size_limit: u64,
        codec: Codec,
        r#type: T,
    ) -> io::Result<Self> {
        let segment_name = Self::segment_name(segment_id);
        let lfile = tmp_file.rename(segment_name)?;
        let mut file = lfile.into_std();
        file.write_all(&Self::gen_header(segment_id))?;
        file.flush()?;
        file.sync_data()?;

        Ok(Self {
            file,
            segment_id,
            size_limit,
            size: 0,
            propose_ids: Vec::new(),
            codec,
            r#type,
        })
    }

    /// Open an existing WAL segment file
    pub(super) fn open(
        lfile: LockedFile,
        size_limit: u64,
        codec: Codec,
        r#type: T,
    ) -> Result<Self, WALError> {
        let mut file = lfile.into_std();
        let size = file.metadata()?.len();
        let mut buf = vec![0; WAL_HEADER_SIZE];
        let _ignore = file.read_exact(&mut buf)?;
        let segment_id = Self::parse_header(&buf)?;

        Ok(Self {
            file,
            segment_id,
            size_limit,
            size,
            propose_ids: Vec::new(),
            codec,
            r#type,
        })
    }

    /// Gets the file name of the WAL segment
    fn segment_name(segment_id: u64) -> String {
        format!("{segment_id:016x}{}", T::ext())
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
    /// | SegmentID                                             |
    /// |------+------+------+------+------+------+------+------|
    /// | Checksum (32bytes) ...                                |
    /// |------+------+------+------+------+------+------+------|
    fn gen_header(segment_id: u64) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend(WAL_MAGIC.to_le_bytes());
        buf.extend(vec![0; 3]);
        buf.push(WAL_VERSION);
        buf.extend(segment_id.to_le_bytes());
        buf.extend(get_checksum::<Sha256>(&buf));
        buf
    }

    /// Parse the header from the given buffer
    #[allow(
        clippy::unwrap_used, // Unwraps are used to convert slice to const length and is safe
        clippy::arithmetic_side_effects, // Arithmetics cannot overflow
        clippy::indexing_slicing // Index slicings are checked
    )]
    fn parse_header(src: &[u8]) -> Result<u64, WALError> {
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
        let segment_id = parse_u64(next_field(8));
        let checksum = next_field(32);

        if !validate_data::<Sha256>(&src[0..24], checksum) {
            return parse_error;
        }

        Ok(segment_id)
    }
}

impl<T, Codec> Segment<T, Codec> {
    /// Writes an item to the segment
    pub(super) fn write_sync<Item>(&mut self, item: Item) -> io::Result<()>
    where
        Codec: Encoder<Item, Error = io::Error>,
    {
        let encoded = self.codec.encode(item)?;
        self.file.write_all(&encoded)?;
        self.size = self.size.overflow_add(encoded.len().numeric_cast());
        self.file.flush()?;
        self.file.sync_data()?;

        Ok(())
    }

    /// Gets all items from the segment
    pub(super) fn get_all<Item, Err>(&mut self) -> Result<Vec<Item>, Err>
    where
        Err: From<io::Error>,
        Codec: Decoder<Item = Item, Error = Err>,
    {
        let mut buf = Vec::new();
        let _ignore = self.file.read_to_end(&mut buf)?;
        let mut pos = 0;
        let mut entries = Vec::new();
        while pos < buf.len() {
            let (item, n) = self.codec.decode(&buf[pos..])?;
            entries.push(item);
            pos += n;
        }
        Ok(entries)
    }

    /// Recover all entries of this segment
    fn recover<C>(&mut self) -> Result<Vec<C>, WALError>
    where
        Codec: Decoder<Item = Vec<DataFrame<C>>, Error = WALError>,
    {
        let frames: Vec<_> = self.get_all()?.into_iter().flatten().collect();
        for frame in frames {
            match frame {
                DataFrame::Insert { propose_id, cmd } => todo!(),
                DataFrame::Remove(_) => todo!(),
            }
        }
    }
}

struct Insert {}

impl SegmentAttr for Insert {
    fn ext() -> String {
        ".inswal".to_string()
    }
}
