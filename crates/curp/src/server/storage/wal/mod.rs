#![allow(unused)] // TODO: remove this until used

/// The WAL codec
mod codec;

/// WAL errors
mod error;

/// File pipeline
mod pipeline;

/// WAL segment
mod segment;

/// File utils
mod util;

/// The magic of the WAL file
const WAL_MAGIC: u32 = 0xd86e_0be2;

/// The current WAL version
const WAL_VERSION: u8 = 0x00;

/// The wal file extension
const WAL_FILE_EXT: &str = ".wal";
