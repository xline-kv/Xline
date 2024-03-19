use std::io;

use thiserror::Error;

/// Errors of the `WALStorage`
#[derive(Debug, Error)]
pub(crate) enum WALError {
    /// Unexpected end of file of the WAL
    #[error("WAL ended")]
    UnexpectedEof,
    /// The WAL corrupt error
    #[error("WAL corrupted: {0}")]
    Corrupted(CorruptType),
    /// The IO error
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
}

/// The type of the `Corrupted` error
#[derive(Debug, Error)]
pub(crate) enum CorruptType {
    /// Corrupt because of decode failure
    #[error("Error occurred when decoding WAL: {0}")]
    Codec(String),
    /// Corrupt because of checksum failure
    #[error("Checksumming for the file has failed")]
    Checksum,
    /// Corrupt because of some logs is missing
    #[error("The recovered logs are not continue")]
    LogNotContinue,
}
