use std::io;

/// Decoding of frames via buffers.
pub trait Decoder {
    /// The type of decoded frames.
    type Item;

    /// The type of unrecoverable frame decoding errors.
    type Error: From<io::Error>;

    /// Attempts to decode a frame from the provided buffer of bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if decoding has failed.
    fn decode(&mut self, src: &[u8]) -> Result<(Self::Item, usize), Self::Error>;
}

/// Trait of helper objects to write out messages as bytes
pub trait Encoder<Item> {
    /// The type of encoding errors.
    type Error: From<io::Error>;

    /// Encodes a frame
    ///
    /// # Errors
    ///
    /// This function will return an error if encoding has failed.
    fn encode(&mut self, item: Item) -> Result<Vec<u8>, Self::Error>;
}
