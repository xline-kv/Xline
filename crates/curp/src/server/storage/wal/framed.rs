use std::io;

/// Decoding of frames via buffers.
pub(super) trait Decoder {
    /// The type of decoded frames.
    type Item;

    /// The type of unrecoverable frame decoding errors.
    type Error: From<io::Error>;

    /// Attempts to decode a frame from the provided buffer of bytes.
    fn decode(&mut self, src: &[u8]) -> Result<(Self::Item, usize), Self::Error>;
}

/// Trait of helper objects to write out messages as bytes
pub(super) trait Encoder<Item> {
    /// The type of encoding errors.
    type Error: From<io::Error>;

    /// Encodes a frame
    fn encode(&mut self, item: Item) -> Result<Vec<u8>, Self::Error>;
}
