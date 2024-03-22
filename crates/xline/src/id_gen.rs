#![allow(unused)]
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp::members::ServerId;

/// Generator of unique id
/// id format:
/// | prefix    | suffix              |
/// | 2 bytes   | 5 bytes   | 1 byte  |
/// | member id | timestamp | cnt     |
#[derive(Debug)]
pub(crate) struct IdGenerator {
    /// prefix of id
    prefix: u64,
    /// suffix of id
    suffix: AtomicU64,
}

impl IdGenerator {
    /// New `IdGenerator`
    pub(crate) fn new(member_id: ServerId) -> Self {
        let prefix = member_id.overflowing_shl(48).0;
        let mut ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {e}"))
            .as_millis();
        ts &= (u128::MAX.overflowing_shr(88).0); // lower 40 bits (128 - 40)
        ts = ts.overflowing_shl(8).0; // shift left 8 bits
        let suffix = AtomicU64::new(ts.numeric_cast());
        Self { prefix, suffix }
    }

    /// Generate next id
    pub(crate) fn next(&self) -> i64 {
        let suffix = self.suffix.fetch_add(1, Ordering::Relaxed);
        let id = self.prefix | suffix;
        (id & 0x7fff_ffff_ffff_ffff).numeric_cast()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_id_generator() {
        let id_gen = IdGenerator::new(0);
        assert_ne!(id_gen.next(), id_gen.next());
        assert_ne!(id_gen.next(), id_gen.next());
        assert_ne!(id_gen.next(), id_gen.next());
    }
}
