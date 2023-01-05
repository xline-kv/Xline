#![allow(unused)]
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use clippy_utilities::{Cast, OverflowArithmetic};

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
    pub(crate) fn new(member_id: u64) -> Self {
        let prefix = member_id.overflowing_shl(48).0;
        let mut tmp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {}", e))
            .as_millis();
        tmp &= (u128::MAX.overflowing_shr(128.overflow_sub(40)).0); // lower 40 bits
        tmp = tmp.overflowing_shl(8).0; // shift left 8 bits
        let suffix = AtomicU64::new(tmp.cast());
        Self { prefix, suffix }
    }

    /// Generate next id
    pub(crate) fn next(&self) -> u64 {
        let suffix = self.suffix.fetch_add(1, Ordering::Release);
        self.prefix | suffix
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
