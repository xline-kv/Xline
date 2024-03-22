use std::sync::atomic::{AtomicU64, Ordering};

use clippy_utilities::NumericCast;

/// Generator of unique lease id
/// Note that this Lease Id generation method may cause collisions,
/// the client should retry after informed by the server.
#[derive(Debug, Default)]
pub struct LeaseIdGenerator {
    /// the current lease id
    id: AtomicU64,
}

impl LeaseIdGenerator {
    /// New `IdGenerator`
    ///
    /// # Panics
    ///
    /// panic if failed to generate random bytes
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap_or_else(|err| {
            panic!("Failed to generate random bytes for lease id generator: {err}");
        });
        let id = AtomicU64::new(u64::from_be_bytes(buf));
        Self { id }
    }

    /// Generate next id
    #[inline]
    pub fn next(&self) -> i64 {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        // to ensure the id is positive
        if id == 0 {
            return self.next();
        }
        // set the highest bit to 0 as we need only need positive values
        (id & 0x7fff_ffff_ffff_ffff).numeric_cast()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn no_duplicates_in_one() {
        let mut exist = HashSet::new();
        let id_gen = LeaseIdGenerator::new();

        for _ in 0..1_000_000 {
            let id = id_gen.next();
            assert!(exist.insert(id), "id {id} is duplicated");
        }
    }

    #[test]
    fn no_duplicates_in_multi() {
        let mut exist = HashSet::new();

        for _ in 0..1000 {
            let id_gen = LeaseIdGenerator::new();
            for _ in 0..1000 {
                let id = id_gen.next();
                assert!(exist.insert(id), "id {id} is duplicated");
            }
        }
    }

    #[test]
    fn leases_are_valid() {
        for _ in 0..1000 {
            let id_gen = LeaseIdGenerator::new();
            for _ in 0..1000 {
                let id = id_gen.next();
                assert!(id > 0, "id {id} is not positive");
            }
        }
    }
}
