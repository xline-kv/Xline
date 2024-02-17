use prost::bytes::{Buf, BufMut};

/// Revision of a key
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct KeyRevision {
    /// Last creation revision
    pub(super) create_revision: i64,
    /// Number of modification since last creation
    pub(super) version: i64,
    /// Last modification revision
    pub(super) mod_revision: i64,
    /// Sub revision in one transaction
    pub(super) sub_revision: i64,
}

/// Revision
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct Revision {
    /// Main revision
    revision: i64,
    /// Sub revision in one transaction or range deletion
    sub_revision: i64,
}

impl Ord for Revision {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.revision
            .cmp(&other.revision)
            .then(self.sub_revision.cmp(&other.sub_revision))
    }
}

impl PartialOrd for Revision {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Revision {
    /// New `Revision`
    pub(crate) fn new(revision: i64, sub_revision: i64) -> Self {
        Self {
            revision,
            sub_revision,
        }
    }

    /// Get revision
    #[must_use]
    #[inline]
    pub fn revision(&self) -> i64 {
        self.revision
    }

    /// Get sub revision
    pub(crate) fn sub_revision(&self) -> i64 {
        self.sub_revision
    }

    /// Encode `Revision` to `Vec<u8>`
    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.put_i64(self.revision);
        buf.put_i64(self.sub_revision);
        buf
    }

    /// Decode `Revision` from `&[u8]`
    /// # Panics
    /// This function panics if there is not enough remaining data in `buf`.
    #[must_use]
    #[inline]
    pub fn decode(mut buf: &[u8]) -> Self {
        Self::new(buf.get_i64(), buf.get_i64())
    }
}

impl KeyRevision {
    /// New `KeyRevision`
    pub(crate) fn new(
        create_revision: i64,
        version: i64,
        mod_revision: i64,
        sub_revision: i64,
    ) -> Self {
        Self {
            create_revision,
            version,
            mod_revision,
            sub_revision,
        }
    }

    /// New a `KeyRevision` to represent deletion
    pub(crate) fn new_deletion(mod_revision: i64, sub_revision: i64) -> Self {
        Self {
            create_revision: 0,
            version: 0,
            mod_revision,
            sub_revision,
        }
    }

    /// If current `KeyRevision` represent deletion
    pub(crate) fn is_deleted(&self) -> bool {
        self.create_revision == 0 && self.version == 0
    }

    /// Create `Revision` from `KeyRevision`
    pub(crate) fn as_revision(&self) -> Revision {
        Revision::new(self.mod_revision, self.sub_revision)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    #[test]
    fn test_revision_encode_to_vec() {
        let revision = Revision::new(1, 2);
        let vec = revision.encode_to_vec();

        let revision2 = Revision::decode(&vec);
        assert_eq!(revision, revision2);
    }
}
