pub use crate::commandpb::KeyRange as EtcdKeyRange;
use curp_external_api::cmd::ConflictCheck;
use serde::{Deserialize, Serialize};
use std::{cmp, ops::Bound};
use tap::Tap;
use tracing::warn;
use utils::interval_map::Interval;

pub type StdBoundRange = std::ops::Range<Bound<Vec<u8>>>;

/// Range start and end to get all keys
pub const UNBOUNDED: &[u8] = &[0_u8];
/// Range end to get one key
pub const ONE_KEY: &[u8] = &[];

pub trait Add1 {
    fn add1(self) -> Self;
}

impl Add1 for Vec<u8> {
    /// Add 1 from the last byte of Vec<u8>
    ///
    /// # Example
    ///
    /// ```rust
    /// use xlineapi::keyrange::Add1;
    /// assert_eq!(vec![5, 6, 7].add1(), vec![5, 6, 8]);
    /// assert_eq!(vec![5, 6, 255].add1(), vec![5, 7]);
    /// assert_eq!(vec![255, 255].add1(), vec![0]);
    /// ```
    fn add1(mut self) -> Self {
        for i in (0..self.len()).rev() {
            if self[i] < 0xFF {
                self[i] = self[i].wrapping_add(1);
                self.truncate(i.wrapping_add(1));
                return self;
            }
        }
        // next prefix does not exist (e.g., 0xffff);
        vec![0]
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum BytesAffine {
    /// Bytes bound, could be either Included or Excluded
    Bytes(Vec<u8>),
    /// Unbounded
    Unbounded,
}

impl BytesAffine {
    pub fn new_key(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(bytes.into())
    }

    pub fn new_unbounded() -> Self {
        Self::Unbounded
    }
}

impl PartialOrd for BytesAffine {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match (self, other) {
            (BytesAffine::Bytes(x), BytesAffine::Bytes(y)) => x.partial_cmp(y),
            (BytesAffine::Bytes(_), BytesAffine::Unbounded) => Some(cmp::Ordering::Less),
            (BytesAffine::Unbounded, BytesAffine::Bytes(_)) => Some(cmp::Ordering::Greater),
            (BytesAffine::Unbounded, BytesAffine::Unbounded) => Some(cmp::Ordering::Equal),
        }
    }
}

impl Ord for BytesAffine {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match (self, other) {
            (BytesAffine::Bytes(x), BytesAffine::Bytes(y)) => x.cmp(y),
            (BytesAffine::Bytes(_), BytesAffine::Unbounded) => cmp::Ordering::Less,
            (BytesAffine::Unbounded, BytesAffine::Bytes(_)) => cmp::Ordering::Greater,
            (BytesAffine::Unbounded, BytesAffine::Unbounded) => cmp::Ordering::Equal,
        }
    }
}

// since we use `BytesAffine` for both Included and Excluded, we don't need to implement `Into<std::ops::Bound>`.

impl EtcdKeyRange {
    pub fn new(key: impl Into<Vec<u8>>, range_end: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            range_end: range_end.into(),
        }
    }
}

/// A Range of Vec<u8>
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum KeyRange {
    /// OneKey, to distinguish from `Prefix` because they all have [a, a+1) form
    OneKey(Vec<u8>),
    /// A [start, end) range.
    ///
    /// Note: The `low` of [`KeyRange::Range`] Interval must be Bytes, because a Interval `low`
    /// must less than `high`, but [`BytesAffine::Unbounded`] is always greater than any Bytes.
    Range(Interval<BytesAffine>),
}

impl KeyRange {
    pub fn new_etcd(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        let key_vec = start.into();
        let range_end_vec = end.into();
        let range_end = match range_end_vec.as_slice() {
            ONE_KEY => return Self::OneKey(key_vec),
            UNBOUNDED => BytesAffine::Unbounded,
            _ => BytesAffine::Bytes(range_end_vec),
        };
        let key = BytesAffine::Bytes(key_vec); // `low` must be Bytes
        debug_assert!(
            key < range_end,
            "key `{key:?}` must be less than range_end `{range_end:?}`"
        );
        Self::Range(Interval::new(key, range_end))
    }

    /// New `KeyRange` only contains one key
    ///
    /// # Panics
    ///
    /// Will panic if key is equal to `UNBOUNDED`
    #[inline]
    pub fn new_one_key(key: impl Into<Vec<u8>>) -> Self {
        let key_vec = key.into();
        assert!(
            key_vec.as_slice() != UNBOUNDED,
            "Unbounded key is not allowed: {key_vec:?}",
        );
        Self::OneKey(key_vec)
    }

    /// New `KeyRange` of all keys
    #[inline]
    pub fn new_all_keys() -> Self {
        Self::Range(Interval::new(
            BytesAffine::Bytes(UNBOUNDED.into()),
            BytesAffine::Unbounded,
        ))
    }

    /// Construct `KeyRange` directly from [`start`, `end`], both included
    ///
    /// # Panics
    ///
    /// Will panic if `start` or `end` is `UNBOUNDED`
    #[inline]
    pub fn new_included(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        let key_vec = start.into();
        let range_end_vec = end.into();
        assert!(
            key_vec.as_slice() != UNBOUNDED && range_end_vec != UNBOUNDED,
            "Unbounded key is not allowed: {key_vec:?}"
        );
        assert!(
            range_end_vec.as_slice() != ONE_KEY,
            "One key range is not allowed: {key_vec:?}"
        );
        let range_end = BytesAffine::Bytes(range_end_vec.add1());
        let key = BytesAffine::Bytes(key_vec);
        KeyRange::Range(Interval::new(key, range_end))
    }

    /// Check if `KeyRange` contains a key
    #[must_use]
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        match self {
            Self::OneKey(k) => k == key,
            Self::Range(r) => {
                let key_aff = BytesAffine::Bytes(key.to_vec());
                r.low <= key_aff && key_aff < r.high
            }
        }
    }

    /// Check if `KeyRange` overlaps with another `KeyRange`
    #[inline]
    pub fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::OneKey(k1), Self::OneKey(k2)) => k1 == k2,
            (Self::Range(r1), Self::Range(r2)) => r1.overlaps(r2),
            (Self::OneKey(k), Self::Range(_)) => other.contains_key(k),
            (Self::Range(_), Self::OneKey(k)) => self.contains_key(&k),
        }
    }

    /// Get end of range with prefix
    ///
    /// User will provide a start key when prefix is true, we need calculate the end key of `KeyRange`
    #[must_use]
    #[inline]
    pub fn get_prefix(key: impl AsRef<[u8]>) -> Vec<u8> {
        key.as_ref().to_vec().add1()
    }

    /// if this range contains all keys
    #[must_use]
    #[inline]
    pub fn is_all_keys(&self) -> bool {
        match self {
            Self::OneKey(_) => false,
            Self::Range(r) => {
                r.low == BytesAffine::Bytes(UNBOUNDED.into()) && r.high == BytesAffine::Unbounded
            }
        }
    }

    /// unpack `KeyRange` to `BytesAffine` tuple
    #[must_use]
    #[inline]
    pub fn into_parts(self) -> (BytesAffine, BytesAffine) {
        match self {
            Self::OneKey(k) => {
                warn!("calling into_parts on KeyRange::OneKey may not be what you want");
                (BytesAffine::Bytes(k.clone()), BytesAffine::Bytes(k.add1()))
            }
            Self::Range(r) => (r.low, r.high),
        }
    }

    /// unpack `KeyRange` to `BytesAffine` tuple
    #[must_use]
    #[inline]
    pub fn into_bounds(self) -> (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>) {
        match self {
            Self::OneKey(k) => (
                std::ops::Bound::Included(k.clone()),
                std::ops::Bound::Included(k),
            ),
            Self::Range(r) => (
                match r.low {
                    BytesAffine::Bytes(k) => std::collections::Bound::Included(k),
                    BytesAffine::Unbounded => std::collections::Bound::Unbounded,
                },
                match r.high {
                    BytesAffine::Bytes(k) => std::collections::Bound::Excluded(k),
                    BytesAffine::Unbounded => std::collections::Bound::Unbounded,
                },
            ),
        }
    }

    /// get the start slice in etcd form of `KeyRange`
    #[must_use]
    #[inline]
    pub fn range_start(&self) -> &[u8] {
        match self {
            KeyRange::OneKey(key_vec) => key_vec.as_slice(),
            KeyRange::Range(Interval { low, .. }) => match low {
                BytesAffine::Bytes(ref k) => k.as_slice(),
                BytesAffine::Unbounded => &[0],
            },
        }
    }

    /// get the end slice in etcd form of `KeyRange`
    #[must_use]
    #[inline]
    pub fn range_end(&self) -> &[u8] {
        match self {
            KeyRange::OneKey(_) => ONE_KEY,
            KeyRange::Range(Interval { high, .. }) => match high {
                BytesAffine::Bytes(ref k) => k.as_slice(),
                BytesAffine::Unbounded => &[0],
            },
        }
    }
}

impl std::ops::RangeBounds<Vec<u8>> for KeyRange {
    /// get the Bound of start in `KeyRange`
    fn start_bound(&self) -> std::collections::Bound<&Vec<u8>> {
        match self {
            Self::OneKey(k) => std::collections::Bound::Included(k),
            Self::Range(r) => match r.low {
                BytesAffine::Bytes(ref k) => std::collections::Bound::Included(k),
                BytesAffine::Unbounded => std::collections::Bound::Unbounded,
            },
        }
    }
    /// get the Bound of end in `KeyRange`
    fn end_bound(&self) -> std::collections::Bound<&Vec<u8>> {
        match self {
            Self::OneKey(k) => std::collections::Bound::Included(k),
            Self::Range(r) => match r.high {
                BytesAffine::Bytes(ref k) => std::collections::Bound::Excluded(k),
                BytesAffine::Unbounded => std::collections::Bound::Unbounded,
            },
        }
    }
}

impl From<EtcdKeyRange> for KeyRange {
    #[inline]
    fn from(range: EtcdKeyRange) -> Self {
        Self::new_etcd(range.key, range.range_end)
    }
}

impl From<KeyRange> for EtcdKeyRange {
    #[inline]
    fn from(range: KeyRange) -> Self {
        match range {
            KeyRange::OneKey(key_vec) => Self {
                key: key_vec,
                range_end: ONE_KEY.into(),
            },
            KeyRange::Range(range) => Self {
                key: match range.low {
                    BytesAffine::Bytes(k) => k,
                    BytesAffine::Unbounded => vec![0],
                },
                range_end: match range.high {
                    BytesAffine::Bytes(k) => k,
                    BytesAffine::Unbounded => vec![0],
                },
            },
        }
    }
}

impl From<KeyRange> for Interval<BytesAffine> {
    #[inline]
    fn from(range: KeyRange) -> Self {
        match range {
            KeyRange::OneKey(key_vec) => Interval::new(
                BytesAffine::Bytes(key_vec.clone()),
                BytesAffine::Bytes(key_vec.tap_mut(|k| k.push(0))),
            ),
            KeyRange::Range(range) => range,
        }
    }
}

impl From<Interval<BytesAffine>> for KeyRange {
    #[inline]
    fn from(range: Interval<BytesAffine>) -> Self {
        Self::Range(range)
    }
}

impl From<KeyRange> for StdBoundRange {
    fn from(value: KeyRange) -> Self {
        match value {
            KeyRange::OneKey(k) => {
                std::ops::Bound::Included(k.clone())..std::ops::Bound::Included(k)
            }
            KeyRange::Range(r) => {
                let start = match r.low {
                    BytesAffine::Bytes(k) => std::ops::Bound::Included(k),
                    BytesAffine::Unbounded => std::ops::Bound::Unbounded,
                };
                let end = match r.high {
                    BytesAffine::Bytes(k) => std::ops::Bound::Excluded(k),
                    BytesAffine::Unbounded => std::ops::Bound::Unbounded,
                };
                start..end
            }
        }
    }
}

impl ConflictCheck for KeyRange {
    /// if `KeyRange` is overlapping (conflict) with another `KeyRange`, return true
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        self.overlaps(&other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_affine_cmp_is_ok() {
        assert_eq!(BytesAffine::new_key("abc"), BytesAffine::new_key("abc"));
        assert!(BytesAffine::new_key("a") < BytesAffine::new_key("b"));
        assert!(BytesAffine::new_key("abcd") < BytesAffine::new_key("b"));
        assert!(BytesAffine::new_key("abcd") < BytesAffine::new_unbounded());
        assert_eq!(BytesAffine::new_unbounded(), BytesAffine::new_unbounded());
    }

    #[test]
    fn construct_from_etcd_range_and_to_etcd_range_is_ok() {
        let range = KeyRange::new_etcd("a", "e");
        assert_eq!(EtcdKeyRange::new("a", "e"), range.into());
        let range = KeyRange::new_etcd("foo", ONE_KEY);
        assert_eq!(EtcdKeyRange::new("foo", ONE_KEY), range.into());
        let range = KeyRange::new_etcd("foo", UNBOUNDED);
        assert_eq!(EtcdKeyRange::new("foo", UNBOUNDED), range.into());
    }

    #[test]
    fn construct_included_range_is_ok() {
        let range = KeyRange::new_included("a", "e");
        match range {
            KeyRange::Range(range) => {
                assert_eq!(range.low, BytesAffine::new_key("a"));
                assert_eq!(range.high, BytesAffine::new_key("f"));
            }
            _ => unreachable!("new_included must be Range"),
        }
    }

    #[test]
    fn key_range_convert_to_interval_is_ok() {
        let range0 = KeyRange::new_etcd("a", "e");
        let range1 = KeyRange::new_one_key("f");
        let interval0: Interval<BytesAffine> = range0.into();
        let interval1: Interval<BytesAffine> = range1.into();
        assert_eq!(interval0.low, BytesAffine::new_key("a"));
        assert_eq!(interval0.high, BytesAffine::new_key("e"));
        assert_eq!(interval1.low, BytesAffine::new_key("f"));
        assert_eq!(interval1.high, BytesAffine::new_key("f\0"));
    }

    #[test]
    fn test_key_range_conflict() {
        let kr1 = KeyRange::new_etcd("a", "e");
        let kr2 = KeyRange::new_one_key("c");
        let kr3 = KeyRange::new_one_key("z");
        assert!(kr1.is_conflict(&kr2));
        assert!(!kr1.is_conflict(&kr3));
        assert!(KeyRange::new_included("a", "z").is_conflict(&KeyRange::new_included("a", "y")));
        assert!(KeyRange::new_included("c", "z").is_conflict(&KeyRange::new_included("a", "d")));
        assert!(KeyRange::new_included("c", "z").is_conflict(&KeyRange::new_included("a", "d")));
        assert!(KeyRange::new_included("a", "g").is_conflict(&KeyRange::new_included("e", "z")));
        assert!(!KeyRange::new_included("a", "c").is_conflict(&KeyRange::new_included("e", "z")));
        assert!(!KeyRange::new_included("c", "f").is_conflict(&KeyRange::new_included("i", "n")));
    }

    #[test]
    fn test_key_range_get_prefix() {
        assert_eq!(KeyRange::get_prefix(b"key"), b"kez");
        assert_eq!(KeyRange::get_prefix(b"z"), b"\x7b");
        assert_eq!(KeyRange::get_prefix(&[255]), b"\0");
    }

    #[test]
    fn test_key_range_contains() {
        let kr1 = KeyRange::new_etcd("a", "e");
        assert!(kr1.contains_key(b"b"));
        assert!(!kr1.contains_key(b"e"));
        let kr2 = KeyRange::new_one_key("c");
        assert!(kr2.contains_key(b"c"));
        assert!(!kr2.contains_key(b"d"));
        let kr3 = KeyRange::new_etcd("c", [0]);
        assert!(kr3.contains_key(b"d"));
        assert!(!kr3.contains_key(b"a"));
        let kr4 = KeyRange::new_etcd([0], "e");
        assert!(kr4.contains_key(b"d"));
        assert!(!kr4.contains_key(b"e"));
    }
}
