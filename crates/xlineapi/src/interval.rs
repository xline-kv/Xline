use std::cmp;

use utils::interval_map::Interval;

use crate::command::KeyRange;

impl From<KeyRange> for Interval<BytesAffine> {
    fn from(range: KeyRange) -> Self {
        let start = range.range_start().to_vec();
        let end = match range.range_end() {
            &[] => {
                let mut end = start.clone();
                end.push(0);
                BytesAffine::Bytes(end)
            }
            &[0] => BytesAffine::Unbounded,
            bytes => BytesAffine::Bytes(bytes.to_vec()),
        };
        Interval::new(BytesAffine::Bytes(start), end)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BytesAffine {
    /// Bytes
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
    fn convert_from_key_range_is_ok() {
        let range0 = KeyRange::new("a", "e");
        let range1 = KeyRange::new_one_key("f");
        let interval0: Interval<BytesAffine> = range0.into();
        let interval1: Interval<BytesAffine> = range1.into();
        assert_eq!(interval0.low, BytesAffine::new_key("a"));
        assert_eq!(interval0.high, BytesAffine::new_key("e"));
        assert_eq!(interval1.low, BytesAffine::new_key("f"));
        assert_eq!(interval1.high, BytesAffine::new_key("f\0"));
    }
}
