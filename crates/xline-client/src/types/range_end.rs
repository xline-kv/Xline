use xlineapi::command::KeyRange;

/// Range end options, indicates how to set `range_end` from a key.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum RangeOption {
    /// Only lookup the given single key. Use empty Vec as `range_end`
    #[default]
    SingleKey,
    /// If set, Xline will lookup all keys match the given prefix
    Prefix,
    /// If set, Xline will lookup all keys that are equal to or greater than the given key
    FromKey,
    /// Set `range_end` directly
    RangeEnd(Vec<u8>),
}

impl RangeOption {
    /// Get the `range_end` for request, and modify key if necessary.
    #[inline]
    pub fn get_range_end(self, key: &mut Vec<u8>) -> Vec<u8> {
        match self {
            RangeOption::SingleKey => vec![],
            RangeOption::Prefix => {
                if key.is_empty() {
                    key.push(0);
                    vec![0]
                } else {
                    KeyRange::get_prefix(key)
                }
            }
            RangeOption::FromKey => {
                if key.is_empty() {
                    key.push(0);
                }
                vec![0]
            }
            RangeOption::RangeEnd(range_end) => range_end,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_range_end() {
        let mut key = vec![];
        assert!(RangeOption::SingleKey.get_range_end(&mut key).is_empty());
        assert!(key.is_empty());
        assert!(RangeOption::FromKey.get_range_end(&mut key).first() == Some(&0));
        assert!(key.first() == Some(&0));
        assert_eq!(
            RangeOption::Prefix.get_range_end(&mut key),
            KeyRange::get_prefix(&key)
        );
        assert_eq!(
            RangeOption::RangeEnd(vec![1, 2, 3]).get_range_end(&mut key),
            vec![1, 2, 3]
        );
    }
}
