use std::collections::BTreeSet;
use std::marker::PhantomData;

/// A joint quorum set
pub(crate) struct Joint<QS, I> {
    /// The quorum sets
    sets: I,
    /// The type of the quorum set
    _qs_type: PhantomData<QS>,
}

impl<QS, I> Joint<QS, I> {
    /// Create a new `Joint`
    pub(crate) fn new(sets: I) -> Self {
        Self {
            sets,
            _qs_type: PhantomData,
        }
    }

    /// Unwrap the inner quorum set
    pub(crate) fn into_inner(self) -> I {
        self.sets
    }
}

impl<QS> Joint<QS, Vec<QS>>
where
    QS: PartialEq + Clone,
{
    /// Generates a new coherent joint quorum set
    pub(crate) fn coherent(&self, other: Self) -> Self {
        if other.sets.iter().any(|set| self.sets.contains(set)) {
            return other;
        }
        // TODO: select the config where the leader is in
        let last = self.sets.last().cloned();
        Self::new(last.into_iter().chain(other.sets).collect())
    }
}

/// A quorum set
pub(crate) trait QuorumSet<I> {
    /// Check if the given set of ids forms a quorum
    ///
    /// A quorum must contains at least f + 1 replicas
    fn is_quorum(&self, ids: I) -> bool;

    /// Check if the given set of ids forms a super quorum
    ///
    /// A super quorum must contains at least f + ⌈f/2⌉ + 1 replicas
    fn is_super_quorum(&self, ids: I) -> bool;

    /// Check if the given set of ids forms a recover quorum
    ///
    /// A recover quorum must contains at least ⌈f/2⌉ + 1 replicas
    fn is_recover_quorum(&self, ids: I) -> bool;
}

#[allow(clippy::arithmetic_side_effects)]
impl<I> QuorumSet<I> for BTreeSet<u64>
where
    I: IntoIterator<Item = u64> + Clone,
{
    fn is_quorum(&self, ids: I) -> bool {
        let num = ids.into_iter().filter(|id| self.contains(id)).count();
        num * 2 > self.len()
    }

    fn is_super_quorum(&self, ids: I) -> bool {
        let num = ids.into_iter().filter(|id| self.contains(id)).count();
        num * 4 >= 3 * self.len()
    }

    fn is_recover_quorum(&self, ids: I) -> bool {
        let num = ids.into_iter().filter(|id| self.contains(id)).count();
        num * 4 > self.len() + 2
    }
}

impl<I, QS> QuorumSet<I> for Joint<QS, Vec<QS>>
where
    I: IntoIterator<Item = u64> + Clone,
    QS: QuorumSet<I>,
{
    fn is_quorum(&self, ids: I) -> bool {
        self.sets.iter().all(|s| s.is_quorum(ids.clone()))
    }

    fn is_super_quorum(&self, ids: I) -> bool {
        self.sets.iter().all(|s| s.is_super_quorum(ids.clone()))
    }

    fn is_recover_quorum(&self, ids: I) -> bool {
        self.sets.iter().all(|s| s.is_recover_quorum(ids.clone()))
    }
}

impl<I, QS> QuorumSet<I> for Joint<QS, &[QS]>
where
    I: IntoIterator<Item = u64> + Clone,
    QS: QuorumSet<I>,
{
    fn is_quorum(&self, ids: I) -> bool {
        self.sets.iter().all(|s| s.is_quorum(ids.clone()))
    }

    fn is_super_quorum(&self, ids: I) -> bool {
        self.sets.iter().all(|s| s.is_super_quorum(ids.clone()))
    }

    fn is_recover_quorum(&self, ids: I) -> bool {
        self.sets.iter().all(|s| s.is_recover_quorum(ids.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Clone)]
    struct MockQuorumSet;

    fn assert_coherent(from: &[BTreeSet<u64>], to: &[BTreeSet<u64>], expect: &[BTreeSet<u64>]) {
        let joint_from = Joint::new(from.to_vec());
        let joint_to = Joint::new(to.to_vec());
        let joint_coherent = joint_from.coherent(joint_to);
        assert_eq!(
            joint_coherent.sets, expect,
            "from: {from:?}, to: {to:?}, expect: {expect:?}"
        );
    }

    #[test]
    fn test_joint_coherent() {
        assert_coherent(
            &[BTreeSet::from([1, 2, 3])],
            &[BTreeSet::from([1, 2, 3])],
            &[BTreeSet::from([1, 2, 3])],
        );
        assert_coherent(
            &[BTreeSet::from([1, 2, 3])],
            &[BTreeSet::from([1, 2, 3, 4])],
            &[BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 3, 4])],
        );
        assert_coherent(
            &[BTreeSet::from([1, 2, 3])],
            &[BTreeSet::from([4, 5])],
            &[BTreeSet::from([1, 2, 3]), BTreeSet::from([4, 5])],
        );
        assert_coherent(
            &[BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 3, 4])],
            &[BTreeSet::from([1, 2, 3, 4])],
            &[BTreeSet::from([1, 2, 3, 4])],
        );
        assert_coherent(
            &[BTreeSet::from([1, 2, 3]), BTreeSet::from([4, 5])],
            &[BTreeSet::from([4, 5])],
            &[BTreeSet::from([4, 5])],
        );
        assert_coherent(
            &[BTreeSet::from([4, 5])],
            &[BTreeSet::from([1, 2, 3]), BTreeSet::from([4, 5])],
            &[BTreeSet::from([1, 2, 3]), BTreeSet::from([4, 5])],
        );
    }

    fn power_set(set: &BTreeSet<u64>) -> Vec<BTreeSet<u64>> {
        (0..(1 << set.len()))
            .map(|i| {
                set.iter()
                    .enumerate()
                    .filter_map(|(j, x)| ((i >> j) & 1 == 1).then_some(*x))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_quorum_should_work() {
        let nodes = vec![1, 2, 3, 4, 5, 6, 7];
        // (quorum, recover_quorum, super_quorum)
        let expected_res = vec![
            (1, 1, 1),
            (2, 2, 2),
            (2, 2, 3),
            (3, 2, 3),
            (3, 2, 4),
            (4, 3, 5),
            (4, 3, 6),
        ];

        for (node_cnt, (quorum, recover_quorum, super_quorum)) in
            nodes.into_iter().zip(expected_res.into_iter())
        {
            let set: BTreeSet<u64> = (0..node_cnt).collect();
            for sub in power_set(&set) {
                let is_quorum = set.is_quorum(sub.clone());
                let is_recover_quorum = set.is_recover_quorum(sub.clone());
                let is_super_quorum = set.is_super_quorum(sub.clone());
                assert_eq!(sub.len() >= quorum, is_quorum);
                assert_eq!(
                    sub.len() >= recover_quorum,
                    is_recover_quorum,
                    "size: {}, sub: {}",
                    set.len(),
                    sub.len()
                );
                assert_eq!(
                    sub.len() >= super_quorum,
                    is_super_quorum,
                    "size: {}, sub: {}",
                    set.len(),
                    sub.len()
                );
            }
        }
    }
}
