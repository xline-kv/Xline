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
        num * 4 > 3 * self.len()
    }

    fn is_recover_quorum(&self, ids: I) -> bool {
        let num = ids.into_iter().filter(|id| self.contains(id)).count();
        num * 4 - 2 > self.len()
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
