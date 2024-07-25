use curp_external_api::cmd::Command;
use curp_external_api::role_change::RoleChange;
use rand::Rng;

use crate::member::Change;
use crate::member::Membership;
use crate::rpc::ProposeId;

use super::RawCurp;

impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Adds a learner to the membership state
    pub(crate) fn add_learner(&self, addrs: &[String]) -> ReturnValueWrapper<Vec<u64>> {
        let mut ms_w = self.ms.write();
        let mut log_w = self.log.write();
        loop {
            let ids = random_ids(addrs.len());
            let change = ids.clone().into_iter().zip(addrs.to_owned()).collect();
            let Some(config) = ms_w.committed().change(Change::AddLearner(change)) else {
                continue;
            };
            ms_w.update_effective(config.clone());
            let st_r = self.st.read();
            let propose_id = ProposeId(rand::random(), 0);
            let _entry = log_w.push(st_r.term, propose_id, config);
            return ReturnValueWrapper::new(ids, propose_id);
        }
    }

    /// Removes a learner from the membership state
    pub(crate) fn remove_learner(&self, ids: Vec<u64>) -> Option<ReturnValueWrapper<()>> {
        let mut ms_w = self.ms.write();
        let mut log_w = self.log.write();
        let config = ms_w.committed().change(Change::RemoveLearner(ids))?;
        ms_w.update_effective(config.clone());
        let st_r = self.st.read();
        let propose_id = ProposeId(rand::random(), 0);
        let _entry = log_w.push(st_r.term, propose_id, config);
        Some(ReturnValueWrapper::new((), propose_id))
    }

    /// Updates the committed membership
    pub(crate) fn commit_membership(&self, config: Membership) {
        let mut ms_w = self.ms.write();
        ms_w.update_commit(config);
    }
}

/// Wrapper for the return value of the raw curp methods
///
/// It wraps the actual return value and the propose id of the request
pub(crate) struct ReturnValueWrapper<T> {
    /// The actual return value
    value: T,
    /// The propose id of the request
    propose_id: ProposeId,
}

impl<T> ReturnValueWrapper<T> {
    /// Creates a new return value wrapper
    pub(crate) fn new(value: T, propose_id: ProposeId) -> Self {
        Self { value, propose_id }
    }

    /// Returns the propose id of the request
    pub(crate) fn propose_id(&self) -> ProposeId {
        self.propose_id
    }

    /// Unwraps the return value
    pub(crate) fn into_inner(self) -> T {
        self.value
    }
}

/// Generate random ids of the given length
fn random_ids(n: usize) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    (0..n).map(|_| rng.gen()).collect()
}
