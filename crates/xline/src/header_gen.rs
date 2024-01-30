use std::sync::Arc;

use curp::members::ServerId;
use parking_lot::Mutex;

use crate::{revision_number::RevisionNumberGenerator, rpc::ResponseHeader};

/// Generator of `ResponseHeader`
#[derive(Debug)]
pub(crate) struct HeaderGenerator {
    /// Id of the cluster
    cluster_id: u64,
    /// Id of the member
    member_id: u64,
    /// Term of curp
    term: Arc<Mutex<u64>>,
    /// Revision of kv store
    general_revision: Arc<RevisionNumberGenerator>,
    /// Revision of auth store
    auth_revision: Arc<RevisionNumberGenerator>,
}

impl HeaderGenerator {
    /// New `HeaderGenerator`
    pub(crate) fn new(cluster_id: u64, member_id: ServerId) -> Self {
        Self {
            cluster_id,
            member_id,
            term: Arc::new(Mutex::new(0)),
            general_revision: Arc::new(RevisionNumberGenerator::default()),
            auth_revision: Arc::new(RevisionNumberGenerator::default()),
        }
    }

    /// Generate `ResponseHeader`
    pub(crate) fn gen_header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            raft_term: *self.term.lock(),
            revision: self.general_revision(),
        }
    }

    /// Generate `ResponseHeader` for auth request
    pub(crate) fn gen_auth_header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            raft_term: *self.term.lock(),
            revision: self.auth_revision(),
        }
    }

    /// Set term
    #[allow(dead_code)] // Will be used in the future
    pub(crate) fn set_term(&self, term: u64) {
        *self.term.lock() = term;
    }

    /// Get general revision
    pub(crate) fn general_revision(&self) -> i64 {
        self.general_revision.get()
    }

    /// Return Arc of general revision
    pub(crate) fn general_revision_arc(&self) -> Arc<RevisionNumberGenerator> {
        Arc::clone(&self.general_revision)
    }

    /// Get auth revision
    pub(crate) fn auth_revision(&self) -> i64 {
        self.auth_revision.get()
    }

    /// Return Arc of auth revision
    pub(crate) fn auth_revision_arc(&self) -> Arc<RevisionNumberGenerator> {
        Arc::clone(&self.auth_revision)
    }
}
