use xlineapi::execute_error::ExecuteError;

use crate::rpc::{CompactionRequest, RangeRequest, Request, TxnRequest};

/// A union of requests that need revision check
pub(crate) enum RevisionRequest<'a> {
    /// Compaction request
    Compaction(&'a CompactionRequest),
    /// Range request
    Range(&'a RangeRequest),
    /// Txn request
    Txn(&'a TxnRequest),
}

impl<'a> From<&'a CompactionRequest> for RevisionRequest<'a> {
    fn from(req: &'a CompactionRequest) -> Self {
        RevisionRequest::Compaction(req)
    }
}

impl<'a> From<&'a RangeRequest> for RevisionRequest<'a> {
    fn from(req: &'a RangeRequest) -> Self {
        RevisionRequest::Range(req)
    }
}

impl<'a> From<&'a TxnRequest> for RevisionRequest<'a> {
    fn from(req: &'a TxnRequest) -> Self {
        RevisionRequest::Txn(req)
    }
}

/// Revision check
pub(crate) trait RevisionCheck {
    /// check if the request is valid given the compacted and current revision
    fn check_revision(
        self,
        compacted_revision: i64,
        current_revision: i64,
    ) -> Result<(), ExecuteError>;
}

impl<'a, T> RevisionCheck for &'a T
where
    &'a T: Into<RevisionRequest<'a>>,
{
    fn check_revision(
        self,
        compacted_revision: i64,
        current_revision: i64,
    ) -> Result<(), ExecuteError> {
        debug_assert!(
            compacted_revision <= current_revision,
            "compacted revision should not larger than current revision"
        );
        let request = self.into();
        match request {
            RevisionRequest::Compaction(r) => {
                if r.revision <= compacted_revision {
                    Err(ExecuteError::RevisionCompacted(
                        r.revision,
                        compacted_revision,
                    ))
                } else if r.revision > current_revision {
                    Err(ExecuteError::RevisionTooLarge(r.revision, current_revision))
                } else {
                    Ok(())
                }
            }
            RevisionRequest::Range(r) => {
                if r.revision > current_revision {
                    Err(ExecuteError::RevisionTooLarge(r.revision, current_revision))
                } else {
                    (r.revision >= compacted_revision || r.revision <= 0)
                        .then_some(())
                        .ok_or(ExecuteError::RevisionCompacted(
                            r.revision,
                            compacted_revision,
                        ))
                }
            }
            RevisionRequest::Txn(r) => {
                for op in r.success.iter().chain(r.failure.iter()) {
                    if let Some(ref req) = op.request {
                        match *req {
                            Request::RequestRange(ref req) => {
                                req.check_revision(compacted_revision, current_revision)?;
                            }
                            Request::RequestTxn(ref req) => {
                                req.check_revision(compacted_revision, current_revision)?;
                            }
                            Request::RequestPut(_) | Request::RequestDeleteRange(_) => (),
                        }
                    }
                }

                Ok(())
            }
        }
    }
}
