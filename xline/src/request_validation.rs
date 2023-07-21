use std::collections::HashSet;

use thiserror::Error;

use crate::{
    rpc::{
        CompactionRequest, DeleteRangeRequest, PutRequest, RangeRequest, Request, RequestOp,
        SortOrder, SortTarget, TxnRequest,
    },
    server::KeyRange,
    storage::ExecuteError,
};

/// Default max txn ops
const DEFAULT_MAX_TXN_OPS: usize = 128;

/// Trait for request validation
pub(crate) trait RequestValidator {
    /// Extra arguments to in validating the request
    type Args;

    /// Validate the request
    fn validation(&self, args: Self::Args) -> Result<(), ValidationError>;
}

impl RequestValidator for RangeRequest {
    type Args = (i64, i64);

    fn validation(
        &self,
        (compacted_revision, current_revision): (i64, i64),
    ) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::new("key is not provided"));
        }
        if !SortOrder::is_valid(self.sort_order) || !SortTarget::is_valid(self.sort_target) {
            return Err(ValidationError::new("invalid sort option"));
        }
        if self.revision > current_revision {
            Err(ValidationError::new(format!(
                "required revision {} is higher than current revision {}",
                self.revision, current_revision
            )))
        } else {
            check_range_compacted(self.revision, compacted_revision)
        }
    }
}

impl RequestValidator for CompactionRequest {
    type Args = (i64, i64);

    fn validation(
        &self,
        (compacted_revision, current_revision): (i64, i64),
    ) -> Result<(), ValidationError> {
        debug_assert!(
            compacted_revision <= current_revision,
            "compacted revision should not larger than current revision"
        );
        if self.revision <= compacted_revision {
            Err(ValidationError::new(format!(
                "required revision {} has been compacted, compacted revision is {}",
                self.revision, compacted_revision
            )))
        } else if self.revision > current_revision {
            Err(ValidationError::new(format!(
                "required revision {} is higher than current revision {}",
                self.revision, current_revision
            )))
        } else {
            Ok(())
        }
    }
}

impl RequestValidator for PutRequest {
    type Args = ();

    fn validation(&self, _args: ()) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::new("key is not provided"));
        }
        if self.ignore_value && !self.value.is_empty() {
            return Err(ValidationError::new("value is provided"));
        }
        if self.ignore_lease && self.lease != 0 {
            return Err(ValidationError::new("lease is provided"));
        }

        Ok(())
    }
}

impl RequestValidator for DeleteRangeRequest {
    type Args = ();

    fn validation(&self, _args: ()) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::new("key is not provided"));
        }

        Ok(())
    }
}

impl RequestValidator for TxnRequest {
    type Args = (i64, i64);

    fn validation(
        &self,
        (compacted_revision, current_revision): (i64, i64),
    ) -> Result<(), ValidationError> {
        let opc = self
            .compare
            .len()
            .max(self.success.len())
            .max(self.failure.len());
        if opc > DEFAULT_MAX_TXN_OPS {
            return Err(ValidationError::new("too many operations in txn selfuest"));
        }
        for c in &self.compare {
            if c.key.is_empty() {
                return Err(ValidationError::new("key is not provided"));
            }
        }
        for op in self.success.iter().chain(self.failure.iter()) {
            if let Some(ref request) = op.request {
                match *request {
                    Request::RequestRange(ref r) => {
                        r.validation((compacted_revision, current_revision))
                    }
                    Request::RequestPut(ref r) => r.validation(()),
                    Request::RequestDeleteRange(ref r) => r.validation(()),
                    Request::RequestTxn(ref r) => {
                        r.validation((compacted_revision, current_revision))
                    }
                }?;
            } else {
                return Err(ValidationError::new("key not found"));
            }
        }

        let _ignore_success = check_intervals(&self.success)?;
        let _ignore_failure = check_intervals(&self.failure)?;

        Ok(())
    }
}

/// check whether the required revision is compacted or not
pub(crate) fn check_range_compacted(
    range_revision: i64,
    compacted_revision: i64,
) -> Result<(), ValidationError> {
    (range_revision >= compacted_revision || range_revision <= 0)
            .then_some(())
            .ok_or(ValidationError::new(format!(
                "required revision {range_revision} has been compacted, compacted revision is {compacted_revision}"
            )))
}

/// Check if puts and deletes overlap
fn check_intervals(ops: &[RequestOp]) -> Result<(HashSet<&[u8]>, Vec<KeyRange>), ValidationError> {
    // TODO: use interval tree is better?

    let mut dels = Vec::new();

    for op in ops {
        if let Some(Request::RequestDeleteRange(ref req)) = op.request {
            // collect dels
            let del = KeyRange::new(req.key.as_slice(), req.range_end.as_slice());
            dels.push(del);
        }
    }

    let mut puts: HashSet<&[u8]> = HashSet::new();

    for op in ops {
        if let Some(Request::RequestTxn(ref req)) = op.request {
            // handle child txn request
            let (success_puts, mut success_dels) = check_intervals(&req.success)?;
            let (failure_puts, mut failure_dels) = check_intervals(&req.failure)?;

            for k in &success_puts {
                if !puts.insert(k) {
                    return Err(ValidationError::new("duplicate key given in txn request"));
                }
                if dels.iter().any(|del| del.contains_key(k)) {
                    return Err(ValidationError::new("duplicate key given in txn request"));
                }
            }

            for k in failure_puts {
                if !puts.insert(k) && !success_puts.contains(k) {
                    // only keys in the puts and not in the success_puts is overlap
                    return Err(ValidationError::new("duplicate key given in txn request"));
                }
                if dels.iter().any(|del| del.contains_key(k)) {
                    return Err(ValidationError::new("duplicate key given in txn request"));
                }
            }

            dels.append(&mut success_dels);
            dels.append(&mut failure_dels);
        }
    }

    for op in ops {
        if let Some(Request::RequestPut(ref req)) = op.request {
            // check puts in this level
            if !puts.insert(&req.key) {
                return Err(ValidationError::new("duplicate key given in txn request"));
            }
            if dels.iter().any(|del| del.contains_key(&req.key)) {
                return Err(ValidationError::new("duplicate key given in txn request"));
            }
        }
    }
    Ok((puts, dels))
}

/// Error type in Validation
#[derive(Error, Debug)]
#[error("{0}")]
pub struct ValidationError(String);

impl ValidationError {
    /// Creates a new `ValidationError`
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl From<ValidationError> for tonic::Status {
    #[inline]
    fn from(err: ValidationError) -> Self {
        tonic::Status::invalid_argument(err.0)
    }
}

impl From<ValidationError> for ExecuteError {
    #[inline]
    fn from(err: ValidationError) -> Self {
        ExecuteError::InvalidRequest(err.0)
    }
}
