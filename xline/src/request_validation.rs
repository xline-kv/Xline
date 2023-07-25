use std::collections::HashSet;

use thiserror::Error;

use crate::{
    rpc::{
        AuthRoleAddRequest, AuthRoleGrantPermissionRequest, AuthUserAddRequest, DeleteRangeRequest,
        PutRequest, RangeRequest, Request, RequestOp, SortOrder, SortTarget, TxnRequest,
    },
    server::KeyRange,
    storage::ExecuteError,
};

/// Default max txn ops
const DEFAULT_MAX_TXN_OPS: usize = 128;

/// Trait for request validation
pub(crate) trait RequestValidator {
    /// Validate the request
    fn validation(&self) -> Result<(), ValidationError>;
}

impl RequestValidator for RangeRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::new("key is not provided"));
        }
        if !SortOrder::is_valid(self.sort_order) || !SortTarget::is_valid(self.sort_target) {
            return Err(ValidationError::new("invalid sort option"));
        }

        Ok(())
    }
}

impl RequestValidator for PutRequest {
    fn validation(&self) -> Result<(), ValidationError> {
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
    fn validation(&self) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::new("key is not provided"));
        }

        Ok(())
    }
}

impl RequestValidator for TxnRequest {
    fn validation(&self) -> Result<(), ValidationError> {
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
                    Request::RequestRange(ref r) => r.validation(),
                    Request::RequestPut(ref r) => r.validation(),
                    Request::RequestDeleteRange(ref r) => r.validation(),
                    Request::RequestTxn(ref r) => r.validation(),
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

impl RequestValidator for AuthUserAddRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.name.is_empty() {
            return Err(ValidationError::new("User name is empty"));
        }
        let need_password = self.options.as_ref().map_or(true, |o| !o.no_password);
        if need_password && self.password.is_empty() && self.hashed_password.is_empty() {
            return Err(ValidationError::new(
                "Password is required but not provided",
            ));
        }

        Ok(())
    }
}

impl RequestValidator for AuthRoleAddRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.name.is_empty() {
            return Err(ValidationError::new("Role name is empty"));
        }

        Ok(())
    }
}

impl RequestValidator for AuthRoleGrantPermissionRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.perm.is_none() {
            return Err(ValidationError::new("Permission not given"));
        }

        Ok(())
    }
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
