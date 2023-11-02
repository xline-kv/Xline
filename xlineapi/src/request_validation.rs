use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    command::KeyRange, AuthRoleAddRequest, AuthRoleGrantPermissionRequest, AuthUserAddRequest,
    DeleteRangeRequest, PutRequest, RangeRequest, Request, RequestOp, SortOrder, SortTarget,
    TxnRequest,
};

/// Default max txn ops
const DEFAULT_MAX_TXN_OPS: usize = 128;

/// Trait for request validation
pub trait RequestValidator {
    /// Validate the request
    fn validation(&self) -> Result<(), ValidationError>;
}

impl RequestValidator for RangeRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::EmptyKey);
        }
        if !SortOrder::is_valid(self.sort_order) || !SortTarget::is_valid(self.sort_target) {
            return Err(ValidationError::InvalidSortOption);
        }

        Ok(())
    }
}

impl RequestValidator for PutRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::EmptyKey);
        }
        if self.ignore_value && !self.value.is_empty() {
            return Err(ValidationError::ValueProvided);
        }
        if self.ignore_lease && self.lease != 0 {
            return Err(ValidationError::LeaseProvided);
        }

        Ok(())
    }
}

impl RequestValidator for DeleteRangeRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::EmptyKey);
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
            return Err(ValidationError::TooManyOps);
        }
        for c in &self.compare {
            if c.key.is_empty() {
                return Err(ValidationError::EmptyKey);
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
                return Err(ValidationError::RequestNotProvided);
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

            for k in success_puts.union(&failure_puts) {
                if !puts.insert(k) {
                    return Err(ValidationError::DuplicateKey);
                }
                if dels.iter().any(|del| del.contains_key(k)) {
                    return Err(ValidationError::DuplicateKey);
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
                return Err(ValidationError::DuplicateKey);
            }
            if dels.iter().any(|del| del.contains_key(&req.key)) {
                return Err(ValidationError::DuplicateKey);
            }
        }
    }
    Ok((puts, dels))
}

impl RequestValidator for AuthUserAddRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.name.is_empty() {
            return Err(ValidationError::UserEmpty);
        }
        let need_password = self.options.as_ref().map_or(true, |o| !o.no_password);
        if need_password && self.password.is_empty() && self.hashed_password.is_empty() {
            return Err(ValidationError::PasswordEmpty);
        }

        Ok(())
    }
}

impl RequestValidator for AuthRoleAddRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.name.is_empty() {
            return Err(ValidationError::RoleEmpty);
        }

        Ok(())
    }
}

impl RequestValidator for AuthRoleGrantPermissionRequest {
    fn validation(&self) -> Result<(), ValidationError> {
        if self.perm.is_none() {
            return Err(ValidationError::PermissionNotGiven);
        }

        Ok(())
    }
}

/// Error type in Validation
#[cfg_attr(test, derive(Default))]
#[derive(Error, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationError {
    /// Key is not provided
    #[cfg_attr(test, default)] // used in tests
    #[error("key is not provided")]
    EmptyKey,
    /// Ignore value is set but value is provided
    #[error("ignore value is set but value is provided")]
    ValueProvided,
    /// Ignore lease is set but lease is provided
    #[error("ignore lease is set but lease is provided")]
    LeaseProvided,
    /// Invalid sort option
    #[error("invalid sort option")]
    InvalidSortOption,
    /// Too many operations in txn request
    #[error("too many operations in txn request")]
    TooManyOps,
    /// Request not provided in operation
    #[error("request not provided in operation")]
    RequestNotProvided,
    /// Duplicate key given in txn request
    #[error("duplicate key given in txn request")]
    DuplicateKey,
    /// User name is empty
    #[error("user name is empty")]
    UserEmpty,
    /// Password is empty
    #[error("password is empty")]
    PasswordEmpty,
    /// Role name is empty
    #[error("role name is empty")]
    RoleEmpty,
    /// Permission not given
    #[error("permission not given")]
    PermissionNotGiven,
}

// The etcd client relies on GRPC error messages for error type interpretation.
// In order to create an etcd-compatible API with Xline, it is necessary to return exact GRPC statuses to the etcd client.
// Refer to `https://github.com/etcd-io/etcd/blob/main/api/v3rpc/rpctypes/error.go` for etcd's error parsing mechanism,
// and refer to `https://github.com/etcd-io/etcd/blob/main/client/v3/doc.go` for how errors are handled by etcd client.
impl From<ValidationError> for tonic::Status {
    #[inline]
    fn from(err: ValidationError) -> Self {
        let (code, message) = match err {
            ValidationError::EmptyKey => (
                tonic::Code::InvalidArgument,
                "etcdserver: key is not provided".to_owned(),
            ),
            ValidationError::ValueProvided => (
                tonic::Code::InvalidArgument,
                "etcdserver: value is provided".to_owned(),
            ),
            ValidationError::LeaseProvided => (
                tonic::Code::InvalidArgument,
                "etcdserver: lease is provided".to_owned(),
            ),
            ValidationError::InvalidSortOption => (
                tonic::Code::InvalidArgument,
                "etcdserver: invalid sort option".to_owned(),
            ),
            ValidationError::TooManyOps => (
                tonic::Code::InvalidArgument,
                "etcdserver: too many operations in txn request".to_owned(),
            ),
            ValidationError::DuplicateKey => (
                tonic::Code::InvalidArgument,
                "etcdserver: duplicate key given in txn request".to_owned(),
            ),
            ValidationError::UserEmpty => (
                tonic::Code::InvalidArgument,
                "etcdserver: user name is empty".to_owned(),
            ),
            ValidationError::RoleEmpty => (
                tonic::Code::InvalidArgument,
                "etcdserver: role name is empty".to_owned(),
            ),
            ValidationError::PermissionNotGiven => (
                tonic::Code::InvalidArgument,
                "etcdserver: permission not given".to_owned(),
            ),
            ValidationError::RequestNotProvided | ValidationError::PasswordEmpty => {
                (tonic::Code::InvalidArgument, err.to_string())
            }
        };

        tonic::Status::new(code, message)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Compare, RequestOp, UserAddOptions};

    struct TestCase<T: RequestValidator> {
        req: T,
        expected_err: ValidationError,
    }

    fn run_test<T: RequestValidator>(testcases: Vec<TestCase<T>>) {
        for testcase in testcases {
            let error = testcase.req.validation().unwrap_err();
            assert_eq!(error, testcase.expected_err);
        }
    }

    #[test]
    fn invalid_range_request_should_have_correct_error_msg() {
        let testcases = vec![
            TestCase {
                req: RangeRequest {
                    key: vec![],
                    ..Default::default()
                },
                expected_err: ValidationError::EmptyKey,
            },
            TestCase {
                req: RangeRequest {
                    key: "k".into(),
                    sort_order: -1,
                    ..Default::default()
                },
                expected_err: ValidationError::InvalidSortOption,
            },
            TestCase {
                req: RangeRequest {
                    key: "k".into(),
                    sort_target: -1,
                    ..Default::default()
                },
                expected_err: ValidationError::InvalidSortOption,
            },
        ];

        run_test(testcases);
    }

    #[test]
    fn invalid_put_request_should_have_correct_error_msg() {
        let testcases = vec![
            TestCase {
                req: PutRequest {
                    key: vec![],
                    value: "v".into(),
                    ..Default::default()
                },
                expected_err: ValidationError::EmptyKey,
            },
            TestCase {
                req: PutRequest {
                    key: "k".into(),
                    value: "v".into(),
                    ignore_value: true,
                    ..Default::default()
                },
                expected_err: ValidationError::ValueProvided,
            },
            TestCase {
                req: PutRequest {
                    key: "k".into(),
                    value: "v".into(),
                    lease: 1,
                    ignore_lease: true,
                    ..Default::default()
                },
                expected_err: ValidationError::LeaseProvided,
            },
        ];

        run_test(testcases);
    }

    #[test]
    fn invalid_delete_request_should_have_correct_error_msg() {
        let testcases = vec![TestCase {
            req: DeleteRangeRequest {
                key: vec![],
                ..Default::default()
            },
            expected_err: ValidationError::EmptyKey,
        }];

        run_test(testcases);
    }

    #[test]
    fn invalid_txn_request_should_have_correct_error_msg() {
        let testcases = vec![
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: vec![],
                        ..Default::default()
                    }],
                    success: vec![],
                    failure: vec![],
                },
                expected_err: ValidationError::EmptyKey,
            },
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![RequestOp { request: None }],
                    failure: vec![],
                },
                expected_err: ValidationError::RequestNotProvided,
            },
            TestCase {
                req: TxnRequest {
                    compare: std::iter::repeat(Compare {
                        key: "k".into(),
                        ..Default::default()
                    })
                    .take(DEFAULT_MAX_TXN_OPS + 1)
                    .collect(),
                    success: vec![],
                    failure: vec![],
                },
                expected_err: ValidationError::TooManyOps,
            },
        ];

        run_test(testcases);
    }

    #[test]
    fn invalid_user_add_request_should_have_correct_error_msg() {
        let testcases = vec![
            TestCase {
                req: AuthUserAddRequest {
                    name: String::new(),
                    password: "pwd".to_owned(),
                    ..Default::default()
                },
                expected_err: ValidationError::UserEmpty,
            },
            TestCase {
                req: AuthUserAddRequest {
                    name: "user".to_owned(),
                    password: String::new(),
                    options: Some(UserAddOptions { no_password: false }),
                    ..Default::default()
                },
                expected_err: ValidationError::PasswordEmpty,
            },
        ];

        run_test(testcases);
    }

    #[test]
    fn invalid_role_add_request_should_have_correct_error_msg() {
        let testcases = vec![TestCase {
            req: AuthRoleAddRequest {
                name: String::new(),
            },
            expected_err: ValidationError::RoleEmpty,
        }];

        run_test(testcases);
    }

    #[test]
    fn invalid_role_grant_perm_request_should_have_correct_error_msg() {
        let testcases = vec![TestCase {
            req: AuthRoleGrantPermissionRequest {
                name: "role".to_owned(),
                perm: None,
            },
            expected_err: ValidationError::PermissionNotGiven,
        }];

        run_test(testcases);
    }

    #[test]
    fn check_intervals_txn_duplicate_should_return_error() {
        let put_op = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: "k".into(),
                ..Default::default()
            })),
        };

        let txn_req_inner = RequestOp {
            request: Some(Request::RequestTxn(TxnRequest {
                compare: vec![Compare {
                    key: "k".into(),
                    ..Default::default()
                }],
                success: vec![put_op.clone()],
                failure: vec![],
            })),
        };

        let testcases = vec![
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![put_op.clone(), put_op],
                    failure: vec![],
                },
                expected_err: ValidationError::DuplicateKey,
            },
            // nested
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![txn_req_inner.clone(), txn_req_inner],
                    failure: vec![],
                },
                expected_err: ValidationError::DuplicateKey,
            },
        ];

        run_test(testcases);
    }

    #[test]
    fn check_intervals_txn_overlap_should_return_error() {
        let put_op = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: "k1".into(),
                ..Default::default()
            })),
        };
        let del_op = RequestOp {
            request: Some(Request::RequestDeleteRange(DeleteRangeRequest {
                key: "k0".into(),
                range_end: "k3".into(),
                ..Default::default()
            })),
        };
        let txn_req_inner_put = RequestOp {
            request: Some(Request::RequestTxn(TxnRequest {
                compare: vec![Compare {
                    key: "k".into(),
                    ..Default::default()
                }],
                success: vec![put_op.clone()],
                failure: vec![],
            })),
        };
        let txn_req_inner_del = RequestOp {
            request: Some(Request::RequestTxn(TxnRequest {
                compare: vec![Compare {
                    key: "k".into(),
                    ..Default::default()
                }],
                success: vec![del_op.clone()],
                failure: vec![],
            })),
        };

        let testcases = vec![
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![del_op.clone(), put_op],
                    failure: vec![],
                },
                expected_err: ValidationError::DuplicateKey,
            },
            // nested
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![txn_req_inner_put.clone(), del_op],
                    failure: vec![],
                },
                expected_err: ValidationError::DuplicateKey,
            },
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![txn_req_inner_del, txn_req_inner_put],
                    failure: vec![],
                },
                expected_err: ValidationError::DuplicateKey,
            },
        ];

        run_test(testcases);
    }

    // FIXME: This test will fail in the current implementation.
    // See https://github.com/xline-kv/Xline/issues/410 for more details
    #[ignore]
    #[test]
    fn check_intervals_txn_nested_overlap_should_return_error() {
        let put_op = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: "k1".into(),
                ..Default::default()
            })),
        };
        let del_op = RequestOp {
            request: Some(Request::RequestDeleteRange(DeleteRangeRequest {
                key: "k0".into(),
                range_end: "k3".into(),
                ..Default::default()
            })),
        };
        let txn_req_inner_put = RequestOp {
            request: Some(Request::RequestTxn(TxnRequest {
                compare: vec![Compare {
                    key: "k".into(),
                    ..Default::default()
                }],
                success: vec![put_op],
                failure: vec![],
            })),
        };
        let txn_req_inner_del = RequestOp {
            request: Some(Request::RequestTxn(TxnRequest {
                compare: vec![Compare {
                    key: "k".into(),
                    ..Default::default()
                }],
                success: vec![del_op],
                failure: vec![],
            })),
        };

        let testcases = vec![
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![txn_req_inner_del.clone(), txn_req_inner_put.clone()],
                    failure: vec![],
                },
                expected_err: ValidationError::DuplicateKey,
            },
            // Swap the two txn request in success.
            // This is to test if the order of the request affect the validation result.
            TestCase {
                req: TxnRequest {
                    compare: vec![Compare {
                        key: "k".into(),
                        ..Default::default()
                    }],
                    success: vec![txn_req_inner_put, txn_req_inner_del],
                    failure: vec![],
                },
                expected_err: ValidationError::DuplicateKey,
            },
        ];

        run_test(testcases);
    }
}
