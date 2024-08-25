use std::collections::{hash_map::Entry, HashMap};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utils::interval_map::{Interval, IntervalMap};
use utils::lca_tree::LCATree;

use crate::{
    interval::BytesAffine, AuthRoleAddRequest, AuthRoleGrantPermissionRequest, AuthUserAddRequest,
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

        check_intervals(&self.success)?;
        check_intervals(&self.failure)?;

        Ok(())
    }
}

type DelsIntervalMap<'a> = IntervalMap<BytesAffine, Vec<usize>>;

fn new_bytes_affine_interval(start: &[u8], key_end: &[u8]) -> Interval<BytesAffine> {
    let high = match key_end {
        &[] => {
            let mut end = start.to_vec();
            end.push(0);
            BytesAffine::Bytes(end)
        }
        &[0] => BytesAffine::Unbounded,
        bytes => BytesAffine::Bytes(bytes.to_vec()),
    };
    Interval::new(BytesAffine::new_key(start), high)
}

/// Check if puts and deletes overlap
fn check_intervals(ops: &[RequestOp]) -> Result<(), ValidationError> {
    let mut lca_tree = LCATree::new();
    // Because `dels` stores Vec<node_idx> corresponding to the interval, merging two `dels` is slightly cumbersome.
    // Here, `dels` are directly passed into the build function
    let mut dels = DelsIntervalMap::new();
    // This function will traverse all RequestOp and collect all the parent nodes corresponding to `put` and `del` operations.
    // During this process, the atomicity of the put operation can be guaranteed.
    let puts = build_interval_tree(ops, &mut dels, &mut lca_tree, 0)?;

    // Now we have `dels` and `puts` which contain all node index corresponding to `del` and `put` ops,
    // we only need to iterate through the puts to find out whether each put overlaps with the del operation in the dels,
    // and even if it overlaps, whether it satisfies lca.depth % 2 == 0.
    for (put_key, put_vec) in puts {
        let put_interval = new_bytes_affine_interval(put_key, &[]);
        let overlaps = dels.find_all_overlap(&put_interval);
        for put_node_idx in put_vec {
            for (_, del_vec) in overlaps.iter() {
                for del_node_idx in del_vec.iter() {
                    let lca_node_idx = lca_tree.find_lca(put_node_idx, *del_node_idx);
                    // lca.depth % 2 == 0 means this lca is on a success or failure branch,
                    // and two nodes on the same branch are prohibited from overlapping.
                    if lca_tree.get_node(lca_node_idx).depth % 2 == 0 {
                        return Err(ValidationError::DuplicateKey);
                    }
                }
            }
        }
    }

    Ok(())
}

fn build_interval_tree<'a>(
    ops: &'a [RequestOp],
    dels_map: &mut DelsIntervalMap<'a>,
    lca_tree: &mut LCATree,
    parent: usize,
) -> Result<HashMap<&'a [u8], Vec<usize>>, ValidationError> {
    let mut puts_map: HashMap<&[u8], Vec<usize>> = HashMap::new();
    for op in ops {
        match op.request {
            Some(Request::RequestDeleteRange(ref req)) => {
                // collect dels
                let cur_node_idx = lca_tree.insert_node(parent);
                let del = new_bytes_affine_interval(req.key.as_slice(), req.range_end.as_slice());
                dels_map.entry(del).or_insert(vec![]).push(cur_node_idx);
            }
            Some(Request::RequestTxn(ref req)) => {
                // RequestTxn is absolutely a node
                let cur_node_idx = lca_tree.insert_node(parent);
                let success_puts_map = if !req.success.is_empty() {
                    // success branch is also a node
                    let success_node_idx = lca_tree.insert_node(cur_node_idx);
                    build_interval_tree(&req.success, dels_map, lca_tree, success_node_idx)?
                } else {
                    HashMap::new()
                };
                let failure_puts_map = if !req.failure.is_empty() {
                    // failure branch is also a node
                    let failure_node_idx = lca_tree.insert_node(cur_node_idx);
                    build_interval_tree(&req.failure, dels_map, lca_tree, failure_node_idx)?
                } else {
                    HashMap::new()
                };
                // success_puts_map and failure_puts_map cannot overlap with other op's puts_map.
                for (sub_put_key, sub_put_node_idx) in success_puts_map.iter() {
                    if puts_map.contains_key(sub_put_key) {
                        return Err(ValidationError::DuplicateKey);
                    }
                    puts_map.insert(&sub_put_key, sub_put_node_idx.to_vec());
                }
                // but they can overlap with each other
                for (sub_put_key, mut sub_put_node_idx) in failure_puts_map.into_iter() {
                    match puts_map.entry(&sub_put_key) {
                        Entry::Vacant(_) => {
                            puts_map.insert(&sub_put_key, sub_put_node_idx);
                        }
                        Entry::Occupied(mut put_entry) => {
                            if !success_puts_map.contains_key(sub_put_key) {
                                return Err(ValidationError::DuplicateKey);
                            }
                            let put_vec = put_entry.get_mut();
                            put_vec.append(&mut sub_put_node_idx);
                        }
                    };
                }
            }
            _ => {}
        }
    }
    // put in RequestPut cannot overlap with all puts in RequestTxn
    for op in ops {
        match op.request {
            Some(Request::RequestPut(ref req)) => {
                if puts_map.contains_key(&req.key.as_slice()) {
                    return Err(ValidationError::DuplicateKey);
                }
                let cur_node_idx = lca_tree.insert_node(parent);
                puts_map.insert(&req.key, vec![cur_node_idx]);
            }
            _ => {}
        }
    }
    Ok(puts_map)
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
