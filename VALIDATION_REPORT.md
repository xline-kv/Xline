# ETCD API Compatibility Test Report

Since the Xline is designed to be fully compatible with the ETCD, ETCD API compatibility test will be performed after each release. All the compatibility tests are running via etcdctl. Read the [documentation](https://github.com/etcd-io/etcd/blob/main/etcdctl/README.md) for more usage information about etcdctl.

Here is the compatibility test report.

## ETCD KV Relevant APIs Tests

| API | etcdctl command | result |
| --- | --- | --- |
| Range(RangeRequest) returns (RangeResponse) | etcdctl get  | passed |
| DeleteRange(DeleteRangeRequest) returns (DeleteRangeResponse) | etcdctl del | passed |
| Put(PutRequest) returns (PutResponse) | etcdctl put | passed |
| Txn(TxnRequest) returns (TxnResponse) | etcdctl txn | passed |
| Compact(CompactionRequest) returns (CompactionResponse) | etcdctl compaction | unimplemented (will cover in v0.3~v0.5) |


## ETCD Watch API Test

| API | etcdctl command | result |
| --- | --- | --- |
| Watch(stream WatchRequest) returns (stream WatchResponse)	| etcdctl watch | 	passed

## ETCD Lease APIs Tests

| API | etcdctl command | result |
| --- | --- | --- |
| LeaseGrant(LeaseGrantRequest) returns (LeaseGrantResponse) |	etcdctl lease grant | passed |
| LeaseRevoke(LeaseRevokeRequest) returns (LeaseRevokeResponse)	| etcdctl lease revoke | passed |
| LeaseKeepAlive(stream LeaseKeepAliveRequest) returns (stream LeaseKeepAliveResponse)	| etcdctl lease keep-alive |	passed |
| LeaseTimeToLive(LeaseTimeToLiveRequest) returns (LeaseTimeToLiveResponse) | etcdctl lease timetolive | passed |
| LeaseLeases(LeaseLeasesRequest) returns (LeaseLeasesResponse) |	etcdctl lease list |	passed |


## ETCD Cluster APIs Tests

ETCD cluster relevant APIs are yet to implement. We will cover them in the future version (v0.3.0 ~ v0.5.0)

## ETCD Maintenance APIs Tests

ETCD maintenance relevant APIs are yet to implement. We will cover them in the future version (v0.3.0 ~ v0.5.0)


## ETCD Auth APIs Tests

| API | etcdctl command | result |
| --- | --- | --- |
| AuthEnable(AuthEnableRequest) returns (AuthEnableResponse) |	etcdctl auth enable	| passed |
| AuthDisable(AuthDisableRequest) returns (AuthDisableResponse) |	etcdctl auth disable | passed |
| AuthStatus(AuthStatusRequest) returns (AuthStatusResponse)	| etcdctl auth status |	passed |


## ETCD User APIs Tests

| API | etcdctl command | result |
| --- | --- | --- |
| UserAdd(AuthUserAddRequest) returns (AuthUserAddResponse) |	etcdctl user add | passed |
| UserGet(AuthUserGetRequest) returns (AuthUserGetResponse) |	etcdctl user get |	passed |
| UserList(AuthUserListRequest) returns (AuthUserListResponse) |	etcdctl user list | passed |
| UserDelete(AuthUserDeleteRequest) returns (AuthUserDeleteResponse) | 	etcdctl user delete	| passed |
| UserChangePassword(AuthUserChangePasswordRequest) returns (AuthUserChangePasswordResponse)	| etcdctl user passwd | passed |
| UserGrantRole(AuthUserGrantRoleRequest) returns (AuthUserGrantRoleResponse) |	etcdctl user grant-role |	passed |
| UserRevokeRole(AuthUserRevokeRoleRequest) returns (AuthUserRevokeRoleResponse)	| etcdctl user revoke-role |	passed |


## ETCD Role APIs Tests

| API | etcdctl command | result |
| --- | --- | --- |
| RoleAdd(AuthRoleAddRequest) returns (AuthRoleAddResponse)	| etcdctl role add |	passed |
| RoleGet(AuthRoleGetRequest) returns (AuthRoleGetResponse) |	etcdctl role get  |	passed |
| RoleList(AuthRoleListRequest) returns (AuthRoleListResponse) |	etcdctl role list |	passed |
| RoleDelete(AuthRoleDeleteRequest) returns (AuthRoleDeleteResponse)	| etcdctl role delete 	| passed |
| RoleGrantPermission(AuthRoleGrantPermissionRequest) returns (AuthRoleGrantPermissionResponse) |	etcdctl role grant-permission  |	passed |
| RoleRevokePermission(AuthRoleRevokePermissionRequest) returns (AuthRoleRevokePermissionResponse) |	etcdctl revoke-permission    |	passed |
