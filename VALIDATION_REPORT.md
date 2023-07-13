# ETCD API Compatibility Test Report

Since the Xline is designed to be fully compatible with the ETCD, ETCD API compatibility test will be performed after each release. All the compatibility tests are running via etcdctl. Read the [documentation](https://github.com/etcd-io/etcd/blob/main/etcdctl/README.md) for more usage information about etcdctl.

Here is the compatibility test report.

## ETCD KV Relevant APIs/Commands Tests

| API | client command | result |
| --- | --- | --- |
| Range(RangeRequest) returns (RangeResponse) | etcdctl get  | passed |
| DeleteRange(DeleteRangeRequest) returns (DeleteRangeResponse) | etcdctl del | passed |
| Put(PutRequest) returns (PutResponse) | etcdctl put | passed |
| Txn(TxnRequest) returns (TxnResponse) | etcdctl txn | passed |
| Compact(CompactionRequest) returns (CompactionResponse) | etcdctl compaction | unimplemented (will cover in v0.3~v0.5) |
| Watch(stream WatchRequest) returns (stream WatchResponse)	| etcdctl watch | 	passed

## ETCD Lease APIs/Commands Tests

| API | client command | result |
| --- | --- | --- |
| LeaseGrant(LeaseGrantRequest) returns (LeaseGrantResponse) |	etcdctl lease grant | passed |
| LeaseRevoke(LeaseRevokeRequest) returns (LeaseRevokeResponse)	| etcdctl lease revoke | passed |
| LeaseKeepAlive(stream LeaseKeepAliveRequest) returns (stream LeaseKeepAliveResponse)	| etcdctl lease keep-alive |	passed |
| LeaseTimeToLive(LeaseTimeToLiveRequest) returns (LeaseTimeToLiveResponse) | etcdctl lease timetolive | passed |
| LeaseLeases(LeaseLeasesRequest) returns (LeaseLeasesResponse) |	etcdctl lease list |	passed |


## ETCD Cluster Maintenance APIs/Commands Tests

ETCD maintenance relevant APIs are yet to fully implement. We will cover them in the future version (v0.3.0 ~ v0.5.0)

### ETCD Cluster APIs/Commands Tests
| API | client command | result |
| --- | --- | --- |
| MemberAdd(MemberAddRequest) returns (MemberAddResponse) |	etcdctl member add newMember --peer-urls=https://127.0.0.1:12345	| unimplemented (will cover in v0.3~v0.5) |
| MemberRemove(MemberRemoveRequest) returns (MemberRemoveResponse) |	etcdctl member remove 2be1eb8f84b7f63e	| unimplemented (will cover in v0.3~v0.5) |
| MemberUpdate(MemberUpdateRequest) returns (MemberUpdateResponse) |	etcdctl member update 2be1eb8f84b7f63e --peer-urls=https://127.0.0.1:11112	| unimplemented (will cover in v0.3~v0.5) |
| MemberList(MemberListRequest) returns (MemberListResponse) |	etcdctl member list	| unimplemented (will cover in v0.3~v0.5) |


### ETCD Snapshot APIs/Commands Tests

The status and restore relevant features doesn't compatible with the etcd client since the Xline snapshot metadata format is totally different from the etcd snapshot format. The status and restore subcommands can be executed via the `xlinectl`. The `xlinectl` is yet to complete.

| API | client command | result |
| --- | --- | --- |
| Snapshot(SnapshotRequest) returns (stream SnapshotResponse) |	etcdctl snapshot save snapshot.db	| passed |


## ETCD Auth APIs/Commands Tests

| API | client command | result |
| --- | --- | --- |
| AuthEnable(AuthEnableRequest) returns (AuthEnableResponse) |	etcdctl auth enable	| passed |
| AuthDisable(AuthDisableRequest) returns (AuthDisableResponse) |	etcdctl auth disable | passed |
| AuthStatus(AuthStatusRequest) returns (AuthStatusResponse)	| etcdctl auth status |	passed |


### ETCD User APIs/Commands Tests

| API | client command | result |
| --- | --- | --- |
| UserAdd(AuthUserAddRequest) returns (AuthUserAddResponse) |	etcdctl user add | passed |
| UserGet(AuthUserGetRequest) returns (AuthUserGetResponse) |	etcdctl user get |	passed |
| UserList(AuthUserListRequest) returns (AuthUserListResponse) |	etcdctl user list | passed |
| UserDelete(AuthUserDeleteRequest) returns (AuthUserDeleteResponse) | 	etcdctl user delete	| passed |
| UserChangePassword(AuthUserChangePasswordRequest) returns (AuthUserChangePasswordResponse)	| etcdctl user passwd | passed |
| UserGrantRole(AuthUserGrantRoleRequest) returns (AuthUserGrantRoleResponse) |	etcdctl user grant-role |	passed |
| UserRevokeRole(AuthUserRevokeRoleRequest) returns (AuthUserRevokeRoleResponse)	| etcdctl user revoke-role |	passed |


### ETCD Role APIs/Commands Tests

| API | client command | result |
| --- | --- | --- |
| RoleAdd(AuthRoleAddRequest) returns (AuthRoleAddResponse)	| etcdctl role add |	passed |
| RoleGet(AuthRoleGetRequest) returns (AuthRoleGetResponse) |	etcdctl role get  |	passed |
| RoleList(AuthRoleListRequest) returns (AuthRoleListResponse) |	etcdctl role list |	passed |
| RoleDelete(AuthRoleDeleteRequest) returns (AuthRoleDeleteResponse)	| etcdctl role delete 	| passed |
| RoleGrantPermission(AuthRoleGrantPermissionRequest) returns (AuthRoleGrantPermissionResponse) |	etcdctl role grant-permission  |	passed |
| RoleRevokePermission(AuthRoleRevokePermissionRequest) returns (AuthRoleRevokePermissionResponse) |	etcdctl revoke-permission    |	passed |
