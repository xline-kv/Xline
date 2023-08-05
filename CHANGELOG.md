# ChangeLog

## v0.5.0

### Features

- Add a new xline-client crate
- Add new `simulation` crate and change dependencies to madsim
- Add madsim specific code in curp
- Support multiplatform quick start
- Implement kv client for xline-client
- Support single node cluster
- Add mock implementation of rocksdb
- Support dns resolution for Xline cluster
- Support single node cluster
- Implement watch client for xline-client
- Support multiplatform artifacts
- Implement maintenance client for xline-client
- Add a new macro which can abort process when panicked
- Implement auth client for xline-client
- Implement lease client for xline-client
- Support grpc health checking protocol
- Implement underlying compact logic
- Add revision check for kv and watch requests
- Add conflict check logic for compact
- Implement compactor task
- Make compact interval and batch_size configurable
- Implement compact consensus process
- Add compact logic in recover process
- Add kv server compact implementation
- Add a new `xlinectl` crate
- Implement lock client for xline-client
- Implement compaction in `xline-client`
- Add `ExecuteError` to tonic::Status conversion
- Add a new `RequestValidator` trait for request validation
- Add revision related errors
- Add revision checker in `kv_store`
- Implement the revision compactor

### Fix Bugs

- Use git version of madsim-tonic
- Avoid losing events when using event_lisenter and tokio::select! together
- Resolve failing tests related to serialized size
- Wait for lease synced in lease keepalive and lease timetolive
- Change `all_crash_and_recovery` to `minority_crash_and_recovery`
- Remove the reapplication of log entries when leader retires
- Only write to storage when after sync in `test_cmd`
- Fix `lock_success` memory ordering
- `range_revision` could be less than or equal to 0 in `check_range_compacted`
- Fix compaction test in xline client

### Refactor

- Move curp public trait interfaces to `curp-external-api` crate
- Move curp test utils to `curp-test-utils` crate
- Use `tempfile` crate to create temporary dirs
- Move xline test utils to `xline-test-utils` crate
- Add lease leases request to request wrapper
- Add lease leases request to xline lease store
- Let lease_leases use curp protocol in lease_server
- Add a common util for `xline-client` tests
- Merge entries and batch_index in raw_curp
- Move kv types in `xline-client` to a separate file
- Remove unnecessary sleep in curp integration tests
- Change leader fetch mechanism and increase sleep time in simulation
- Refactor curp_server storage implementation
- Change `old_leader_will_discard_spec_exe_cmds` to `old_leader_will_keep_original_states`
- Remove unnecessary mutable borrow in `xline-client`
- Refactor compact mod
- Make the `Client::connect` interface more ergonomic
- Re-export `xlineapi` in `xline-client`
- Update maintenance tests in `xline-client`
- Remove `Clone` trait bound in `connect`
- Add a new trait that control the printer type in xlinectl
- Return revision in execution result
- Add ttl option in lock request
- Remove unnecessary mutable borrow in lock client
- Add clean up logic when lock failed
- Use a different approach to implement lock contention test
- Modify visibility of some types and modules
- Move xlineapi export to types
- Return `RpcError` instead of `ProposeError` in `ConnectApi`
- Move `Error` associate type from `CommandExecutor` to `Command`
- Split errors to curp errors and user defined errors
- Expand `ExecuteError` to use distinct types
- Handle `CommandProposeError` in client propose result
- Move sync error type to `error.rs`
- Change code in `kv_server` to use `RequestValidator`
- Add validation check for request in `kv_store`
- Implement auth request validation
- Move revision check back to `kv_server`
- Add a `RevisionCheck` trait to unify check for request revisions
- Break down the dependency between CurpServer and CurpClient
- Improve the compatibility with etcdctl
- Refactor auto compactor implementation
- Refactor the error handling logic in compact

## v0.4.1

### Features
* Add watch progress notify mechanism (issue #309), resolve in pr #278


### Fix Bugs
- Fix a bug #285 that command workers will panic in the integration tests, resolve in pr #286.
- Fix a bug #291 that the integration test case `test_kv_authorization` will block in some rare situations, resolve in pr #292
- Fix a bug #252 that the xline client watch work abnormally after terminating it by sending it a SIGINT signal, resolve in pr #255
- Fix bugs #284 and #303 that the integration test case `test_lock_timeout` will panic and the lock validation test will block, resolve in pr #312


### Refactors
* Refactor `XlineServer` to break some giant structures and methods into smaller ones, improving the readability(issue #293), resolve in pr #294
* Refactor curp fast read implementation(issue #270), resolve in pr #297
* Improve the read and write logic for the `RocksSnapshot` (issue #263), resolve in pr #264
* Refactor the watch server's implementation(issue #253), resolve in pr #262, #268, #278
* Refactor the kv server's implementation(issue #250), resolve in pr #260
* Refactor the lease server's implementation(issue #251), resolve in pr #276
* Using a better way to generate stream(issue #248), resolve in pr #249


## v0.4.0

### Features
1. Introduce batching mechanism to improve network bandwidth utilization
2. Implement the snapshot feature for CURP consensus protocol,
3. Implement the snapshot relevant API，which is compatible with etcdctl. The rest of other APIs in etcdctl maintenance will be implemented in the future.

### Fix Bugs
1. Fix a bug that commands will execute out of order in some concurrent cases (issue #197）, resolve in the pr #195
2. Fix a bug that the gc task will panic during benchmark(issue #206), resolve in the pr #210
3. Fix a bug that the lock feature will work abnormally in some cases(issue #209), resolve in the pr #212
4. Fix a bug that some concurrent put requests will get wrong revisions (issue #209), resolve in the pr #238

## v0.3.0

### Features
* Implement a persistent storage layer to enable durability, including:
  * Implement a storage engine layer to abstract the concrete storage engine, like `rocksdb`,
    and enable upper layer storage function (#185, #187)
  * Enable recover loggic for `curp` and `xline` (#194, #184)

### Fix Bugs
* Fix concurrent cmd order bug (#197)

## v0.2.0

### Features
* Enable Xline to boot up from the config file `xline_server.conf` (#145)
* Support ETCD APIs, like the lease api and the lock api  (#142, #153)
* Enable the recovery mechanism in CURP module (#146)
* Add ETCD APIs compatibility test (test report: (report)[./VALIDATION_REPORT.md])

### Fix Bugs
* Fix panic in benchmark (#123)
* Fix the issue that modify kv pairs will fail after watch them in `etcdctl` (#148)


## v0.1.0

### What is it?

`Xline` is a geo-distributed KV store for metadata management, which is based on the `Curp` protocol.

### Why make it?

Existing distributed KV stores mostly adopt the `Raft` consensus protocol, which takes two RTTs to complete a request. When deployed in a single data center, the latency between nodes is low, so it will not have a big impact on performance. However, when deployed across data centers, the latency between nodes may be tens or hundreds of milliseconds, at which point the `Raft` protocol will become a performance bottleneck. The `Curp` protocol is designed to solve this problem. It can reduce one RTT when commands do not conflict, thus improving performance.

### What does it provide?
- Etcd Compatible API
  - `Kv` service
  - `Watch` service
  - `Auth` service
- basic implementation of the `Curp` protocol
- basic `Xline` client (use `Curp` directly)
- benchmark tool

### Usage
[Usage doc](./USAGE.md)

### Note

In this release, we only provide binary files for X86_64 linux. Other platforms need to be compiled by yourself. we will add more support in the future.


### Links
- GitHub: https://github.com/datenlord/Xline
- Crate: https://crates.io/crates/xline
- Docs: https://docs.rs/xline
- Paper of Curp: https://www.usenix.org/system/files/nsdi19-park.pdf
