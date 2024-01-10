# ChangeLog

## v0.6.1

### Features

* implement retry layer eda7302
* implement publish for new client c2404e8
* client will send physical compact request to business server dbb6a84
* implement WAL codec afa7c3d
* add wal errors df4039d
* add WAL file utils 31cf545

### Fixes

* allow slow round when get Duplicated error db13bb3
* fix retry policy da4fa03
* fix cluster version update 69fcb2f
* atomic update in cluster state and leader state b889fc7
* fix compact physical logic f14b88f
* fix madsim test d1b0b89
* fix ce event tx logs 8055282
* shutdown cluster before drop runtime 7fdb8da
* add a limit when leader tries to shtuduwn a node that has been shutdown 844fc53
* fix cluster shutdown after member add 5dc185b
* update abort_on_panic macro and fix read_state test 53e6c45
* fix xline_update_node test and update comments a4dee03

## v0.6.0

### Features

* support pre vote phase a3c6840
* discover cluster in shutdown propose_conf_change and fetch_read_state bfb009e
* client update cluster info 9a95915
* return new cluster at server side a85df5a
* add cluster version to client 7728bb3
* add JSON printer in xlinectl 342d83f
* implement member command in xlinectl d6fa954
* implement `compaction` command in xlinectl 44b2249
* fallback promote conf change deb1eff
* implement `lock` command in `xlinectl` f9a0bbc
* implement `watch` command in `xlinectl` fd1f4e3
* implement `txn` command in `xlinectl` bfe038f
* merge fetch leader into fetch cluster 58c619b
* implement cluster client 5dbb9f6
* implement basic exponential backoff for client retires ce68124
* implement a portion of status rpc bed768c
* merge fetch leader into fetch cluster 24af599
* add cluster server ce578d1
* implement `role` commands in `xlinectl` 51c19bc
* support multiple address listening ae7742c
* implement `user` commands in `xlinectl` 0aca7ab
* implement `auth` command in `xlinectl` 6dbffa9
* implement `snapshot` command in `xlinectl` 7e7fad5
* implement `lease` commands in `xlinectl` 7b394ac
* implement `delete` command in `xlinectl` bda9797
* add `EntryData`, `LogEntry` can contain content other than commands 22f34e8

### Fixes

* fix watcher ac6a56c, closes #505
* fix reelect test fe5b7de
* add linearizable parameter in FetchClusterRequest 452c245
* fix linearizable read in member_list e726fa8
* fix member add aea430c
* fix blocking in madsim f0ffc88
* benchmark client cannot connect to server 18ca614
* remove `stop` in simulation tests 2bc596a
* add ucp recovery after new leader is elected 32694b0, closes #438
* fix shutdown in leader keep alive 6d05901
* check the password on leader 0f30a29
* CURP TLA+ quorum calculation ef35c2c
* improve readability of bootstrap errors 6b94865
* improve readability of bootstrap errors 1b6ff5c
* remove recovery of uncommitted pool 2a034e9
* CURP TLA+ quorum calculation & property check fadc656
* fix propose doesn't handle SyncedError and reduce code duplication 5126802

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
