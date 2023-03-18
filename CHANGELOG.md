# ChangeLog

## v0.3.0

### Features:
* Implement a persistent storage layer to enable durability, including:
  * Implement a storage engine layer to abstract the concrete storage engine, like `rocksdb`,
    and enable upper layer storage function (#185, #187)
  * Enable recover loggic for `curp` and `xline` (#194, #184)

### Fix Bugs:
* Fix concurrent cmd order bug (#197)

## v0.2.0

### Features:
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
