# xline-client

Official Xline API client for Rust that supports the [CURP](https://github.com/xline-kv/Xline/tree/master/crates/curp) protocol

# Pre-requisites
- Install `protobuf-compiler` as mentioned in [QuickStart](https://github.com/xline-kv/Xline/blob/master/doc/quick-start/README.md#install-dependencies)

## Features

`xline-client` runs the CURP protocol on the client side for maximal performance.

## Supported APIs

- KV
  - [x] Put
  - [x] Range
  - [x] Delete
  - [x] Transaction
  - [x] Compact
- Lease
  - [x] Grant
  - [x] Revoke
  - [x] KeepAlive
  - [x] TimeToLive
  - [x] Leases
- Watch
  - [x] WatchCreate
  - [x] WatchCancel
- Auth
  - [x] Authenticate
  - [x] RoleAdd
  - [x] RoleGet
  - [x] RoleList
  - [x] RoleDelete
  - [x] RoleGrantPermission
  - [x] RoleRevokePermission
  - [x] UserAdd
  - [x] UserGet
  - [x] UserList
  - [x] UserDelete
  - [x] UserChangePassword
  - [x] UserGrantRole
  - [x] UserRevokeRole
  - [x] AuthEnable
  - [x] AuthDisable
  - [x] AuthStatus
- Cluster
  - [ ] MemberAdd
  - [ ] MemberRemove
  - [ ] MemberUpdate
  - [x] MemberList
  - [ ] MemberPromote
- Election
  - [ ] Campaign
  - [ ] Proclaim
  - [ ] Resign
  - [ ] Leader
  - [ ] Observe
- Lock
  - [x] Lock
  - [x] Unlock
- Maintenance
  - [ ] Alarm
  - [ ] Status
  - [ ] Defragment
  - [ ] Hash
  - [x] Snapshot
  - [ ] MoveLeader

Note that certain APIs that have not been implemented in Xline will also not be implemented in `xline-client`.

## Getting Started

Add `xline-client` to your `Cargo.toml`:

```toml
[dependencies]
xline-client = { git = "https://github.com/xline-kv/Xline.git", package = "xline-client" }
```
To create a xline client:

 ```rust, no_run
 use xline_client::{
     types::kv::{PutRequest, RangeRequest},
     Client, ClientOptions,
 };
 use anyhow::Result;

 #[tokio::main]
 async fn main() -> Result<()> {
     // the name and address of all curp members
     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

     let mut client = Client::connect(curp_members, ClientOptions::default())
         .await?
         .kv_client();

     client.put(PutRequest::new("key", "value")).await?;

     let resp = client.range(RangeRequest::new("key")).await?;

     if let Some(kv) = resp.kvs.first() {
         println!(
             "got key: {}, value: {}",
             String::from_utf8_lossy(&kv.key),
             String::from_utf8_lossy(&kv.value)
         );
     }

     Ok(())
 }
 ```

## Examples

You can find them in [examples](https://github.com/xline-kv/Xline/tree/master/crates/xline-client/examples)

## Xline Compatibility

We aim to maintain compatibility with each corresponding Xline version, and update this library with each new Xline release.

The current library version has been tested to work with Xline v0.4.1.

## Documentation

Checkout the [API document](https://docs.rs) (currently unavailable) on docs.rs
