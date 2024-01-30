# utils

This crate provides a set of utilities for locks.

## Usage

Add this to your Cargo.toml :
```Toml
[dependencies]
utils = { path = "../utils", features = ["std"] }
```

Write your code like this:
```Rust
use utils::std_lock::{MutexMap, RwLockMap};
use std::sync::{Mutex, RwLock};

let mu = Mutex::new(1);
mu.map_lock(|mut g| {
    *g = 3;
})?;
let val = mu.map_lock(|g| *g)?;
assert_eq!(val, 3);

let rwlock = RwLock::new(1);
rwlock.map_write(|mut g| {
    *g = 3;
})?;
let val = rwlock.map_read(|g| *g)?;
assert_eq!(val, 3);
```

## Features
- `std`: utils for `std::sync::Mutex` and `std::sync::RwLock`
- `parking_lot`: utils for` parking_lot::Mutex` and `parking_lot::RwLock`
- `tokio`: utils for `tokio::sync::Mutex` and `tokio::sync::RwLock`
