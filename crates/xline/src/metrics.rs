use clippy_utilities::NumericCast;
use opentelemetry::{
    metrics::{Counter, MetricsError},
    KeyValue,
};
use tracing::error;
use utils::define_metrics;

define_metrics! {
    "xline",
    slow_read_indexes_total: Counter<u64> = meter()
        .u64_counter("slow_read_index")
        .with_description("The total number of pending read indexes not in sync with leader's or timed out read index requests.")
        .init(),
    read_indexes_failed_total: Counter<u64> = meter()
        .u64_counter("read_index_failed")
        .with_description("The total number of failed read indexes seen.")
        .init(),
    lease_expired_total: Counter<u64> = meter()
        .u64_counter("lease_expired")
        .with_description("The total number of expired leases.")
        .init()
}

impl Metrics {
    /// Register metrics
    pub(super) fn register_callback() -> Result<(), MetricsError> {
        let meter = meter();
        let (fd_used, fd_limit, current_version, current_rust_version) = (
            meter
                .u64_observable_gauge("fd_used")
                .with_description("The number of used file descriptors.")
                .init(),
            meter
                .u64_observable_gauge("fd_limit")
                .with_description("The file descriptor limit.")
                .init(),
            meter
                .u64_observable_gauge("current_version")
                .with_description("Which version is running. 1 for 'server_version' label with current version.")
                .init(),
            meter
                .u64_observable_gauge("current_rust_version")
                .with_description("Which Rust version server is running with. 1 for 'server_rust_version' label with current version.")
                .init(),
        );

        _ = meter.register_callback(&[fd_used.as_any(), fd_limit.as_any()], move |observer| {
            #[allow(unsafe_code)] // we need this
            #[allow(clippy::multiple_unsafe_ops_per_block)] // Is it bad?
            let limit = unsafe {
                let mut rlimit = nix::libc::rlimit {
                    rlim_cur: 0, // soft limit
                    rlim_max: 0, // hard limit
                };
                let errno = nix::libc::getrlimit(nix::libc::RLIMIT_NOFILE, &mut rlimit);
                if errno < 0 {
                    error!("invoke getrlimit failed!");
                    None
                } else {
                    Some(rlimit.rlim_cur)
                }
            };

            match fd_used_count() {
                Ok(used) => observer.observe_u64(&fd_used, used, &[]),
                Err(err) => error!("{err}"),
            }

            if let Some(limit) = limit {
                observer.observe_u64(&fd_limit, limit.numeric_cast(), &[]);
            }
        })?;

        _ = meter.register_callback(
            &[current_version.as_any(), current_rust_version.as_any()],
            move |observer| {
                let crate_version: &str = env!("CARGO_PKG_VERSION");
                let rust_version: &str = env!("CARGO_PKG_RUST_VERSION");
                observer.observe_u64(
                    &current_version,
                    1,
                    &[KeyValue::new("server_version", crate_version)],
                );
                observer.observe_u64(
                    &current_rust_version,
                    1,
                    &[KeyValue::new("server_rust_version", rust_version)],
                );
            },
        )?;

        Ok(())
    }
}

/// Get the actual fd used on macOS
#[allow(
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::cast_sign_loss
)] // safe
#[allow(unsafe_code)]
#[cfg(target_os = "macos")]
fn fd_used_count() -> anyhow::Result<u64> {
    use std::{
        os::raw::{c_int, c_void},
        ptr::null_mut,
    };

    use anyhow::anyhow;

    /// Copying the related definitions from the headers
    #[repr(C)]
    #[derive(Copy, Clone, Default)]
    #[allow(clippy::missing_docs_in_private_items)] // I don't know what they are.
    #[allow(non_camel_case_types)] // c style
    struct proc_fd_info {
        proc_fd: i32,
        proc_fd_type: u32,
    }

    extern "C" {
        /// From undocumented `libproc.h`
        #[allow(unused)] // ??
        fn proc_pidinfo(
            pid: c_int,
            flavor: c_int,
            arg: u64,
            buffer: *mut c_void,
            size: c_int,
        ) -> c_int;
    }

    let pid = std::process::id() as c_int;
    let fds_flavor: c_int = 1;
    let buffer_size_bytes = unsafe { proc_pidinfo(pid, fds_flavor, 0, null_mut(), 0) };
    if buffer_size_bytes < 0 {
        return Err(anyhow!("invoke proc_pidinfo failed"));
    }
    let fds_buffer_length: usize = buffer_size_bytes as usize / std::mem::size_of::<proc_fd_info>();
    let mut buf: Vec<proc_fd_info> = vec![proc_fd_info::default(); fds_buffer_length];
    buf.shrink_to_fit();
    let actual_buffer_size_bytes = unsafe {
        proc_pidinfo(
            pid,
            fds_flavor,
            0,
            buf.as_mut_ptr().cast::<std::ffi::c_void>(),
            buffer_size_bytes,
        )
    };
    if actual_buffer_size_bytes < 0 {
        return Err(anyhow!("invoke proc_pidinfo failed"));
    }
    if actual_buffer_size_bytes >= buffer_size_bytes {
        return Err(anyhow!("allocated buffer too small"));
    }
    buf.truncate(actual_buffer_size_bytes as usize / std::mem::size_of::<proc_fd_info>());
    Ok(buf.len() as u64)
}

/// Get the fd used on other platform
#[allow(clippy::as_conversions, clippy::arithmetic_side_effects)] // safe
#[cfg(not(target_os = "macos"))] // actually works on linux, but could be compiled on other platform
fn fd_used_count() -> anyhow::Result<u64> {
    let path = std::path::Path::new("/proc/self/fd");
    if path.exists() {
        // Keep same with etcd.
        // The fd count obtained under concurrent requests
        // to `/metrics`` will be slightly higher than normal.
        // TODO: maybe we could add a mutex on path?
        // Opening this folder will also increase a file descriptor.
        return Ok(std::fs::read_dir(path)?.count() as u64 - 1);
    }
    Ok(0)
}
