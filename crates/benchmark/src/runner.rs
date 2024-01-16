use std::{
    collections::HashMap,
    fmt::Write as _,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Result;
use clippy_utilities::{NumericCast, OverflowArithmetic};
use indicatif::ProgressBar;
use rand::RngCore;
use tokio::{
    sync::{
        mpsc::{self, Receiver},
        Barrier,
    },
    time::{Duration, Instant},
};
use tracing::debug;
use utils::config::ClientConfig;
use xline_client::{types::kv::PutRequest, ClientOptions};

use crate::{args::Commands, bench_client::BenchClient, Benchmark};

/// Result of request
#[derive(Debug)]
struct CmdResult {
    /// elapsed time
    elapsed: Duration,
    /// Error message of request
    error: Option<String>,
}

/// `CommandRunner` is the main struct for running commands.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct CommandRunner {
    /// errors of requests
    errors: HashMap<String, usize>,
    /// Args of benchmark
    args: Benchmark,
}

#[derive(Debug, Default)]

/// Stats of benchmark
pub struct Stats {
    /// all latencies
    pub latencies: Vec<Duration>,
    /// total time
    pub total: Duration,
    /// slowest latency
    pub slowest: Duration,
    /// fastest latency
    pub fastest: Duration,
    /// average latency
    pub avg: Duration,
    /// Operations per second
    pub qps: f64,
}

impl Stats {
    /// New `Stats`
    fn new() -> Self {
        Self::default()
    }

    /// Summary of stats
    #[allow(clippy::let_underscore_must_use)] // the writeln! will not fail thus always return an Ok
    pub fn summary(&self) -> String {
        let mut s = String::from("\nSummary:\n");
        _ = writeln!(s, "  Total:        {:.4} secs", self.total.as_secs_f64());
        _ = writeln!(s, "  Slowest:      {:.4} secs", self.slowest.as_secs_f64());
        _ = writeln!(s, "  Fastest:      {:.4} secs", self.fastest.as_secs_f64());
        _ = writeln!(s, "  Average:      {:.4} secs", self.avg.as_secs_f64());
        _ = writeln!(s, "  Requests/sec: {:.2}", self.qps);
        s
    }

    /// Calculate the histogram from the latencies.
    #[allow(clippy::indexing_slicing)]
    #[allow(clippy::let_underscore_must_use)] // the writeln! will not fail thus always return an Ok
    #[allow(clippy::arithmetic_side_effects)] // benchmark only
    pub fn histogram(&self) -> String {
        let size = 10;
        let mut buckets = Vec::with_capacity(size);
        let mut counts = vec![0; size];
        let gap = (self.slowest - self.fastest) / (size.numeric_cast::<u32>().overflow_sub(1));
        for i in 0..size.numeric_cast::<u32>().overflow_sub(1) {
            buckets.push(self.fastest + gap * i);
        }
        buckets.push(self.slowest);
        let mut idx = 0;

        for latency in &self.latencies {
            while latency > &buckets[idx] {
                idx = idx.overflow_add(1);
            }
            counts[idx] = counts[idx].overflow_add(1);
        }
        let max = counts.iter().max().copied().unwrap_or_default();
        let mut s = String::from("\nResponse time histogram:\n");
        for i in 0..size {
            let bar_len = counts[i].overflow_mul(40).overflow_div(max);
            _ = writeln!(
                s,
                "  {:.4}\t[{}]\t| {}",
                buckets[i].as_secs_f64(),
                counts[i],
                "∎".repeat(bar_len)
            );
        }
        s
    }
}

impl CommandRunner {
    /// New `CommandRunner`
    #[inline]
    #[must_use]
    pub fn new(args: Benchmark) -> Self {
        Self {
            errors: HashMap::new(),
            args,
        }
    }

    /// Run benchmark
    ///
    /// # Errors
    ///
    /// Errors if the benchmark fails.
    #[inline]
    pub async fn run(&mut self) -> Result<Stats> {
        let clients = self.create_clients().await?;
        match self.args.command {
            Commands::Put {
                key_size,
                val_size,
                total,
                key_space_size,
                sequential_keys,
            } => {
                self.put_bench(
                    clients,
                    key_size,
                    val_size,
                    total,
                    key_space_size,
                    sequential_keys,
                )
                .await
            }
        }
    }

    /// Create clients
    async fn create_clients(&self) -> Result<Vec<BenchClient>> {
        let mut clients = Vec::with_capacity(self.args.clients);
        let client_options = ClientOptions::default().with_client_config(ClientConfig::new(
            Duration::from_secs(10),
            Duration::from_secs(5),
            Duration::from_millis(250),
            Duration::from_millis(10_000),
            3,
            true,
            Duration::from_secs(1),
        ));
        let addrs = self
            .args
            .endpoints
            .iter()
            .map(|addr| {
                if let Some((_id, addr)) = addr.split_once('=') {
                    addr.to_owned()
                } else {
                    unreachable!("cannot parse endpoints")
                }
            })
            .collect::<Vec<_>>();
        for _ in 0..self.args.clients {
            let client =
                BenchClient::new(addrs.clone(), self.args.use_curp, client_options.clone()).await?;
            clients.push(client);
        }
        Ok(clients)
    }

    /// Run put benchmark
    async fn put_bench(
        &mut self,
        clients: Vec<BenchClient>,
        key_size: usize,
        val_size: usize,
        total: usize,
        key_space_size: usize,
        sequential_keys: bool,
    ) -> Result<Stats> {
        let count = Arc::new(AtomicUsize::new(0));
        let b = Arc::new(Barrier::new(clients.len().overflow_add(1)));
        let (tx, rx) = mpsc::channel(clients.len());

        let mut val = vec![0u8; val_size];
        rand::thread_rng().fill_bytes(&mut val);
        let val = Arc::new(val);

        let mut handles = Vec::with_capacity(clients.len());
        for mut client in clients {
            let c = Arc::clone(&b);
            let count_clone = Arc::clone(&count);
            let val_clone = Arc::clone(&val);
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                let mut key = vec![0u8; key_size];
                _ = c.wait().await;
                loop {
                    let idx = count_clone.fetch_add(1, Ordering::Relaxed);
                    if idx >= total {
                        break;
                    }
                    if sequential_keys {
                        Self::fill_usize_to_buf(&mut key, idx.overflow_rem(key_space_size));
                    } else {
                        Self::fill_usize_to_buf(
                            &mut key,
                            rand::random::<usize>().overflow_rem(key_space_size),
                        );
                    }
                    let start = Instant::now();
                    let result = client
                        .put(PutRequest::new(key.as_slice(), val_clone.as_slice()))
                        .await;
                    let cmd_result = CmdResult {
                        elapsed: start.elapsed(),
                        error: result.err().map(|e| format!("{e:?}")),
                    };
                    assert!(
                        tx_clone.send(cmd_result).await.is_ok(),
                        "failed to send cmd result"
                    );
                }
            });
            handles.push(handle);
        }
        drop(tx);
        let stats = self.collecter(rx, b).await;
        for handle in handles {
            handle.await?;
        }
        Ok(stats)
    }

    /// Collect `CmdResult` and process them to `Stats`
    #[allow(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        clippy::float_arithmetic,
        clippy::arithmetic_side_effects
    )]
    async fn collecter(&mut self, mut rx: Receiver<CmdResult>, barrier: Arc<Barrier>) -> Stats {
        let bar_len = match self.args.command {
            Commands::Put { total, .. } => total,
        };
        let bar = Arc::new(ProgressBar::new(bar_len.numeric_cast()));

        if bar.is_hidden() {
            let bar_clone = Arc::clone(&bar);
            let _handle = tokio::spawn(async move {
                while !bar_clone.is_finished() {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    debug!("progress: {}/{}", bar_clone.position(), bar_len);
                }
            });
        }
        let mut stats = Stats::new();
        _ = barrier.wait().await;
        debug!("Collecting benchmark results...");
        let start = Instant::now();
        while let Some(result) = rx.recv().await {
            if let Some(err) = result.error {
                let entry = self.errors.entry(err).or_insert(0);
                *entry = entry.overflow_add(1);
                continue;
            }
            bar.inc(1);
            stats.latencies.push(result.elapsed);
        }
        assert!(
            !stats.latencies.is_empty(),
            "All requests failed! {:?}",
            self.errors
                .iter()
                .max_by(|x, y| x.1.cmp(y.1))
                .map(|(err, _)| err)
        );
        stats.total = start.elapsed();
        stats.qps = stats.latencies.len() as f64 / stats.total.as_secs_f64();
        stats.avg =
            stats.latencies.iter().sum::<Duration>() / stats.latencies.len().numeric_cast::<u32>();

        stats.latencies.sort();
        if let Some(fastest) = stats.latencies.first() {
            stats.fastest = *fastest;
        }
        if let Some(slowest) = stats.latencies.last() {
            stats.slowest = *slowest;
        }
        stats
    }

    /// Fill `usize` to `buf`
    fn fill_usize_to_buf(buf: &mut [u8], value: usize) {
        let mut value = value;
        for pos in buf {
            *pos = (value & 0xff).numeric_cast();
            value = value.overflow_shr(8);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_fill_usize_to_buf() {
        let mut buf = vec![0; 8];

        CommandRunner::fill_usize_to_buf(&mut buf, 1);
        assert_eq!(buf, vec![1, 0, 0, 0, 0, 0, 0, 0]);

        CommandRunner::fill_usize_to_buf(&mut buf, 256);
        assert_eq!(buf, vec![0, 1, 0, 0, 0, 0, 0, 0]);

        CommandRunner::fill_usize_to_buf(&mut buf, 257);
        assert_eq!(buf, vec![1, 1, 0, 0, 0, 0, 0, 0]);
    }
}
