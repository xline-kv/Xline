use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

use tokio::sync::watch;
use tracing::{info, warn};

/// Shutdown Signal
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Signal {
    /// The initial state, the cluster is running.
    Running,
    /// Cluster shutdown signal.
    ClusterShutdown,
    /// Self shutdown signal.
    SelfShutdown,
}

/// Shutdown trigger.
#[derive(Debug, Clone)]
pub struct Trigger {
    /// Shutdown trigger Inner.
    inner: Arc<TriggerInner>,
}

/// Shutdown trigger Inner.
#[derive(Debug)]
struct TriggerInner {
    /// Sender for shutdown signal.
    trigger: watch::Sender<Signal>,
    /// State of mpsc channel.
    mpmc_channel_shutdown: AtomicBool,
    /// Count of sync follower tasks.
    sync_follower_task_count: AtomicU32,
    /// Shutdown Applied
    leader_notified: AtomicBool,
}

impl TriggerInner {
    /// Check if the mpsc channel and sync follower daemon has been shutdown.
    /// and send the shutdown signal when both are shutdown.
    fn check_and_shutdown(&self) {
        if self.mpmc_channel_shutdown.load(Ordering::Relaxed)
            && self.sync_follower_task_count.load(Ordering::Relaxed) == 0
            && self.leader_notified.load(Ordering::Relaxed)
        {
            self.self_shutdown();
        }
    }

    /// Send the shutdown signal
    fn self_shutdown(&self) {
        info!("send self shutdown signal");
        if self.trigger.send(Signal::SelfShutdown).is_err() {
            warn!("no listener waiting for shutdown");
        };
    }
}

impl Trigger {
    /// Get the current state of the shutdown trigger.
    #[inline]
    #[must_use]
    pub fn state(&self) -> Signal {
        *self.inner.trigger.borrow()
    }

    /// Send the shutdown signal
    #[inline]
    pub fn self_shutdown(&self) {
        info!("send self shutdown signal");
        if self.inner.trigger.send(Signal::SelfShutdown).is_err() {
            warn!("no listener waiting for shutdown");
        };
    }

    /// Mark mpsc channel shutdown.
    #[inline]
    pub fn mark_channel_shutdown(&self) {
        info!("mark mpmc channel shutdown");
        self.inner
            .mpmc_channel_shutdown
            .store(true, Ordering::Relaxed);
    }

    /// Mark leader notified
    #[inline]
    pub fn mark_leader_notified(&self) {
        info!("mark leader notified");
        self.inner.leader_notified.store(true, Ordering::Relaxed);
    }

    /// Check if the mpsc channel and sync follower daemon has been shutdown.
    /// and send the shutdown signal when both are shutdown.
    #[inline]
    pub fn check_and_shutdown(&self) {
        self.inner.check_and_shutdown();
    }

    /// Send the shutdown signal
    #[inline]
    pub fn cluster_shutdown(&self) {
        if self.inner.trigger.send(Signal::ClusterShutdown).is_err() {
            warn!("no listener waiting for shutdown");
        };
    }

    /// Send the shutdown signal and wait for all listeners drop.
    #[inline]
    pub async fn self_shutdown_and_wait(&self) {
        if self.inner.trigger.send(Signal::SelfShutdown).is_err() {
            warn!("no listener waiting for shutdown");
            return;
        };
        self.inner.trigger.closed().await;
    }

    /// Creates a new `Listener` for this `Trigger`.
    #[inline]
    #[must_use]
    pub fn subscribe(&self) -> Listener {
        Listener {
            listener: self.inner.trigger.subscribe(),
        }
    }

    /// Get `SyncFollowerToken`
    #[inline]
    #[must_use]
    pub fn token(&self) -> SyncFollowerToken {
        _ = self
            .inner
            .sync_follower_task_count
            .fetch_add(1, Ordering::Relaxed);
        SyncFollowerToken {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Get receiver count.
    #[inline]
    #[must_use]
    pub fn receiver_count(&self) -> usize {
        self.inner.trigger.receiver_count()
    }

    /// Check if the shutdown trigger has been closed.
    #[inline]
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.trigger.is_closed()
    }
}

/// Shutdown listener.
#[derive(Debug, Clone)]
pub struct Listener {
    /// The watch receiver.
    listener: watch::Receiver<Signal>,
}

impl Listener {
    /// Wait for the shutdown signal.
    #[inline]
    pub async fn wait(&mut self) -> Option<Signal> {
        let current = *self.listener.borrow();
        let sig = match current {
            Signal::ClusterShutdown => Signal::ClusterShutdown,
            Signal::SelfShutdown => Signal::SelfShutdown,
            Signal::Running => {
                self.listener.changed().await.ok()?;
                *self.listener.borrow()
            }
        };
        Some(sig)
    }

    /// Wait for the shutdown signal.
    #[inline]
    pub async fn wait_self_shutdown(&mut self) {
        loop {
            if matches!(*self.listener.borrow(), Signal::SelfShutdown) {
                break;
            }
            let _ig = self.listener.changed().await;
        }
    }

    /// Check if the shutdown signal has been sent.
    #[inline]
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        !matches!(*self.listener.borrow(), Signal::Running)
    }
}

/// sync follower token, used to track the sync follower tasks.
#[derive(Debug)]
pub struct SyncFollowerToken {
    /// Shutdown trigger Inner.
    inner: Arc<TriggerInner>,
}

impl Drop for SyncFollowerToken {
    #[inline]
    fn drop(&mut self) {
        _ = self
            .inner
            .sync_follower_task_count
            .fetch_sub(1, Ordering::Relaxed);
        self.inner.check_and_shutdown();
    }
}

/// Create a channel for shutdown.
#[must_use]
#[inline]
pub fn channel() -> (Trigger, Listener) {
    let (tx, rx) = watch::channel(Signal::Running);
    let trigger = Trigger {
        inner: Arc::new(TriggerInner {
            trigger: tx,
            mpmc_channel_shutdown: AtomicBool::new(false),
            sync_follower_task_count: AtomicU32::new(0),
            leader_notified: AtomicBool::new(false),
        }),
    };
    let listener = Listener { listener: rx };
    (trigger, listener)
}
