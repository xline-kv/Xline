use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::watch;
use tracing::{debug, warn};

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
pub struct TriggerInner {
    /// Sender for shutdown signal.
    trigger: watch::Sender<Signal>,
    /// State of mpsc channel.
    mpmc_channel_shutdown: AtomicBool,
    /// State of sync follower daemon.
    sync_follower_daemon_shutdown: AtomicBool,
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
        debug!("send self shutdown signal");
        if self.inner.trigger.send(Signal::SelfShutdown).is_err() {
            warn!("no listener waiting for shutdown");
        };
    }

    /// Mark mpsc channel shutdown.
    #[inline]
    pub fn mark_channel_shutdown(&self) {
        debug!("mark mpmc channel shutdown");
        self.inner
            .mpmc_channel_shutdown
            .store(true, Ordering::Relaxed);
    }

    /// Mark sync daemon shutdown.
    #[inline]
    pub fn mark_sync_daemon_shutdown(&self) {
        debug!("mark sync followers daemon shutdown");
        self.inner
            .sync_follower_daemon_shutdown
            .store(true, Ordering::Relaxed);
    }

    /// Reset sync daemon shutdown.
    #[inline]
    pub fn reset_sync_daemon_shutdown(&self) {
        debug!("reset sync followers daemon shutdown");
        self.inner
            .sync_follower_daemon_shutdown
            .store(false, Ordering::Relaxed);
    }

    /// Check if the mpsc channel and sync follower daemon has been shutdown.
    /// and send the shutdown signal when both are shutdown.
    #[inline]
    pub fn check_and_shutdown(&self) {
        if self.inner.mpmc_channel_shutdown.load(Ordering::Relaxed)
            && self
                .inner
                .sync_follower_daemon_shutdown
                .load(Ordering::Relaxed)
        {
            self.self_shutdown();
        }
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
        self.listener.changed().await.ok()?;
        Some(*self.listener.borrow())
    }

    /// Wait for the shutdown signal.
    #[inline]
    pub async fn wait_self_shutdown(&mut self) {
        loop {
            let _ig = self.listener.changed().await;
            if matches!(*self.listener.borrow(), Signal::SelfShutdown) {
                break;
            }
        }
    }

    /// Check if the shutdown signal has been sent.
    #[inline]
    pub fn is_shutdown(&mut self) -> bool {
        !matches!(*self.listener.borrow(), Signal::Running)
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
            sync_follower_daemon_shutdown: AtomicBool::new(false),
        }),
    };
    let listener = Listener { listener: rx };
    (trigger, listener)
}
