use tokio::sync::broadcast;

/// Shutdown broadcast wrapper
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: bool,
    /// The receive half of the channel used to listen for shutdown.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.shutdown {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let _trigger = self.notify.recv().await;

        // Remember that the signal has been received.
        self.shutdown = true;
    }

    /// Try to receive the shutdown notice
    #[allow(dead_code)]
    pub(crate) fn try_recv(&mut self) -> bool {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.shutdown {
            return true;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        if self.notify.try_recv().is_ok() {
            self.shutdown = true;
            true
        } else {
            false
        }
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        Self {
            shutdown: false,
            notify: self.notify.resubscribe(),
        }
    }
}
