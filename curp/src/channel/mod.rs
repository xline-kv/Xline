use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime},
};

use event_listener::{Event, EventListener};
use itertools::Itertools;
use parking_lot::Mutex;
use thiserror::Error;

use crate::cmd::ConflictCheck;

pub(crate) mod key_mpsc;
pub(crate) mod key_spmc;

/// If two key sets conflict with each other
fn keys_conflict<K: ConflictCheck>(ks1: &[K], ks2: &[K]) -> bool {
    ks1.iter()
        .cartesian_product(ks2.iter())
        .any(|(k1, k2)| k1.is_conflict(k2))
}

/// The inner type for Keys and Messages combined structure
struct KeysMessageInner<K, M> {
    /// The keys
    keys: Vec<K>,
    /// The message
    msg: Arc<Mutex<M>>,
}

impl<K, M> KeysMessageInner<K, M> {
    /// Create a new `KeysMessageInner` structure
    fn new(keys: Vec<K>, msg: M) -> Self {
        Self {
            keys,
            msg: Arc::new(Mutex::new(msg)),
        }
    }

    /// map the inner message to a closure
    fn map_msg<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut M) -> R,
    {
        f(&mut *self.msg.lock())
    }

    /// Get keys ref
    fn keys(&self) -> &[K] {
        self.keys.as_ref()
    }
}

/// implement Hash and Eq trait for the message type
macro_rules! hash_eq {
    ($message: ident) => {
        impl<K, M> Hash for $message<K, M> {
            /// Hash based on the rc pointer value
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                Arc::<_>::as_ptr(&self.inner).hash(state);
            }
        }

        impl<K, M> PartialEq for $message<K, M> {
            /// Compare the rc pointer
            fn eq(&self, other: &Self) -> bool {
                Arc::<_>::as_ptr(&self.inner) == Arc::<_>::as_ptr(&other.inner)
            }
        }

        impl<K, M> Eq for $message<K, M> {}
    };
}
use hash_eq;

/// the send error
#[derive(Error, Debug)]
pub(crate) enum SendError {
    /// Channel stop working
    #[error("channel stopped")]
    ChannelStop,
}

/// the recv error
#[derive(Error, Debug)]
pub(crate) enum RecvError {
    /// Channel stop working
    #[error("channel stopped")]
    ChannelStop,
    /// Timeout
    #[error("timeout")]
    Timeout,
    /// Current no available
    #[error("Current there's no available message")]
    NoAvailable,
}

/// This is a kind of key-based channel.
///
/// It receives and sends messages as usual, but the recv end can't receive messages for a
/// the same key simutanously. Any message with the same key is blocked, if there's a previous
/// message not ready or not complete.
struct KeybasedChannel<KM> {
    /// Proceeders that arrive eailer and have key confliction with this message, we only record count
    proceeder: HashMap<KM, u64>,
    /// Successors that arrive later and have key confliction with this message
    successor: HashMap<KM, HashSet<KM>>,
    /// The real queue the receivers get the message from.
    inner: VecDeque<KM>,
    /// The notifier notifies the new arrived message in the `inner`
    new_msg_event: Event,
    /// Is it still working?
    is_working: bool,
}

impl<KM> KeybasedChannel<KM> {
    /// Flush the inner queue to the `new_queue`
    fn flush_queue(&mut self, new_queue: &mut VecDeque<KM>) {
        assert!(new_queue.is_empty());
        std::mem::swap(&mut self.inner, new_queue);
    }

    /// Mark the channel stop working due to one of the sides (tx or rx) is not available
    fn mark_stop(&mut self) {
        self.is_working = false;
        // Notify the rx to get the channel stop event
        self.new_msg_event.notify(1);
    }
}

/// Channel Sender Inner
struct KeybasedSenderInner<KM> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<KM>>>,
}

/// If all the Sender is dropped, the channel should be closed
impl<KM> Drop for KeybasedSenderInner<KM> {
    fn drop(&mut self) {
        self.channel.lock().mark_stop();
    }
}

/// Channel Receiver Inner
pub(crate) struct KeybasedReceiverInner<KM> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<KM>>>,
    /// Local message buf
    buf: VecDeque<KM>,
}

/// The return value of `get_msg_or_listener` method
enum MessageOrListener<M> {
    /// The message
    Message(M),
    /// The listener
    Listen(EventListener),
    /// Channel stopped
    Stop,
}

impl<KM> KeybasedReceiverInner<KM> {
    /// check if it's still working
    fn is_working(&self) -> bool {
        self.channel.lock().is_working
    }

    /// Get a message from the channel or a waiting listener
    fn get_msg_or_listener(&mut self) -> MessageOrListener<KM> {
        if !self.is_working() {
            return MessageOrListener::Stop;
        }
        if let Some(buf_msg) = self.buf.pop_front() {
            return MessageOrListener::Message(buf_msg);
        }

        // local buffer is empty
        let mut channel = self.channel.lock();
        let mut listener = None;
        loop {
            channel.flush_queue(&mut self.buf);
            match self.buf.pop_front() {
                Some(msg) => return MessageOrListener::Message(msg),
                None => {
                    if let Some(l) = listener {
                        return MessageOrListener::Listen(l);
                    }
                    listener = Some(channel.new_msg_event.listen());
                }
            }
        }
    }

    /// Receive a message
    pub(crate) fn recv(&mut self) -> Result<KM, RecvError> {
        loop {
            match self.get_msg_or_listener() {
                MessageOrListener::Message(msg) => return Ok(msg),
                MessageOrListener::Listen(l) => l.wait(),
                MessageOrListener::Stop => return Err(RecvError::ChannelStop),
            }
        }
    }

    /// Receive a message with a `timeout`.
    #[allow(clippy::expect_used, clippy::unwrap_in_result)]
    pub(crate) fn recv_timeout(&mut self, mut timeout: Duration) -> Result<KM, RecvError> {
        let mut start = SystemTime::now();
        loop {
            match self.get_msg_or_listener() {
                MessageOrListener::Message(msg) => return Ok(msg),
                MessageOrListener::Listen(l) => {
                    if !l.wait_timeout(timeout) {
                        return Err(RecvError::Timeout);
                    }

                    // SystemTime::elapsed should always succeed in this case
                    let elapsed = start
                        .elapsed()
                        .expect("SystemTime::elapsed should always succeed in this case, maybe system time changed?");
                    if elapsed > timeout {
                        return Err(RecvError::Timeout);
                    }
                    start = SystemTime::now();
                    timeout -= elapsed;
                }
                MessageOrListener::Stop => return Err(RecvError::ChannelStop),
            }
        }
    }

    /// Try to receive a message
    pub(crate) fn try_recv(&mut self) -> Result<KM, RecvError> {
        match self.get_msg_or_listener() {
            MessageOrListener::Message(msg) => Ok(msg),
            MessageOrListener::Listen(_) => Err(RecvError::NoAvailable),
            MessageOrListener::Stop => Err(RecvError::ChannelStop),
        }
    }

    /// Receive a message async.
    pub(crate) async fn async_recv(&mut self) -> Result<KM, RecvError> {
        loop {
            match self.get_msg_or_listener() {
                MessageOrListener::Message(msg) => return Ok(msg),
                MessageOrListener::Listen(l) => l.await,
                MessageOrListener::Stop => return Err(RecvError::ChannelStop),
            }
        }
    }
}

impl<KM> Drop for KeybasedReceiverInner<KM> {
    fn drop(&mut self) {
        self.channel.lock().mark_stop();
    }
}
