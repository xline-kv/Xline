use std::cmp::Eq;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clippy_utilities::OverflowArithmetic;
use event_listener::{Event, EventListener};
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;

use crate::cmd::ConflictCheck;

/// The inner type for Keys and Messages combined structure
struct KeysMessageInner<K, M> {
    /// The keys
    keys: Vec<K>,
    /// The message
    message: M,
}

impl<K, M> KeysMessageInner<K, M> {
    /// Create a new `KeysMessageInner` structure
    fn new(keys: Vec<K>, message: M) -> Self {
        Self { keys, message }
    }
}

/// Keys and Messages combined structure
pub(crate) struct KeysMessage<K, M> {
    /// Inner
    inner: Arc<KeysMessageInner<K, M>>,
}

impl<K, M> Hash for KeysMessage<K, M> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::<KeysMessageInner<K, M>>::as_ptr(&self.inner).hash(state);
    }
}

impl<K, M> PartialEq for KeysMessage<K, M> {
    fn eq(&self, other: &Self) -> bool {
        Arc::<KeysMessageInner<K, M>>::as_ptr(&self.inner)
            == Arc::<KeysMessageInner<K, M>>::as_ptr(&other.inner)
    }
}

impl<K, M> Eq for KeysMessage<K, M> {}

impl<K, M> Clone for KeysMessage<K, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::<_>::clone(&self.inner),
        }
    }
}

#[allow(unsafe_code)]
unsafe impl<K: Send, M: Send> Send for KeysMessage<K, M> {}

impl<K, M> KeysMessage<K, M> {
    /// Create a new `KeysMessage`
    fn new(keys: Vec<K>, message: M) -> Self {
        Self {
            inner: Arc::new(KeysMessageInner::new(keys, message)),
        }
    }

    /// Get keys
    fn keys(&self) -> &[K] {
        self.inner.keys.as_ref()
    }

    /// Get the message
    #[allow(dead_code)]
    fn message(&self) -> &M {
        &self.inner.message
    }
}

/// The `HashSet` for holding the successors
type Successors<K, M> = HashSet<KeysMessage<K, M>>;

/// This is a kind of key-based channel. It receives and sends messages as usual, but the recv end can't receive messages for the same key simutanously.
/// Any message received comes with a notifier, and dropping the notifier breaks the blocking barrier for the key. When the notifier is holded, any message
/// with the same key is blocked.
struct KeybasedChannel<K: Eq + Hash + ConflictCheck, M> {
    /// Proceeders that arrive eailer and have key confliction with this message, we only record count
    proceeder: HashMap<KeysMessage<K, M>, u64>,
    /// Successors that arrive later and have key confliction with this message
    successor: HashMap<KeysMessage<K, M>, Successors<K, M>>,
    /// The real queue the receivers get the message from.
    inner: VecDeque<KeysMessage<K, M>>,
    /// The notifier notifies the new arrived message in the `inner`
    new_msg_event: Event,
    /// The message complete sender. Once a message is complete, the corresponding key is sent via this sender to unblock following same-key messages.
    msg_complete_tx: UnboundedSender<KeysMessage<K, M>>,
}

/// If two key sets conflict with each other
fn keys_conflict<K: ConflictCheck>(ks1: &[K], ks2: &[K]) -> bool {
    ks1.iter()
        .cartesian_product(ks2.iter())
        .any(|(k1, k2)| k1.is_conflict(k2))
}

impl<K: Eq + Hash + Clone + ConflictCheck, M> KeybasedChannel<K, M> {
    /// Insert successors and proceeders info and return if it conflicts with any previous message
    fn insert_conflict(&mut self, new_km: KeysMessage<K, M>) -> bool {
        let proceeder_cnt: u64 = self
            .successor
            .iter_mut()
            .filter_map(|(k, v)| {
                keys_conflict(k.keys(), new_km.keys()).then(|| {
                    let _ignore = v.insert(new_km.clone());
                    1
                })
            })
            .sum();
        let is_conflict = proceeder_cnt != 0;
        let _ignore = self.successor.insert(new_km.clone(), HashSet::new());

        if is_conflict {
            let _ignore2 = self.proceeder.insert(new_km, proceeder_cnt);
        }
        is_conflict
    }

    /// Append a key and message to this `KeybasedChannel`
    fn append(&mut self, keys: &[K], msg: M) {
        let km = KeysMessage::new(keys.to_vec(), msg);
        if !self.insert_conflict(km.clone()) {
            self.inner.push_back(km);
            self.new_msg_event.notify(1);
        }
    }

    /// Move a message for the `key` from pending to inner
    fn mark_complete(&mut self, km: &KeysMessage<K, M>) {
        if let Some(successors) = self.successor.get(km) {
            let notify_cnt: usize = successors
                .iter()
                .map(|s| {
                    let is_ready = if let Some(proceeder) = self.proceeder.get_mut(s) {
                        *proceeder = proceeder.overflow_sub(1);
                        *proceeder == 0
                    } else {
                        false
                    };

                    if is_ready {
                        let _ignore = self.proceeder.remove(s);
                        self.inner.push_back(s.clone());
                        1
                        // self.new_msg_event.notify(1);
                    } else {
                        0
                    }
                })
                .sum();

            if notify_cnt > 0 {
                self.new_msg_event.notify(notify_cnt);
            }
        }

        let _ignore = self.successor.remove(km);
    }

    /// Swap a new queue with the inner queue
    fn swap_inner(&mut self, new_queue: &mut VecDeque<KeysMessage<K, M>>) {
        assert!(new_queue.is_empty());
        std::mem::swap(&mut self.inner, new_queue);
    }
}

#[derive(Clone)]
/// The Sender for the `KeybasedChannel`
pub(crate) struct KeybasedChannelSender<K: Eq + Hash + ConflictCheck, M> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<K, M>>>,
}

impl<K: Eq + Hash + Clone + ConflictCheck, M> KeybasedChannelSender<K, M> {
    /// Send a key and message to the channel
    #[allow(dead_code)]
    pub(crate) fn send(&self, keys: &[K], msg: M) -> UnboundedSender<KeysMessage<K, M>> {
        let mut channel = self.channel.lock();
        channel.append(keys, msg);
        channel.msg_complete_tx.clone()
    }
}

/// The Receiver for the `KeybasedChannel`
pub(crate) struct KeybasedChannelReceiver<K: Eq + Hash + ConflictCheck, M> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<K, M>>>,
    /// Local message buf
    buf: VecDeque<KeysMessage<K, M>>,
    /// The join handler for the task, which handles the message complete event.
    handle: JoinHandle<()>,
}

impl<K: Eq + Hash + ConflictCheck, M> Drop for KeybasedChannelReceiver<K, M> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The return value of `get_msg_or_listener` method
enum MessageOrListener<M> {
    /// The message
    Message(M),
    /// The listener
    Listen(EventListener),
}

impl<K: Eq + Hash + Clone + ConflictCheck, M> KeybasedChannelReceiver<K, M> {
    /// Get a message from the channel or a waiting listener
    #[allow(clippy::expect_used, clippy::unwrap_in_result)]
    fn get_msg_or_listener(&mut self) -> MessageOrListener<KeysMessage<K, M>> {
        if let Some(buf_msg) = self.buf.pop_front() {
            return MessageOrListener::Message(buf_msg);
        }

        let mut channel = self.channel.lock();
        channel.swap_inner(&mut self.buf);
        match self.buf.pop_front() {
            Some(msg) => MessageOrListener::Message(msg),
            None => MessageOrListener::Listen(channel.new_msg_event.listen()),
        }
    }

    /// Receive a message
    /// Return (message, `msg_complete_sender`)
    #[allow(dead_code)]
    pub(crate) fn recv(&mut self) -> KeysMessage<K, M> {
        loop {
            match self.get_msg_or_listener() {
                MessageOrListener::Message(msg) => return msg,
                MessageOrListener::Listen(l) => l.wait(),
            }
        }
    }

    /// Receive a message with a `timeout`.
    /// Return None if `timeout` hits,
    /// otherwise return Some((message, `msg_complete_sender`))
    #[allow(dead_code, clippy::expect_used, clippy::unwrap_in_result)]
    pub(crate) fn recv_timeout(&mut self, mut timeout: Duration) -> Option<KeysMessage<K, M>> {
        let mut start = SystemTime::now();
        loop {
            match self.get_msg_or_listener() {
                MessageOrListener::Message(msg) => return Some(msg),
                MessageOrListener::Listen(l) => {
                    if !l.wait_timeout(timeout) {
                        return None;
                    }

                    // SystemTime::elapsed should always succeed in this case
                    let elapsed = start
                        .elapsed()
                        .expect("SystemTime::elapsed should always succeed in this case");
                    if elapsed > timeout {
                        return None;
                    }
                    start = SystemTime::now();
                    timeout -= elapsed;
                }
            }
        }
    }

    /// Receive a message async.
    #[allow(dead_code)]
    pub(crate) async fn async_recv(&mut self) -> KeysMessage<K, M> {
        loop {
            match self.get_msg_or_listener() {
                MessageOrListener::Message(msg) => return msg,
                MessageOrListener::Listen(l) => l.await,
            }
        }
    }
}

/// Create a `KeybasedQueue`
/// Return (sender, receiver)
#[allow(dead_code)]
pub(crate) fn channel<
    K: Clone + Eq + Hash + Send + Sync + ConflictCheck + 'static,
    M: Send + 'static,
>() -> (KeybasedChannelSender<K, M>, KeybasedChannelReceiver<K, M>) {
    let (tx, mut rx) = unbounded_channel();

    let inner_channel = Arc::new(Mutex::new(KeybasedChannel {
        inner: VecDeque::new(),
        new_msg_event: Event::new(),
        msg_complete_tx: tx,
        proceeder: HashMap::new(),
        successor: HashMap::new(),
    }));

    let channel4complete = Arc::<_>::clone(&inner_channel);
    let handle = tokio::spawn(async move {
        while let Some(key) = rx.recv().await {
            let mut channel = channel4complete.lock();
            channel.mark_complete(&key);
        }
    });

    (
        KeybasedChannelSender {
            channel: Arc::<_>::clone(&inner_channel),
        },
        KeybasedChannelReceiver {
            channel: inner_channel,
            buf: VecDeque::new(),
            handle,
        },
    )
}

#[cfg(test)]
mod test {
    use std::{thread::sleep, time::Duration};

    use crate::keybased_channel::channel;

    #[allow(clippy::unwrap_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_channel() {
        let (tx, mut rx) = channel::<String, String>();

        let complete = tx.send(&["1".to_owned()], "A".to_owned());
        tx.send(&["1".to_owned()], "B".to_owned());
        tx.send(&["2".to_owned()], "C".to_owned());
        let resv_result = rx.recv_timeout(Duration::from_secs(1));
        assert!(resv_result.is_some());
        let msg = resv_result.unwrap();
        assert_eq!(msg.message(), "A");

        let second_msg = rx.recv();
        assert_eq!(second_msg.message(), "C");

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_none());
        assert!(complete.send(msg).is_ok());
        let third_msg = rx.recv();
        assert_eq!(third_msg.message(), "B");
    }

    #[allow(clippy::unwrap_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_channel_async() {
        let (tx, mut rx) = channel::<String, String>();

        let complete = tx.send(&["1".to_owned()], "A".to_owned());
        tx.send(&["1".to_owned()], "B".to_owned());
        let msg = rx.recv();
        assert_eq!(msg.message(), "A");

        let (oneshot_tx, mut oneshot_rx) = tokio::sync::oneshot::channel();
        let _ignore = tokio::spawn(async move {
            let second_msg = rx.async_recv().await;
            assert_eq!(second_msg.message(), "B");

            let _ignore = oneshot_tx.send(1);
        });

        sleep(Duration::from_secs(1));
        assert!(oneshot_rx.try_recv().is_err());
        assert!(complete.send(msg).is_ok());

        let oneshot_result = oneshot_rx.await;
        assert!(oneshot_result.is_ok());
        assert_eq!(oneshot_result.unwrap(), 1);
    }
}
