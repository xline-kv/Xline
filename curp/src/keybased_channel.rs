use std::cmp::Eq;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use event_listener::{Event, EventListener};
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;

use crate::cmd::ConflictCheck;

/// Keys and Messages combined structure
#[derive(Hash, PartialEq, Eq)]
pub(crate) struct KeysMessage<K, M> {
    /// The keys
    keys: Vec<K>,
    /// The message
    message: M,
}

#[allow(unsafe_code)]
unsafe impl<K: Send, M: Send> Send for KeysMessage<K, M> {}

impl<K, M> KeysMessage<K, M> {
    /// Create a new `KeysMessage`
    fn new(keys: Vec<K>, message: M) -> Self {
        Self { keys, message }
    }

    /// Get keys
    fn keys(&self) -> &[K] {
        self.keys.as_ref()
    }

    /// Get the message
    #[allow(dead_code)]
    fn message(&self) -> &M {
        &self.message
    }
}

/// The `HashSet` for holding the proceeders
type Proceeders<K, M> = HashSet<Arc<KeysMessage<K, M>>>;
/// The `HashSet` for holding the successors
type Successors<K, M> = HashSet<Arc<KeysMessage<K, M>>>;

/// This is a kind of key-based channel. It receives and sends messages as usual, but the recv end can't receive messages for the same key simutanously.
/// Any message received comes with a notifier, and dropping the notifier breaks the blocking barrier for the key. When the notifier is holded, any message
/// with the same key is blocked.
struct KeybasedChannel<K: Eq + Hash + ConflictCheck, M: Eq + Hash> {
    /// Proceeders that arrive eailer and have key confliction with this message
    proceeder: HashMap<Arc<KeysMessage<K, M>>, Proceeders<K, M>>,
    /// Successors that arrive later and have key confliction with this message
    successor: HashMap<Arc<KeysMessage<K, M>>, Successors<K, M>>,
    /// The real queue the receivers get the message from.
    inner: VecDeque<Arc<KeysMessage<K, M>>>,
    /// The notifier notifies the new arrived message in the `inner`
    new_msg_event: Event,
    /// The message complete sender. Once a message is complete, the corresponding key is sent via this sender to unblock following same-key messages.
    msg_complete_tx: UnboundedSender<Arc<KeysMessage<K, M>>>,
}

/// If two key sets conflict with each other
fn keys_conflict<K: ConflictCheck>(ks1: &[K], ks2: &[K]) -> bool {
    ks1.iter()
        .cartesian_product(ks2.iter())
        .any(|(k1, k2)| k1.is_conflict(k2))
}

impl<K: Eq + Hash + Clone + ConflictCheck, M: Eq + Hash> KeybasedChannel<K, M> {
    /// Insert successors and proceeders info and return if it conflicts with any previous message
    fn insert_conflict(&mut self, new_km: Arc<KeysMessage<K, M>>) -> bool {
        let proceders: HashSet<_> = self
            .successor
            .iter_mut()
            .filter_map(|(k, v)| {
                keys_conflict(k.keys(), new_km.keys()).then(|| {
                    let _ignore = v.insert(Arc::<_>::clone(&new_km));
                    Arc::<_>::clone(k)
                })
            })
            .collect();
        let is_conflict = !proceders.is_empty();
        let _ignore = self
            .successor
            .insert(Arc::<_>::clone(&new_km), HashSet::new());

        if is_conflict {
            let _ignore2 = self.proceeder.insert(new_km, proceders);
        }
        is_conflict
    }

    /// Append a key and message to this `KeybasedChannel`
    fn append(&mut self, keys: &[K], msg: M) {
        let km = Arc::new(KeysMessage::new(keys.to_vec(), msg));
        if !self.insert_conflict(Arc::<_>::clone(&km)) {
            self.inner.push_back(km);
            self.new_msg_event.notify(1);
        }
    }

    /// Move a message for the `key` from pending to inner
    fn mark_complete(&mut self, km: &Arc<KeysMessage<K, M>>) {
        if let Some(successors) = self.successor.get(km) {
            for s in successors.iter() {
                let is_ready = if let Some(proceeder) = self.proceeder.get_mut(s) {
                    let _ignore = proceeder.remove(km);
                    proceeder.is_empty()
                } else {
                    false
                };

                if is_ready {
                    let _ignore = self.proceeder.remove(s);
                    self.inner.push_back(Arc::<_>::clone(s));
                    self.new_msg_event.notify(1);
                }
            }
        }

        let _ignore = self.successor.remove(km);
    }

    /// Try to receive a message from this Queue.
    /// Return `None` if there's nothing in the `inner`, otherwise
    /// return `Some(message)`
    fn try_recv(&mut self) -> Option<Arc<KeysMessage<K, M>>> {
        self.inner.pop_front()
    }
}

/// The Sender for the `KeybasedChannel`
pub(crate) struct KeybasedChannelSender<K: Eq + Hash + ConflictCheck, M: Eq + Hash> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<K, M>>>,
}

impl<K: Eq + Hash + Clone + ConflictCheck, M: Eq + Hash> KeybasedChannelSender<K, M> {
    /// Send a key and message to the channel
    #[allow(dead_code)]
    pub(crate) fn send(&self, keys: &[K], msg: M) -> UnboundedSender<Arc<KeysMessage<K, M>>> {
        let mut channel = self.channel.lock();
        channel.append(keys, msg);
        channel.msg_complete_tx.clone()
    }
}

/// The Receiver for the `KeybasedChannel`
pub(crate) struct KeybasedChannelReceiver<K: Eq + Hash + ConflictCheck, M: Eq + Hash> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<K, M>>>,
    /// The join handler for the task, which handles the message complete event.
    handle: JoinHandle<()>,
}

impl<K: Eq + Hash + ConflictCheck, M: Eq + Hash> Drop for KeybasedChannelReceiver<K, M> {
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

impl<K: Eq + Hash + Clone + ConflictCheck, M: Eq + Hash> KeybasedChannelReceiver<K, M> {
    /// Get a message from the channel or a waiting listener
    #[allow(clippy::expect_used, clippy::unwrap_in_result)]
    fn get_msg_or_listener(&self) -> MessageOrListener<Arc<KeysMessage<K, M>>> {
        let mut channel = self.channel.lock();
        let mut local_listener: Option<EventListener> = None;
        loop {
            match channel.try_recv() {
                Some(msg) => return MessageOrListener::Message(msg),
                #[allow(clippy::match_ref_pats)] // don't move listener out
                None => match &local_listener {
                    &Some(_) => {
                        break;
                    }
                    &None => local_listener = Some(channel.new_msg_event.listen()),
                },
            }
        }
        MessageOrListener::Listen(local_listener.expect("local_listener should not be none"))
    }

    /// Receive a message
    /// Return (message, `msg_complete_sender`)
    #[allow(dead_code)]
    pub(crate) fn recv(&self) -> Arc<KeysMessage<K, M>> {
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
    pub(crate) fn recv_timeout(&self, mut timeout: Duration) -> Option<Arc<KeysMessage<K, M>>> {
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
    pub(crate) async fn async_recv(&self) -> Arc<KeysMessage<K, M>> {
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
    M: Eq + Hash + Send + Sync + 'static,
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
        let (tx, rx) = channel::<String, String>();

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
        let (tx, rx) = channel::<String, String>();

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
