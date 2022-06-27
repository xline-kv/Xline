use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use event_listener::{Event, EventListener};
use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;

/// This is a kind of key-based channel. It receives and sends messages as usual, but the recv end can't receive messages for the same key simutanously.
/// Any message received comes with a notifier, and dropping the notifier breaks the blocking barrier for the key. When the notifier is holded, any message
/// with the same key is blocked.
struct KeybasedChannel<K: Eq + Hash, M> {
    /// The message waiting pool, meaning the same key message is the inner or has not been completed.
    pending: HashMap<K, VecDeque<M>>,
    /// The real queue the receivers get the message from.
    inner: VecDeque<(K, M)>,
    /// The keys in the `pending` or the `inner`
    keys: HashSet<K>,
    /// The notifier notifies the new arrived message in the `inner`
    new_msg_event: Event,
    /// The message complete sender. Once a message is complete, the corresponding key is send via this sender to unblock following same-key messages.
    msg_complete_tx: UnboundedSender<K>,
}

impl<K: Eq + Hash + Clone, M> KeybasedChannel<K, M> {
    /// Insert a key and message into the pending pool
    fn insert_pending(&mut self, key: K, msg: M) {
        let entry = self.pending.entry(key).or_insert_with(|| VecDeque::new());
        entry.push_back(msg);
    }

    /// Insert a key and message to the inner queue
    fn insert_inner(&mut self, key: K, msg: M) {
        self.inner.push_back((key, msg));
    }

    /// Append a key and message to this `KeybasedChannel`
    fn append(&mut self, key: K, msg: M) {
        if self.keys.contains(&key) {
            self.insert_pending(key, msg);
        } else {
            let _old = self.keys.insert(key.clone());
            self.insert_inner(key, msg);
            self.new_msg_event.notify(1);
        }
    }

    /// Move a message for the `key` from pending to inner
    fn move_pending_to_inner(&mut self, key: &K) {
        let msg = self
            .pending
            .get_mut(key)
            .map(|msgs| (msgs.pop_front(), msgs.is_empty()));

        if let Some((option_m, need_clean)) = msg {
            if let Some(m) = option_m {
                self.insert_inner(key.clone(), m);
                self.new_msg_event.notify(1);
            }

            if need_clean {
                // remove the key from the pending if the key's queue is empty
                let _empty_queue = self.pending.remove(key);
            }
        }
    }

    /// Try to receive a message from this Queue.
    /// Return `None` if there's nothing in the `inner`, otherwise
    /// return `Some(message)`
    fn try_recv(&mut self) -> Option<M> {
        self.inner.pop_front().map(|(key, msg)| {
            if !self.pending.contains_key(&key) {
                let _key = self.keys.remove(&key);
            }
            msg
        })
    }
}

/// The Sender for the `KeybasedChannel`
pub(crate) struct KeybasedChannelSender<K: Eq + Hash, M> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<K, M>>>,
}

impl<K: Eq + Hash + Clone, M> KeybasedChannelSender<K, M> {
    /// Send a key and message to the channel
    #[allow(dead_code)]
    pub(crate) fn send(&self, key: K, msg: M) -> UnboundedSender<K> {
        let mut channel = self.channel.lock();
        channel.append(key, msg);
        channel.msg_complete_tx.clone()
    }
}

/// The Receiver for the `KeybasedChannel`
pub(crate) struct KeybasedChannelReceiver<K: Eq + Hash, M> {
    /// The channel
    channel: Arc<Mutex<KeybasedChannel<K, M>>>,
    /// The join handler for the task, which handles the message complete event.
    handle: JoinHandle<()>,
}

impl<K: Eq + Hash, M> Drop for KeybasedChannelReceiver<K, M> {
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

impl<K: Eq + Hash + Clone, M> KeybasedChannelReceiver<K, M> {
    /// Get a message from the channel or a waiting listener
    #[allow(clippy::expect_used, clippy::unwrap_in_result)]
    fn get_msg_or_listener(&self) -> MessageOrListener<M> {
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
    pub(crate) fn recv(&self) -> M {
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
    pub(crate) fn recv_timeout(&self, mut timeout: Duration) -> Option<M> {
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
    pub(crate) async fn async_recv(&self) -> M {
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
pub(crate) fn channel<K: Clone + Eq + Hash + Send + 'static, M: Send + 'static>(
) -> (KeybasedChannelSender<K, M>, KeybasedChannelReceiver<K, M>) {
    let (tx, mut rx) = unbounded_channel();

    let inner_channel = Arc::new(Mutex::new(KeybasedChannel {
        pending: HashMap::new(),
        inner: VecDeque::new(),
        keys: HashSet::new(),
        new_msg_event: Event::new(),
        msg_complete_tx: tx,
    }));

    let channel4complete = Arc::<_>::clone(&inner_channel);
    let handle = tokio::spawn(async move {
        while let Some(key) = rx.recv().await {
            let mut channel = channel4complete.lock();
            channel.move_pending_to_inner(&key);
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

        let complete = tx.send("1".to_owned(), "A".to_owned());
        tx.send("1".to_owned(), "B".to_owned());
        tx.send("2".to_owned(), "C".to_owned());
        let resv_result = rx.recv_timeout(Duration::from_secs(1));
        assert!(resv_result.is_some());
        let msg = resv_result.unwrap();
        assert_eq!(msg, "A");

        let second_msg = rx.recv();
        assert_eq!(second_msg, "C");

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_none());
        assert!(complete.send("1".to_owned()).is_ok());
        let third_msg = rx.recv();
        assert_eq!(third_msg, "B".to_owned());
    }

    #[allow(clippy::unwrap_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_channel_async() {
        let (tx, rx) = channel::<String, String>();

        let complete = tx.send("1".to_owned(), "A".to_owned());
        tx.send("1".to_owned(), "B".to_owned());
        let msg = rx.recv();
        assert_eq!(msg, "A");

        let (oneshot_tx, mut oneshot_rx) = tokio::sync::oneshot::channel();
        let _ignore = tokio::spawn(async move {
            let second_msg = rx.async_recv().await;
            assert_eq!(second_msg, "B".to_owned());

            let _ignore = oneshot_tx.send(1);
        });

        sleep(Duration::from_secs(1));
        assert!(oneshot_rx.try_recv().is_err());
        assert!(complete.send("1".to_owned()).is_ok());

        let oneshot_result = oneshot_rx.await;
        assert!(oneshot_result.is_ok());
        assert_eq!(oneshot_result.unwrap(), 1);
    }
}
