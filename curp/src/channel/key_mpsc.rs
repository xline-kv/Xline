//! This is a multi-producer and single-consumer channel.
//!
//! The channel has the following features:
//! 1. The message coupled with keys, any two message with conflicting keys are conflicted.
//! 2. Any message send to the channel is control by a ready token returned by the sender API.
//! 3. Unready message is not received by the receiver and it blocks all the following conflict messages

use std::{
    cmp::Eq,
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    sync::Arc,
    time::Duration,
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use event_listener::Event;
use parking_lot::Mutex;

use crate::cmd::ConflictCheck;

use super::{
    hash_eq, KeyBasedChannel, KeyBasedReceiverInner, KeyBasedSenderInner, KeysMessageInner,
    RecvError, SendError,
};

/// Keys and Messages combined structure
pub(crate) struct MpscKeysMessage<K, M> {
    /// Inner
    inner: Arc<KeysMessageInner<K, M>>,
    /// If the message is ready
    ready: Arc<Mutex<bool>>,
}

hash_eq!(MpscKeysMessage);

impl<K, M> Clone for MpscKeysMessage<K, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::<_>::clone(&self.inner),
            ready: Arc::<_>::clone(&self.ready),
        }
    }
}

#[allow(unsafe_code)]
unsafe impl<K: Send, M: Send> Send for MpscKeysMessage<K, M> {}

impl<K, M> MpscKeysMessage<K, M> {
    /// Create a new `KeysMessage`
    fn new(keys: Vec<K>, message: M) -> Self {
        Self {
            inner: Arc::new(KeysMessageInner::new(keys, message)),
            ready: Arc::new(Mutex::new(false)),
        }
    }

    /// modify data in the message if necessary
    pub(crate) fn keys(&self) -> &[K] {
        self.inner.keys()
    }

    /// map the inner message to a closure
    pub(crate) fn map_msg<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut M) -> R,
    {
        self.inner.map_msg(f)
    }

    /// Tell if the message is ready
    pub(crate) fn ready(&self) -> bool {
        *self.ready.lock()
    }

    /// Mark the message as ready
    pub(crate) fn mark_ready(&self) {
        *self.ready.lock() = true;
    }
}

impl<K: Eq + Hash + Clone + ConflictCheck, M> KeyBasedChannel<MpscKeysMessage<K, M>> {
    /// Insert successors and predecessors info
    fn insert_graph(&mut self, new_km: MpscKeysMessage<K, M>) {
        let predecessor_cnt = self
            .successor
            .iter_mut()
            .filter_map(|(k, v)| {
                super::keys_conflict(k.keys(), new_km.keys()).then(|| {
                    let _ignore = v.insert(new_km.clone());
                })
            })
            .count()
            .numeric_cast();
        // the message can only be inserted once, so we ignore the return value
        let _ignore = self.successor.insert(new_km.clone(), HashSet::new());

        if predecessor_cnt != 0 {
            // the message can only be inserted once, so we ignore the return value
            let _ignore2 = self.predecessor.insert(new_km, predecessor_cnt);
        }
    }

    /// Append a key and message to this `KeyBasedChannel`
    fn append(&mut self, keys: &[K], msg: M) -> MpscKeysMessage<K, M> {
        let km = MpscKeysMessage::new(keys.to_vec(), msg);
        self.insert_graph(km.clone());
        km
    }

    /// Move a message for the `key` from pending to inner recursively
    fn mark_ready(&mut self, km: &MpscKeysMessage<K, M>) {
        km.mark_ready();
        let mut ready_pool = VecDeque::new();
        let mut ready_cnt = 0;
        if self.predecessor.get(km).is_none() {
            ready_pool.push_back(km.clone());
            ready_cnt = 1;
        }

        while let Some(cur) = ready_pool.pop_front() {
            if let Some(successor) = self.successor.remove(&cur) {
                for s in successor {
                    let (no_predecessor, has_pre) = if let Some(s_p) = self.predecessor.get_mut(&s)
                    {
                        *s_p = s_p.overflow_sub(1);
                        (*s_p == 0, true)
                    } else {
                        unreachable!("Any successor should have a predecessor");
                    };

                    if no_predecessor && has_pre {
                        let _ignore_removed = self.predecessor.remove(&s);
                    }

                    if no_predecessor && s.ready() {
                        ready_pool.push_back(s);
                        ready_cnt = ready_cnt.overflow_add(1);
                    }
                }
            }
            self.inner.push_back(cur);
        }

        if ready_cnt > 0 {
            self.new_msg_event.notify(ready_cnt);
        }
    }
}

/// The sender inner type
type MpscKeyBasedSenderInner<K, M> = KeyBasedSenderInner<MpscKeysMessage<K, M>>;

#[derive(Clone)]
/// The Sender for the `KeyBasedChannel`
pub(crate) struct MpscKeyBasedSender<K, M> {
    /// The channel
    inner: Arc<MpscKeyBasedSenderInner<K, M>>,
}

impl<K: Eq + Hash + Clone + ConflictCheck + Send + 'static, M: Send + 'static>
    MpscKeyBasedSender<K, M>
{
    /// Create a new sender
    fn new(channel: Arc<Mutex<KeyBasedChannel<MpscKeysMessage<K, M>>>>) -> Self {
        Self {
            inner: Arc::new(MpscKeyBasedSenderInner { channel }),
        }
    }

    /// Send a key and message to the channel.
    /// If the channel closed, returns `Err(SendError::ChannelStop)`; otherwise returns a `Ok(Event)`
    pub(crate) fn send(&self, keys: &[K], msg: M) -> Result<Event, SendError> {
        let mut channel = self.inner.channel.lock();
        if channel.is_working {
            let km = channel.append(keys, msg);
            let notify = Event::new();
            let waiter = notify.listen();
            let channel_clone = Arc::clone(&self.inner.channel);
            let _ignore_handler = tokio::spawn(async move {
                waiter.await;
                channel_clone.lock().mark_ready(&km);
            });
            Ok(notify)
        } else {
            Err(SendError::ChannelStop)
        }
    }
}

/// The Receiver for the `KeyBasedChannel`
pub(crate) struct MpscKeyBasedReceiver<K, M> {
    /// The inner receiver
    inner: KeyBasedReceiverInner<MpscKeysMessage<K, M>>,
}

impl<K: Eq + Hash + Clone + ConflictCheck, M> MpscKeyBasedReceiver<K, M> {
    /// Receive a message
    #[allow(dead_code)]
    pub(crate) fn recv(&mut self) -> Result<MpscKeysMessage<K, M>, RecvError> {
        self.inner.recv()
    }

    /// Receive a message with a `timeout`.
    #[allow(dead_code)]
    pub(crate) fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<MpscKeysMessage<K, M>, RecvError> {
        self.inner.recv_timeout(timeout)
    }

    #[allow(dead_code)]
    /// Receive a message async.
    pub(crate) async fn async_recv(&mut self) -> Result<MpscKeysMessage<K, M>, RecvError> {
        self.inner.async_recv().await
    }

    /// Try to receive a message
    pub(crate) fn try_recv(&mut self) -> Result<MpscKeysMessage<K, M>, RecvError> {
        self.inner.try_recv()
    }
}

/// Create a `KeyBasedQueue`
/// Return (sender, receiver)
pub(crate) fn channel<
    K: Clone + Eq + Hash + Send + Sync + ConflictCheck + 'static,
    M: Send + 'static,
>() -> (MpscKeyBasedSender<K, M>, MpscKeyBasedReceiver<K, M>) {
    let inner_channel = Arc::new(Mutex::new(KeyBasedChannel {
        inner: VecDeque::new(),
        new_msg_event: Event::new(),
        predecessor: HashMap::new(),
        successor: HashMap::new(),
        is_working: true,
    }));

    (
        MpscKeyBasedSender::new(Arc::<_>::clone(&inner_channel)),
        MpscKeyBasedReceiver {
            inner: KeyBasedReceiverInner {
                channel: inner_channel,
                buf: VecDeque::new(),
            },
        },
    )
}

#[cfg(test)]
mod test {
    use std::{thread::sleep, time::Duration};

    use super::super::{RecvError, SendError};
    use super::channel;

    #[allow(clippy::expect_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_channel_in_order() {
        let (tx, mut rx) = channel::<String, String>();

        let ready1 = tx
            .send(&["1".to_owned()], "A".to_owned())
            .expect("First message should send success");
        let ready2 = tx
            .send(&["1".to_owned()], "B".to_owned())
            .expect("Second message should send success");
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)),
            Err(RecvError::Timeout)
        ));

        ready1.notify(1);
        let first_msg = rx.recv().expect("First message should recv success");
        first_msg.map_msg(|msg| {
            assert_eq!(*msg, "A");
        });

        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)),
            Err(RecvError::Timeout)
        ));
        ready2.notify(1);
        let second_msg = rx.recv().expect("Second message should recv success");
        second_msg.map_msg(|msg| {
            assert_eq!(*msg, "B");
        });
    }

    #[allow(clippy::expect_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_channel_out_of_order() {
        let (tx, mut rx) = channel::<String, String>();

        let ready1 = tx
            .send(&["1".to_owned()], "A".to_owned())
            .expect("First message should send success");
        let ready2 = tx
            .send(&["1".to_owned()], "B".to_owned())
            .expect("Second message should send success");
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)),
            Err(RecvError::Timeout)
        ));

        ready2.notify(1);
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)),
            Err(RecvError::Timeout)
        ));

        ready1.notify(1);
        let first_msg = rx.recv().expect("First message should recv success");
        first_msg.map_msg(|msg| {
            assert_eq!(*msg, "A");
        });
        let second_msg = rx.recv().expect("Second message should recv success");
        second_msg.map_msg(|msg| {
            assert_eq!(*msg, "B");
        });
    }

    #[test]
    fn test_recv_close_channel() {
        let (tx, rx) = channel::<String, String>();

        drop(rx);

        assert!(matches!(
            tx.send(&["1".to_owned()], "A".to_owned()),
            Err(SendError::ChannelStop)
        ));
    }

    #[test]
    fn test_send_close_channel() {
        let (tx, mut rx) = channel::<String, String>();

        drop(tx);

        assert!(matches!(rx.recv(), Err(RecvError::ChannelStop)));
    }

    #[allow(clippy::expect_used, clippy::unwrap_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_channel_async() {
        let (tx, mut rx) = channel::<String, String>();

        let ready1 = tx.send(&["1".to_owned()], "A".to_owned()).expect("Async");

        let (oneshot_tx, mut oneshot_rx) = tokio::sync::oneshot::channel();
        let _ignore = tokio::spawn(async move {
            let msg = rx.async_recv().await.expect("Async recv should success");
            msg.map_msg(|m| {
                assert_eq!(*m, "A");
            });
            let _ignore = oneshot_tx.send(1);
        });

        sleep(Duration::from_secs(1));
        assert!(oneshot_rx.try_recv().is_err());
        ready1.notify(1);

        let oneshot_result = oneshot_rx.await;
        assert!(oneshot_result.is_ok());
        assert_eq!(oneshot_result.unwrap(), 1);
    }

    #[allow(clippy::expect_used, clippy::unwrap_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_1000_send() {
        let (tx, mut rx) = channel::<String, String>();

        let _tx_hold = tx.clone();
        let handle = tokio::spawn(async move {
            let mut readys = vec![];
            for i in 0..1000 {
                let r = tx
                    .send(&["1".to_owned()], format!("1_{}", i))
                    .expect("Async send should success");
                readys.push(r);
            }

            readys
        });
        let mut readys = handle.await.expect("task1 should success");
        readys.reverse();
        for r in readys {
            r.notify(1);
        }

        let mut recv_cnt = 0;
        while recv_cnt < 1000 {
            assert!(
                rx.recv_timeout(Duration::from_secs(10)).is_ok(),
                "cannot recv value within timeout limit"
            );
            recv_cnt += 1;
        }
    }

    #[allow(clippy::expect_used, clippy::unwrap_used, unused_results)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_parallel() {
        let (tx, mut rx) = channel::<String, String>();

        let tx1 = tx.clone();
        let handle1 = tokio::spawn(async move {
            let mut readys = vec![];
            for i in 0..1000 {
                let r = tx1
                    .send(&["1".to_owned()], format!("1_{}", i))
                    .expect("Async send should success");
                readys.push(r);
            }

            readys
        });

        let tx2 = tx.clone();
        let handle2 = tokio::spawn(async move {
            let mut readys = vec![];
            for i in 0..1000 {
                let r = tx2
                    .send(&["1".to_owned()], format!("2_{}", i))
                    .expect("Async send should success");
                readys.push(r);
            }

            readys
        });

        for r in handle1.await.expect("task1 should success") {
            r.notify(1);
        }
        for r in handle2.await.expect("task2 should success") {
            r.notify(1);
        }

        let mut recv_cnt = 0;
        while recv_cnt < 2000 {
            assert!(
                rx.recv_timeout(Duration::from_secs(10)).is_ok(),
                "cannot recv value within timeout limit"
            );
            recv_cnt += 1;
        }
    }
}
