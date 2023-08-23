use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use event_listener::Event;
use parking_lot::RwLock;
use tokio::sync::mpsc::Sender;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tower::discover::Change;
use tracing::debug;

use crate::rpc::proto::protocol_client::ProtocolClient;

/// Mocked Channel
#[derive(Clone, Debug)]
pub(crate) struct MockedChannel {
    /// Real channel
    channels: Arc<RwLock<HashMap<String, Channel>>>,
    /// Emit event when channel changed
    event: Arc<Event>,
}

impl MockedChannel {
    /// Mock `balance_channel` method
    pub(crate) fn balance_channel(capacity: usize) -> (Self, Sender<Change<String, Endpoint>>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Change<String, Endpoint>>(capacity);
        let channels = Arc::new(RwLock::new(HashMap::new()));
        let channels_clone = Arc::clone(&channels);
        let event = Arc::new(Event::new());
        let event_clone = Arc::clone(&event);
        let _ig = madsim::task::spawn(async move {
            while let Some(change) = rx.recv().await {
                match change {
                    Change::Insert(key, endpoint) => {
                        // we do need to get a channel here
                        let channel = loop {
                            if let Ok(chan) = endpoint.connect().await {
                                break chan;
                            } else {
                                madsim::time::sleep(Duration::from_millis(100)).await;
                            }
                        };
                        debug!("successfully established connect to {endpoint:?}");
                        let _ig = channels_clone.write().insert(key, channel);
                        event_clone.notify(usize::MAX);
                    }
                    Change::Remove(key) => {
                        let _ig = channels_clone.write().remove(&key);
                    }
                }
            }
        });
        (Self { channels, event }, tx)
    }
}

/// Mocked Protocol Client
#[derive(Debug, Clone)]
pub(crate) struct MockedProtocolClient<T> {
    /// The index of real channel to achieve round-robin mechanism
    idx: Arc<AtomicI32>,
    /// Emit event when channel changed
    event: Arc<Event>,
    /// Mocked channel
    channel: MockedChannel,
    /// Current real protocol client, used to attach the lifetime with MockedProtocolClient
    current: Option<ProtocolClient<Channel>>,
    /// PhantomData
    _fake: PhantomData<T>,
}

impl<T> MockedProtocolClient<T> {
    /// Mock `new` method
    pub(crate) fn new(channel: MockedChannel) -> Self {
        Self {
            idx: Arc::new(AtomicI32::new(0)),
            event: Arc::clone(&channel.event),
            channel,
            current: None,
            _fake: PhantomData,
        }
    }
}

// It is verbose to implement all method in `ProtocolClient` for `MockedProtocolClient`
// So just implement DerefMut here.
// Since `ProtocolClient` only invokes methods on its `&mut self` receiver, it is sufficient to implement DerefMut.
// Indeed, apart from employing techniques like `Box::leak`, implementing Deref is simply unreachable. That is because
// we need to attach the lifetime of `ProtocolClient<Channel>`(generated within the deref call) to MockedProtocolClient,
// which is impossible behind an immutable reference.
impl<T> Deref for MockedProtocolClient<T> {
    type Target = ProtocolClient<Channel>;

    fn deref(&self) -> &Self::Target {
        unreachable!("only deref_mut is allowed")
    }
}

impl<T> DerefMut for MockedProtocolClient<T> {
    #[allow(clippy::as_conversions)] // safe to convert
    #[allow(clippy::cast_possible_truncation)] // length of channel is not too big
    #[allow(clippy::cast_possible_wrap)] // length of channel is not too big
    fn deref_mut(&mut self) -> &mut Self::Target {
        loop {
            let channels = self.channel.channels.read();
            let len = channels.len();
            if len == 0 {
                drop(channels); // release read lock
                self.event.listen().wait();
                continue;
            }
            let idx = self.idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let idx = idx.rem_euclid(len as i32) as usize;
            let channel = channels
                .values()
                .nth(idx)
                .unwrap_or_else(|| unreachable!("unexpected channels idx: {}, len: {}", idx, len));
            return self.current.insert(ProtocolClient::new(channel.clone()));
        }
    }
}
