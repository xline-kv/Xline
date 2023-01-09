use std::collections::{HashMap, HashSet};

use clippy_utilities::NumericCast;
use tracing::{error, info};

use crate::cmd::ConflictCheck;

/// Call `DoneNotifier::done` when the process of the msg has finished
pub(crate) struct DoneNotifier {
    /// Notifier
    notifier: flume::Sender<u64>,
    /// The id of the msg
    id: u64,
}

impl DoneNotifier {
    /// Will mark the msg done
    pub(crate) fn notify(self) -> Result<(), flume::SendError<u64>> {
        self.notifier.send(self.id)
    }
}

/// The msg need to have token that implements `ConflictCheck`
pub(crate) trait ConflictCheckedMsg: Send + 'static {
    /// The token that implements `ConflictCheck`
    type Token: ConflictCheck + Send + Sync;
    /// Get a reference to the token
    fn token(&self) -> Self::Token;
}

/// The filter will block any msg if its predecessors(msgs that arrive earlier and conflict with it) haven't finished process
struct Filter<M: ConflictCheckedMsg> {
    /// Message buffer
    buffer: HashMap<u64, M>,
    /// Buffered tokens
    // Why do we need `buffer_keys` here: the `KeyBasedMessage` in `buffer` will be handed out the user, but we still need the keys in the msg to maintain our graph until the msg is marked done by the user
    buffer_tokens: HashMap<u64, M::Token>,
    /// Successors that arrive later with keys that conflict with this message
    successors: HashMap<u64, HashSet<u64>>,
    /// Predecessors that arrive earlier than this message
    predecessors_cnt: HashMap<u64, u64>,
    /// Ready tx, will send ready values (msg, done_notifier) to the user
    filter_tx: flume::Sender<(M, DoneNotifier)>,
    /// Will be handed out to the users for them to mark msgs done
    done_tx: flume::Sender<u64>,
    /// Id assign
    cur_id: u64,
}

impl<M: ConflictCheckedMsg> Filter<M> {
    /// Create a new filter that checks conflict in between msgs
    fn new(filter_tx: flume::Sender<(M, DoneNotifier)>, done_tx: flume::Sender<u64>) -> Self {
        Self {
            buffer: HashMap::new(),
            buffer_tokens: HashMap::new(),
            successors: HashMap::new(),
            predecessors_cnt: HashMap::new(),
            filter_tx,
            done_tx,
            cur_id: 0,
        }
    }

    /// Insert a new msg into the filter
    fn insert(&mut self, msg: M) {
        let id = self.cur_id;
        self.cur_id = self.cur_id.wrapping_add(1);

        // insert the msg into dependency graph
        let predecessor_cnt = self
            .successors
            .iter_mut()
            .filter_map(|(predecessor_id, successors)| {
                #[allow(clippy::expect_used)]
                // buffer keys should have same ids with successors because they are inserted and removed together
                let pre = self
                    .buffer_tokens
                    .get(predecessor_id)
                    .expect("no such predecessor in buffered keys");
                msg.token().is_conflict(pre).then(|| {
                    assert!(successors.insert(id), "should insert only once");
                })
            })
            .count()
            .numeric_cast();

        assert!(
            self.predecessors_cnt.insert(id, predecessor_cnt).is_none(),
            "a msg can't be inserted twice"
        );
        assert!(
            self.buffer_tokens.insert(id, msg.token()).is_none(),
            "a msg can't be inserted twice"
        );
        assert!(
            self.successors.insert(id, HashSet::new()).is_none(),
            "a msg can't be inserted twice"
        );

        if predecessor_cnt > 0 {
            // insert the msg into the buffer if it has conflict with others
            assert!(self.buffer.insert(id, msg).is_none(), "previously inserted");
        } else {
            self.hand_out(id, msg);
        }
    }

    /// Mark a msg done, will release blocked msgs
    fn mark_done(&mut self, id: u64) {
        #[allow(clippy::unwrap_used)] // must be in it
        let successors = self.successors.remove(&id).unwrap();

        for successor_id in successors {
            #[allow(clippy::integer_arithmetic)]
            // if predecessor_cnt equals 0, it should have been moved out before
            let predecessor_cnt = {
                #[allow(clippy::unwrap_used)] // must be there
                let predecessor_cnt = self.predecessors_cnt.get_mut(&successor_id).unwrap();
                *predecessor_cnt -= 1;
                *predecessor_cnt
            };
            // have no conflict anymore, ready to be sent to the user
            if predecessor_cnt == 0 {
                #[allow(clippy::unwrap_used)] // must be in buffer
                let msg = self.buffer.remove(&successor_id).unwrap();
                self.hand_out(successor_id, msg);
            }
        }

        debug_assert!(
            !self.buffer.contains_key(&id),
            "msg should be sent to the user"
        );
        assert!(
            self.buffer_tokens.remove(&id).is_some(),
            "token should not be removed before marked done"
        );
        assert!(
            self.predecessors_cnt.remove(&id).is_some(),
            "predecessor cnt should not be removed before marked done"
        );
    }

    /// Hand out the msg to the exe workers
    fn hand_out(&self, id: u64, msg: M) {
        let done_notifier = DoneNotifier {
            notifier: self.done_tx.clone(),
            id,
        };
        if let Err(e) = self.filter_tx.send((msg, done_notifier)) {
            error!("failed to send msg through filter, {e}");
        }
    }
}

/// Create conflict checked channel. The channel guarantees there will be no conflicted msgs received by multiple receivers at the same time.
// Message flow:
// send_tx -> filter_rx -> filter -> filter_tx -> recv_rx -> done_tx -> done_rx
pub(crate) fn channel<M: ConflictCheckedMsg>(
) -> (flume::Sender<M>, flume::Receiver<(M, DoneNotifier)>) {
    // recv from user, insert it into filter
    let (send_tx, filter_rx) = flume::unbounded();
    // recv from filter, pass the msg to user
    let (filter_tx, recv_rx) = flume::unbounded();
    // recv from user to mark a msg done
    let (done_tx, done_rx) = flume::unbounded();
    let _ignore = tokio::spawn(async move {
        let mut filter = Filter::new(filter_tx, done_tx);
        #[allow(clippy::integer_arithmetic, clippy::pattern_type_mismatch)]
        // tokio internal triggers
        loop {
            tokio::select! {
                biased; // cleanup filter first so that the buffer in filter can be kept as small as possible
                Ok(id) = done_rx.recv_async() => {
                    filter.mark_done(id);
                },
                Ok(msg) = filter_rx.recv_async() => {
                    filter.insert(msg);
                },
                else => {
                    info!("channel stopped");
                    return;
                }
            }
        }
    });
    (send_tx, recv_rx)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;

    #[allow(clippy::rc_buffer)]
    struct Msg {
        token: Token,
    }

    #[allow(clippy::rc_buffer)]
    struct Token(Arc<String>);

    impl ConflictCheck for Token {
        fn is_conflict(&self, other: &Self) -> bool {
            self.0.is_conflict(&other.0)
        }
    }

    impl ConflictCheckedMsg for Msg {
        type Token = Token;

        fn token(&self) -> Self::Token {
            Token(Arc::clone(&self.token.0))
        }
    }

    #[allow(clippy::unwrap_used)]
    #[tokio::test]
    async fn order() {
        let (tx, rx) = channel::<Msg>();
        let msg1 = Msg {
            token: Token(Arc::new("1".to_owned())),
        };
        let msg2 = Msg {
            token: Token(Arc::new("1".to_owned())),
        };
        let msg3 = Msg {
            token: Token(Arc::new("3".to_owned())),
        };

        tx.send(msg1).unwrap();
        tx.send(msg2).unwrap();
        tx.send(msg3).unwrap();

        let (r1, done1) = rx.recv_async().await.unwrap();
        assert_eq!(r1.token.0.as_str(), "1");

        let (r3, done3) = rx.recv_async().await.unwrap();
        assert_eq!(r3.token.0.as_str(), "3");

        assert!(
            tokio::time::timeout(Duration::from_millis(500), rx.recv_async())
                .await
                .is_err()
        );
        done1.notify().unwrap();
        done3.notify().unwrap();

        let (r2, done2) = rx.recv_async().await.unwrap();
        assert_eq!(r2.token.0.as_str(), "1");
        done2.notify().unwrap();
    }
}
