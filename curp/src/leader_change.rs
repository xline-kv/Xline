#[cfg(test)]
use mockall::automock;

/// Callback when the leadership changes
#[cfg_attr(test, automock)]
pub trait LeaderChange: Send + Sync + std::fmt::Debug {
    /// will be invoked when the current server is a leader
    fn on_leader(&self);

    /// will be invoked when the current server is not a leader
    fn on_follower(&self);
}
