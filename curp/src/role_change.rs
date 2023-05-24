#[cfg(test)]
use mockall::automock;

/// Callback when the leadership changes
#[cfg_attr(test, automock)]
pub trait RoleChange: Send + Sync + std::fmt::Debug {
    /// The `on_election_win` will be invoked when the current server win the election.
    /// It means that the current server's role will change from Candidate to Leader.
    fn on_election_win(&self);

    /// The `on_calibrate` will be invoked when the current server has been calibrated.
    /// It means that the current server's role will change from Leader to Follower.
    fn on_calibrate(&self);
}
