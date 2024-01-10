#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] //  introduced by mock!

use mockall::mock;

/// Callback when the leadership changes
pub trait RoleChange: Clone + Send + Sync + 'static {
    /// The `on_election_win` will be invoked when the current server win the election.
    /// It means that the current server's role will change from Candidate to Leader.
    fn on_election_win(&self);

    /// The `on_calibrate` will be invoked when the current server has been calibrated.
    /// It means that the current server's role will change from Leader to Follower.
    fn on_calibrate(&self);
}

mock! {
    RoleChange {}

    impl Clone for RoleChange {
        fn clone(&self) -> Self;
    }

    impl RoleChange for RoleChange {
        fn on_election_win(&self);
        fn on_calibrate(&self);
    }
}
