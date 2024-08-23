//  AFTER_SYNC     LEASE_KEEP_ALIVE
//      |                  |
//  KV_UPDATES      TONIC_SERVER
//       \        /      |
//      WATCH_TASK  CONF_CHANGE
//
// Other tasks like `CompactBg`, `GcSpecPool`, `GcCmdBoard`, `RevokeExpiredLeases`, `SyncVictims`,
// `Election`, and `AutoCompactor` do not have dependent tasks.

// NOTE: In integration tests, we use bottom tasks, like `WatchTask` and `ConfChange`,
// which are not dependent on other tasks to detect the curp group is closed or not. If you want
// to refactor the task group, don't forget to modify the `BOTTOM_TASKS` in `crates/curp/tests/it/common/curp_group.rs`
// to prevent the integration tests from failing.

/// Generate enum with iterator
macro_rules! enum_with_iter {
    ( $($variant:ident),* $(,)? ) => {
        /// Task name
        #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
        #[non_exhaustive]
        #[allow(missing_docs)]
        pub enum TaskName {
            $($variant),*
        }

        impl TaskName {
            /// Get iter of all task names
            #[inline]
            pub fn iter() -> impl Iterator<Item = TaskName> {
                static VARIANTS: &'static [TaskName] = &[
                    $(TaskName::$variant),*
                ];
                VARIANTS.iter().copied()
            }
        }
    }
}
enum_with_iter! {
    CompactBg,
    KvUpdates,
    WatchTask,
    LeaseKeepAlive,
    TonicServer,
    Election,
    SyncFollower,
    ConfChange,
    GcClientLease,
    RevokeExpiredLeases,
    SyncVictims,
    AutoCompactor,
    AfterSync,
    HandlePropose,
}

impl TaskName {
    /// Returns `true` if the task is cancel safe
    pub(super) fn cancel_safe(self) -> bool {
        match self {
            TaskName::HandlePropose | TaskName::AfterSync => true,
            TaskName::CompactBg
            | TaskName::KvUpdates
            | TaskName::WatchTask
            | TaskName::LeaseKeepAlive
            | TaskName::TonicServer
            | TaskName::Election
            | TaskName::SyncFollower
            | TaskName::ConfChange
            | TaskName::GcClientLease
            | TaskName::RevokeExpiredLeases
            | TaskName::SyncVictims
            | TaskName::AutoCompactor => false,
        }
    }
}

/// All edges of task graph, the first item in each pair must be shut down before the second item
pub const ALL_EDGES: [(TaskName, TaskName); 5] = [
    (TaskName::AfterSync, TaskName::KvUpdates),
    (TaskName::KvUpdates, TaskName::WatchTask),
    (TaskName::LeaseKeepAlive, TaskName::TonicServer),
    (TaskName::TonicServer, TaskName::WatchTask),
    (TaskName::TonicServer, TaskName::ConfChange),
];
