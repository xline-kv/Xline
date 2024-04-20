//  CONFLICT_CHECKED_MPMC
//            |
//       CMD_WORKER            LEASE_KEEP_ALIVE
//         /     \                    |
//  COMPACT_BG  KV_UPDATES      TONIC_SERVER       ELECTION
//                    \        /      |      \       /
//                   WATCH_TASK  CONF_CHANGE  LOG_PERSIST

// NOTE: In integration tests, we use bottom tasks, like `WatchTask`, `ConfChange`, and `LogPersist`,
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
    ConflictCheckedMpmc,
    CmdWorker,
    CompactBg,
    KvUpdates,
    WatchTask,
    LeaseKeepAlive,
    TonicServer,
    LogPersist,
    Election,
    SyncFollower,
    ConfChange,
    GcSpecPool,
    GcCmdBoard,
    RevokeExpiredLeases,
    SyncVictims,
    AutoCompactor,
}

/// All edges of task graph, the first item in each pair must be shut down before the second item
pub const ALL_EDGES: [(TaskName, TaskName); 9] = [
    (TaskName::ConflictCheckedMpmc, TaskName::CmdWorker),
    (TaskName::CmdWorker, TaskName::CompactBg),
    (TaskName::CmdWorker, TaskName::KvUpdates),
    (TaskName::KvUpdates, TaskName::WatchTask),
    (TaskName::LeaseKeepAlive, TaskName::TonicServer),
    (TaskName::TonicServer, TaskName::WatchTask),
    (TaskName::TonicServer, TaskName::ConfChange),
    (TaskName::TonicServer, TaskName::LogPersist),
    (TaskName::Election, TaskName::LogPersist),
];
