#![allow(clippy::module_name_repetitions)]

/// Common operations for conflict pools
pub trait ConflictPoolOp {
    /// Entry of the pool
    type Entry;

    /// Removes a command from the pool
    fn remove(&mut self, entry: Self::Entry);

    /// Returns all commands in the pool
    fn all(&self) -> Vec<Self::Entry>;

    /// Clears all entries in the pool
    fn clear(&mut self);

    /// Returns the number of commands in the pool
    fn len(&self) -> usize;

    /// Checks if the pool contains some commands that will conflict with all other commands
    fn is_empty(&self) -> bool;
}

/// Speculative pool operations
pub trait SpeculativePoolOp: ConflictPoolOp {
    /// Inserts a command in to the pool
    ///
    /// Returns the entry if a conflict is detected
    fn insert_if_not_conflict(&mut self, entry: Self::Entry) -> Option<Self::Entry>;
}

/// Uncommitted pool operations
pub trait UncommittedPoolOp: ConflictPoolOp {
    /// Inserts a command in to the pool
    ///
    /// Returns `true` if a conflict is detected
    fn insert(&mut self, entry: Self::Entry) -> bool;

    /// Returns all commands in the pool that conflicts with the given command
    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry>;
}
