#![allow(clippy::module_name_repetitions)]

/// Insert into speculative pool
pub trait SpeculativePool {
    /// Entry of the pool
    type Entry;

    /// Inserts a command in to the pool
    ///
    /// Returns `Some(Entry)` if a conflict is detected
    fn insert(&mut self, entry: Self::Entry) -> Option<Self::Entry>;

    /// Returns the number of commands in the pool
    fn len(&self) -> usize;

    /// Checks if the pool contains some commands that will conflict with all other commands
    fn is_empty(&self) -> bool;

    /// Removes a command from the pool
    fn remove(&mut self, entry: Self::Entry);

    /// Returns all commands in the pool
    fn all(&self) -> Vec<Self::Entry>;

    /// Clears all entries in the pool
    fn clear(&mut self);
}

/// Insert into speculative pool
pub trait UncommittedPool {
    /// Entry of the pool
    type Entry;
    /// Inserts a command in to the pool
    ///
    /// Returns `true` if a conflict is detected
    fn insert(&mut self, entry: Self::Entry) -> bool;

    /// Returns all commands in the pool that conflicts with the given command
    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry>;

    /// Returns all commands in the pool
    fn all(&self) -> Vec<Self::Entry>;

    /// Returns the number of commands in the pool
    fn len(&self) -> usize;

    /// Checks if the pool will conflict with all commands
    fn is_empty(&self) -> bool;

    /// Removes a command from the pool
    fn remove(&mut self, entry: Self::Entry);

    /// Clears all entries in the pool
    fn clear(&mut self);
}
