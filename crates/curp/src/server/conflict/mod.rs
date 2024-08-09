/// Speculative pool
pub(crate) mod spec_pool_new;

/// Uncommitted pool
pub(crate) mod uncommitted_pool;

#[cfg(test)]
mod tests;

/// Conflict pool used in tests
#[doc(hidden)]
pub mod test_pools;
