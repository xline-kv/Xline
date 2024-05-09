use std::{fs::File, sync::Arc};

use curp_external_api::cmd::Command;

use crate::rpc::ProposeId;

use self::{config::WALConfig, error::WALError};

/// WAL codec
mod codec;

/// WAL error
mod error;

/// WAL config
mod config;

/// WAL segment
mod segment;

/// Operations of speculative pool WAL
pub(crate) trait PoolWALOps<C: Command> {
    /// Insert a command to WAL
    fn insert(&self, propose_id: ProposeId, cmd: Arc<C>) -> Result<(), WALError>;

    /// Removes a command from WAL
    fn remove(&self, propose_id: ProposeId) -> Result<(), WALError>;

    /// Recover all commands stored in WAL
    fn recover(&self) -> Result<Vec<C>, WALError>;
}

/// WAL of speculative pool
struct SpeculativePoolWAL {
    /// WAL config
    config: WALConfig,
}

impl<C: Command> PoolWALOps<C> for SpeculativePoolWAL {
    fn insert(&self, propose_id: ProposeId, cmd: Arc<C>) -> Result<(), WALError> {
        todo!()
    }

    fn remove(&self, propose_id: ProposeId) -> Result<(), WALError> {
        todo!()
    }

    fn recover(&self) -> Result<Vec<C>, WALError> {
        todo!()
    }
}

struct InsertWAL {
    /// WAL segments
    segments: Vec<Segment>,
    /// Next segment id
    next_segment_id: u64,
}

struct RemoveWAL {}
