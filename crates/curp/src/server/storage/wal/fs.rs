#[cfg(not(madsim))]
pub(super) use std::fs::*;

#[cfg(madsim)]
pub(super) use super::mock::fs::*;
