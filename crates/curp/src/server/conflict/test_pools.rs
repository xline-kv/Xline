use curp_external_api::{
    cmd::ConflictCheck,
    conflict::{SpeculativePool, UncommittedPool},
};
use curp_test_utils::test_cmd::TestCommand;

use super::CommandEntry;

#[derive(Debug, Default)]
pub struct TestSpecPool {
    cmds: Vec<CommandEntry<TestCommand>>,
}

impl SpeculativePool for TestSpecPool {
    type Entry = CommandEntry<TestCommand>;

    #[inline]
    fn insert(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
        if self.cmds.iter().any(|t| t.is_conflict(&entry)) {
            return Some(entry);
        }
        self.cmds.push(entry);
        None
    }

    #[inline]
    fn len(&self) -> usize {
        self.cmds.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }

    #[inline]
    fn remove(&mut self, entry: Self::Entry) {
        if let Some(idx) = self.cmds.iter().position(|c| *c == entry) {
            let _ignore = self.cmds.remove(idx);
        }
    }

    #[inline]
    fn all(&self) -> Vec<Self::Entry> {
        self.cmds.clone()
    }

    #[inline]
    fn clear(&mut self) {
        self.cmds.clear();
    }
}

#[derive(Debug, Default)]
pub struct TestUncomPool {
    cmds: Vec<CommandEntry<TestCommand>>,
}

impl UncommittedPool for TestUncomPool {
    type Entry = CommandEntry<TestCommand>;

    #[inline]
    fn insert(&mut self, entry: Self::Entry) -> bool {
        let conflict = self.cmds.iter().any(|t| t.is_conflict(&entry));
        self.cmds.push(entry);
        conflict
    }

    #[inline]
    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry> {
        self.cmds
            .iter()
            .filter(|t| t.is_conflict(entry))
            .map(Clone::clone)
            .collect()
    }

    #[inline]
    fn all(&self) -> Vec<Self::Entry> {
        self.cmds.clone()
    }

    #[inline]
    fn len(&self) -> usize {
        self.cmds.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }

    #[inline]
    fn remove(&mut self, entry: Self::Entry) {
        if let Some(idx) = self.cmds.iter().position(|c| *c == entry) {
            let _ignore = self.cmds.remove(idx);
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.cmds.clear();
    }
}
