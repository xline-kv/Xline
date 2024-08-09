use std::sync::Arc;

use curp_external_api::conflict::{ConflictPoolOp, SpeculativePoolOp, UncommittedPoolOp};

use super::spec_pool_new::SpeculativePool;
use crate::{
    rpc::{PoolEntry, ProposeId},
    server::conflict::uncommitted_pool::UncommittedPool,
};

#[derive(Debug, Default)]
struct TestSp {
    entries: Vec<PoolEntry<i32>>,
}

impl ConflictPoolOp for TestSp {
    type Entry = PoolEntry<i32>;

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&mut self, entry: &Self::Entry) {
        if let Some(pos) = self
            .entries
            .iter()
            .position(|e| e.as_ref() == entry.as_ref())
        {
            self.entries.remove(pos);
        }
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.entries.clone()
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

impl SpeculativePoolOp for TestSp {
    fn insert_if_not_conflict(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
        if self.entries.iter().any(|e| e.as_ref() == entry.as_ref()) {
            return Some(entry);
        }
        self.entries.push(entry);
        None
    }
}

#[derive(Debug, Default)]
struct TestUcp {
    entries: Vec<PoolEntry<i32>>,
}

impl ConflictPoolOp for TestUcp {
    type Entry = PoolEntry<i32>;

    fn all(&self) -> Vec<Self::Entry> {
        self.entries.clone()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&mut self, entry: &Self::Entry) {
        if let Some(pos) = self
            .entries
            .iter()
            .position(|e| e.as_ref() == entry.as_ref())
        {
            self.entries.remove(pos);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

impl UncommittedPoolOp for TestUcp {
    fn insert(&mut self, entry: Self::Entry) -> bool {
        let conflict = self.entries.iter().any(|e| e.as_ref() == entry.as_ref());
        self.entries.push(entry);
        conflict
    }

    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry> {
        self.entries
            .iter()
            .filter_map(|e| (e.as_ref() == entry.as_ref()).then_some(e.clone()))
            .collect()
    }
}

#[test]
fn conflict_should_be_detected_in_sp() {
    let mut sp = SpeculativePool::new(vec![Box::new(TestSp::default())]);
    let entry1 = PoolEntry::new(ProposeId::default(), Arc::new(0));
    let entry2 = PoolEntry::new(ProposeId::default(), Arc::new(1));
    assert!(sp.insert(entry1.clone()).is_none());
    assert!(sp.insert(entry2).is_none());
    assert!(sp.insert(entry1.clone()).is_some());
    sp.remove(&entry1);
    assert!(sp.insert(entry1).is_none());
}

#[test]
fn sp_should_returns_all_entries() {
    let mut sp = SpeculativePool::new(vec![Box::new(TestSp::default())]);
    let entries: Vec<_> = (0..10)
        .map(|i| PoolEntry::new(ProposeId::default(), Arc::new(i)))
        .collect();
    for e in entries.clone() {
        sp.insert(e);
    }
    // conflict entries should not be inserted
    for e in entries.clone() {
        assert!(sp.insert(e).is_some());
    }
    let results = sp.all();
    assert_eq!(entries, results);
    assert_eq!(sp.len(), 10);
}

#[test]
fn conflict_should_be_detected_in_ucp() {
    let mut ucp = UncommittedPool::new(vec![Box::new(TestUcp::default())]);
    let entry1 = PoolEntry::new(ProposeId::default(), Arc::new(0));
    let entry2 = PoolEntry::new(ProposeId::default(), Arc::new(1));
    assert!(!ucp.insert(&entry1));
    assert!(!ucp.insert(&entry2));
    assert!(ucp.insert(&entry1));
    ucp.remove(&entry1);
    // Ucp allows conflict cmds to co-exist in the same pool.
    // Therefore, we should still get `conflict=true`
    assert!(ucp.insert(&entry1));
    ucp.remove(&entry1);
    ucp.remove(&entry1);
    assert!(!ucp.insert(&entry1));
}

#[test]
fn ucp_should_returns_all_entries() {
    let mut ucp = UncommittedPool::new(vec![Box::new(TestUcp::default())]);
    let entries: Vec<_> = (0..10)
        .map(|i| PoolEntry::new(ProposeId::default(), Arc::new(i)))
        .collect();
    for e in &entries {
        ucp.insert(e);
    }
    for e in &entries {
        assert!(ucp.insert(&e));
    }
    let results = ucp.all();

    let expect: Vec<_> = entries.clone().into_iter().chain(entries).collect();
    assert_eq!(expect, results);
}

#[test]
fn ucp_should_returns_all_conflict_entries() {
    let mut ucp = UncommittedPool::new(vec![Box::new(TestUcp::default())]);
    let entries: Vec<_> = (0..10)
        .map(|i| PoolEntry::new(ProposeId::default(), Arc::new(i)))
        .collect();
    for e in &entries {
        ucp.insert(e);
        ucp.insert(e);
    }
    for e in entries {
        let mut all = ucp.all_conflict(&e);
        all.sort();
        assert_eq!(all, vec![e.clone(), e.clone()]);
    }
}
