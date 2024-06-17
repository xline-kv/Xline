use std::{cmp::Ordering, sync::Arc};

use curp_external_api::conflict::{ConflictPoolOp, SpeculativePoolOp, UncommittedPoolOp};

use super::{spec_pool_new::SpeculativePool, CommandEntry};
use crate::{
    rpc::{ConfChange, PoolEntry, PoolEntryInner, ProposeId},
    server::conflict::uncommitted_pool::UncommittedPool,
};

#[derive(Debug, Default)]
struct TestSp {
    entries: Vec<CommandEntry<i32>>,
}

impl ConflictPoolOp for TestSp {
    type Entry = CommandEntry<i32>;

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&mut self, entry: Self::Entry) {
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
    entries: Vec<CommandEntry<i32>>,
}

impl ConflictPoolOp for TestUcp {
    type Entry = CommandEntry<i32>;

    fn all(&self) -> Vec<Self::Entry> {
        self.entries.clone()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&mut self, entry: Self::Entry) {
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

impl Eq for PoolEntry<i32> {}

impl PartialOrd for PoolEntry<i32> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[allow(clippy::pattern_type_mismatch)]
        match (&self.inner, &other.inner) {
            (PoolEntryInner::Command(a), PoolEntryInner::Command(b)) => a.partial_cmp(&b),
            (PoolEntryInner::Command(_), PoolEntryInner::ConfChange(_)) => Some(Ordering::Less),
            (PoolEntryInner::ConfChange(_), PoolEntryInner::Command(_)) => Some(Ordering::Greater),
            (PoolEntryInner::ConfChange(a), PoolEntryInner::ConfChange(b)) => {
                for (ae, be) in a.iter().zip(b.iter()) {
                    let ord = ae.change_type.cmp(&be.change_type).then(
                        ae.node_id
                            .cmp(&be.node_id)
                            .then(ae.address.cmp(&be.address)),
                    );
                    if !matches!(ord, Ordering::Equal) {
                        return Some(ord);
                    }
                }
                if a.len() > b.len() {
                    return Some(Ordering::Greater);
                }
                return Some(Ordering::Less);
            }
        }
    }
}

impl Ord for PoolEntry<i32> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
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
    sp.remove(entry1.clone());
    assert!(sp.insert(entry1).is_none());
}

#[test]
fn conf_change_should_conflict_with_all_entries_in_sp() {
    let mut sp = SpeculativePool::new(vec![Box::new(TestSp::default())]);
    let entry1 = PoolEntry::new(ProposeId::default(), Arc::new(0));
    let entry2 = PoolEntry::new(ProposeId::default(), Arc::new(1));
    let entry3 = PoolEntry::<i32>::new(ProposeId::default(), vec![ConfChange::default()]);
    let entry4 = PoolEntry::<i32>::new(
        ProposeId::default(),
        vec![ConfChange {
            change_type: 0,
            node_id: 1,
            address: vec![],
        }],
    );
    assert!(sp.insert(entry3.clone()).is_none());
    assert!(sp.insert(entry1.clone()).is_some());
    assert!(sp.insert(entry2.clone()).is_some());
    assert!(sp.insert(entry4).is_some());
    sp.remove(entry3.clone());
    assert!(sp.insert(entry1).is_none());
    assert!(sp.insert(entry3).is_some());
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
    assert!(!ucp.insert(entry1.clone()));
    assert!(!ucp.insert(entry2));
    assert!(ucp.insert(entry1.clone()));
    ucp.remove(entry1.clone());
    // Ucp allows conflict cmds to co-exist in the same pool.
    // Therefore, we should still get `conflict=true`
    assert!(ucp.insert(entry1.clone()));
    ucp.remove(entry1.clone());
    ucp.remove(entry1.clone());
    assert!(!ucp.insert(entry1));
}

#[test]
fn conf_change_should_conflict_with_all_entries_in_ucp() {
    let mut ucp = UncommittedPool::new(vec![Box::new(TestUcp::default())]);
    let entry1 = PoolEntry::new(ProposeId::default(), Arc::new(0));
    let entry2 = PoolEntry::new(ProposeId::default(), Arc::new(1));
    let entry3 = PoolEntry::<i32>::new(ProposeId::default(), vec![ConfChange::default()]);
    let entry4 = PoolEntry::<i32>::new(
        ProposeId::default(),
        vec![ConfChange {
            change_type: 0,
            node_id: 1,
            address: vec![],
        }],
    );
    assert!(!ucp.insert(entry3.clone()));
    assert!(ucp.insert(entry1.clone()));
    assert!(ucp.insert(entry4.clone()));
    ucp.remove(entry3.clone());
    ucp.remove(entry4.clone());
    assert!(!ucp.insert(entry2));
    assert!(ucp.insert(entry3));
}

#[test]
fn ucp_should_returns_all_entries() {
    let mut ucp = UncommittedPool::new(vec![Box::new(TestUcp::default())]);
    let entries: Vec<_> = (0..10)
        .map(|i| PoolEntry::new(ProposeId::default(), Arc::new(i)))
        .collect();
    for e in entries.clone() {
        ucp.insert(e);
    }
    for e in entries.clone() {
        assert!(ucp.insert(e));
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
        ucp.insert(e.clone());
        ucp.insert(e.clone());
    }
    let conf_change = PoolEntry::<i32>::new(ProposeId::default(), vec![ConfChange::default()]);
    ucp.insert(conf_change.clone());
    for e in entries {
        let mut all = ucp.all_conflict(e.clone());
        all.sort();
        assert_eq!(all, vec![e.clone(), e.clone(), conf_change.clone()]);
    }
}
