use std::sync::Arc;

use curp::rpc::{PoolEntry, ProposeId};
use curp_external_api::conflict::{ConflictPoolOp, SpeculativePoolOp, UncommittedPoolOp};
use xlineapi::{
    command::Command, AuthEnableRequest, AuthRoleAddRequest, DeleteRangeRequest, LeaseGrantRequest,
    LeaseRevokeRequest, PutRequest, RequestWrapper,
};

use super::spec_pool::{KvSpecPool, LeaseSpecPool};
use crate::{
    conflict::{
        spec_pool::ExclusiveSpecPool,
        uncommitted_pool::{ExclusiveUncomPool, KvUncomPool, LeaseUncomPool},
    },
    storage::lease_store::LeaseCollection,
};

#[test]
fn kv_sp_operations_are_ok() {
    let mut sp = KvSpecPool::default();
    let mut gen = EntryGenerator::default();
    let entry1 = gen.gen_put("a");
    let entry2 = gen.gen_put("b");
    let entry3 = gen.gen_delete_range("a", "d");
    let entry4 = gen.gen_delete_range("c", "e");
    assert!(sp.insert_if_not_conflict(entry1.clone()).is_none());
    assert!(sp.insert_if_not_conflict(entry1.clone()).is_some());
    assert!(sp.insert_if_not_conflict(entry2.clone()).is_none());
    assert!(sp.insert_if_not_conflict(entry3.clone()).is_some());
    assert!(sp.insert_if_not_conflict(entry4.clone()).is_none());
    compare_commands(
        sp.all(),
        vec![entry1.clone(), entry2.clone(), entry4.clone()],
    );
    assert_eq!(sp.len(), 3);
    sp.remove(&entry1.clone());
    assert!(sp.insert_if_not_conflict(entry3.clone()).is_some());
    sp.remove(&entry2.clone());
    assert!(sp.insert_if_not_conflict(entry3.clone()).is_some());
    sp.remove(&entry4.clone());
    assert!(sp.insert_if_not_conflict(entry3.clone()).is_none());
    sp.clear();
    assert!(sp.is_empty());
    assert_eq!(sp.len(), 0);
}

#[test]
fn kv_ucp_operations_are_ok() {
    let mut ucp = KvUncomPool::default();
    let mut gen = EntryGenerator::default();
    let entry1 = gen.gen_put("a");
    let entry2 = gen.gen_put("a");
    let entry3 = gen.gen_put("b");
    let entry4 = gen.gen_delete_range("a", "d");
    let entry5 = gen.gen_delete_range("c", "e");
    let entry6 = gen.gen_delete_range("c", "f");
    assert!(!ucp.insert(entry1.clone()));
    assert!(ucp.insert(entry2.clone()));
    assert!(!ucp.insert(entry3.clone()));
    assert!(ucp.insert(entry4.clone()));
    assert!(ucp.insert(entry5.clone()));
    compare_commands(
        ucp.all(),
        vec![
            entry1.clone(),
            entry2.clone(),
            entry3.clone(),
            entry4.clone(),
            entry5.clone(),
        ],
    );
    assert_eq!(ucp.len(), 5);
    compare_commands(
        ucp.all_conflict(&entry1),
        vec![entry1.clone(), entry2.clone(), entry4.clone()],
    );
    compare_commands(
        ucp.all_conflict(&entry6),
        vec![entry4.clone(), entry5.clone()],
    );
    ucp.remove(&entry4.clone());
    ucp.remove(&entry5.clone());
    assert!(!ucp.insert(entry6.clone()));
    ucp.clear();
    assert!(ucp.is_empty());
    assert_eq!(ucp.len(), 0);
}

#[test]
fn lease_sp_operations_are_ok() {
    let mut sp = LeaseSpecPool::default();
    let mut gen = EntryGenerator::default();
    let entry1 = gen.gen_lease_grant(0);
    let entry2 = gen.gen_lease_grant(1);
    let entry3 = gen.gen_lease_revoke(0);
    let entry4 = gen.gen_lease_revoke(1);
    assert!(sp.insert_if_not_conflict(entry1.clone()).is_none());
    assert!(sp.insert_if_not_conflict(entry1.clone()).is_some());
    assert!(sp.insert_if_not_conflict(entry2.clone()).is_none());
    assert!(sp.insert_if_not_conflict(entry3.clone()).is_some());
    assert!(sp.insert_if_not_conflict(entry4.clone()).is_some());
    compare_commands(sp.all(), vec![entry1.clone(), entry2.clone()]);
    assert_eq!(sp.len(), 2);
    sp.remove(&entry1);
    sp.remove(&entry2);
    assert!(sp.insert_if_not_conflict(entry3).is_none());
    assert!(sp.insert_if_not_conflict(entry4).is_none());
    sp.clear();
    assert!(sp.is_empty());
    assert_eq!(sp.len(), 0);
}

#[test]
fn lease_ucp_operations_are_ok() {
    let mut ucp = LeaseUncomPool::default();
    let mut gen = EntryGenerator::default();
    let entry1 = gen.gen_lease_grant(0);
    let entry2 = gen.gen_lease_grant(0);
    let entry3 = gen.gen_lease_grant(1);
    let entry4 = gen.gen_lease_revoke(0);
    let entry5 = gen.gen_lease_revoke(1);
    assert!(!ucp.insert(entry1.clone()));
    assert!(ucp.insert(entry2.clone()));
    assert!(!ucp.insert(entry3.clone()));
    assert!(ucp.insert(entry4.clone()));
    assert!(ucp.insert(entry5.clone()));
    compare_commands(
        ucp.all(),
        vec![
            entry1.clone(),
            entry2.clone(),
            entry3.clone(),
            entry4.clone(),
            entry5.clone(),
        ],
    );
    assert_eq!(ucp.len(), 5);
    compare_commands(
        ucp.all_conflict(&entry1),
        vec![entry1.clone(), entry2.clone(), entry4.clone()],
    );
    compare_commands(
        ucp.all_conflict(&entry3),
        vec![entry3.clone(), entry5.clone()],
    );
    ucp.remove(&entry3.clone());
    ucp.remove(&entry5.clone());
    assert!(!ucp.insert(entry5.clone()));
    ucp.clear();
    assert!(ucp.is_empty());
    assert_eq!(ucp.len(), 0);
}

#[test]
fn exclusive_sp_operations_are_ok() {
    let mut sp = ExclusiveSpecPool::default();
    let mut gen = EntryGenerator::default();
    let entry1 = gen.gen_auth_enable();
    let entry2 = gen.gen_role_add();
    assert!(sp.insert_if_not_conflict(entry1.clone()).is_some());
    assert!(sp.insert_if_not_conflict(entry1.clone()).is_some());
    assert!(sp.insert_if_not_conflict(entry2.clone()).is_some());
    compare_commands(sp.all(), vec![entry1.clone()]);
    assert_eq!(sp.len(), 1);
    sp.remove(&entry1);
    assert!(sp.insert_if_not_conflict(entry2).is_some());
    sp.clear();
    assert!(sp.is_empty());
    assert_eq!(sp.len(), 0);
}

#[test]
fn exclusive_ucp_operations_are_ok() {
    let mut ucp = ExclusiveUncomPool::default();
    let mut gen = EntryGenerator::default();
    let entry1 = gen.gen_auth_enable();
    let entry2 = gen.gen_role_add();
    assert!(ucp.insert(entry1.clone()));
    assert!(ucp.insert(entry2.clone()));
    compare_commands(ucp.all(), vec![entry1.clone(), entry2.clone()]);
    compare_commands(
        ucp.all_conflict(&entry1),
        vec![entry1.clone(), entry2.clone()],
    );
    assert_eq!(ucp.len(), 2);
    ucp.remove(&entry1.clone());
    ucp.remove(&entry2.clone());
    assert!(ucp.insert(entry1.clone()));
    ucp.clear();
    assert!(ucp.is_empty());
    assert_eq!(ucp.len(), 0);
}

#[test]
fn sp_kv_then_revoke_conflict_ok() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut sp = KvSpecPool::new(Arc::clone(&lease_collection));

    let mut gen = EntryGenerator::default();
    // suppose initially we have a key "foo" associated with lease id 1
    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();

    let kv_put = gen.gen_put("foo");
    let kv_delete = gen.gen_delete_range("foo", "foz");
    let lease_revoke = gen.gen_lease_revoke(1);

    // put conflicts with lease revoke
    assert!(sp.insert_if_not_conflict(kv_put.clone()).is_none());
    assert!(sp.insert_if_not_conflict(lease_revoke.clone()).is_some());
    sp.remove(&kv_put);
    assert!(sp.insert_if_not_conflict(lease_revoke.clone()).is_none());
    sp.remove(&lease_revoke.clone());

    // delete range conflicts with lease revoke
    assert!(sp.insert_if_not_conflict(kv_delete.clone()).is_none());
    assert!(sp.insert_if_not_conflict(lease_revoke.clone()).is_some());
    sp.remove(&kv_delete);
    assert!(sp.insert_if_not_conflict(lease_revoke.clone()).is_none());
    sp.remove(&lease_revoke);
}

#[test]
fn sp_revoke_then_kv_conflict_ok() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut sp = LeaseSpecPool::new(Arc::clone(&lease_collection));

    let mut gen = EntryGenerator::default();
    // suppose initially we have a key "foo" associated with lease id 1
    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();

    let kv_put = gen.gen_put("foo");
    let kv_delete = gen.gen_delete_range("foo", "foz");
    let lease_revoke = gen.gen_lease_revoke(1);

    // lease revoke conflicts with put
    assert!(sp.insert_if_not_conflict(lease_revoke.clone()).is_none());
    assert!(sp.insert_if_not_conflict(kv_put.clone()).is_some());
    sp.remove(&lease_revoke.clone());
    assert!(sp.insert_if_not_conflict(kv_put.clone()).is_none());
    sp.remove(&kv_put.clone());

    // lease revoke conflicts with delete range
    assert!(sp.insert_if_not_conflict(lease_revoke.clone()).is_none());
    assert!(sp.insert_if_not_conflict(kv_delete.clone()).is_some());
    sp.remove(&lease_revoke.clone());
    assert!(sp.insert_if_not_conflict(kv_delete.clone()).is_none());
    sp.remove(&kv_delete.clone());
}

#[test]
fn ucp_kv_then_revoke_conflict_ok() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut ucp = KvUncomPool::new(Arc::clone(&lease_collection));

    let mut gen = EntryGenerator::default();
    // suppose initially we have a key "foo" associated with lease id 1
    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();

    let kv_put = gen.gen_put("foo");
    let kv_delete = gen.gen_delete_range("foo", "foz");
    let lease_revoke = gen.gen_lease_revoke(1);

    // put conflicts with lease revoke
    assert!(!ucp.insert(kv_put.clone()));
    assert!(ucp.insert(lease_revoke.clone()));
    ucp.remove(&kv_put);
    ucp.remove(&lease_revoke.clone());
    assert!(!ucp.insert(lease_revoke.clone()));
    ucp.remove(&lease_revoke.clone());

    // delete range conflicts with lease revoke
    assert!(!ucp.insert(kv_delete.clone()));
    assert!(ucp.insert(lease_revoke.clone()));
    ucp.remove(&kv_delete);
    ucp.remove(&lease_revoke.clone());
    assert!(!ucp.insert(lease_revoke.clone()));
}

#[test]
fn ucp_revoke_then_kv_conflict_ok() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut ucp = LeaseUncomPool::new(Arc::clone(&lease_collection));

    let mut gen = EntryGenerator::default();
    // suppose initially we have a key "foo" associated with lease id 1
    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();

    let kv_put = gen.gen_put("foo");
    let kv_delete = gen.gen_delete_range("foo", "foz");
    let lease_revoke = gen.gen_lease_revoke(1);

    // lease revoke conflicts with put
    assert!(!ucp.insert(lease_revoke.clone()));
    assert!(ucp.insert(kv_put.clone()));
    ucp.remove(&lease_revoke.clone());
    ucp.remove(&kv_put.clone());
    assert!(!ucp.insert(kv_put.clone()));
    ucp.remove(&kv_put.clone());

    // lease revoke conflicts with delete range
    assert!(!ucp.insert(lease_revoke.clone()));
    assert!(ucp.insert(kv_delete.clone()));
    ucp.remove(&lease_revoke.clone());
    ucp.remove(&kv_delete.clone());
    assert!(!ucp.insert(kv_delete.clone()));
}

/*
The following test cases verify that insert and remove operations in a
conflict pool are independent of any external state.

Specifically, we need to query `LeaseCollection` for kv and lease requests
and the `LeaseCollection` might be mutated sometime between a insert and
a remove, potentially leading to an inconsist state in our conflict pool.
*/

#[test]
fn kv_sp_mutation_no_side_effect() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut sp = KvSpecPool::new(Arc::clone(&lease_collection));
    let mut gen = EntryGenerator::default();

    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();
    let kv_put = gen.gen_put("foo");
    let lease_revoke = gen.gen_lease_revoke(1);

    sp.insert_if_not_conflict(lease_revoke.clone());
    assert!(sp.insert_if_not_conflict(kv_put.clone()).is_some());
    // Here we detach the lease from the lease collection,
    // ensuring that the mutation of `LeaseCollection`
    // won't affect the behavior of our conflict pool.
    lease_collection.detach(1, "foo".as_bytes()).unwrap();
    sp.remove(&lease_revoke);
    assert!(sp.insert_if_not_conflict(kv_put).is_none());
}

#[test]
fn lease_sp_mutation_no_side_effect() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut sp = LeaseSpecPool::new(Arc::clone(&lease_collection));
    let mut gen = EntryGenerator::default();

    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();
    let kv_put = gen.gen_put("foo");
    let lease_revoke = gen.gen_lease_revoke(1);

    sp.insert_if_not_conflict(kv_put.clone());
    assert!(sp.insert_if_not_conflict(lease_revoke.clone()).is_some());
    lease_collection.detach(1, "foo".as_bytes()).unwrap();
    sp.remove(&kv_put);
    assert!(sp.insert_if_not_conflict(kv_put).is_none());
}

#[test]
fn kv_ucp_mutation_no_side_effect() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut ucp = KvUncomPool::new(Arc::clone(&lease_collection));
    let mut gen = EntryGenerator::default();

    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();
    let kv_put = gen.gen_put("foo");
    let lease_revoke = gen.gen_lease_revoke(1);

    ucp.insert(lease_revoke.clone());
    assert!(!ucp.all_conflict(&kv_put).is_empty());
    lease_collection.detach(1, "foo".as_bytes()).unwrap();
    ucp.remove(&lease_revoke);
    assert!(ucp.all_conflict(&kv_put).is_empty());
}

#[test]
fn lease_ucp_mutation_no_side_effect() {
    let lease_collection = Arc::new(LeaseCollection::new(60));
    let mut ucp = LeaseUncomPool::new(Arc::clone(&lease_collection));
    let mut gen = EntryGenerator::default();

    lease_collection.grant(1, 60, true);
    lease_collection
        .attach(1, "foo".as_bytes().to_vec())
        .unwrap();
    let kv_put = gen.gen_put("foo");
    let lease_revoke = gen.gen_lease_revoke(1);

    ucp.insert(kv_put.clone());
    assert!(!ucp.all_conflict(&lease_revoke).is_empty());
    lease_collection.detach(1, "foo".as_bytes()).unwrap();
    ucp.remove(&kv_put);
    assert!(ucp.all_conflict(&lease_revoke).is_empty());
}

fn compare_commands(mut a: Vec<PoolEntry<Command>>, mut b: Vec<PoolEntry<Command>>) {
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b);
}

#[derive(Default)]
struct EntryGenerator {
    id: u64,
}

impl EntryGenerator {
    fn gen_put(&mut self, key: &str) -> PoolEntry<Command> {
        self.gen_entry(RequestWrapper::PutRequest(PutRequest {
            key: key.as_bytes().to_vec(),
            ..Default::default()
        }))
    }

    fn gen_delete_range(&mut self, key: &str, range_end: &str) -> PoolEntry<Command> {
        self.gen_entry(RequestWrapper::DeleteRangeRequest(DeleteRangeRequest {
            key: key.as_bytes().to_vec(),
            range_end: range_end.as_bytes().to_vec(),
            ..Default::default()
        }))
    }

    fn gen_lease_grant(&mut self, id: i64) -> PoolEntry<Command> {
        self.gen_entry(RequestWrapper::LeaseGrantRequest(LeaseGrantRequest {
            id,
            ..Default::default()
        }))
    }

    fn gen_lease_revoke(&mut self, id: i64) -> PoolEntry<Command> {
        self.gen_entry(RequestWrapper::LeaseRevokeRequest(LeaseRevokeRequest {
            id,
        }))
    }

    fn gen_auth_enable(&mut self) -> PoolEntry<Command> {
        self.gen_entry(RequestWrapper::AuthEnableRequest(AuthEnableRequest {}))
    }

    fn gen_role_add(&mut self) -> PoolEntry<Command> {
        self.gen_entry(RequestWrapper::AuthRoleAddRequest(
            AuthRoleAddRequest::default(),
        ))
    }

    fn gen_entry(&mut self, req: RequestWrapper) -> PoolEntry<Command> {
        self.id += 1;
        let cmd = Command::new(req);
        PoolEntry::new(ProposeId(0, self.id), Arc::new(cmd))
    }
}
