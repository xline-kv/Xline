use std::sync::Arc;

use curp::{rpc::ProposeId, server::conflict::CommandEntry};
use curp_external_api::conflict::{ConflictPoolOp, SpeculativePoolOp, UncommittedPoolOp};
use xlineapi::{
    command::{Command, KeyRange},
    AuthEnableRequest, AuthRoleAddRequest, DeleteRangeRequest, LeaseGrantRequest,
    LeaseRevokeRequest, PutRequest, RequestWrapper,
};

use super::spec_pool::{KvSpecPool, LeaseSpecPool};
use crate::conflict::{
    spec_pool::ExclusiveSpecPool,
    uncommitted_pool::{ExclusiveUncomPool, KvUncomPool, LeaseUncomPool},
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
    sp.remove(entry1.clone());
    assert!(sp.insert_if_not_conflict(entry3.clone()).is_some());
    sp.remove(entry2.clone());
    assert!(sp.insert_if_not_conflict(entry3.clone()).is_some());
    sp.remove(entry4.clone());
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
    ucp.remove(entry4.clone());
    ucp.remove(entry5.clone());
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
    sp.remove(entry1);
    sp.remove(entry2);
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
    ucp.remove(entry3.clone());
    ucp.remove(entry5.clone());
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
    sp.remove(entry1);
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
    ucp.remove(entry1.clone());
    ucp.remove(entry2.clone());
    assert!(ucp.insert(entry1.clone()));
    ucp.clear();
    assert!(ucp.is_empty());
    assert_eq!(ucp.len(), 0);
}

fn compare_commands(mut a: Vec<CommandEntry<Command>>, mut b: Vec<CommandEntry<Command>>) {
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b);
}

#[derive(Default)]
struct EntryGenerator {
    id: u64,
}

impl EntryGenerator {
    fn gen_put(&mut self, key: &str) -> CommandEntry<Command> {
        self.gen_entry(
            vec![KeyRange::new_one_key(key)],
            RequestWrapper::PutRequest(PutRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            }),
        )
    }

    fn gen_delete_range(&mut self, key: &str, range_end: &str) -> CommandEntry<Command> {
        self.gen_entry(
            vec![KeyRange::new(key, range_end)],
            RequestWrapper::DeleteRangeRequest(DeleteRangeRequest {
                key: key.as_bytes().to_vec(),
                range_end: range_end.as_bytes().to_vec(),
                ..Default::default()
            }),
        )
    }

    fn gen_lease_grant(&mut self, id: i64) -> CommandEntry<Command> {
        self.gen_entry(
            vec![],
            RequestWrapper::LeaseGrantRequest(LeaseGrantRequest {
                id,
                ..Default::default()
            }),
        )
    }

    fn gen_lease_revoke(&mut self, id: i64) -> CommandEntry<Command> {
        self.gen_entry(
            vec![],
            RequestWrapper::LeaseRevokeRequest(LeaseRevokeRequest { id }),
        )
    }

    fn gen_auth_enable(&mut self) -> CommandEntry<Command> {
        self.gen_entry(
            vec![],
            RequestWrapper::AuthEnableRequest(AuthEnableRequest {}),
        )
    }

    fn gen_role_add(&mut self) -> CommandEntry<Command> {
        self.gen_entry(
            vec![],
            RequestWrapper::AuthRoleAddRequest(AuthRoleAddRequest::default()),
        )
    }

    fn gen_entry(&mut self, keys: Vec<KeyRange>, req: RequestWrapper) -> CommandEntry<Command> {
        self.id += 1;
        let cmd = Command::new(keys, req);
        CommandEntry::new(ProposeId(0, self.id), Arc::new(cmd))
    }
}
