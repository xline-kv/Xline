use std::{fmt, sync::Arc};

use prost::Message;

use crate::{
    rpc::{Role, User},
    storage::{storage_api::StorageApi, ExecuteError},
};

/// User table
pub(crate) const USER_TABLE: &str = "user";
/// Role table
pub(crate) const ROLE_TABLE: &str = "role";
/// Auth table
pub(crate) const AUTH_TABLE: &str = "auth";
/// Key of `AuthEnable`
pub(crate) const AUTH_ENABLE_KEY: &[u8] = b"enable";
/// Key of `AuthRevision`
pub(crate) const AUTH_REVISION_KEY: &[u8] = b"revision";
/// Root user
pub(crate) const ROOT_USER: &str = "root";
/// Root role
pub(crate) const ROOT_ROLE: &str = "root";

/// Auth store inner
pub(crate) struct AuthStoreBackend<DB>
where
    DB: StorageApi,
{
    /// DB to store key value
    db: Arc<DB>,
}

impl<DB> fmt::Debug for AuthStoreBackend<DB>
where
    DB: StorageApi,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthStoreBackend")
            .field("db", &self.db)
            .finish()
    }
}

impl<DB> AuthStoreBackend<DB>
where
    DB: StorageApi,
{
    /// New `AuthStoreBackend`
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    /// get user by username
    pub(crate) fn get_user(&self, username: &str) -> Result<User, ExecuteError> {
        match self.db.get_value(USER_TABLE, username)? {
            Some(value) => Ok(User::decode(value.as_slice()).unwrap_or_else(|e| {
                panic!("Failed to decode user from value, error: {e:?}, value: {value:?}");
            })),
            None => Err(ExecuteError::user_not_found(username)),
        }
    }

    /// get role by rolename
    pub(crate) fn get_role(&self, rolename: &str) -> Result<Role, ExecuteError> {
        match self.db.get_value(ROLE_TABLE, rolename)? {
            Some(value) => Ok(Role::decode(value.as_slice()).unwrap_or_else(|e| {
                panic!("Failed to decode role from value, error: {e:?}, value: {value:?}");
            })),
            None => Err(ExecuteError::role_not_found(rolename)),
        }
    }

    /// Get all users in the `AuthStore`
    pub(crate) fn get_all_users(&self) -> Result<Vec<User>, ExecuteError> {
        let users = self
            .db
            .get_all(USER_TABLE)?
            .into_iter()
            .map(|(_, user)| {
                User::decode(user.as_slice()).unwrap_or_else(|e| {
                    panic!("Failed to decode user from value, error: {e:?}, user: {user:?}");
                })
            })
            .collect();
        Ok(users)
    }

    /// Get all roles in the `AuthStore`
    pub(crate) fn get_all_roles(&self) -> Result<Vec<Role>, ExecuteError> {
        let roles = self
            .db
            .get_all(ROLE_TABLE)?
            .into_iter()
            .map(|(_, value)| {
                Role::decode(value.as_slice()).unwrap_or_else(|e| {
                    panic!("Failed to decode role from value, error: {e:?}, value: {value:?}");
                })
            })
            .collect();
        Ok(roles)
    }

    /// get auth enable
    pub(crate) fn get_enable(&self) -> Result<bool, ExecuteError> {
        if let Some(enabled) = self.db.get_value(AUTH_TABLE, AUTH_ENABLE_KEY)? {
            match enabled.first() {
                Some(&value) => Ok(value != 0),
                None => unreachable!("enabled should not be empty"),
            }
        } else {
            Ok(bool::default())
        }
    }

    /// get auth revision
    pub(crate) fn get_revision(&self) -> Result<i64, ExecuteError> {
        if let Some(revision) = self.db.get_value(AUTH_TABLE, AUTH_REVISION_KEY)? {
            let rev = i64::decode(revision.as_slice()).unwrap_or_else(|e| {
                panic!("Auth Revision maybe Corrupted: cannot decode revision from auth, {e:?}")
            });
            Ok(rev)
        } else {
            Ok(1)
        }
    }

    #[cfg(test)]
    pub(crate) fn flush_ops(
        &self,
        ops: Vec<crate::storage::db::WriteOp>,
    ) -> Result<(), ExecuteError> {
        self.db.flush_ops(ops)
    }
}
