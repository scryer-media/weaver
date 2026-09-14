use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;

use super::ApiKeyAuthRow;
use crate::auth::repository::AuthCredentials;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedLoginAuth {
    pub username: String,
    pub password_hash: String,
    pub jwt_secret: [u8; 32],
}

impl CachedLoginAuth {
    pub fn new(
        username: impl Into<String>,
        password_hash: impl Into<String>,
        jwt_secret: [u8; 32],
    ) -> Self {
        Self {
            username: username.into(),
            password_hash: password_hash.into(),
            jwt_secret,
        }
    }

    pub fn from_credentials(credentials: AuthCredentials, jwt_secret: [u8; 32]) -> Self {
        Self::new(credentials.username, credentials.password_hash, jwt_secret)
    }
}

/// Wakes long-lived sessions whenever the credentials they authenticated with
/// may have been revoked. Requests re-check on every call; an open socket only
/// knows to look again when told.
#[derive(Debug, Clone)]
struct RevocationSignal(Arc<watch::Sender<()>>);

impl Default for RevocationSignal {
    fn default() -> Self {
        Self(Arc::new(watch::Sender::new(())))
    }
}

impl RevocationSignal {
    fn notify(&self) {
        self.0.send_replace(());
    }

    fn subscribe(&self) -> watch::Receiver<()> {
        self.0.subscribe()
    }
}

#[derive(Debug, Clone, Default)]
pub struct LoginAuthCache {
    auth: Arc<RwLock<Option<CachedLoginAuth>>>,
    changes: RevocationSignal,
}

impl LoginAuthCache {
    pub fn from_credentials(credentials: Option<AuthCredentials>, jwt_secret: [u8; 32]) -> Self {
        let cache = Self::default();
        cache.replace_credentials(credentials, jwt_secret);
        cache
    }

    pub fn snapshot(&self) -> Option<CachedLoginAuth> {
        self.auth
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn replace(&self, auth: Option<CachedLoginAuth>) {
        *self
            .auth
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = auth;
        self.changes.notify();
    }

    /// Resolves after every credential or signing-secret replacement.
    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }

    pub fn replace_credentials(&self, credentials: Option<AuthCredentials>, jwt_secret: [u8; 32]) {
        self.replace(
            credentials
                .map(|credentials| CachedLoginAuth::from_credentials(credentials, jwt_secret)),
        );
    }

    pub fn clear(&self) {
        self.replace(None);
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApiKeyCache {
    rows: Arc<RwLock<HashMap<[u8; 32], ApiKeyAuthRow>>>,
    changes: RevocationSignal,
}

impl ApiKeyCache {
    pub fn from_rows(rows: Vec<ApiKeyAuthRow>) -> Self {
        let cache = Self::default();
        cache.replace_rows(rows);
        cache
    }

    pub fn get(&self, key_hash: &[u8; 32]) -> Option<ApiKeyAuthRow> {
        self.rows
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key_hash)
            .cloned()
    }

    pub fn upsert(&self, row: ApiKeyAuthRow) {
        self.rows
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(row.key_hash, row);
    }

    pub fn remove_by_id(&self, id: i64) {
        self.rows
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, row| row.id != id);
        self.changes.notify();
    }

    pub fn replace_rows(&self, rows: Vec<ApiKeyAuthRow>) {
        *self
            .rows
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            rows.into_iter().map(|row| (row.key_hash, row)).collect();
        self.changes.notify();
    }

    /// Resolves after every removal or wholesale replacement of the key set.
    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerScope {
    Local,
    Read,
    Control,
    Admin,
}

impl CallerScope {
    pub fn can_read(&self) -> bool {
        true
    }

    pub fn can_control(&self) -> bool {
        matches!(
            self,
            CallerScope::Local | CallerScope::Control | CallerScope::Admin
        )
    }

    pub fn is_admin(&self) -> bool {
        matches!(self, CallerScope::Local | CallerScope::Admin)
    }
}

#[cfg(test)]
mod tests;
