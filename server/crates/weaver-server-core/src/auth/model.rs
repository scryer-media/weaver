use std::sync::{Arc, RwLock};

use super::service::derive_jwt_secret;
use crate::auth::repository::AuthCredentials;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedLoginAuth {
    pub username: String,
    pub password_hash: String,
    pub jwt_secret: [u8; 32],
}

impl CachedLoginAuth {
    pub fn new(username: impl Into<String>, password_hash: impl Into<String>) -> Self {
        let username = username.into();
        let password_hash = password_hash.into();
        let jwt_secret = derive_jwt_secret(&password_hash);
        Self {
            username,
            password_hash,
            jwt_secret,
        }
    }

    pub fn from_credentials(credentials: AuthCredentials) -> Self {
        Self::new(credentials.username, credentials.password_hash)
    }
}

#[derive(Debug, Clone, Default)]
pub struct LoginAuthCache(Arc<RwLock<Option<CachedLoginAuth>>>);

impl LoginAuthCache {
    pub fn from_credentials(credentials: Option<AuthCredentials>) -> Self {
        let cache = Self::default();
        cache.replace_credentials(credentials);
        cache
    }

    pub fn snapshot(&self) -> Option<CachedLoginAuth> {
        self.0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn replace(&self, auth: Option<CachedLoginAuth>) {
        *self
            .0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = auth;
    }

    pub fn replace_credentials(&self, credentials: Option<AuthCredentials>) {
        self.replace(credentials.map(CachedLoginAuth::from_credentials));
    }

    pub fn clear(&self) {
        self.replace(None);
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
