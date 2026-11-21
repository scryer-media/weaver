// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The keys that sign login tokens, CSRF tokens and script-run tokens. None
// is stored: each is derived from the instance's encryption master key, so
// a copy of the database alone cannot forge a token, and a changed password
// (a new hash) is a new login key.

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database;
use weaver_server_db::persistence::encryption::EncryptionKey;

use super::service::sign_hs256;

// HKDF-SHA256 info label of the session signing salt. Changing it signs
// every session out, so a new derivation takes a new version.
pub const SESSION_SIGNING_SALT_INFO: &[u8] = b"weaver/session-signing-salt/v1";

// HKDF-SHA256 info label of the script-run signing key. Changing it ends
// every script run's token, so a new derivation takes a new version.
pub const SCRIPT_RUN_SIGNING_KEY_INFO: &[u8] = b"weaver/script-run-signing-key/v1";

#[derive(Debug, thiserror::Error)]
pub enum SigningKeyError {
    #[error("no encryption master key is set; tokens cannot be signed")]
    MissingMasterKey,
    #[error("signing key derivation failed: {0}")]
    Derivation(String),
}

impl From<SigningKeyError> for StateError {
    fn from(error: SigningKeyError) -> Self {
        StateError::Database(error.to_string())
    }
}

// The derived signing keys of one master key.
#[derive(Clone, PartialEq, Eq)]
pub struct SigningKeys {
    session_salt: [u8; 32],
    script_run_key: [u8; 32],
}

impl std::fmt::Debug for SigningKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKeys").finish_non_exhaustive()
    }
}

impl SigningKeys {
    pub fn derive(master_key: &EncryptionKey) -> Result<Self, SigningKeyError> {
        let derive = |info| {
            master_key
                .derive_key(info)
                .map_err(SigningKeyError::Derivation)
        };
        Ok(Self {
            session_salt: derive(SESSION_SIGNING_SALT_INFO)?,
            script_run_key: derive(SCRIPT_RUN_SIGNING_KEY_INFO)?,
        })
    }

    // The keys of the master key `db` holds. A database without one has no
    // keys: never a fallback key.
    pub fn for_database(db: &Database) -> Result<Self, SigningKeyError> {
        Self::derive(
            db.encryption_key()
                .ok_or(SigningKeyError::MissingMasterKey)?,
        )
    }

    // The key that signs the login and CSRF tokens of the credential whose
    // stored hash is `password_hash`: HMAC-SHA256 keyed by the session salt.
    pub fn login_key(&self, password_hash: &str) -> [u8; 32] {
        sign_hs256(&self.session_salt, password_hash.as_bytes())
            .try_into()
            .expect("HMAC-SHA256 gives 32 bytes")
    }

    // The key script-run tokens are signed and verified with.
    pub fn script_run_key(&self) -> &[u8; 32] {
        &self.script_run_key
    }

    #[cfg(test)]
    pub(crate) fn session_salt(&self) -> &[u8; 32] {
        &self.session_salt
    }
}

#[cfg(test)]
mod tests;
