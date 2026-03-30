use async_graphql::{Context, Error, ErrorExtensions, Guard, Result};
use sha2::{Digest, Sha256};

/// Represents who is making the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerScope {
    /// Local web UI — no token, full access.
    Local,
    /// API key with read-only scope.
    Read,
    /// API key with queue control scope.
    Control,
    /// API key with admin scope.
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

/// Guard that rejects requests from callers without read scope.
pub struct ReadGuard;

impl Guard for ReadGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| internal_error("missing caller scope"))?;
        if scope.can_read() {
            Ok(())
        } else {
            Err(graphql_error("FORBIDDEN", "read scope required"))
        }
    }
}

/// Guard that rejects requests from non-admin callers.
pub struct AdminGuard;

impl Guard for AdminGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| internal_error("missing caller scope"))?;
        if scope.is_admin() {
            Ok(())
        } else {
            Err(graphql_error("FORBIDDEN", "admin scope required"))
        }
    }
}

/// Guard that rejects requests from callers without queue-control scope.
pub struct ControlGuard;

impl Guard for ControlGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| internal_error("missing caller scope"))?;
        if scope.can_control() {
            Ok(())
        } else {
            Err(graphql_error("FORBIDDEN", "control scope required"))
        }
    }
}

pub fn graphql_error(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message.into()).extend_with(|_, ext| {
        ext.set("code", code);
    })
}

pub fn internal_error(message: impl Into<String>) -> Error {
    graphql_error("INTERNAL", format!("internal: {}", message.into()))
}

/// Generate a new API key: `wvr_<32 hex chars>` (16 random bytes).
pub fn generate_api_key() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("getrandom failed");
    format!("wvr_{}", hex::encode(bytes))
}

/// Hash a raw key to its SHA-256 digest for storage/lookup.
pub fn hash_api_key(raw_key: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hasher.finalize().into()
}

/// Hash a password with argon2id for storage.
pub fn hash_password(password: &str) -> Result<String, String> {
    use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
    let salt = SaltString::generate(&mut OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("argon2 hash failed: {e}"))
}

/// Verify a password against a stored hash (argon2id or legacy scrypt).
pub fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    argon2::Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// Returns true if the stored hash uses a legacy algorithm that should be
/// re-hashed on next successful login.
pub fn needs_rehash(hash: &str) -> bool {
    !hash.starts_with("$argon2")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_format() {
        let key = generate_api_key();
        assert!(key.starts_with("wvr_"));
        assert_eq!(key.len(), 4 + 32); // "wvr_" + 32 hex chars
    }

    #[test]
    fn hash_deterministic() {
        let h1 = hash_api_key("wvr_abc123");
        let h2 = hash_api_key("wvr_abc123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_different_keys() {
        let h1 = hash_api_key("wvr_abc123");
        let h2 = hash_api_key("wvr_def456");
        assert_ne!(h1, h2);
    }

    #[test]
    fn scope_is_admin() {
        assert!(CallerScope::Local.is_admin());
        assert!(CallerScope::Admin.is_admin());
        assert!(!CallerScope::Read.is_admin());
        assert!(!CallerScope::Control.is_admin());
    }

    #[test]
    fn scope_permissions() {
        assert!(CallerScope::Read.can_read());
        assert!(!CallerScope::Read.can_control());
        assert!(CallerScope::Control.can_control());
        assert!(CallerScope::Admin.can_control());
    }

    #[test]
    fn password_hash_and_verify() {
        let hash = hash_password("hunter2").unwrap();
        assert!(hash.starts_with("$argon2"));
        assert!(verify_password("hunter2", &hash));
        assert!(!verify_password("wrong", &hash));
        assert!(!needs_rehash(&hash));
    }

    #[test]
    fn legacy_scrypt_needs_rehash() {
        assert!(needs_rehash("$scrypt$ln=17,r=8,p=1$somesalt$somehash"));
    }
}
