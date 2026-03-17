use async_graphql::{Context, Guard, Result};
use sha2::{Digest, Sha256};

/// Represents who is making the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerScope {
    /// Local web UI — no token, full access.
    Local,
    /// API key with integration scope.
    Integration,
    /// API key with admin scope.
    Admin,
}

impl CallerScope {
    pub fn is_admin(&self) -> bool {
        matches!(self, CallerScope::Local | CallerScope::Admin)
    }
}

/// Guard that rejects requests from non-admin callers.
pub struct AdminGuard;

impl Guard for AdminGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| async_graphql::Error::new("internal: missing caller scope"))?;
        if scope.is_admin() {
            Ok(())
        } else {
            Err(async_graphql::Error::new("Forbidden: admin scope required"))
        }
    }
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

/// Hash a password with scrypt for storage.
pub fn hash_password(password: &str) -> Result<String, String> {
    use scrypt::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
    let salt = SaltString::generate(&mut OsRng);
    let params = scrypt::Params::recommended();
    let hasher = scrypt::Scrypt;
    hasher
        .hash_password_customized(password.as_bytes(), None, None, params, &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("scrypt hash failed: {e}"))
}

/// Verify a password against a stored scrypt hash.
pub fn verify_password(password: &str, hash: &str) -> bool {
    use scrypt::password_hash::{PasswordHash, PasswordVerifier};
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    scrypt::Scrypt
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
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
        assert!(!CallerScope::Integration.is_admin());
    }

    #[test]
    fn password_hash_and_verify() {
        let hash = hash_password("hunter2").unwrap();
        assert!(hash.starts_with("$scrypt$"));
        assert!(verify_password("hunter2", &hash));
        assert!(!verify_password("wrong", &hash));
    }
}
