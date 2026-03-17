//! Minimal HS256 JWT implementation for login authentication.
//!
//! No external JWT crate — HS256 is just HMAC-SHA256 over base64url-encoded
//! header and payload.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// JWT claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (username).
    pub sub: String,
    /// Issued at (Unix timestamp).
    pub iat: u64,
    /// Expiry (Unix timestamp).
    pub exp: u64,
}

/// JWT verification error.
#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("malformed token")]
    Malformed,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("token expired")]
    Expired,
    #[error("invalid claims: {0}")]
    InvalidClaims(String),
}

/// Derive the HMAC secret from the stored scrypt password hash.
///
/// SHA-256 of the hash string — deterministic, changes when the password
/// changes (which invalidates all existing JWTs).
pub fn derive_jwt_secret(password_hash: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(password_hash.as_bytes());
    hasher.finalize().into()
}

/// Create a signed HS256 JWT.
pub fn create_jwt(username: &str, secret: &[u8], ttl_secs: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let header = base64url_encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let claims = Claims {
        sub: username.to_string(),
        iat: now,
        exp: now + ttl_secs,
    };
    let payload = base64url_encode(&serde_json::to_vec(&claims).expect("claims serialization"));

    let signing_input = format!("{header}.{payload}");
    let signature = sign_hs256(secret, signing_input.as_bytes());
    let sig_b64 = base64url_encode(&signature);

    format!("{signing_input}.{sig_b64}")
}

/// Verify a signed HS256 JWT and return the claims.
pub fn verify_jwt(token: &str, secret: &[u8]) -> Result<Claims, JwtError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(JwtError::Malformed);
    }

    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let expected_sig = sign_hs256(secret, signing_input.as_bytes());
    let actual_sig = base64url_decode(parts[2]).map_err(|_| JwtError::Malformed)?;

    // Constant-time comparison.
    if !constant_time_eq(&expected_sig, &actual_sig) {
        return Err(JwtError::InvalidSignature);
    }

    let payload_bytes = base64url_decode(parts[1]).map_err(|_| JwtError::Malformed)?;
    let claims: Claims = serde_json::from_slice(&payload_bytes)
        .map_err(|e| JwtError::InvalidClaims(e.to_string()))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if claims.exp < now {
        return Err(JwtError::Expired);
    }

    Ok(claims)
}

fn sign_hs256(secret: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn base64url_encode(data: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    URL_SAFE_NO_PAD.encode(data)
}

fn base64url_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    URL_SAFE_NO_PAD.decode(s)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Default JWT lifetime: 30 days.
pub const JWT_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_create_verify() {
        let secret = derive_jwt_secret("$scrypt$fake_hash");
        let token = create_jwt("admin", &secret, 3600);
        let claims = verify_jwt(&token, &secret).unwrap();
        assert_eq!(claims.sub, "admin");
    }

    #[test]
    fn wrong_secret_fails() {
        let secret1 = derive_jwt_secret("hash1");
        let secret2 = derive_jwt_secret("hash2");
        let token = create_jwt("admin", &secret1, 3600);
        assert!(matches!(
            verify_jwt(&token, &secret2),
            Err(JwtError::InvalidSignature)
        ));
    }

    #[test]
    fn expired_token_fails() {
        let secret = derive_jwt_secret("test");
        let token = create_jwt("admin", &secret, 0); // 0 TTL = already expired
        // Sleep briefly to ensure we're past the expiry.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(matches!(verify_jwt(&token, &secret), Err(JwtError::Expired)));
    }

    #[test]
    fn malformed_token_fails() {
        let secret = derive_jwt_secret("test");
        assert!(matches!(
            verify_jwt("not.a.valid.token", &secret),
            Err(JwtError::Malformed)
        ));
        assert!(matches!(
            verify_jwt("only-one-part", &secret),
            Err(JwtError::Malformed)
        ));
    }

    #[test]
    fn derive_secret_changes_with_hash() {
        let s1 = derive_jwt_secret("hash_a");
        let s2 = derive_jwt_secret("hash_b");
        assert_ne!(s1, s2);
    }
}
