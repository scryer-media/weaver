use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iat: u64,
    pub exp: u64,
}

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

pub fn generate_api_key() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("getrandom failed");
    format!("wvr_{}", hex::encode(bytes))
}

pub fn hash_api_key(raw_key: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hasher.finalize().into()
}

pub fn derive_jwt_secret(password_hash: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(password_hash.as_bytes());
    hasher.finalize().into()
}

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

pub fn verify_jwt(token: &str, secret: &[u8]) -> Result<Claims, JwtError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(JwtError::Malformed);
    }

    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let expected_sig = sign_hs256(secret, signing_input.as_bytes());
    let actual_sig = base64url_decode(parts[2]).map_err(|_| JwtError::Malformed)?;

    if !constant_time_eq(&expected_sig, &actual_sig) {
        return Err(JwtError::InvalidSignature);
    }

    let payload_bytes = base64url_decode(parts[1]).map_err(|_| JwtError::Malformed)?;
    let claims: Claims = serde_json::from_slice(&payload_bytes)
        .map_err(|error| JwtError::InvalidClaims(error.to_string()))?;

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
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn base64url_encode(data: &[u8]) -> String {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    URL_SAFE_NO_PAD.encode(data)
}

fn base64url_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
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

pub const JWT_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[cfg(test)]
mod tests;
