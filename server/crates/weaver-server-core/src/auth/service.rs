use hmac::{Hmac, Mac, digest::KeyInit};
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

#[derive(Debug, thiserror::Error)]
pub enum JwtSecretError {
    #[error("JWT signing secret must be 64 hex characters")]
    InvalidLength,
    #[error("JWT signing secret is not valid hex: {0}")]
    InvalidHex(#[from] hex::FromHexError),
}

/// Share a small CPU/memory budget across browser password operations.
/// Move the permit into the blocking closure so cancellation cannot free it early.
pub fn password_work_permit() -> Result<tokio::sync::OwnedSemaphorePermit, &'static str> {
    static WORK: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    WORK.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(2)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| "password verification is busy; try again shortly")
}

pub fn generate_api_key() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("getrandom failed");
    format!("wvr_{}", hex::encode(bytes))
}

/// Generate an opaque browser credential. It deliberately has no API-key
/// prefix, so browser cookies can never be mistaken for programmatic keys.
pub fn generate_browser_session_secret() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("getrandom failed");
    hex::encode(bytes)
}

/// Stable per-session CSRF value. Only a verifier is persisted; this value is
/// regenerated from the server secret after a browser reload or process restart.
pub fn derive_browser_csrf_token(session_token: &str, server_secret: &[u8; 32]) -> String {
    hex::encode(sign_hs256(
        server_secret,
        format!("browser-csrf-v1:{session_token}").as_bytes(),
    ))
}

pub fn hash_api_key(raw_key: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hasher.finalize().into()
}

pub fn generate_jwt_secret() -> [u8; 32] {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("getrandom failed");
    bytes
}

pub fn encode_jwt_secret(secret: &[u8; 32]) -> String {
    hex::encode(secret)
}

pub fn decode_jwt_secret(value: &str) -> Result<[u8; 32], JwtSecretError> {
    if value.len() != 64 {
        return Err(JwtSecretError::InvalidLength);
    }
    let bytes = hex::decode(value)?;
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| JwtSecretError::InvalidLength)
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
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(secret)
        .expect("HMAC-SHA256 accepts any key length");
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

/// Compare a browser CSRF verifier using the existing MAC implementation's
/// constant-time tag verification, without exposing a prefix comparison.
pub fn verify_browser_csrf_token(token: &str, verifier: &str) -> bool {
    let Ok(expected): Result<[u8; 32], _> = hex::decode(verifier).and_then(|bytes| {
        bytes
            .try_into()
            .map_err(|_| hex::FromHexError::InvalidStringLength)
    }) else {
        return false;
    };
    let domain = b"weaver-browser-csrf-verifier-v1";
    let expected_tag = sign_hs256(domain, &expected);
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(domain)
        .expect("HMAC-SHA256 accepts any key length");
    mac.update(&hash_api_key(token));
    mac.verify_slice(&expected_tag).is_ok()
}

pub const JWT_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[cfg(test)]
mod tests;
