//! Encryption at rest for sensitive values (NNTP passwords, RSS credentials).
//!
//! Uses AES-256-GCM with a 32-byte master key stored in platform-native secure storage.
//! Encrypted values use the format `enc:v1:<base64(nonce || ciphertext || tag)>`.
//! Values without this prefix pass through unchanged (backward compatibility).

pub(crate) mod keystore;

mod key_file;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::{Engine, engine::general_purpose::STANDARD};

use std::path::PathBuf;

const ENCRYPTED_PREFIX: &str = "enc:v1:";
const NONCE_LEN: usize = 12;

/// A 32-byte AES-256-GCM key for encrypting/decrypting sensitive values at rest.
#[derive(Clone)]
pub struct EncryptionKey {
    key_bytes: [u8; 32],
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("key_bytes", &"[REDACTED]")
            .finish()
    }
}

impl EncryptionKey {
    pub fn from_base64(encoded: &str) -> Result<Self, String> {
        let decoded = STANDARD
            .decode(encoded.trim())
            .map_err(|e| format!("invalid base64: {e}"))?;
        if decoded.len() != 32 {
            return Err(format!(
                "encryption key must be exactly 32 bytes, got {}",
                decoded.len()
            ));
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&decoded);
        Ok(Self { key_bytes: bytes })
    }

    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self { key_bytes: bytes }
    }

    pub fn to_base64(&self) -> String {
        STANDARD.encode(self.key_bytes)
    }
}

/// Encrypt a plaintext string. Returns `enc:v1:<base64(nonce || ciphertext || tag)>`.
pub fn encrypt_value(key: &EncryptionKey, plaintext: &str) -> Result<String, String> {
    let cipher = Aes256Gcm::new_from_slice(&key.key_bytes)
        .map_err(|e| format!("failed to create cipher: {e}"))?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| format!("encryption failed: {e}"))?;

    let mut combined = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ciphertext);

    Ok(format!("{ENCRYPTED_PREFIX}{}", STANDARD.encode(&combined)))
}

/// Decrypt a stored value. If it doesn't have the `enc:v1:` prefix, return as-is (plaintext passthrough).
pub fn decrypt_value(key: &EncryptionKey, stored: &str) -> Result<String, String> {
    let Some(encoded) = stored.strip_prefix(ENCRYPTED_PREFIX) else {
        return Ok(stored.to_string());
    };

    let combined = STANDARD
        .decode(encoded.trim())
        .map_err(|e| format!("invalid base64 in encrypted value: {e}"))?;

    if combined.len() < NONCE_LEN + 16 {
        // 16 = GCM tag length
        return Err("encrypted value too short".to_string());
    }

    let (nonce_bytes, ciphertext) = combined.split_at(NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = Aes256Gcm::new_from_slice(&key.key_bytes)
        .map_err(|e| format!("failed to create cipher: {e}"))?;

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| "decryption failed (wrong key or corrupted data)".to_string())?;

    String::from_utf8(plaintext).map_err(|e| format!("decrypted value is not valid UTF-8: {e}"))
}

/// Check if a value is encrypted (has the `enc:v1:` prefix).
pub fn is_encrypted(value: &str) -> bool {
    value.starts_with(ENCRYPTED_PREFIX)
}

/// Encrypt a value if it's not already encrypted. Returns as-is if already encrypted or empty/None.
pub(crate) fn maybe_encrypt(
    key: Option<&EncryptionKey>,
    value: &Option<String>,
) -> Result<Option<String>, String> {
    let Some(v) = value.as_deref() else {
        return Ok(None);
    };
    if v.is_empty() || is_encrypted(v) {
        return Ok(Some(v.to_string()));
    }
    let Some(key) = key else {
        return Err("encryption key is required to store secrets".to_string());
    };
    encrypt_value(key, v).map(Some)
}

/// Encrypt a secret for new writes. Plaintext compatibility is read-only: callers
/// that store new secrets should fail if no encryption key is available.
pub(crate) fn encrypt_secret_for_write(
    key: Option<&EncryptionKey>,
    value: &Option<String>,
) -> Result<Option<String>, String> {
    let Some(v) = value.as_deref() else {
        return Ok(None);
    };
    if v.is_empty() || is_encrypted(v) {
        return Ok(Some(v.to_string()));
    }
    let Some(key) = key else {
        return Err("encryption key is required to store secrets".to_string());
    };
    encrypt_value(key, v).map(Some)
}

/// Decrypt a value if it's encrypted. Returns as-is if not encrypted or empty/None.
pub(crate) fn maybe_decrypt(key: Option<&EncryptionKey>, value: Option<String>) -> Option<String> {
    let v = value?;
    if v.is_empty() || !is_encrypted(&v) {
        return Some(v);
    }
    let Some(key) = key else {
        return Some(v);
    };
    match decrypt_value(key, &v) {
        Ok(decrypted) => Some(decrypted),
        Err(e) => {
            tracing::warn!("failed to decrypt value: {e}");
            Some(v)
        }
    }
}

/// Ensure an encryption master key is available.
///
/// Priority:
/// 1. `WEAVER_ENCRYPTION_KEY` env var explicit override
/// 2. Platform keystores (Docker secret, OS keychain, key file)
/// 3. Auto-generate an in-memory ephemeral key, warn
pub fn ensure_encryption_key(data_dir: Option<PathBuf>) -> Result<EncryptionKey, String> {
    // 1. Env var explicit override. Non-interactive tooling and profiling
    // should be able to avoid platform keychain prompts entirely.
    if let Ok(env_key) = std::env::var("WEAVER_ENCRYPTION_KEY") {
        let trimmed = env_key.trim().to_string();
        if !trimmed.is_empty() {
            let key = EncryptionKey::from_base64(&trimmed)
                .map_err(|e| format!("invalid WEAVER_ENCRYPTION_KEY: {e}"))?;
            tracing::info!("using encryption master key from WEAVER_ENCRYPTION_KEY");
            return Ok(key);
        }
    }

    let stores = keystore::platform_keystores(data_dir);

    // 2. Persistent platform keystores
    for store in &stores {
        match store.get_key() {
            Ok(Some(key_b64)) => {
                let key = EncryptionKey::from_base64(&key_b64)
                    .map_err(|e| format!("invalid key in {}: {e}", store.name()))?;
                tracing::info!("using encryption master key from {}", store.name());
                return Ok(key);
            }
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!("could not read from {}: {e}", store.name());
                continue;
            }
        }
    }

    // 3. Ephemeral in-memory key. Do not persist this automatically.
    let key = EncryptionKey::generate();
    tracing::warn!(
        "generated ephemeral encryption master key (in memory only); configure the platform \
         keystore or set WEAVER_ENCRYPTION_KEY as an escape hatch to decrypt secrets across \
         restarts"
    );
    Ok(key)
}

#[cfg(test)]
mod tests;
