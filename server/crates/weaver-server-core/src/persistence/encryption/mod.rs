//! Encryption at rest for sensitive values (NNTP passwords, RSS credentials).
//!
//! Uses AES-256-GCM with a 32-byte master key stored in platform-native secure storage.
//! Encrypted values use the format `enc:v1:<base64(nonce || ciphertext || tag)>`.
//! Values without this prefix pass through unchanged (backward compatibility).

pub(crate) mod keystore;

#[cfg(any(target_os = "linux", target_os = "macos"))]
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
/// 3. Generate and create a key in a writable platform store, without overwriting
/// 4. Auto-generate an in-memory ephemeral key when no store supports creation
pub fn ensure_encryption_key(data_dir: Option<PathBuf>) -> Result<EncryptionKey, String> {
    ensure_encryption_key_for_state(data_dir, false)
}

/// Ensure a key is available without ever replacing a missing key when
/// encrypted credentials already exist. A fresh instance may create a key;
/// an existing encrypted instance must recover the original key or fail.
pub fn ensure_encryption_key_for_state(
    data_dir: Option<PathBuf>,
    encrypted_credentials_exist: bool,
) -> Result<EncryptionKey, String> {
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
                return Err(format!(
                    "failed to read encryption key from {}: {e}",
                    store.name()
                ));
            }
        }
    }

    if encrypted_credentials_exist {
        return Err(
            "encrypted credentials exist but no encryption key is available; refusing to \
             generate a replacement key"
                .to_string(),
        );
    }

    // 3. Persist a generated key only in stores with safe create-only semantics.
    // Environment variables and Docker secrets returned above and are never
    // copied into the data directory.
    let key = EncryptionKey::generate();
    let encoded = key.to_base64();
    for store in &stores {
        match store.create_key_if_absent(&encoded) {
            Ok(Some(persisted)) => {
                let key = EncryptionKey::from_base64(&persisted)
                    .map_err(|e| format!("invalid generated key in {}: {e}", store.name()))?;
                tracing::info!("persisted encryption master key in {}", store.name());
                return Ok(key);
            }
            Ok(None) => continue,
            Err(e) => {
                return Err(format!(
                    "failed to persist generated encryption key in {}: {e}",
                    store.name()
                ));
            }
        }
    }

    // 4. Platforms without a writable store retain the existing ephemeral fallback.
    tracing::warn!(
        "generated ephemeral encryption master key (in memory only); configure the platform \
         keystore or set WEAVER_ENCRYPTION_KEY as an escape hatch to decrypt secrets across \
         restarts"
    );
    Ok(key)
}

pub fn backup_key_source_name(data_dir: Option<PathBuf>, current_key: &EncryptionKey) -> String {
    let encoded = current_key.to_base64();
    if std::env::var("WEAVER_ENCRYPTION_KEY")
        .ok()
        .is_some_and(|value| value.trim() == encoded)
    {
        return "WEAVER_ENCRYPTION_KEY".into();
    }
    for store in keystore::platform_keystores(data_dir) {
        if store
            .get_key()
            .ok()
            .flatten()
            .is_some_and(|value| value.trim() == encoded)
        {
            return store.name().to_string();
        }
    }
    "ephemeral".into()
}

pub fn validate_restore_encryption_key(
    data_dir: Option<PathBuf>,
    restored_key: &str,
) -> Result<String, String> {
    let restored = EncryptionKey::from_base64(restored_key)?.to_base64();
    if let Ok(value) = std::env::var("WEAVER_ENCRYPTION_KEY")
        && !value.trim().is_empty()
    {
        return if value.trim() == restored {
            Ok("WEAVER_ENCRYPTION_KEY (matched)".into())
        } else {
            Err("WEAVER_ENCRYPTION_KEY does not match the backup key".into())
        };
    }

    let stores = keystore::platform_keystores(data_dir);
    for store in &stores {
        if let Some(existing) = store.get_key()? {
            if store.can_replace() {
                return Ok(format!("{} (replace)", store.name()));
            }
            return if existing.trim() == restored {
                Ok(format!("{} (matched)", store.name()))
            } else {
                Err(format!("{} does not match the backup key", store.name()))
            };
        }
    }
    stores
        .iter()
        .find(|store| store.can_replace())
        .map(|store| format!("{} (create)", store.name()))
        .ok_or_else(|| "no persistent Weaver-managed encryption keystore is available".to_string())
}

pub fn promote_restore_encryption_key(
    data_dir: Option<PathBuf>,
    restored_key: &str,
) -> Result<EncryptionKey, String> {
    let restored = EncryptionKey::from_base64(restored_key)?;
    let encoded = restored.to_base64();
    if let Ok(value) = std::env::var("WEAVER_ENCRYPTION_KEY")
        && !value.trim().is_empty()
    {
        if value.trim() == encoded {
            return Ok(restored);
        }
        return Err("WEAVER_ENCRYPTION_KEY does not match the backup key".into());
    }

    let stores = keystore::platform_keystores(data_dir);
    for store in &stores {
        if let Some(existing) = store.get_key()? {
            if existing.trim() == encoded {
                return Ok(restored);
            }
            if store.replace_key(&encoded)? {
                return Ok(restored);
            }
            return Err(format!(
                "{} is externally managed and does not match",
                store.name()
            ));
        }
    }
    for store in &stores {
        if store.replace_key(&encoded)? {
            return Ok(restored);
        }
    }
    Err("no persistent Weaver-managed encryption keystore is available".into())
}

#[cfg(test)]
mod tests;
