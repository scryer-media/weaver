//! Encryption at rest for sensitive values (NNTP passwords, RSS credentials).
//!
//! Uses AES-256-GCM with a 32-byte master key stored in platform-native secure storage.
//! Encrypted values use the format `enc:v1:<base64(nonce || ciphertext || tag)>`.
//! Values without this prefix pass through unchanged (backward compatibility).

pub(crate) mod keystore;

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
pub(crate) fn maybe_encrypt(key: Option<&EncryptionKey>, value: &Option<String>) -> Option<String> {
    let v = value.as_deref()?;
    if v.is_empty() || is_encrypted(v) {
        return Some(v.to_string());
    }
    let Some(key) = key else {
        return Some(v.to_string());
    };
    match encrypt_value(key, v) {
        Ok(encrypted) => Some(encrypted),
        Err(e) => {
            tracing::warn!("failed to encrypt value: {e}");
            Some(v.to_string())
        }
    }
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
/// 1. `WEAVER_ENCRYPTION_KEY` env var
/// 2. Platform keystores (Docker secret, OS keychain, key file)
/// 3. Auto-generate, store in best available keystore, warn
pub fn ensure_encryption_key(data_dir: Option<PathBuf>) -> Result<EncryptionKey, String> {
    let stores = keystore::platform_keystores(data_dir);

    // 1. Env var
    if let Ok(env_key) = std::env::var("WEAVER_ENCRYPTION_KEY") {
        let trimmed = env_key.trim().to_string();
        if !trimmed.is_empty() {
            let key = EncryptionKey::from_base64(&trimmed)
                .map_err(|e| format!("invalid WEAVER_ENCRYPTION_KEY: {e}"))?;
            opportunistic_store(&stores, &key);
            tracing::info!("using encryption master key from WEAVER_ENCRYPTION_KEY");
            return Ok(key);
        }
    }

    // 2. Platform keystores
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

    // 3. Auto-generate
    let key = EncryptionKey::generate();
    let stored_in = try_store_new_key(&stores, &key);
    match stored_in {
        Some(name) => {
            tracing::warn!(
                "generated new encryption master key and stored in {name} — \
                 all sensitive settings (passwords) are encrypted with this key"
            );
        }
        None => {
            tracing::warn!(
                "generated new encryption master key (in memory only) — \
                 set WEAVER_ENCRYPTION_KEY to persist it across restarts\n\n  \
                 WEAVER_ENCRYPTION_KEY={}\n",
                key.to_base64()
            );
        }
    }
    Ok(key)
}

fn try_store_new_key(
    stores: &[Box<dyn keystore::KeyStore>],
    key: &EncryptionKey,
) -> Option<&'static str> {
    for store in stores {
        match store.set_key(&key.to_base64()) {
            Ok(()) => return Some(store.name()),
            Err(e) => {
                tracing::debug!("could not store key in {}: {e}", store.name());
                continue;
            }
        }
    }
    None
}

fn opportunistic_store(stores: &[Box<dyn keystore::KeyStore>], key: &EncryptionKey) {
    let key_b64 = key.to_base64();
    for store in stores {
        match store.get_key() {
            Ok(None) => match store.set_key(&key_b64) {
                Ok(()) => {
                    tracing::info!("copied encryption key to {}", store.name());
                    return;
                }
                Err(_) => continue,
            },
            Ok(Some(existing)) if existing == key_b64 => return,
            Ok(Some(_)) => match store.set_key(&key_b64) {
                Ok(()) => {
                    tracing::info!("updated stale encryption key in {}", store.name());
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "{} has a different encryption key but could not be updated: {e}",
                        store.name()
                    );
                    continue;
                }
            },
            Err(_) => continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_round_trip() {
        let key = EncryptionKey::generate();
        let plaintext = "secret-nntp-password";
        let encrypted = encrypt_value(&key, plaintext).unwrap();

        assert!(encrypted.starts_with(ENCRYPTED_PREFIX));
        assert_ne!(encrypted, plaintext);

        let decrypted = decrypt_value(&key, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn plaintext_passthrough() {
        let key = EncryptionKey::generate();
        let plaintext = "not-encrypted-value";
        let result = decrypt_value(&key, plaintext).unwrap();
        assert_eq!(result, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = EncryptionKey::generate();
        let key2 = EncryptionKey::generate();
        let encrypted = encrypt_value(&key1, "secret").unwrap();
        let result = decrypt_value(&key2, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn key_base64_round_trip() {
        let key = EncryptionKey::generate();
        let encoded = key.to_base64();
        let decoded = EncryptionKey::from_base64(&encoded).unwrap();
        assert_eq!(key.key_bytes, decoded.key_bytes);
    }

    #[test]
    fn empty_string_encrypts() {
        let key = EncryptionKey::generate();
        let encrypted = encrypt_value(&key, "").unwrap();
        let decrypted = decrypt_value(&key, &encrypted).unwrap();
        assert_eq!(decrypted, "");
    }

    #[test]
    fn is_encrypted_detection() {
        assert!(is_encrypted("enc:v1:abc123"));
        assert!(!is_encrypted("plain-value"));
        assert!(!is_encrypted(""));
    }

    #[test]
    fn maybe_encrypt_none_returns_none() {
        let key = EncryptionKey::generate();
        assert!(maybe_encrypt(Some(&key), &None).is_none());
    }

    #[test]
    fn maybe_encrypt_empty_returns_empty() {
        let key = EncryptionKey::generate();
        assert_eq!(
            maybe_encrypt(Some(&key), &Some(String::new())),
            Some(String::new())
        );
    }

    #[test]
    fn maybe_encrypt_decrypt_round_trip() {
        let key = EncryptionKey::generate();
        let original = Some("my-password".to_string());
        let encrypted = maybe_encrypt(Some(&key), &original);
        assert!(encrypted.as_ref().unwrap().starts_with(ENCRYPTED_PREFIX));
        let decrypted = maybe_decrypt(Some(&key), encrypted);
        assert_eq!(decrypted, original);
    }

    #[test]
    fn maybe_decrypt_no_key_returns_as_is() {
        let val = Some("enc:v1:something".to_string());
        let result = maybe_decrypt(None, val.clone());
        assert_eq!(result, val);
    }
}
