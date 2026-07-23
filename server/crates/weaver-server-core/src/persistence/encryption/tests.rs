use super::*;

#[cfg(any(target_os = "linux", target_os = "macos"))]
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl EnvVarGuard {
    fn set(key: &'static str, value: String) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

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
    assert!(maybe_encrypt(Some(&key), &None).unwrap().is_none());
}

#[test]
fn maybe_encrypt_empty_returns_empty() {
    let key = EncryptionKey::generate();
    assert_eq!(
        maybe_encrypt(Some(&key), &Some(String::new())).unwrap(),
        Some(String::new())
    );
}

#[test]
fn maybe_encrypt_requires_key_for_plaintext() {
    assert!(maybe_encrypt(None, &Some("my-password".to_string())).is_err());
}

#[test]
fn maybe_encrypt_decrypt_round_trip() {
    let key = EncryptionKey::generate();
    let original = Some("my-password".to_string());
    let encrypted = maybe_encrypt(Some(&key), &original).unwrap();
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

#[test]
fn encrypt_secret_for_write_requires_key_for_plaintext() {
    let plaintext = Some("new-password".to_string());
    assert!(encrypt_secret_for_write(None, &plaintext).is_err());

    let key = EncryptionKey::generate();
    let encrypted = encrypt_secret_for_write(Some(&key), &plaintext).unwrap();
    assert!(encrypted.as_ref().unwrap().starts_with(ENCRYPTED_PREFIX));

    assert_eq!(encrypt_secret_for_write(None, &None).unwrap(), None);
    assert_eq!(
        encrypt_secret_for_write(None, &Some(String::new())).unwrap(),
        Some(String::new())
    );
    assert_eq!(
        encrypt_secret_for_write(None, &encrypted).unwrap(),
        encrypted
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn env_key_wins_over_persistent_store() {
    let _env_lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let stored_key = EncryptionKey::generate();
    let env_key = EncryptionKey::generate();
    let key_path = dir.path().join("encryption.key");
    std::fs::write(&key_path, stored_key.to_base64()).unwrap();

    let _env_guard = EnvVarGuard::set("WEAVER_ENCRYPTION_KEY", env_key.to_base64());

    let loaded_key = ensure_encryption_key(Some(dir.path().to_path_buf())).unwrap();

    assert_eq!(loaded_key.to_base64(), env_key.to_base64());
    assert_ne!(loaded_key.to_base64(), stored_key.to_base64());
    assert_eq!(
        std::fs::read_to_string(key_path).unwrap(),
        stored_key.to_base64()
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn generated_key_is_persisted_and_reused() {
    let _env_lock = ENV_LOCK.lock().unwrap();
    let _env_guard = EnvVarGuard::set("WEAVER_ENCRYPTION_KEY", String::new());
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("encryption.key");

    let generated = ensure_encryption_key(Some(dir.path().to_path_buf())).unwrap();
    assert_eq!(
        std::fs::read_to_string(&key_path).unwrap(),
        generated.to_base64()
    );

    let reloaded = ensure_encryption_key(Some(dir.path().to_path_buf())).unwrap();
    assert_eq!(reloaded.to_base64(), generated.to_base64());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(key_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn env_key_is_not_copied_to_key_file() {
    let _env_lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let env_key = EncryptionKey::generate();
    let _env_guard = EnvVarGuard::set("WEAVER_ENCRYPTION_KEY", env_key.to_base64());

    let loaded = ensure_encryption_key(Some(dir.path().to_path_buf())).unwrap();

    assert_eq!(loaded.to_base64(), env_key.to_base64());
    assert!(!dir.path().join("encryption.key").exists());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn empty_existing_key_file_is_not_overwritten() {
    let _env_lock = ENV_LOCK.lock().unwrap();
    let _env_guard = EnvVarGuard::set("WEAVER_ENCRYPTION_KEY", String::new());
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("encryption.key");
    std::fs::write(&key_path, "").unwrap();

    let error = ensure_encryption_key(Some(dir.path().to_path_buf())).unwrap_err();

    assert!(error.contains("is empty"));
    assert_eq!(std::fs::read_to_string(key_path).unwrap(), "");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn missing_key_is_not_replaced_over_encrypted_state() {
    let _env_lock = ENV_LOCK.lock().unwrap();
    let _env_guard = EnvVarGuard::set("WEAVER_ENCRYPTION_KEY", String::new());
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("encryption.key");

    let error = ensure_encryption_key_for_state(Some(dir.path().to_path_buf()), true).unwrap_err();

    assert!(error.contains("encrypted credentials exist"));
    assert!(error.contains("refusing to generate a replacement key"));
    assert!(!key_path.exists());
}

#[test]
fn persisted_credentials_validate_the_selected_key_before_startup() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = crate::Database::open(&directory.path().join("weaver.db")).unwrap();
    let persisted_key = EncryptionKey::generate();
    db.set_encryption_key(persisted_key.clone());
    db.insert_server(&crate::servers::ServerConfig {
        id: 1,
        host: "news.example.invalid".to_string(),
        port: 119,
        tls: false,
        username: Some("weaver".to_string()),
        password: Some("persisted-secret".to_string()),
        connections: 1,
        active: true,
        supports_pipelining: false,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: Default::default(),
        tls_ca_cert: None,
    })
    .unwrap();

    assert!(db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(&persisted_key).unwrap();

    let wrong_key = EncryptionKey::generate();
    let error = db.validate_encrypted_credentials(&wrong_key).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot decrypt persisted server credential 1")
    );
}

#[test]
fn persisted_jwt_secret_is_encrypted_state_and_rejects_the_wrong_key() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = crate::Database::open(&directory.path().join("weaver.db")).unwrap();
    let persisted_key = EncryptionKey::generate();
    db.set_encryption_key(persisted_key.clone());
    let persisted_secret = db.get_or_create_jwt_signing_secret().unwrap();

    assert!(db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(&persisted_key).unwrap();

    let wrong_key = EncryptionKey::generate();
    assert!(db.validate_encrypted_credentials(&wrong_key).is_err());
    assert_eq!(
        db.get_or_create_jwt_signing_secret().unwrap(),
        persisted_secret,
        "validation must not rotate persisted JWT state"
    );
}
