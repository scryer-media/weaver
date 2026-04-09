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
