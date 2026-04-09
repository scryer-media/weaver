use super::*;

#[test]
fn key_format() {
    let key = generate_api_key();
    assert!(key.starts_with("wvr_"));
    assert_eq!(key.len(), 4 + 32);
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
    let token = create_jwt("admin", &secret, 0);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert!(matches!(
        verify_jwt(&token, &secret),
        Err(JwtError::Expired)
    ));
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
