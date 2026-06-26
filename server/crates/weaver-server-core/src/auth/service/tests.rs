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
    let secret = [7u8; 32];
    let token = create_jwt("admin", &secret, 3600);
    let claims = verify_jwt(&token, &secret).unwrap();
    assert_eq!(claims.sub, "admin");
}

#[test]
fn wrong_secret_fails() {
    let secret1 = [1u8; 32];
    let secret2 = [2u8; 32];
    let token = create_jwt("admin", &secret1, 3600);
    assert!(matches!(
        verify_jwt(&token, &secret2),
        Err(JwtError::InvalidSignature)
    ));
}

#[test]
fn expired_token_fails() {
    let secret = [3u8; 32];
    let token = create_jwt("admin", &secret, 0);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert!(matches!(
        verify_jwt(&token, &secret),
        Err(JwtError::Expired)
    ));
}

#[test]
fn malformed_token_fails() {
    let secret = [4u8; 32];
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
fn jwt_secret_hex_roundtrip() {
    let secret = generate_jwt_secret();
    let encoded = encode_jwt_secret(&secret);
    assert_eq!(encoded.len(), 64);
    assert_eq!(decode_jwt_secret(&encoded).unwrap(), secret);
}

#[test]
fn jwt_secret_decode_rejects_malformed_values() {
    assert!(matches!(
        decode_jwt_secret("short"),
        Err(JwtSecretError::InvalidLength)
    ));
    assert!(matches!(
        decode_jwt_secret(&"z".repeat(64)),
        Err(JwtSecretError::InvalidHex(_))
    ));
}
