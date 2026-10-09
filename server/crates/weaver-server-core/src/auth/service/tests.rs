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

fn run_claims(exp: u64) -> ScriptRunClaims {
    ScriptRunClaims {
        run_id: "run-1".into(),
        instance_id: "instance-1".into(),
        job_id: Some(42),
        exp,
    }
}

#[test]
fn a_script_run_token_says_which_run_it_is_for() {
    let secret = [5u8; 32];
    let claims = run_claims(u64::MAX);
    let token = create_script_run_jwt(&claims, &secret);
    assert!(is_signed_token_shape(&token));
    assert_eq!(verify_script_run_jwt(&token, &secret).unwrap(), claims);

    let jobless = ScriptRunClaims {
        job_id: None,
        ..claims
    };
    let token = create_script_run_jwt(&jobless, &secret);
    assert_eq!(verify_script_run_jwt(&token, &secret).unwrap(), jobless);

    assert!(matches!(
        verify_script_run_jwt(&token, &[6u8; 32]),
        Err(JwtError::InvalidSignature)
    ));
    assert!(matches!(
        verify_script_run_jwt(&create_script_run_jwt(&run_claims(0), &secret), &secret),
        Err(JwtError::Expired)
    ));
}

#[test]
fn a_login_token_and_a_script_run_token_are_never_taken_for_each_other() {
    let secret = [8u8; 32];
    let login = create_jwt("admin", &secret, 3600);
    let run = create_script_run_jwt(&run_claims(u64::MAX), &secret);
    assert!(matches!(
        verify_script_run_jwt(&login, &secret),
        Err(JwtError::InvalidClaims(_))
    ));
    assert!(matches!(
        verify_jwt(&run, &secret),
        Err(JwtError::InvalidClaims(_))
    ));
}

#[test]
fn only_a_token_this_server_could_have_signed_has_its_shape() {
    for other in [
        "",
        "wvr_0123456789abcdef0123456789abcdef",
        "a.b.c",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..signature",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature.more",
    ] {
        assert!(!is_signed_token_shape(other), "{other}");
    }
    assert!(is_signed_token_shape(&create_jwt("admin", &[9u8; 32], 60)));
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

#[test]
fn setup_codes_are_two_hyphenated_groups_of_three() {
    for _ in 0..64 {
        let code = generate_setup_code();
        assert!(is_setup_code(&code), "{code}");
        assert_eq!(code.as_bytes()[3], b'-', "{code}");
        assert_eq!(normalize_setup_code(&code).len(), SETUP_CODE_LENGTH);
    }
    assert!(!is_setup_code("K7PM2X"));
    assert!(!is_setup_code("K7P-M2"));
    assert!(!is_setup_code("K0P-M2X"));
}

#[test]
fn setup_codes_compare_without_case_hyphen_or_spacing() {
    assert_eq!(normalize_setup_code(" k7p-m2x "), "K7PM2X");
    assert_eq!(normalize_setup_code("K7P M2X"), "K7PM2X");
    assert_eq!(normalize_setup_code("K7PM2X"), "K7PM2X");
}

#[test]
fn setup_code_is_found_in_a_banner_row_or_a_json_message() {
    assert_eq!(
        find_setup_code(&format!("{SETUP_CODE_MARKER}K7P-M2X")),
        Some("K7P-M2X")
    );
    assert_eq!(
        find_setup_code(&format!("#   {SETUP_CODE_MARKER}K7P-M2X        #")),
        Some("K7P-M2X")
    );
    assert_eq!(
        find_setup_code(&format!(
            r#"{{"fields":{{"message":"{SETUP_CODE_MARKER}K7P-M2X. Enter it"}}}}"#
        )),
        Some("K7P-M2X")
    );
    assert_eq!(
        find_setup_code(&format!("{SETUP_CODE_MARKER}K7P-M2XY")),
        None
    );
    assert_eq!(find_setup_code(&format!("{SETUP_CODE_MARKER}K7PM2X")), None);
    assert_eq!(find_setup_code(&format!("{SETUP_CODE_MARKER}K7P")), None);
    assert_eq!(find_setup_code("unrelated setup code"), None);
}
