use super::*;
use argon2::password_hash::{PasswordHasher, SaltString};
use scrypt::password_hash::rand_core::OsRng;

fn fixed_scrypt_hash(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    scrypt::Scrypt
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

#[test]
fn password_hash_verification() {
    let hash = hash_password("hunter2").unwrap();
    assert!(verify_password("hunter2", &hash));
    assert!(!verify_password("wrong", &hash));
    assert!(!needs_rehash(&hash));
}

#[test]
fn legacy_scrypt_verification() {
    let hash = fixed_scrypt_hash("hunter2");
    assert!(verify_password("hunter2", &hash));
    assert!(!verify_password("wrong", &hash));
    assert!(needs_rehash(&hash));
}

#[test]
fn reject_unknown_hash_format() {
    assert!(!verify_password("hunter2", "not-a-phc-hash"));
    assert!(!verify_password("hunter2", "$bcrypt$v=2b$bad"));
    assert!(!needs_rehash("not-a-phc-hash"));
}
