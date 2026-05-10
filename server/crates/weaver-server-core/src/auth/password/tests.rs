use super::*;

#[test]
fn password_hash_verification() {
    let hash = hash_password("hunter2").unwrap();
    assert!(verify_password("hunter2", &hash));
    assert!(!verify_password("wrong", &hash));
}

#[test]
fn reject_unknown_hash_format() {
    assert!(!verify_password("hunter2", "not-a-phc-hash"));
    assert!(!verify_password(
        "hunter2",
        "$scrypt$ln=16,r=8,p=1$MDAwMDAwMDAwMDAwMDAwMA$MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA"
    ));
    assert!(!verify_password("hunter2", "$bcrypt$v=2b$bad"));
}
