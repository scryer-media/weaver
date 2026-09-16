use super::*;

#[test]
fn socks5_password_requires_username() {
    let mut p = profile(1);
    assert!(p.validate().is_ok());
    p.secrets.password = Some("fixture-password".into());
    assert!(
        p.validate()
            .unwrap_err()
            .to_string()
            .contains("requires a username")
    );
    p.secrets.username = Some(String::new());
    assert!(p.validate().is_err());
    p.secrets.username = Some("fixture-user".into());
    assert!(p.validate().is_ok());
    p.secrets.password = None;
    assert!(p.validate().is_ok());
}
