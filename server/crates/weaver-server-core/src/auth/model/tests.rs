use super::*;

#[test]
fn scope_permissions() {
    assert!(CallerScope::Local.is_admin());
    assert!(CallerScope::Admin.is_admin());
    assert!(!CallerScope::Read.is_admin());
    assert!(!CallerScope::Control.is_admin());
    assert!(CallerScope::Read.can_read());
    assert!(!CallerScope::Read.can_control());
    assert!(CallerScope::Control.can_control());
    assert!(CallerScope::Admin.can_control());
}

#[test]
fn login_auth_cache_roundtrip() {
    let cache = LoginAuthCache::default();
    assert!(cache.snapshot().is_none());

    let auth = CachedLoginAuth::new("admin", "$argon2id$hash");
    cache.replace(Some(auth.clone()));
    assert_eq!(cache.snapshot(), Some(auth));

    cache.clear();
    assert!(cache.snapshot().is_none());
}
