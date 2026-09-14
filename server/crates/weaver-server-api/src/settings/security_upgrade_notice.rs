//! The one-time notice for an install still on the access settings from
//! before 0.12.0.
//!
//! Upgrading never changes who gets in, so such an install carries on exactly
//! as before and nothing in the interface says a simpler model exists, or that
//! moving to it is a restart with one environment variable. The notice says so
//! once. Whether it has been seen is kept here rather than in the browser, so
//! a second browser is not told again; moving the install to the new model
//! ends it too, because the notice has nothing left to say.

use async_graphql::SimpleObject;
use weaver_server_core::Database;
use weaver_server_core::auth::LoginAuthCache;
use weaver_server_core::runtime::environment::detect_runtime_environment;
use weaver_server_core::security::RuntimeSecurityConfig;

use crate::observability::spawn_blocking_db;

const SETTING_SECURITY_UPGRADE_NOTICE: &str = "security_upgrade_notice";
const NOTICE_DISMISSED: &str = "dismissed";

#[derive(Debug, Clone, SimpleObject)]
pub struct SecurityUpgradeNotice {
    /// Whether the browser should show the notice.
    pub pending: bool,
    /// `native`, `docker` or `container`: where the variable has to be set.
    pub deployment: String,
    /// `linux`, `macos`, `windows` or `unknown`.
    pub operating_system: String,
    /// Whether a login exists. Without one, moving opens setup to create it.
    pub login_enabled: bool,
}

fn pending(legacy_access: bool, stored: Option<&str>) -> bool {
    legacy_access && stored != Some(NOTICE_DISMISSED)
}

fn notice(pending: bool, auth_cache: &LoginAuthCache) -> SecurityUpgradeNotice {
    let environment = detect_runtime_environment();
    SecurityUpgradeNotice {
        pending,
        deployment: environment.deployment.as_str().to_string(),
        operating_system: environment.operating_system.as_str().to_string(),
        login_enabled: auth_cache.snapshot().is_some(),
    }
}

pub(crate) async fn status(
    db: &Database,
    security: &RuntimeSecurityConfig,
    auth_cache: &LoginAuthCache,
) -> async_graphql::Result<SecurityUpgradeNotice> {
    let legacy_access = !security.authenticated_access_mode();
    let stored = if legacy_access {
        let db = db.clone();
        spawn_blocking_db("settings.security_upgrade_notice.read", move || {
            db.get_setting(SETTING_SECURITY_UPGRADE_NOTICE)
        })
        .await?
    } else {
        None
    };
    Ok(notice(
        pending(legacy_access, stored.as_deref()),
        auth_cache,
    ))
}

pub(crate) async fn dismiss(
    db: &Database,
    auth_cache: &LoginAuthCache,
) -> async_graphql::Result<SecurityUpgradeNotice> {
    let db = db.clone();
    spawn_blocking_db("settings.security_upgrade_notice.write", move || {
        db.set_setting(SETTING_SECURITY_UPGRADE_NOTICE, NOTICE_DISMISSED)
    })
    .await?;
    Ok(notice(false, auth_cache))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_install_on_the_older_settings_is_told() {
        assert!(pending(true, None));
        assert!(!pending(false, None));
    }

    #[test]
    fn a_dismissed_notice_never_returns() {
        assert!(!pending(true, Some(NOTICE_DISMISSED)));
    }
}
