// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::{Database, StateError};

pub fn apply_pre_0_9_bind_compat_shim(
    db: &Database,
    pre_migration_schema_version: Option<i64>,
    env_bind_address: Option<&str>,
) -> Result<bool, StateError> {
    weaver_server_migrations::upgrade_compat::apply_pre_0_9_bind_compat_shim(
        db,
        pre_migration_schema_version,
        env_bind_address,
    )
}

#[cfg(test)]
mod tests {
    use crate::operations::recovery::credentials::StoredCredentialQueries;
    use crate::settings::persistence::SettingsPersistenceQueries;
    use crate::settings::queries::SettingsQueries;
    use crate::settings::service::SettingsServiceQueries;
    use std::net::{IpAddr, Ipv4Addr};
    use weaver_server_base::security::{SETTING_ACCESS_MODE, SETTING_HTTP_BIND_ADDRESS};
    use weaver_server_migrations::upgrade_compat::FIRST_0_9_MIGRATION_VERSION;

    use super::*;
    use weaver_server_base::security::{
        BindAddressSource, RuntimeSecurityConfig, resolve_bind_address,
    };

    // 0.8.3's last migration, the common upgrade case.
    const LEDGER_0_8_3: i64 = 39;
    // 0.7.8's last migration.
    const LEDGER_0_7_8: i64 = 37;

    struct Boot {
        shimmed: bool,
        security: RuntimeSecurityConfig,
    }

    // The serve command's startup order in miniature: the shim runs first,
    // then the stored setting is settled into the live config exactly as
    // startup settles it, so what the test observes is what the boot binds.
    fn boot(db: &Database, pre_migration_schema_version: Option<i64>, env: Option<&str>) -> Boot {
        let shimmed = apply_pre_0_9_bind_compat_shim(db, pre_migration_schema_version, env)
            .expect("shim ran");

        let mut security = RuntimeSecurityConfig::default();
        if let Some(value) = env {
            let (address, source) =
                resolve_bind_address(Some(value), None).expect("test env address parses");
            security.http_bind_address = address;
            security.bind_address_source = source;
        }
        security.apply_stored_bind_address(
            db.get_setting(SETTING_HTTP_BIND_ADDRESS)
                .expect("settings readable")
                .as_deref(),
        );
        Boot { shimmed, security }
    }

    fn stored_bind(db: &Database) -> Option<String> {
        db.get_setting(SETTING_HTTP_BIND_ADDRESS)
            .expect("settings readable")
    }

    fn wide() -> IpAddr {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }

    fn loopback() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    #[test]
    fn a_fresh_install_keeps_the_loopback_default() {
        let db = Database::open_in_memory().unwrap();

        let boot = boot(&db, None, None);

        assert!(!boot.shimmed);
        assert_eq!(stored_bind(&db), None);
        assert_eq!(boot.security.http_bind_address, loopback());
        assert_eq!(
            boot.security.bind_address_source,
            BindAddressSource::Default
        );
    }

    #[test]
    fn a_pre_0_9_upgrade_keeps_its_network_wide_bind_on_the_first_boot() {
        let db = Database::open_in_memory().unwrap();

        let boot = boot(&db, Some(LEDGER_0_8_3), None);

        assert!(boot.shimmed);
        assert_eq!(stored_bind(&db).as_deref(), Some("0.0.0.0"));
        // The point of the exercise: this boot already serves the whole
        // network, rather than the one after it.
        assert_eq!(boot.security.http_bind_address, wide());
        assert_eq!(
            boot.security.bind_address_source,
            BindAddressSource::Setting
        );
        // Reachability is preserved; the access question is still unanswered,
        // so the setup wizard must still be offered.
        assert_eq!(db.get_setting(SETTING_ACCESS_MODE).unwrap(), None);
        assert!(!boot.security.security_configured());
    }

    // The one test that does not hand the ledger state in: a database file
    // left at 0.8.3's last migration, opened for real, must report itself as a
    // pre-0.9 upgrade — which is the half of the mechanism a decision function
    // tested in isolation cannot prove.
    #[test]
    fn a_real_pre_0_9_database_file_reports_a_pre_0_9_ledger() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("weaver.db");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&format!("sqlite://{}?mode=rwc", path.display()))
                .await
                .expect("test database opens");
            let catalog = crate::schema_migrations::embedded_catalog().expect("catalog");
            let payload = crate::schema_migrations::embedded_payload_bytes().expect("payload");
            crate::schema_migrations::replay_catalog_into_fresh_db(
                &pool,
                &catalog,
                &payload,
                Some(39),
                true,
            )
            .await
            .expect("0.8.3-shaped database");
            pool.close().await;
        });
        drop(runtime);

        // Opening runs 0040-0042; the answer must still describe the ledger as
        // it was found, not as this open left it.
        let db = Database::open(&path).expect("database opens");
        assert_eq!(db.pre_migration_schema_version(), Some(39));

        let upgraded = boot(&db, db.pre_migration_schema_version(), None);
        assert!(upgraded.shimmed);
        assert_eq!(upgraded.security.http_bind_address, wide());

        // The same call on a database this process creates reports nothing,
        // which is what keeps a fresh install on loopback.
        let fresh = Database::open(&dir.path().join("fresh.db")).expect("database opens");
        assert_eq!(fresh.pre_migration_schema_version(), None);
        assert!(!boot(&fresh, fresh.pre_migration_schema_version(), None).shimmed);
    }

    #[test]
    fn published_homebrew_0_7_8_database_keeps_encryption_and_bind_on_upgrade() {
        // Produced by the published 0.7.8 Apple Silicon binary from the
        // plaintext below using this fixed 32-byte key. Keeping the old
        // ciphertext here makes this a cross-version compatibility fixture,
        // rather than a round trip through 0.9's own encryption code.
        const KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        const PLAINTEXT: &str = "brew-secret";
        const CIPHERTEXT: &str = "enc:v1:+91fp7tOWIhqdjBbuRoZ4YLAwScwPfE9KHfueKckM+AYfUeBCvNc";

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("weaver.db");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&format!("sqlite://{}?mode=rwc", path.display()))
                .await
                .expect("test database opens");
            let catalog = crate::schema_migrations::embedded_catalog().expect("catalog");
            let payload = crate::schema_migrations::embedded_payload_bytes().expect("payload");
            crate::schema_migrations::replay_catalog_into_fresh_db(
                &pool,
                &catalog,
                &payload,
                Some(LEDGER_0_7_8),
                true,
            )
            .await
            .expect("0.7.8-shaped database");
            sqlx::query(
                "INSERT INTO servers (id, host, port, tls, username, password) \
                 VALUES (1, 'news.example.invalid', 119, 0, 'brew-user', ?)",
            )
            .bind(CIPHERTEXT)
            .execute(&pool)
            .await
            .expect("0.7.8 encrypted server fixture");
            pool.close().await;
        });
        drop(runtime);

        let mut db = Database::open(&path).expect("0.7.8 database upgrades");
        assert_eq!(db.pre_migration_schema_version(), Some(LEDGER_0_7_8));

        let key = crate::persistence::encryption::EncryptionKey::from_base64(KEY)
            .expect("fixture key parses");
        db.validate_encrypted_credentials(&key)
            .expect("0.7.8 ciphertext decrypts after upgrade");
        db.set_encryption_key(key);
        let config = db.load_config().expect("upgraded config loads");
        assert_eq!(config.servers.len(), 1);
        assert_eq!(config.servers[0].password.as_deref(), Some(PLAINTEXT));

        let upgraded = boot(&db, db.pre_migration_schema_version(), None);
        assert!(upgraded.shimmed);
        assert_eq!(stored_bind(&db).as_deref(), Some("0.0.0.0"));
        assert_eq!(upgraded.security.http_bind_address, wide());
    }

    #[test]
    fn both_pre_0_9_release_lines_are_treated_alike() {
        for ledger in [LEDGER_0_7_8, LEDGER_0_8_3] {
            let db = Database::open_in_memory().unwrap();

            let boot = boot(&db, Some(ledger), None);

            assert!(boot.shimmed, "ledger {ledger} must be shim-eligible");
            assert_eq!(stored_bind(&db).as_deref(), Some("0.0.0.0"));
            assert_eq!(boot.security.http_bind_address, wide());
        }
    }

    #[test]
    fn a_stored_bind_address_is_never_overwritten() {
        let db = Database::open_in_memory().unwrap();
        db.set_setting(SETTING_HTTP_BIND_ADDRESS, "192.0.2.10")
            .unwrap();

        let boot = boot(&db, Some(LEDGER_0_8_3), None);

        assert!(!boot.shimmed);
        assert_eq!(stored_bind(&db).as_deref(), Some("192.0.2.10"));
        assert_eq!(
            boot.security.http_bind_address,
            "192.0.2.10".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn a_completed_setup_blocks_the_shim() {
        let db = Database::open_in_memory().unwrap();
        db.set_setting(SETTING_ACCESS_MODE, "login_required")
            .unwrap();

        let boot = boot(&db, Some(LEDGER_0_8_3), None);

        assert!(!boot.shimmed);
        assert_eq!(stored_bind(&db), None);
        assert_eq!(boot.security.http_bind_address, loopback());
    }

    #[test]
    fn an_environment_pinned_bind_blocks_the_shim() {
        let db = Database::open_in_memory().unwrap();

        let boot = boot(&db, Some(LEDGER_0_8_3), Some("127.0.0.1"));

        assert!(!boot.shimmed);
        // Nothing is written underneath the environment, so removing the
        // variable later restores the default rather than a value the operator
        // never chose.
        assert_eq!(stored_bind(&db), None);
        assert_eq!(boot.security.http_bind_address, loopback());
        assert_eq!(
            boot.security.bind_address_source,
            BindAddressSource::Environment
        );

        // Even a blank value counts as the operator configuring the address.
        let db = Database::open_in_memory().unwrap();
        assert!(!apply_pre_0_9_bind_compat_shim(&db, Some(LEDGER_0_8_3), Some("")).unwrap());
        assert_eq!(stored_bind(&db), None);
    }

    #[test]
    fn a_database_that_has_already_run_0_9_is_left_alone() {
        for ledger in [FIRST_0_9_MIGRATION_VERSION, 42] {
            let db = Database::open_in_memory().unwrap();

            let boot = boot(&db, Some(ledger), None);

            assert!(!boot.shimmed, "ledger {ledger} must not be shim-eligible");
            assert_eq!(stored_bind(&db), None);
            assert_eq!(boot.security.http_bind_address, loopback());
        }
    }

    #[test]
    fn the_shim_never_runs_a_second_time() {
        let db = Database::open_in_memory().unwrap();
        assert!(boot(&db, Some(LEDGER_0_8_3), None).shimmed);

        // The operator narrows the address in Settings afterwards. The ledger
        // still reports the pre-0.9 maximum on this boot only because the test
        // says so; a real second boot reports 42. Either way the stored answer
        // is the operator's, and re-widening it would be the worst outcome the
        // shim could have.
        db.set_setting(SETTING_HTTP_BIND_ADDRESS, "127.0.0.1")
            .unwrap();

        let second = boot(&db, Some(LEDGER_0_8_3), None);

        assert!(!second.shimmed);
        assert_eq!(stored_bind(&db).as_deref(), Some("127.0.0.1"));
        assert_eq!(second.security.http_bind_address, loopback());
        assert_eq!(
            second.security.bind_address_source,
            BindAddressSource::Setting
        );
    }
}
