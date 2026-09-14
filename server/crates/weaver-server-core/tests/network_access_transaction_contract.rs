use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::Executor;
use weaver_server_core::Database;
use weaver_server_core::security::{
    SETTING_HTTP_BIND_ADDRESS, SETTING_SECURITY_POLICY_REVISION, SETTING_TRUSTED_NETWORKS,
    SETTING_TRUSTED_PROXIES,
};
use weaver_server_core::settings::persistence::{
    AuthenticatedNetworkAccessUpdate, NetworkBindAddressUpdate,
};

fn update() -> AuthenticatedNetworkAccessUpdate {
    AuthenticatedNetworkAccessUpdate {
        trusted_proxies_json: Some(r#"["10.0.0.5/32"]"#.to_string()),
        trusted_networks_json: Some(r#"["203.0.113.0/24"]"#.to_string()),
        bind_address: NetworkBindAddressUpdate::Set("0.0.0.0".to_string()),
    }
}

fn seed(db: &Database) {
    db.set_setting(SETTING_TRUSTED_NETWORKS, r#"["198.51.100.0/24"]"#)
        .unwrap();
    db.set_setting(SETTING_HTTP_BIND_ADDRESS, "127.0.0.1")
        .unwrap();
    db.set_setting(SETTING_TRUSTED_PROXIES, r#"["192.0.2.5/32"]"#)
        .unwrap();
    db.set_setting(SETTING_SECURITY_POLICY_REVISION, "legacy-v1")
        .unwrap();
}

fn assert_rolled_back(db: &Database) {
    assert_eq!(
        db.get_setting(SETTING_TRUSTED_NETWORKS).unwrap().as_deref(),
        Some(r#"["198.51.100.0/24"]"#)
    );
    assert_eq!(
        db.get_setting(SETTING_HTTP_BIND_ADDRESS)
            .unwrap()
            .as_deref(),
        Some("127.0.0.1")
    );
    assert_eq!(
        db.get_setting(SETTING_TRUSTED_PROXIES).unwrap().as_deref(),
        Some(r#"["192.0.2.5/32"]"#)
    );
    assert_eq!(
        db.get_setting(SETTING_SECURITY_POLICY_REVISION)
            .unwrap()
            .as_deref(),
        Some("legacy-v1")
    );
}

#[test]
fn sqlite_network_policy_failure_rolls_back_every_setting() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("network-policy.db");
    let db = Database::open(&path).unwrap();
    seed(&db);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let url = format!("sqlite://{}", path.display());
    runtime.block_on(async {
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        pool.execute(
            "CREATE TRIGGER reject_network_policy_revision
             BEFORE INSERT ON settings
             WHEN NEW.key = 'security_policy_revision'
             BEGIN SELECT RAISE(ABORT, 'injected rollback'); END",
        )
        .await
        .unwrap();
        pool.close().await;
    });
    assert!(db.update_authenticated_network_access(&update()).is_err());
    assert_rolled_back(&db);
}

#[test]
fn postgres_network_policy_failure_rolls_back_every_setting() {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        return;
    };
    if base_url.trim().is_empty() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let pool = runtime
        .block_on(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&base_url),
        )
        .unwrap();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("network_policy_contract_{}_{}", std::process::id(), suffix);
    runtime
        .block_on(
            sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}"))).execute(&pool),
        )
        .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
    let old_url = std::env::var_os("WEAVER_DATABASE_URL");
    unsafe { std::env::set_var("WEAVER_DATABASE_URL", &url) };
    let directory = tempfile::tempdir().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let db = weaver_server_core::persistence::open_database(directory.path()).unwrap();
        seed(&db);
        runtime.block_on(sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE FUNCTION {schema}.reject_network_policy_revision() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF NEW.key = 'security_policy_revision' THEN RAISE EXCEPTION 'injected rollback'; END IF;
               RETURN NEW;
             END;
             $$"
        ))).execute(&pool)).unwrap();
        runtime
            .block_on(
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "CREATE TRIGGER reject_network_policy_revision
             BEFORE INSERT OR UPDATE ON {schema}.settings
             FOR EACH ROW EXECUTE FUNCTION {schema}.reject_network_policy_revision()"
                )))
                .execute(&pool),
            )
            .unwrap();
        assert!(db.update_authenticated_network_access(&update()).is_err());
        assert_rolled_back(&db);
    }));
    unsafe {
        match old_url {
            Some(value) => std::env::set_var("WEAVER_DATABASE_URL", value),
            None => std::env::remove_var("WEAVER_DATABASE_URL"),
        }
    }
    runtime
        .block_on(
            sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
                .execute(&pool),
        )
        .unwrap();
    runtime.block_on(pool.close());
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
