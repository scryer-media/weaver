use std::time::{SystemTime, UNIX_EPOCH};

use weaver_server_core::Database;
use weaver_server_core::auth::hash_api_key;
use weaver_server_core::auth::repository::BrowserSession;

fn session(label: &str, remembered: bool) -> BrowserSession {
    BrowserSession {
        token_hash: hex::encode(hash_api_key(label)),
        csrf_verifier: hex::encode(hash_api_key(&format!("csrf-{label}"))),
        origin: "https://weaver.example.test".into(),
        client_ip: Some("192.168.1.50".into()),
        remembered,
        created_at: 1_000,
        expires_at: 2_000,
        revoked_at: None,
    }
}

fn assert_session_lifecycle(db: &Database) {
    let first = session("first-browser", true);
    let second = session("second-browser", false);
    db.create_browser_session(&first).unwrap();
    db.create_browser_session(&second).unwrap();

    assert_eq!(
        db.get_active_browser_session(&first.token_hash, 1_999)
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        db.get_active_browser_session(&second.token_hash, 1_999)
            .unwrap(),
        Some(second.clone())
    );
    assert!(
        db.get_active_browser_session("first-browser", 1_500)
            .unwrap()
            .is_none(),
        "lookup requires the stored verifier, never a raw credential"
    );
    assert!(
        db.get_active_browser_session(&first.token_hash, 2_000)
            .unwrap()
            .is_none(),
        "a session expires at its boundary, not one second afterward"
    );

    let mut collision = first.clone();
    collision.origin = "https://attacker.example.test".into();
    collision.expires_at = 9_000;
    assert!(db.create_browser_session(&collision).is_err());
    assert_eq!(
        db.get_active_browser_session(&first.token_hash, 1_500)
            .unwrap(),
        Some(first.clone()),
        "a duplicate credential must not replace origin or expiry"
    );

    db.revoke_browser_session(&first.token_hash, 1_500).unwrap();
    db.revoke_browser_session(&first.token_hash, 1_501).unwrap();
    assert!(
        db.get_active_browser_session(&first.token_hash, 1_502)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.get_active_browser_session(&second.token_hash, 1_502)
            .unwrap(),
        Some(second.clone()),
        "logging out one browser must leave a different browser active"
    );

    db.revoke_all_browser_sessions(1_503).unwrap();
    assert!(
        db.get_active_browser_session(&second.token_hash, 1_504)
            .unwrap()
            .is_none()
    );
    let third = session("fresh-password-login", true);
    db.create_browser_session(&third).unwrap();
    assert!(
        db.get_active_browser_session(&third.token_hash, 1_505)
            .unwrap()
            .is_some(),
        "revocation must not prevent subsequent authenticated session creation"
    );
}

#[test]
fn sqlite_browser_session_lifecycle() {
    let db = Database::open_in_memory().unwrap();
    assert_session_lifecycle(&db);
}

#[test]
fn sqlite_session_revocation_survives_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sessions.db");
    let revoked = session("revoked-browser", true);
    let active = session("active-browser", true);
    {
        let db = Database::open(&path).unwrap();
        db.create_browser_session(&revoked).unwrap();
        db.create_browser_session(&active).unwrap();
        db.revoke_browser_session(&revoked.token_hash, 1_500)
            .unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    assert!(
        reopened
            .get_active_browser_session(&revoked.token_hash, 1_501)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reopened
            .get_active_browser_session(&active.token_hash, 1_501)
            .unwrap(),
        Some(active)
    );
}

#[test]
fn postgres_browser_session_lifecycle_and_reopening() {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        eprintln!("PostgreSQL session contract requires WEAVER_TEST_POSTGRES_URL");
        return;
    };
    if base_url.trim().is_empty() {
        eprintln!("PostgreSQL session contract requires nonempty WEAVER_TEST_POSTGRES_URL");
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
    let schema = format!("browser_session_contract_{}_{}", std::process::id(), suffix);
    runtime
        .block_on(
            sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}"))).execute(&pool),
        )
        .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
    let old_url = std::env::var_os("WEAVER_DATABASE_URL");
    // This is the only test in this binary that reads or changes this variable.
    // The other tests open explicit SQLite paths without environment resolution.
    unsafe { std::env::set_var("WEAVER_DATABASE_URL", &url) };
    let directory = tempfile::tempdir().unwrap();
    let result = std::panic::catch_unwind(|| {
        let db = weaver_server_core::persistence::open_database(directory.path()).unwrap();
        assert_eq!(db.engine_name(), "postgres");
        assert_session_lifecycle(&db);
        db.revoke_all_browser_sessions(1_506).unwrap();
        drop(db);
        let reopened = weaver_server_core::persistence::open_database(directory.path()).unwrap();
        assert!(
            reopened
                .get_active_browser_session(
                    &session("fresh-password-login", true).token_hash,
                    1_507
                )
                .unwrap()
                .is_none(),
            "PostgreSQL revocation must survive a new database handle"
        );
    });
    // No other test in this binary accesses this environment variable.
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
