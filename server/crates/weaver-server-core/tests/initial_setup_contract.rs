use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use weaver_server_core::Database;
use weaver_server_core::auth::repository::{
    BrowserSession, InitialAuthenticatedSetup, InitialSetupOutcome, SETUP_COMPLETED_SETTING_KEY,
    SETUP_PENDING_SETTING_KEY,
};

fn session(label: &str) -> BrowserSession {
    BrowserSession {
        token_hash: format!("token-{label}"),
        csrf_verifier: format!("csrf-{label}"),
        origin: "https://setup.example.test".into(),
        client_ip: None,
        remembered: false,
        created_at: 100,
        expires_at: 10_000,
        revoked_at: None,
    }
}

fn setup(label: &str) -> InitialAuthenticatedSetup {
    InitialAuthenticatedSetup {
        username: format!("admin-{label}"),
        password_hash: format!("hash-{label}"),
        jwt_secret: [7; 32],
        browser_session: session(label),
        completed_at: 100,
        bind_address: Some("127.0.0.1".into()),
        trusted_networks: None,
    }
}

fn initialize_encryption(db: &mut Database) {
    let key = weaver_server_core::persistence::encryption::ensure_encryption_key(None).unwrap();
    db.set_encryption_key(key);
}

fn assert_contract(db: &Database) {
    assert_eq!(
        db.complete_initial_authenticated_setup(&setup("missing"))
            .unwrap(),
        InitialSetupOutcome::AlreadyCompleted,
        "setup cannot claim an established or unmarked database"
    );
    assert!(db.get_auth_credentials().unwrap().is_none());

    db.mark_initial_setup_pending().unwrap();
    let duplicate = session("collision");
    db.create_browser_session(&duplicate).unwrap();
    let mut failing = setup("collision");
    failing.browser_session = duplicate.clone();
    assert!(db.complete_initial_authenticated_setup(&failing).is_err());
    assert!(db.get_auth_credentials().unwrap().is_none());
    assert!(
        db.get_setting(SETUP_COMPLETED_SETTING_KEY)
            .unwrap()
            .is_none()
    );
    assert!(db.get_setting(SETUP_PENDING_SETTING_KEY).unwrap().is_some());
    assert_eq!(
        db.get_setting(weaver_server_core::security::SETTING_SECURITY_POLICY_REVISION)
            .unwrap()
            .as_deref(),
        Some(weaver_server_core::security::AUTHENTICATED_POLICY_REVISION),
        "a failed setup must retain the pre-established authenticated policy"
    );

    let successful = setup("winner");
    assert_eq!(
        db.complete_initial_authenticated_setup(&successful)
            .unwrap(),
        InitialSetupOutcome::Created
    );
    assert_eq!(
        db.get_auth_credentials().unwrap().unwrap().username,
        "admin-winner"
    );
    assert!(db.get_setting(SETUP_PENDING_SETTING_KEY).unwrap().is_none());
    assert!(
        db.get_setting(SETUP_COMPLETED_SETTING_KEY)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        db.get_active_browser_session(&successful.browser_session.token_hash, 101)
            .unwrap(),
        Some(successful.browser_session.clone())
    );
    assert_eq!(
        db.complete_initial_authenticated_setup(&setup("loser"))
            .unwrap(),
        InitialSetupOutcome::AlreadyCompleted
    );
}

#[test]
fn sqlite_initial_setup_is_atomic_and_persistent() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("setup.db");
    {
        let mut db = Database::open(&path).unwrap();
        initialize_encryption(&mut db);
        assert_contract(&db);
    }
    let mut reopened = Database::open(&path).unwrap();
    initialize_encryption(&mut reopened);
    assert!(reopened.get_auth_credentials().unwrap().is_some());
    assert!(
        reopened
            .get_setting(SETUP_COMPLETED_SETTING_KEY)
            .unwrap()
            .is_some()
    );
}

#[test]
fn sqlite_concurrent_initial_setup_has_exactly_one_winner() {
    let mut database = Database::open_in_memory().unwrap();
    initialize_encryption(&mut database);
    database.mark_initial_setup_pending().unwrap();
    let database = Arc::new(database);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for label in ["one", "two"] {
        let db = database.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            db.complete_initial_authenticated_setup(&setup(label))
                .unwrap()
        }));
    }
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == InitialSetupOutcome::Created)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == InitialSetupOutcome::AlreadyCompleted)
            .count(),
        1
    );
}

#[test]
fn postgres_initial_setup_is_atomic_and_persistent() {
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
    let schema = format!("initial_setup_contract_{}_{}", std::process::id(), suffix);
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
    let result = std::panic::catch_unwind(|| {
        let mut first = weaver_server_core::persistence::open_database(directory.path()).unwrap();
        initialize_encryption(&mut first);
        first.mark_initial_setup_pending().unwrap();
        let mut second = weaver_server_core::persistence::open_database(directory.path()).unwrap();
        initialize_encryption(&mut second);
        let first = Arc::new(first);
        let second = Arc::new(second);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let left_db = first.clone();
        let left_barrier = barrier.clone();
        let left = std::thread::spawn(move || {
            left_barrier.wait();
            (
                "postgres-one",
                left_db
                    .complete_initial_authenticated_setup(&setup("postgres-one"))
                    .unwrap(),
            )
        });
        let right_db = second.clone();
        let right = std::thread::spawn(move || {
            barrier.wait();
            (
                "postgres-two",
                right_db
                    .complete_initial_authenticated_setup(&setup("postgres-two"))
                    .unwrap(),
            )
        });
        let results = [left.join().unwrap(), right.join().unwrap()];
        let winner = results
            .iter()
            .find(|(_, outcome)| *outcome == InitialSetupOutcome::Created)
            .unwrap()
            .0;
        assert_eq!(
            results
                .iter()
                .filter(|(_, outcome)| *outcome == InitialSetupOutcome::Created)
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|(_, outcome)| *outcome == InitialSetupOutcome::AlreadyCompleted)
                .count(),
            1
        );
        assert_eq!(
            first.get_auth_credentials().unwrap().unwrap().username,
            format!("admin-{winner}")
        );
        assert!(
            first
                .get_setting(SETUP_PENDING_SETTING_KEY)
                .unwrap()
                .is_none()
        );
        assert!(
            first
                .get_active_browser_session(&setup(winner).browser_session.token_hash, 101)
                .unwrap()
                .is_some()
        );
        drop(first);
        drop(second);
        let mut reopened =
            weaver_server_core::persistence::open_database(directory.path()).unwrap();
        initialize_encryption(&mut reopened);
        assert!(reopened.get_auth_credentials().unwrap().is_some());
    });
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
