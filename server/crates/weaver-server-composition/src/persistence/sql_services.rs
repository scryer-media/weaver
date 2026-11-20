// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{StateError, postgres_migrations};
use crate::schema_migrations::{self, MigrationMode};
pub(crate) use weaver_server_db::persistence::sql_services::*;

impl MigrationRunner for MigrationMode {
    async fn sqlite_version(&self, pool: &sqlx::SqlitePool) -> Result<Option<i64>, StateError> {
        schema_migrations::max_recorded_migration_version(pool).await
    }

    async fn migrate_sqlite(&self, pool: &sqlx::SqlitePool) -> Result<(), StateError> {
        schema_migrations::run_embedded_migrations(pool, *self).await
    }

    async fn postgres_version(&self, pool: &sqlx::PgPool) -> Result<Option<i64>, StateError> {
        postgres_migrations::max_recorded_migration_version(pool).await
    }

    async fn migrate_postgres(&self, pool: &sqlx::PgPool) -> Result<(), StateError> {
        postgres_migrations::run_migrations(pool, *self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{ConnectOptions, Connection};
    use std::time::Duration;

    #[test]
    fn sqlite_url_with_create_adds_rwc_for_paths() {
        assert_eq!(
            sqlite_url_with_create("/tmp/weaver.db"),
            "sqlite:///tmp/weaver.db?mode=rwc"
        );
    }

    #[test]
    fn sqlite_url_with_create_keeps_existing_mode() {
        assert_eq!(
            sqlite_url_with_create("sqlite:///tmp/weaver.db?mode=ro"),
            "sqlite:///tmp/weaver.db?mode=ro"
        );
    }

    #[test]
    fn sqlite_url_with_create_uses_shared_memory_url() {
        assert_eq!(
            sqlite_url_with_create("sqlite://:memory:"),
            "sqlite://file::memory:?mode=memory&cache=shared"
        );
    }

    #[test]
    fn synchronous_commit_defaults_off_to_match_the_sqlite_posture() {
        assert_eq!(parse_postgres_synchronous_commit(None), "off");
    }

    #[test]
    fn synchronous_commit_accepts_postgres_vocabulary_case_insensitively() {
        assert_eq!(parse_postgres_synchronous_commit(Some(" ON ")), "on");
        assert_eq!(parse_postgres_synchronous_commit(Some("local")), "local");
        assert_eq!(
            parse_postgres_synchronous_commit(Some("remote_apply")),
            "remote_apply"
        );
    }

    #[test]
    fn synchronous_commit_rejects_values_postgres_would_not_accept() {
        // The value lands in connection startup options, so anything outside
        // the setting's own vocabulary falls back to the default instead of
        // being passed through.
        assert_eq!(parse_postgres_synchronous_commit(Some("true")), "off");
        assert_eq!(
            parse_postgres_synchronous_commit(Some("off; DROP TABLE jobs")),
            "off"
        );
    }

    #[tokio::test]
    async fn a_memory_database_keeps_its_cache_size() {
        let services = SqliteServices::new_url("sqlite://:memory:", MigrationMode::Apply)
            .await
            .unwrap();
        let (cache_size,): (i64,) = sqlx::query_as("PRAGMA cache_size")
            .fetch_one(services.pool())
            .await
            .unwrap();
        assert_eq!(cache_size, -16000);
    }

    async fn file_services(dir: &tempfile::TempDir) -> (SqliteServices, String) {
        let path = dir.path().join("weaver.db");
        let services = SqliteServices::new_path(&path, MigrationMode::Apply)
            .await
            .unwrap();
        let url = sqlite_url_with_create(&path.to_string_lossy());
        (services, url)
    }

    // A transaction holds SQLite's write lock, as a long gated commit does. A
    // connection opened with the pool's options, given no busy wait at all,
    // still opens and reads: nothing it runs on open takes the write lock.
    // The same open that also sets auto_vacuum, as every pool connection did,
    // fails busy at once, which is what the pin guards against.
    #[tokio::test]
    async fn a_pool_connection_opens_while_a_write_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let (services, url) = file_services(&dir).await;
        sqlx::query("CREATE TABLE pin (v INTEGER)")
            .execute(services.pool())
            .await
            .unwrap();

        let mut holder = services.pool().acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *holder)
            .await
            .unwrap();
        sqlx::query("INSERT INTO pin (v) VALUES (1)")
            .execute(&mut *holder)
            .await
            .unwrap();

        let no_wait = sqlite_connect_options(&url, false)
            .unwrap()
            .busy_timeout(Duration::ZERO);
        let mut fresh = no_wait.connect().await.unwrap();
        let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM pin")
            .fetch_one(&mut fresh)
            .await
            .unwrap();
        assert_eq!(rows, 0);
        fresh.close().await.unwrap();

        let error = no_wait
            .pragma("auto_vacuum", "INCREMENTAL")
            .connect()
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("5"),
            "{error}"
        );

        sqlx::query("COMMIT").execute(&mut *holder).await.unwrap();
    }

    // The migration connection sets auto_vacuum once and the file keeps it.
    #[tokio::test]
    async fn auto_vacuum_is_set_once_at_migration() {
        let dir = tempfile::tempdir().unwrap();
        let (services, _) = file_services(&dir).await;
        let (mode,): (i64,) = sqlx::query_as("PRAGMA auto_vacuum")
            .fetch_one(services.pool())
            .await
            .unwrap();
        assert_eq!(mode, 2, "INCREMENTAL");
    }

    // A file pool opens every connection and never drops one, so no request
    // pays for an open.
    #[test]
    fn a_file_pool_keeps_every_connection() {
        let options = sqlite_pool_options(false);
        assert_eq!(options.get_min_connections(), options.get_max_connections());
        assert_eq!(options.get_idle_timeout(), None);
        assert_eq!(options.get_max_lifetime(), None);
    }
}
