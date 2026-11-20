// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::script_concurrency_v58::*;

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;
    use crate::post_processing::model::PostProcessingSettings;

    async fn below_schema_58() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        super::super::replay_catalog_into_fresh_db(
            &pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            Some(SCHEMA_VERSION - 1),
            true,
        )
        .await
        .unwrap();
        pool
    }

    async fn upgrade_to_58(pool: &SqlitePool) {
        apply_version_range(
            pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            MigrationInstallKind::Upgrade,
            SCHEMA_VERSION,
            SCHEMA_VERSION,
        )
        .await
        .unwrap();
    }

    async fn save(pool: &SqlitePool, value: &str) {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(SETTINGS_KEY)
        .bind(value)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn saved(pool: &SqlitePool) -> Option<String> {
        sqlx::query_scalar("SELECT value FROM settings WHERE key = ?1")
            .bind(SETTINGS_KEY)
            .fetch_optional(pool)
            .await
            .unwrap()
    }

    fn with_concurrency(value: u64) -> String {
        format!(
            r#"{{"executionEnabled":true,"concurrency":{value},"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":["iso"]}}"#
        )
    }

    #[test]
    fn the_step_value_is_the_build_default() {
        assert_eq!(
            u64::from(PostProcessingSettings::default().concurrency),
            RAISED_TO
        );
    }

    #[tokio::test]
    async fn an_upgrade_raises_a_saved_4_to_32_and_keeps_the_rest() {
        let pool = below_schema_58().await;
        save(&pool, &with_concurrency(4)).await;

        upgrade_to_58(&pool).await;

        let settings: PostProcessingSettings =
            serde_json::from_str(&saved(&pool).await.unwrap()).unwrap();
        assert_eq!(settings.concurrency, 32);
        assert!(settings.execution_enabled);
        assert_eq!(settings.unacceptable_extensions, vec!["iso".to_string()]);
    }

    #[tokio::test]
    async fn an_upgrade_keeps_a_saved_64() {
        let pool = below_schema_58().await;
        let original = with_concurrency(64);
        save(&pool, &original).await;

        upgrade_to_58(&pool).await;

        assert_eq!(saved(&pool).await, Some(original));
    }

    #[tokio::test]
    async fn an_upgrade_without_saved_settings_writes_nothing() {
        let pool = below_schema_58().await;
        upgrade_to_58(&pool).await;
        assert_eq!(saved(&pool).await, None);
    }
}
