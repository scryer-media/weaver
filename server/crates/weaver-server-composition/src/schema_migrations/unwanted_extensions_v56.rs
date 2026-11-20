// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::unwanted_extensions_v56::*;

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;
    use crate::post_processing::model::PostProcessingSettings;

    async fn at_schema_56() -> SqlitePool {
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

    async fn upgrade_to_57(pool: &SqlitePool) {
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

    fn list(raw: &str) -> Vec<String> {
        serde_json::from_str::<PostProcessingSettings>(raw)
            .unwrap()
            .unacceptable_extensions
    }

    #[test]
    fn the_step_list_is_the_build_default() {
        assert_eq!(
            PostProcessingSettings::default().unacceptable_extensions,
            DEFAULT_LIST.map(String::from).to_vec()
        );
    }

    #[tokio::test]
    async fn an_upgrade_fills_an_empty_saved_list_and_keeps_the_rest() {
        let pool = at_schema_56().await;
        save(
            &pool,
            r#"{"executionEnabled":true,"concurrency":2,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":[]}"#,
        )
        .await;

        upgrade_to_57(&pool).await;

        let raw = saved(&pool).await.unwrap();
        assert_eq!(list(&raw), DEFAULT_LIST.map(String::from).to_vec());
        let settings: PostProcessingSettings = serde_json::from_str(&raw).unwrap();
        assert!(settings.execution_enabled);
        assert_eq!(settings.concurrency, 2);
    }

    #[tokio::test]
    async fn an_upgrade_keeps_a_list_the_operator_set() {
        let pool = at_schema_56().await;
        let original = r#"{"executionEnabled":false,"concurrency":4,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":["iso"]}"#;
        save(&pool, original).await;

        upgrade_to_57(&pool).await;

        assert_eq!(saved(&pool).await.as_deref(), Some(original));
    }

    #[tokio::test]
    async fn an_upgrade_without_saved_settings_writes_nothing() {
        let pool = at_schema_56().await;
        upgrade_to_57(&pool).await;
        assert_eq!(saved(&pool).await, None);
    }

    #[tokio::test]
    async fn a_list_cleared_after_the_upgrade_stays_cleared() {
        let pool = at_schema_56().await;
        upgrade_to_57(&pool).await;
        // At the raised concurrency, so a later step leaves the document as it is.
        let cleared = r#"{"executionEnabled":false,"concurrency":32,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":[]}"#;
        save(&pool, cleared).await;

        // Every later version replays over the cleared list without the step.
        let catalog = embedded_catalog().unwrap();
        if catalog.max_version() > SCHEMA_VERSION {
            apply_version_range(
                &pool,
                &catalog,
                &embedded_payload_bytes().unwrap(),
                MigrationInstallKind::Upgrade,
                SCHEMA_VERSION + 1,
                catalog.max_version(),
            )
            .await
            .unwrap();
        }

        assert_eq!(saved(&pool).await.as_deref(), Some(cleared));
        assert!(list(cleared).is_empty());
    }
}
