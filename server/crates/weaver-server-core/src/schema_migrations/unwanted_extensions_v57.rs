//! Migration 57, upgrade step: an install that never set an unwanted
//! extension list gets the default one.
//!
//! Earlier builds shipped the list empty, which turned the check off, so an
//! executable posting was delivered like any other file. A saved settings
//! document whose list is empty or absent is given the default list. A
//! database that never saved the settings already reads the defaults, so it
//! is left alone. The step runs once, on the upgrade to 57: an operator who
//! clears the list afterwards keeps it cleared.
//!
//! The settings key, the field and the list are spelled out here as they
//! stood at schema 57, so later changes to the settings type do not change
//! what this step does.

use serde_json::Value;

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "default_unwanted_extensions_v57";
/// The schema that first ships a default list. A backup taken below it can
/// carry the empty list earlier builds defaulted to.
pub(crate) const SCHEMA_VERSION: i64 = 57;

const SETTINGS_KEY: &str = "post_processing.settings.v2";
const FIELD: &str = "unacceptableExtensions";
/// Stored sorted, as a saved list is normalized.
const DEFAULT_LIST: [&str; 10] = [
    "bat", "cmd", "com", "exe", "js", "lnk", "msi", "ps1", "scr", "vbs",
];

/// Run the step on a connection that is already inside the transaction it
/// belongs to.
pub(crate) async fn fill_default_unwanted_extensions(
    conn: &mut SqlConn<'_>,
) -> Result<(), StateError> {
    let Some(row) = conn
        .fetch_all(
            "SELECT value FROM settings WHERE key = {}",
            &[SqlArg::Text(SETTINGS_KEY.into())],
        )
        .await?
        .into_iter()
        .next()
    else {
        return Ok(());
    };
    let raw = row.text("value")?;
    let Some(filled) = filled(&raw) else {
        return Ok(());
    };
    conn.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[SqlArg::Text(filled), SqlArg::Text(SETTINGS_KEY.into())],
    )
    .await?;
    Ok(())
}

/// The settings document with the default list, or `None` when it already
/// names extensions or cannot be read.
fn filled(raw: &str) -> Option<String> {
    let mut document: Value = match serde_json::from_str(raw) {
        Ok(document) => document,
        Err(_) => {
            tracing::warn!(
                "the saved post-processing settings could not be read and were not given the default unwanted extensions"
            );
            return None;
        }
    };
    let object = document.as_object_mut()?;
    let empty = match object.get(FIELD) {
        None | Some(Value::Null) => true,
        Some(Value::Array(list)) => list.is_empty(),
        Some(_) => false,
    };
    if !empty {
        return None;
    }
    object.insert(
        FIELD.into(),
        Value::Array(DEFAULT_LIST.iter().map(|ext| Value::from(*ext)).collect()),
    );
    serde_json::to_string(&document).ok()
}

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
        let cleared = r#"{"executionEnabled":false,"concurrency":4,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":[]}"#;
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
