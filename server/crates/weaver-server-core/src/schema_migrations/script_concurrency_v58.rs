// Migration 58, upgrade step: a saved script concurrency below 32 is raised
// to 32.
//
// One concurrency setting now bounds every script weaver runs, where it used
// to bound only the scripts a download waits for, with a default of 4 and a
// ceiling of 8. A saved value under the new default is raised to it; a value
// at or above it is kept. A database that never saved the settings already
// reads the new default, so it is left alone. The step runs once, on the
// upgrade to 58: an operator who lowers the value afterwards keeps it.
//
// The settings key, the field and the value are spelled out here as they
// stood at schema 58, so later changes to the settings type do not change
// what this step does.

use serde_json::Value;

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "raise_script_concurrency_v58";
// The schema that first raises the setting. A backup taken below it can
// carry a value chosen under the old meaning.
pub(crate) const SCHEMA_VERSION: i64 = 58;

const SETTINGS_KEY: &str = "post_processing.settings.v2";
const FIELD: &str = "concurrency";
const RAISED_TO: u64 = 32;

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn raise_script_concurrency(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
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
    let Some(raised) = raised(&raw) else {
        return Ok(());
    };
    conn.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[SqlArg::Text(raised), SqlArg::Text(SETTINGS_KEY.into())],
    )
    .await?;
    Ok(())
}

// The settings document with the concurrency raised, or `None` when it is
// already at least 32 or cannot be read.
fn raised(raw: &str) -> Option<String> {
    let mut document: Value = match serde_json::from_str(raw) {
        Ok(document) => document,
        Err(_) => {
            tracing::warn!(
                "the saved post-processing settings could not be read and their script concurrency was not raised"
            );
            return None;
        }
    };
    let object = document.as_object_mut()?;
    let low = match object.get(FIELD) {
        Some(Value::Number(value)) => value.as_u64().is_none_or(|value| value < RAISED_TO),
        _ => false,
    };
    if !low {
        return None;
    }
    object.insert(FIELD.into(), Value::from(RAISED_TO));
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
