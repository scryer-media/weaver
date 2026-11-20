// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::script_instances_v55::*;

#[cfg(test)]
mod tests {
    use super::super::{embedded_catalog, embedded_payload_bytes, replay_catalog_into_fresh_db};
    use super::planned_warnings;
    use crate::Database;
    use crate::post_processing::instances::PostProcessingInstancesQueries;
    use crate::post_processing::instances::{InstanceTrigger, ScriptInstance, resolve_instances};
    use crate::post_processing::model::{GlobalScriptsRun, ScriptEventLabel};
    use crate::post_processing::secrets::PostProcessingSecretsQueries;
    use crate::post_processing::settings::PostProcessingSettingsQueries;
    use crate::settings::queries::SettingsQueries;
    use serde_json::{Value, json};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::fs;
    use std::path::{Path, PathBuf};

    const LISTS_KEY: &str = "post_processing.script_lists.v1";
    const OPTIONS_KEY: &str = "post_processing.script_options.v1";
    const DIRECTORY_KEY: &str = "post_processing.script_directory.v1";

    fn bare(root: &Path, name: &str, header: &str) {
        fs::write(root.join(name), format!("#!/bin/sh\n{header}\n")).unwrap();
    }

    fn stored(
        instance: &ScriptInstance,
    ) -> (&str, InstanceTrigger, Vec<&str>, bool, bool, Option<u64>) {
        (
            instance.script.as_str(),
            instance.trigger,
            instance.categories.iter().map(String::as_str).collect(),
            instance.enabled,
            instance.blocking,
            instance.timeout_seconds,
        )
    }

    async fn upgraded(root: &Path, scripts: &Path, lists: Value) -> (Database, Vec<String>) {
        upgraded_with_options(root, scripts, lists, json!({})).await
    }

    async fn upgraded_with_options(
        root: &Path,
        scripts: &Path,
        lists: Value,
        options: Value,
    ) -> (Database, Vec<String>) {
        let path = root.join("weaver.db");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        replay_catalog_into_fresh_db(
            &pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            Some(50),
            true,
        )
        .await
        .unwrap();
        let lists = lists.to_string();
        let options = options.to_string();
        for (key, value) in [
            (DIRECTORY_KEY, scripts.to_string_lossy().into_owned()),
            (LISTS_KEY, lists.clone()),
            (OPTIONS_KEY, options.clone()),
        ] {
            sqlx::query("INSERT INTO settings (key, value) VALUES (?1, ?2)")
                .bind(key)
                .bind(value)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
        let warnings = planned_warnings(scripts, &lists, &options).unwrap();
        let db = Database::open(&path).unwrap();
        // The instances hold the wiring now, so what it was saved as is gone.
        for key in [LISTS_KEY, OPTIONS_KEY] {
            assert_eq!(db.get_setting(key).unwrap(), None, "{key}");
        }
        (db, warnings)
    }

    fn runs_for_tv(db: &Database, event: ScriptEventLabel) -> Vec<(String, Vec<String>)> {
        let settings = db.post_processing_settings().unwrap();
        assert_eq!(
            settings.global_scripts_run,
            GlobalScriptsRun::OnlyWithoutCategoryScripts
        );
        resolve_instances(
            &db.script_instances().unwrap(),
            &event,
            Some("tv"),
            settings.global_scripts_run,
        )
        .into_iter()
        .map(|instance| (instance.script.as_str().to_string(), instance.categories))
        .collect()
    }

    fn scripts(root: &Path) -> PathBuf {
        let scripts = root.join("scripts");
        fs::create_dir_all(&scripts).unwrap();
        let scripts = fs::canonicalize(&scripts).unwrap();
        bare(&scripts, "post.sh", "### NZBGET POST-PROCESSING SCRIPT ###");
        bare(
            &scripts,
            "tv-post.sh",
            "### NZBGET POST-PROCESSING SCRIPT ###",
        );
        scripts
    }

    #[tokio::test]
    async fn scripts_whose_names_differ_only_by_case_get_distinct_secrets_through_open() {
        let root = tempfile::tempdir().unwrap();
        // Neither script is on disk, so the saved options alone say what
        // each instance holds; no case-folding filesystem is involved.
        let scripts = root.path().join("scripts");
        fs::create_dir_all(&scripts).unwrap();
        let scripts = fs::canonicalize(&scripts).unwrap();
        let (db, _) = upgraded_with_options(
            root.path(),
            &scripts,
            json!({"global": [{"script": "Notify.sh"}, {"script": "notify.sh"}]}),
            json!({
                "Notify.sh": {"secrets": [{"name": "Token", "ciphertext": "sealed-upper"}]},
                "notify.sh": {"secrets": [{"name": "Token", "ciphertext": "sealed-lower"}]},
            }),
        )
        .await;

        let secrets = db.secrets().unwrap();
        let mut names = secrets
            .iter()
            .map(|secret| secret.name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["Notify.sh Token", "notify.sh Token 2"]);
        let id_of = |name: &str| {
            secrets
                .iter()
                .find(|secret| secret.name == name)
                .unwrap()
                .id
                .clone()
        };
        let linked = db
            .script_instances()
            .unwrap()
            .iter()
            .map(|instance| {
                (
                    instance.script.as_str().to_string(),
                    instance
                        .inputs
                        .iter()
                        .map(|input| {
                            (
                                input.name.as_str().to_string(),
                                input.secret.as_ref().map(|secret| secret.id.clone()),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            linked,
            [
                (
                    "Notify.sh".to_string(),
                    vec![("Token".to_string(), Some(id_of("Notify.sh Token")))]
                ),
                (
                    "notify.sh".to_string(),
                    vec![("Token".to_string(), Some(id_of("notify.sh Token 2")))]
                ),
            ]
        );
        // Each secret carries its own script's ciphertext across unchanged.
        let datastore = db.datastore();
        let rows = db
            .run_sql_blocking_read(async move {
                crate::persistence::sql_runtime::SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT name, value FROM secrets ORDER BY value",
                    &[],
                )
                .await
            })
            .unwrap()
            .into_iter()
            .map(|row| (row.text("name").unwrap(), row.text("value").unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                ("notify.sh Token 2".to_string(), "sealed-lower".to_string()),
                ("Notify.sh Token".to_string(), "sealed-upper".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn a_category_list_with_every_script_turned_off_lets_scripts_for_every_category_run() {
        let root = tempfile::tempdir().unwrap();
        let scripts = scripts(root.path());
        let (db, warnings) = upgraded(
            root.path(),
            &scripts,
            json!({
                "global": [{"script": "post.sh"}],
                "categories": {"tv": [{"script": "tv-post.sh", "enabled": false}]},
            }),
        )
        .await;
        use InstanceTrigger::PostProcessing;
        assert_eq!(
            db.script_instances()
                .unwrap()
                .iter()
                .map(stored)
                .collect::<Vec<_>>(),
            [
                ("post.sh", PostProcessing, vec![], true, true, None),
                ("tv-post.sh", PostProcessing, vec!["tv"], false, true, None),
            ]
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("every script in its list turned off"));
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::PostProcessing),
            [("post.sh".to_string(), vec![])]
        );
        db.close().unwrap();
    }

    #[tokio::test]
    async fn an_empty_category_list_lets_scripts_for_every_category_run() {
        let root = tempfile::tempdir().unwrap();
        let scripts = scripts(root.path());
        let (db, warnings) = upgraded(
            root.path(),
            &scripts,
            json!({
                "global": [{"script": "post.sh"}],
                "categories": {"tv": []},
            }),
        )
        .await;
        assert_eq!(
            db.script_instances()
                .unwrap()
                .iter()
                .map(stored)
                .collect::<Vec<_>>(),
            [(
                "post.sh",
                InstanceTrigger::PostProcessing,
                vec![],
                true,
                true,
                None
            )]
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("empty script list"));
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::PostProcessing),
            [("post.sh".to_string(), vec![])]
        );
        db.close().unwrap();
    }

    #[tokio::test]
    async fn shipped_upgrade_preserves_post_processing_without_enabling_new_triggers() {
        let root = tempfile::tempdir().unwrap();
        let scripts = scripts(root.path());
        bare(
            &scripts,
            "multi.sh",
            "### NZBGET POST-PROCESSING/QUEUE/SCAN/SCHEDULER/FEED SCRIPT ###\n### TASK TIME: 03:30 ###",
        );
        let (db, _) = upgraded(
            root.path(),
            &scripts,
            json!({
                "global":[{"script":"multi.sh", "blocking":false, "timeoutSeconds":42}],
                "categories":{"tv":[{"script":"tv-post.sh"}]}
            }),
        )
        .await;
        let instances = db.script_instances().unwrap();
        assert_eq!(
            instances.iter().map(stored).collect::<Vec<_>>(),
            [
                (
                    "multi.sh",
                    InstanceTrigger::PostProcessing,
                    vec![],
                    true,
                    false,
                    Some(42)
                ),
                (
                    "tv-post.sh",
                    InstanceTrigger::PostProcessing,
                    vec!["tv"],
                    true,
                    true,
                    None
                ),
            ]
        );
        assert!(
            instances
                .iter()
                .all(|job| job.schedule.times.is_empty() && !job.schedule.run_at_startup)
        );
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::PostProcessing),
            [("tv-post.sh".into(), vec!["tv".into()])]
        );
        db.close().unwrap();
        let reopened = Database::open(&root.path().join("weaver.db")).unwrap();
        assert_eq!(reopened.script_instances().unwrap().len(), 2);
    }
}
