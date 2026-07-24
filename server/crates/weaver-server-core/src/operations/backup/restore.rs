use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{Connection, Row, Sqlite, Transaction};

use super::logical::{
    append_table_objects, read_table_objects, rewrite_table_objects, visit_table_objects,
};
use super::manifest::{BackupManifest, BackupServiceError, RestoreOptions};
use crate::Database;
use crate::settings::Config;
#[cfg(test)]
use crate::{
    JobInfo, JobStatus, job_status_from_persisted_str, runtime_lanes_from_status_snapshot,
};

struct BackupDbRewrite {
    backup_db_path: PathBuf,
    data_dir: String,
    intermediate_dir: Option<String>,
    complete_dir: Option<String>,
    category_updates: Vec<(u32, String)>,
    path_remaps: Vec<PathRemap>,
}

#[derive(Clone, Debug)]
struct PathRemap {
    source: String,
    target: PathBuf,
}

struct ResolvedRestorePaths {
    data: PathBuf,
    intermediate: PathBuf,
    complete: PathBuf,
}

fn resolve_restore_paths(
    options: &RestoreOptions,
) -> Result<ResolvedRestorePaths, BackupServiceError> {
    let data = absolute_restore_path("data_dir", &options.data_dir)?;
    let intermediate = match normalized_optional_path(&options.intermediate_dir) {
        Some(path) => absolute_restore_path("intermediate_dir", &path)?,
        None => data.join("intermediate"),
    };
    let complete = match normalized_optional_path(&options.complete_dir) {
        Some(path) => absolute_restore_path("complete_dir", &path)?,
        None => data.join("complete"),
    };
    Ok(ResolvedRestorePaths {
        data,
        intermediate,
        complete,
    })
}

pub(crate) fn normalize_restore_options(
    mut options: RestoreOptions,
) -> Result<RestoreOptions, BackupServiceError> {
    let resolved = resolve_restore_paths(&options)?;
    let category_remaps = category_remap_map(&options)?;
    options.data_dir = resolved.data.display().to_string();
    options.intermediate_dir = normalized_optional_path(&options.intermediate_dir)
        .map(|_| resolved.intermediate.display().to_string());
    options.complete_dir = normalized_optional_path(&options.complete_dir)
        .map(|_| resolved.complete.display().to_string());
    for remap in &mut options.category_remaps {
        let name = remap.category_name.trim().to_string();
        remap.new_dest_dir = category_remaps
            .get(&name)
            .expect("validated category remap")
            .display()
            .to_string();
        remap.category_name = name;
    }
    Ok(options)
}

fn absolute_restore_path(label: &str, value: &str) -> Result<PathBuf, BackupServiceError> {
    let value = value.trim();
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty() || !path.is_absolute() {
        return Err(BackupServiceError::Validation(format!(
            "{label} must be an absolute path"
        )));
    }
    if value.contains('\0')
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(BackupServiceError::Validation(format!(
            "{label} contains an unsafe path component"
        )));
    }
    Ok(path
        .components()
        .filter(|component| *component != Component::CurDir)
        .collect())
}

fn normalized_optional_path(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn category_remap_map(
    options: &RestoreOptions,
) -> Result<BTreeMap<String, PathBuf>, BackupServiceError> {
    let mut remaps = BTreeMap::new();
    for remap in &options.category_remaps {
        let category = remap.category_name.trim();
        if category.is_empty() {
            return Err(BackupServiceError::Validation(
                "category remap name must not be empty".into(),
            ));
        }
        let destination = absolute_restore_path("category remap destination", &remap.new_dest_dir)?;
        if remaps.insert(category.to_string(), destination).is_some() {
            return Err(BackupServiceError::Validation(format!(
                "category {category} is remapped more than once"
            )));
        }
    }
    Ok(remaps)
}

fn validate_used_category_remaps(
    remaps: &BTreeMap<String, PathBuf>,
    used: &BTreeSet<String>,
) -> Result<(), BackupServiceError> {
    let unused = remaps
        .keys()
        .filter(|category| !used.contains(*category))
        .cloned()
        .collect::<Vec<_>>();
    if unused.is_empty() {
        Ok(())
    } else {
        Err(BackupServiceError::Validation(format!(
            "category remaps do not match required source categories: {}",
            unused.join(", ")
        )))
    }
}

fn normalize_path_remaps(remaps: &mut Vec<PathRemap>) -> Result<(), BackupServiceError> {
    let mut unique = BTreeMap::<String, PathRemap>::new();
    for remap in remaps.drain(..) {
        if remap.source.trim().is_empty() {
            continue;
        }
        let key = stored_path_match_key(&remap.source);
        if let Some(existing) = unique.get(&key) {
            if existing.target != remap.target {
                return Err(BackupServiceError::Validation(format!(
                    "source path {} has conflicting restore destinations",
                    remap.source
                )));
            }
            continue;
        }
        unique.insert(key, remap);
    }
    *remaps = unique.into_values().collect();
    remaps.sort_by(|left, right| {
        stored_path_match_key(&right.source)
            .len()
            .cmp(&stored_path_match_key(&left.source).len())
    });
    Ok(())
}

fn stored_path_match_key(value: &str) -> String {
    let windows = is_windows_stored_path(value);
    let normalized = normalize_stored_path(value, windows);
    if windows {
        normalized.to_ascii_lowercase()
    } else {
        normalized
    }
}

fn is_windows_stored_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\'))
        || value.starts_with(r"\\")
}

fn normalize_stored_path(value: &str, windows: bool) -> String {
    let mut normalized = if windows {
        value.trim().replace('\\', "/")
    } else {
        value.trim().to_string()
    };
    while normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }
    normalized
}

fn rewrite_stored_prefix(path: &str, source: &str, target: &Path) -> Option<PathBuf> {
    let windows = is_windows_stored_path(source);
    let path = normalize_stored_path(path, windows);
    let source = normalize_stored_path(source, windows);
    if source.is_empty() || path.len() < source.len() || !path.is_char_boundary(source.len()) {
        return None;
    }
    let prefix = &path[..source.len()];
    let prefix_matches = if windows {
        prefix.eq_ignore_ascii_case(&source)
    } else {
        prefix == source
    };
    if !prefix_matches {
        return None;
    }
    let remainder = if source == "/" {
        path.strip_prefix('/').unwrap_or(&path)
    } else if path.len() == source.len() {
        ""
    } else if path.as_bytes().get(source.len()) == Some(&b'/') {
        &path[source.len() + 1..]
    } else {
        return None;
    };
    let mut rewritten = target.to_path_buf();
    for component in remainder
        .split('/')
        .filter(|component| !component.is_empty())
    {
        match component {
            "." => {}
            ".." => return None,
            component => rewritten.push(component),
        }
    }
    Some(rewritten)
}

pub(super) fn stored_path_has_prefix(path: &str, source: &str) -> bool {
    rewrite_stored_prefix(path, source, Path::new("")).is_some()
}

pub(crate) fn rewrite_backup_db_for_restore(
    backup_db_path: &Path,
    source_config: &Config,
    options: &RestoreOptions,
) -> Result<(), BackupServiceError> {
    let resolved = resolve_restore_paths(options)?;
    let remap_map = category_remap_map(options)?;
    let old_data = source_config.data_dir.clone();
    let old_intermediate = source_config.intermediate_dir();
    let old_complete = source_config.complete_dir();
    let mut path_remaps = vec![
        PathRemap {
            source: old_intermediate,
            target: resolved.intermediate.clone(),
        },
        PathRemap {
            source: old_complete.clone(),
            target: resolved.complete.clone(),
        },
        PathRemap {
            source: old_data,
            target: resolved.data.clone(),
        },
    ];

    let mut missing = Vec::new();
    let mut used_remaps = BTreeSet::new();
    let mut category_updates = Vec::new();
    for category in &source_config.categories {
        let Some(dest_dir) = &category.dest_dir else {
            continue;
        };
        let new_dest = if let Some(rewritten) =
            rewrite_stored_prefix(dest_dir, &old_complete, &resolved.complete)
        {
            rewritten
        } else if let Some(mapped) = remap_map.get(category.name.trim()) {
            used_remaps.insert(category.name.trim().to_string());
            mapped.clone()
        } else {
            missing.push(category.name.clone());
            continue;
        };

        path_remaps.push(PathRemap {
            source: dest_dir.clone(),
            target: new_dest.clone(),
        });
        category_updates.push((category.id, new_dest.display().to_string()));
    }

    if !missing.is_empty() {
        missing.sort();
        return Err(BackupServiceError::MissingCategoryRemaps(
            missing.join(", "),
        ));
    }
    validate_used_category_remaps(&remap_map, &used_remaps)?;
    normalize_path_remaps(&mut path_remaps)?;

    rewrite_backup_db_artifact_blocking(BackupDbRewrite {
        backup_db_path: backup_db_path.to_path_buf(),
        data_dir: resolved.data.display().to_string(),
        intermediate_dir: normalized_optional_path(&options.intermediate_dir),
        complete_dir: normalized_optional_path(&options.complete_dir),
        category_updates,
        path_remaps,
    })?;

    let backup_db = Database::open(backup_db_path)
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    let rewritten = backup_db
        .load_config()
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    backup_db
        .checkpoint_sqlite()
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    backup_db
        .close()
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    rewritten
        .validate()
        .map_err(|errors| BackupServiceError::Validation(errors.join("; ")))?;

    Ok(())
}

fn rewrite_backup_db_artifact_blocking(rewrite: BackupDbRewrite) -> Result<(), BackupServiceError> {
    let handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        runtime.block_on(rewrite_backup_db_artifact(rewrite))
    });
    handle.join().map_err(|_| {
        BackupServiceError::Validation("backup rewrite worker thread panicked".to_string())
    })?
}

async fn rewrite_backup_db_artifact(rewrite: BackupDbRewrite) -> Result<(), BackupServiceError> {
    let BackupDbRewrite {
        backup_db_path,
        data_dir,
        intermediate_dir,
        complete_dir,
        category_updates,
        path_remaps,
    } = rewrite;
    let options = SqliteConnectOptions::new()
        .filename(&backup_db_path)
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_millis(5_000));
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    let mut tx = conn
        .begin()
        .await
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;

    set_or_insert_setting(&mut tx, "data_dir", &data_dir).await?;

    let history = sqlx::query("SELECT job_id, output_dir, nzb_path FROM job_history")
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    for row in history {
        let job_id: i64 = row
            .try_get("job_id")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let output_dir = row
            .try_get::<Option<String>, _>("output_dir")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?
            .map(|path| rewrite_path_with_roots(&path, &path_remaps));
        let nzb_path = row
            .try_get::<Option<String>, _>("nzb_path")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?
            .map(|path| rewrite_path_with_roots(&path, &path_remaps));
        sqlx::query("UPDATE job_history SET output_dir = ?, nzb_path = ? WHERE job_id = ?")
            .bind(output_dir)
            .bind(nzb_path)
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    }

    sqlx::query(
        "UPDATE post_processing_extension_revisions
            SET managed_path = NULL, discovered_source_path = NULL",
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    rewrite_legacy_machine_paths(&mut tx, &path_remaps).await?;

    match &intermediate_dir {
        Some(value) if !value.trim().is_empty() => {
            set_or_insert_setting(&mut tx, "intermediate_dir", value.trim()).await?
        }
        _ => {
            sqlx::query("DELETE FROM settings WHERE key = ?")
                .bind("intermediate_dir")
                .execute(&mut *tx)
                .await
                .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        }
    }

    match &complete_dir {
        Some(value) if !value.trim().is_empty() => {
            set_or_insert_setting(&mut tx, "complete_dir", value.trim()).await?
        }
        _ => {
            sqlx::query("DELETE FROM settings WHERE key = ?")
                .bind("complete_dir")
                .execute(&mut *tx)
                .await
                .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        }
    }

    for (category_id, new_dest) in category_updates {
        let updated = sqlx::query("UPDATE categories SET dest_dir = ? WHERE id = ?")
            .bind(new_dest)
            .bind(i64::from(category_id))
            .execute(&mut *tx)
            .await
            .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        if updated.rows_affected() == 0 {
            return Err(BackupServiceError::Validation(format!(
                "category id {category_id} not found in backup"
            )));
        }
    }

    tx.commit()
        .await
        .map_err(|e| BackupServiceError::Validation(e.to_string()))
}

async fn rewrite_legacy_machine_paths(
    tx: &mut Transaction<'_, Sqlite>,
    roots: &[PathRemap],
) -> Result<(), BackupServiceError> {
    let settings = sqlx::query(
        "SELECT key, value FROM settings
          WHERE key IN ('watch_folder.path', 'post_processing.settings.v1')",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    let mut disable_watch_folder = false;
    for row in settings {
        let key: String = row
            .try_get("key")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let value: String = row
            .try_get("value")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let rewritten = if key == "watch_folder.path" {
            let mapped =
                rewrite_first_matching_prefix(&value, roots).map(|path| path.display().to_string());
            disable_watch_folder = mapped.is_none();
            mapped.unwrap_or_default()
        } else {
            rewrite_post_processing_settings(&value, roots)
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?
        };
        set_or_insert_setting(tx, &key, &rewritten).await?;
    }
    if disable_watch_folder {
        set_or_insert_setting(tx, "watch_folder.mode", "off").await?;
    }

    let attempts =
        sqlx::query("SELECT attempt_id, working_directory FROM post_processing_attempts")
            .fetch_all(&mut **tx)
            .await
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    for row in attempts {
        let attempt_id: String = row
            .try_get("attempt_id")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let working_directory: Option<String> = row
            .try_get("working_directory")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let Some(working_directory) = working_directory
            .as_deref()
            .and_then(|path| rewrite_first_matching_prefix(path, roots))
        else {
            continue;
        };
        sqlx::query(
            "UPDATE post_processing_attempts
                SET working_directory = ?
              WHERE attempt_id = ?",
        )
        .bind(working_directory.display().to_string())
        .bind(attempt_id)
        .execute(&mut **tx)
        .await
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    }

    let profile_steps = sqlx::query(
        "SELECT profile_id, step_index, policy_json FROM post_processing_profile_steps",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    for row in profile_steps {
        let profile_id: String = row
            .try_get("profile_id")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let step_index: i64 = row
            .try_get("step_index")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let raw: String = row
            .try_get("policy_json")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let rewritten = rewrite_approved_roots_json(&raw, roots)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        sqlx::query(
            "UPDATE post_processing_profile_steps
                SET policy_json = ?
              WHERE profile_id = ? AND step_index = ?",
        )
        .bind(rewritten)
        .bind(profile_id)
        .bind(step_index)
        .execute(&mut **tx)
        .await
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    }

    let plans = sqlx::query("SELECT job_id, plan_json FROM post_processing_job_plans")
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    for row in plans {
        let job_id: i64 = row
            .try_get("job_id")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let raw: String = row
            .try_get("plan_json")
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let rewritten = rewrite_approved_roots_json(&raw, roots)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        sqlx::query("UPDATE post_processing_job_plans SET plan_json = ? WHERE job_id = ?")
            .bind(rewritten)
            .bind(job_id)
            .execute(&mut **tx)
            .await
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    }
    Ok(())
}

fn rewrite_path_with_roots(path: &str, roots: &[PathRemap]) -> String {
    rewrite_first_matching_prefix(path, roots)
        .map_or_else(|| path.to_string(), |path| path.display().to_string())
}

async fn set_or_insert_setting(
    conn: &mut Transaction<'_, Sqlite>,
    key: &str,
    value: &str,
) -> Result<(), BackupServiceError> {
    let updated = sqlx::query("UPDATE settings SET value = ? WHERE key = ?")
        .bind(value)
        .bind(key)
        .execute(&mut **conn)
        .await
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    if updated.rows_affected() == 0 {
        sqlx::query("INSERT INTO settings (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&mut **conn)
            .await
            .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    }
    Ok(())
}

pub(crate) fn rewrite_logical_bundle_for_restore(
    root: &Path,
    manifest: &mut BackupManifest,
    options: &RestoreOptions,
) -> Result<(), BackupServiceError> {
    let resolved = resolve_restore_paths(options)?;
    let remaps = category_remap_map(options)?;
    let old_data = manifest.source_paths.data_dir.clone();
    let old_intermediate = manifest.source_paths.intermediate_dir.clone();
    let old_complete = manifest.source_paths.complete_dir.clone();
    let mut path_remaps = vec![
        PathRemap {
            source: old_intermediate,
            target: resolved.intermediate.clone(),
        },
        PathRemap {
            source: old_complete.clone(),
            target: resolved.complete.clone(),
        },
        PathRemap {
            source: old_data,
            target: resolved.data.clone(),
        },
    ];
    let mut category_destinations = BTreeMap::new();
    let mut missing = Vec::new();
    let mut used_remaps = BTreeSet::new();
    visit_table_objects(root, "categories", |row| {
        let Some(name) = row.get("name").and_then(serde_json::Value::as_str) else {
            return Ok(());
        };
        let Some(destination) = row.get("dest_dir").and_then(serde_json::Value::as_str) else {
            return Ok(());
        };
        let target = if let Some(rewritten) =
            rewrite_stored_prefix(destination, &old_complete, &resolved.complete)
        {
            rewritten
        } else if let Some(mapped) = remaps.get(name.trim()) {
            used_remaps.insert(name.trim().to_string());
            mapped.clone()
        } else {
            missing.push(name.to_string());
            return Ok(());
        };
        path_remaps.push(PathRemap {
            source: destination.to_string(),
            target: target.clone(),
        });
        category_destinations.insert(name.to_string(), target);
        Ok(())
    })
    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    if !missing.is_empty() {
        missing.sort();
        missing.dedup();
        return Err(BackupServiceError::MissingCategoryRemaps(
            missing.join(", "),
        ));
    }
    validate_used_category_remaps(&remaps, &used_remaps)?;
    normalize_path_remaps(&mut path_remaps)?;

    let source_settings = read_table_objects(root, "settings")
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    let source_setting_keys = source_settings
        .iter()
        .filter_map(|row| row.get("key").and_then(serde_json::Value::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let source_watch_path = source_settings.iter().find_map(|row| {
        (row.get("key").and_then(serde_json::Value::as_str) == Some("watch_folder.path"))
            .then(|| row.get("value").and_then(serde_json::Value::as_str))
            .flatten()
    });
    let restored_watch_path = source_watch_path
        .and_then(|path| rewrite_first_matching_prefix(path, &path_remaps))
        .map(|path| path.display().to_string());
    let disable_watch_folder = source_watch_path.is_some() && restored_watch_path.is_none();
    rewrite_manifest_table(root, manifest, "settings", |row| {
        let Some(key) = row.get("key").and_then(serde_json::Value::as_str) else {
            return Ok(());
        };
        let replacement = match key {
            "data_dir" => resolved.data.display().to_string(),
            "intermediate_dir" => resolved.intermediate.display().to_string(),
            "complete_dir" => resolved.complete.display().to_string(),
            "watch_folder.path" => restored_watch_path.clone().unwrap_or_default(),
            "watch_folder.mode" if disable_watch_folder => "off".into(),
            "post_processing.settings.v1" => rewrite_post_processing_settings(
                row.get("value")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
                &path_remaps,
            )?,
            _ => return Ok(()),
        };
        row.insert("value".into(), serde_json::Value::String(replacement));
        Ok(())
    })?;
    let settings_metadata = manifest.tables.get("settings").ok_or_else(|| {
        BackupServiceError::Validation("logical backup has no settings table".into())
    })?;
    if !settings_metadata
        .columns
        .iter()
        .any(|column| column == "key")
        || !settings_metadata
            .columns
            .iter()
            .any(|column| column == "value")
    {
        return Err(BackupServiceError::Validation(
            "logical backup settings table is missing key/value columns".into(),
        ));
    }
    let setting_template = source_settings.first().cloned().unwrap_or_else(|| {
        settings_metadata
            .columns
            .iter()
            .map(|column| (column.clone(), serde_json::Value::Null))
            .collect()
    });
    let missing_settings = [
        ("data_dir", resolved.data.display().to_string()),
        (
            "intermediate_dir",
            resolved.intermediate.display().to_string(),
        ),
        ("complete_dir", resolved.complete.display().to_string()),
    ]
    .into_iter()
    .filter(|(key, _)| !source_setting_keys.contains(key))
    .map(|(key, value)| {
        let mut row = setting_template.clone();
        row.insert("key".into(), serde_json::Value::String(key.into()));
        row.insert("value".into(), serde_json::Value::String(value));
        row
    })
    .collect::<Vec<_>>();
    append_manifest_table_rows(root, manifest, "settings", &missing_settings)?;
    rewrite_manifest_table(root, manifest, "categories", |row| {
        let name = row
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if row.get("dest_dir").is_none_or(serde_json::Value::is_null) {
            return Ok(());
        }
        let rewritten = category_destinations.get(name).ok_or_else(|| {
            crate::StateError::Database(format!(
                "category {name} has no resolved restore destination"
            ))
        })?;
        row.insert(
            "dest_dir".into(),
            serde_json::Value::String(rewritten.display().to_string()),
        );
        Ok(())
    })?;
    rewrite_manifest_table(root, manifest, "job_history", |row| {
        for column in ["output_dir", "nzb_path"] {
            rewrite_path_value(row, column, &path_remaps);
        }
        Ok(())
    })?;
    rewrite_manifest_table(
        root,
        manifest,
        "post_processing_extension_revisions",
        |row| {
            row.insert("discovered_source_path".into(), serde_json::Value::Null);
            let Some(digest) = row.get("digest").and_then(serde_json::Value::as_str) else {
                return Ok(());
            };
            if row
                .get("managed_path")
                .is_some_and(|value| !value.is_null())
            {
                let hex = digest.strip_prefix("blake3:").ok_or_else(|| {
                    crate::StateError::Database("invalid managed package digest".into())
                })?;
                row.insert(
                    "managed_path".into(),
                    serde_json::Value::String(
                        resolved
                            .data
                            .join("managed-extensions/blake3")
                            .join(hex)
                            .display()
                            .to_string(),
                    ),
                );
            }
            Ok(())
        },
    )?;
    rewrite_manifest_table(root, manifest, "post_processing_attempts", |row| {
        rewrite_path_value(row, "working_directory", &path_remaps);
        Ok(())
    })?;
    rewrite_manifest_table(root, manifest, "post_processing_profile_steps", |row| {
        rewrite_json_column(row, "policy_json", &path_remaps)?;
        Ok(())
    })?;
    rewrite_manifest_table(root, manifest, "post_processing_job_plans", |row| {
        rewrite_json_column(row, "plan_json", &path_remaps)?;
        Ok(())
    })?;
    rewrite_manifest_table(root, manifest, "post_processing_runs", |row| {
        rewrite_json_column(row, "plan_json", &path_remaps)?;
        Ok(())
    })?;

    manifest.source_paths.data_dir = resolved.data.display().to_string();
    manifest.source_paths.intermediate_dir = resolved.intermediate.display().to_string();
    manifest.source_paths.complete_dir = resolved.complete.display().to_string();
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(manifest)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?,
    )
    .map_err(super::manifest::io_err)?;
    Ok(())
}

fn rewrite_manifest_table<F>(
    root: &Path,
    manifest: &mut BackupManifest,
    table: &str,
    rewrite: F,
) -> Result<(), BackupServiceError>
where
    F: FnMut(&mut serde_json::Map<String, serde_json::Value>) -> Result<(), crate::StateError>,
{
    let Some(source_metadata) = manifest.tables.get(table) else {
        return Ok(());
    };
    let source_columns = source_metadata.columns.clone();
    let mut metadata = rewrite_table_objects(root, table, rewrite)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    let rewritten_columns = metadata
        .columns
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let expected_columns = source_columns
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    if metadata.rows != 0 && rewritten_columns != expected_columns {
        return Err(BackupServiceError::Validation(format!(
            "rewriting backup table {table} changed its source columns"
        )));
    }
    metadata.columns = source_columns;
    manifest
        .part_checksums
        .insert(format!("tables/{table}.ndjson"), metadata.checksum.clone());
    manifest.tables.insert(table.to_string(), metadata);
    Ok(())
}

fn append_manifest_table_rows(
    root: &Path,
    manifest: &mut BackupManifest,
    table: &str,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), BackupServiceError> {
    let metadata = manifest
        .tables
        .get(table)
        .ok_or_else(|| BackupServiceError::Validation(format!("backup has no {table} table")))?;
    let metadata = append_table_objects(root, table, rows, metadata)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    manifest
        .part_checksums
        .insert(format!("tables/{table}.ndjson"), metadata.checksum.clone());
    manifest.tables.insert(table.to_string(), metadata);
    Ok(())
}

fn rewrite_path_value(
    row: &mut serde_json::Map<String, serde_json::Value>,
    column: &str,
    roots: &[PathRemap],
) {
    let Some(path) = row.get(column).and_then(serde_json::Value::as_str) else {
        return;
    };
    let Some(rewritten) = rewrite_first_matching_prefix(path, roots) else {
        return;
    };
    row.insert(
        column.into(),
        serde_json::Value::String(rewritten.display().to_string()),
    );
}

fn rewrite_first_matching_prefix(path: &str, roots: &[PathRemap]) -> Option<PathBuf> {
    roots
        .iter()
        .find_map(|remap| rewrite_stored_prefix(path, &remap.source, &remap.target))
}

fn rewrite_post_processing_settings(
    raw: &str,
    roots: &[PathRemap],
) -> Result<String, crate::StateError> {
    let mut value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| crate::StateError::Database(error.to_string()))?;
    let object = value.as_object_mut().ok_or_else(|| {
        crate::StateError::Database("post-processing settings are not a JSON object".into())
    })?;
    for key in ["allowedRoots", "allowed_roots"] {
        if let Some(allowed) = object.get_mut(key) {
            rewrite_root_array(allowed, roots)?;
        }
    }
    serde_json::to_string(&value).map_err(|error| crate::StateError::Database(error.to_string()))
}

fn rewrite_json_column(
    row: &mut serde_json::Map<String, serde_json::Value>,
    column: &str,
    roots: &[PathRemap],
) -> Result<(), crate::StateError> {
    let Some(raw) = row.get(column).and_then(serde_json::Value::as_str) else {
        return Ok(());
    };
    let rewritten = rewrite_approved_roots_json(raw, roots)?;
    row.insert(column.into(), serde_json::Value::String(rewritten));
    Ok(())
}

fn rewrite_approved_roots_json(
    raw: &str,
    roots: &[PathRemap],
) -> Result<String, crate::StateError> {
    let mut value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| crate::StateError::Database(error.to_string()))?;
    rewrite_approved_roots(&mut value, roots)?;
    serde_json::to_string(&value).map_err(|error| crate::StateError::Database(error.to_string()))
}

fn rewrite_approved_roots(
    value: &mut serde_json::Value,
    roots: &[PathRemap],
) -> Result<(), crate::StateError> {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                if matches!(key.as_str(), "approved_roots" | "approvedRoots") {
                    rewrite_root_array(value, roots)?;
                } else {
                    rewrite_approved_roots(value, roots)?;
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                rewrite_approved_roots(value, roots)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn rewrite_root_array(
    value: &mut serde_json::Value,
    roots: &[PathRemap],
) -> Result<(), crate::StateError> {
    let values = value.as_array_mut().ok_or_else(|| {
        crate::StateError::Database("approved filesystem roots are not an array".into())
    })?;
    let mut rewritten = Vec::with_capacity(values.len());
    for value in values.iter() {
        let root = value.as_str().ok_or_else(|| {
            crate::StateError::Database("approved filesystem root is not a string".into())
        })?;
        let Some(path) = rewrite_first_matching_prefix(root, roots) else {
            continue;
        };
        let path = serde_json::Value::String(path.display().to_string());
        if !rewritten.contains(&path) {
            rewritten.push(path);
        }
    }
    *values = rewritten;
    Ok(())
}

#[cfg(test)]
fn job_info_from_history(row: crate::JobHistoryRow) -> JobInfo {
    let status = job_status_from_persisted_str(&row.status, row.error_message.as_deref());
    let (download_state, post_state, run_state) = runtime_lanes_from_status_snapshot(&status);

    JobInfo {
        job_id: crate::jobs::ids::JobId(row.job_id),
        job_hash: row.job_hash.as_ref().and_then(|value| {
            (value.len() == 32).then(|| {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(value);
                hash
            })
        }),
        name: row.name,
        status: status.clone(),
        download_state,
        post_state,
        run_state,
        progress: 1.0,
        total_bytes: row.total_bytes,
        downloaded_bytes: row.downloaded_bytes,
        optional_recovery_bytes: row.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
        phase_progress: Vec::new(),
        failed_bytes: row.failed_bytes,
        health: row.health,
        total_files: 0,
        completed_files: 0,
        remaining_par_files: 0,
        password: None,
        category: row.category,
        metadata: row
            .metadata
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default(),
        output_dir: row.output_dir,
        error: if let JobStatus::Failed { error } = &status {
            Some(error.clone())
        } else {
            None
        },
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        created_at_epoch_ms: row.created_at as f64 * 1000.0,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PathRemap, absolute_restore_path, job_info_from_history, rewrite_path_with_roots,
        stored_path_has_prefix,
    };
    use std::path::PathBuf;

    #[test]
    fn path_rewrite_prefers_specific_complete_and_intermediate_roots() {
        let new_data = PathBuf::from("/new");
        let new_complete = PathBuf::from("/library");
        let new_intermediate = PathBuf::from("/scratch");
        let roots = [
            PathRemap {
                source: "/old/intermediate".into(),
                target: new_intermediate.clone(),
            },
            PathRemap {
                source: "/old/complete".into(),
                target: new_complete.clone(),
            },
            PathRemap {
                source: "/old".into(),
                target: new_data,
            },
        ];

        assert_eq!(
            rewrite_path_with_roots("/old/complete/movie/file.mkv", &roots),
            new_complete.join("movie/file.mkv").display().to_string()
        );
        assert_eq!(
            rewrite_path_with_roots("/old/intermediate/job/file.part", &roots),
            new_intermediate.join("job/file.part").display().to_string()
        );
        assert_eq!(
            rewrite_path_with_roots("/old/complete-ish/file", &roots),
            "/new/complete-ish/file"
        );
    }

    #[test]
    fn path_rewrite_handles_foreign_windows_paths() {
        let roots = [PathRemap {
            source: r"C:\Weaver\Data\Complete".into(),
            target: PathBuf::from("/library"),
        }];

        assert_eq!(
            rewrite_path_with_roots(r"c:\weaver\data\complete\Movie\Feature.mkv", &roots),
            PathBuf::from("/library/Movie/Feature.mkv")
                .display()
                .to_string()
        );
        assert!(stored_path_has_prefix(
            r"C:\Weaver\Data\Complete\Movie",
            r"c:\weaver\data\complete"
        ));
        assert!(!stored_path_has_prefix(
            r"C:\Weaver\Data\Complete-ish\Movie",
            r"C:\Weaver\Data\Complete"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn restore_targets_reject_parent_traversal() {
        assert!(absolute_restore_path("data_dir", "/safe/../escape").is_err());
        assert_eq!(
            absolute_restore_path("data_dir", "/safe/./target").unwrap(),
            PathBuf::from("/safe/target")
        );
    }

    #[test]
    fn history_snapshot_preserves_repairing_and_moving_status_shapes() {
        let repairing = job_info_from_history(crate::JobHistoryRow {
            job_id: 1,
            job_hash: None,
            name: "repairing".into(),
            status: "repairing".into(),
            error_message: None,
            total_bytes: 1,
            downloaded_bytes: 1,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
        });
        assert_eq!(repairing.status, crate::JobStatus::Repairing);
        assert_eq!(repairing.download_state, crate::DownloadState::Complete);
        assert_eq!(repairing.post_state, crate::PostState::Repairing);

        let moving = job_info_from_history(crate::JobHistoryRow {
            job_id: 2,
            job_hash: None,
            name: "moving".into(),
            status: "moving".into(),
            error_message: None,
            total_bytes: 1,
            downloaded_bytes: 1,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
        });
        assert_eq!(moving.status, crate::JobStatus::Moving);
        assert_eq!(moving.download_state, crate::DownloadState::Complete);
        assert_eq!(moving.post_state, crate::PostState::Finalizing);
    }
}
