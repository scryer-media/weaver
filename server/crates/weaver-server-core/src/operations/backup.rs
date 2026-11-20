// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[cfg(test)]
use crate::{
    Database, HistoryFilter, SchedulerHandle, persistence::database_target::DatabaseTarget,
    rss::RssService,
};
#[cfg(test)]
use std::{fs::File, path::PathBuf};

#[cfg(test)]
use weaver_server_pipeline::operations::backup::LEGACY_V1_STABLE_TABLES;
pub use weaver_server_pipeline::operations::backup::{
    OperationsBackupQueries, StableStateExport, create_backup_temp_dir,
};
use weaver_server_pipeline::operations::backup::{catalog, logical, permissions};
mod archive;
mod automatic;
pub(crate) use logical::OperationsBackupLogicalInternalQueries;
mod manifest;
mod pending;
mod restore;
mod service;
mod stored;
mod upgrade;
pub use upgrade::{
    prepare_upgrade_backup, record_started_version, reset_automatic_backup_settings,
    skip_upgrade_backup,
};

#[cfg(test)]
pub(crate) use self::manifest::{BACKUP_SCOPE, required_category_remaps};
pub use self::manifest::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupServiceError, BackupSourcePaths,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, RestoreOptions, RestoreReport,
};
pub use self::service::BackupService;
pub use automatic::{AutoBackupSettings, AutoBackupSettingsInput, compute_next_auto_backup_run_at};
pub use logical::TablePartMetadata;
pub use pending::{PendingRestoreOutcome, apply_pending_restore};
pub use stored::{
    BACKUP_EXECUTION_TIMEOUT, BackupArtifactStatus, BackupInfo, BackupSettings, BackupTrigger,
};

#[cfg(test)]
fn artifact_datastore(pool: &sqlx::SqlitePool) -> crate::persistence::sql_runtime::StoreDatastore {
    crate::persistence::sql_runtime::StoreDatastore::Sqlite {
        pool: pool.clone(),
        writer_gate: weaver_server_db::persistence::sqlite_writer::new_writer_gate(None),
    }
}

#[cfg(test)]
async fn open_postgres_backup_test_db(test_name: &str) -> Option<(Database, sqlx::PgPool, String)> {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is not set");
        return None;
    };
    if base_url.trim().is_empty() {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is empty");
        return None;
    }

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!(
        "weaver_backup_{test_name}_{}_{}",
        std::process::id(),
        suffix
    );
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    let create = format!("CREATE SCHEMA {schema}");
    crate::persistence::sql_runtime::SqlRuntime::execute(
        crate::persistence::sql_runtime::StoreDatastore::Postgres {
            pool: admin.clone(),
        }
        .write_exec(),
        &create,
        &[],
    )
    .await
    .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
    let mut db = Database::open_target(DatabaseTarget::PostgresUrl(url)).unwrap();
    db.set_encryption_key(crate::persistence::encryption::EncryptionKey::generate());
    Some((db, admin, schema))
}

#[cfg(test)]
async fn drop_postgres_backup_test_schema(admin: sqlx::PgPool, schema: String) {
    let drop_schema = format!("DROP SCHEMA IF EXISTS {schema} CASCADE");
    crate::persistence::sql_runtime::SqlRuntime::execute(
        crate::persistence::sql_runtime::StoreDatastore::Postgres {
            pool: admin.clone(),
        }
        .write_exec(),
        &drop_schema,
        &[],
    )
    .await
    .unwrap();
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod backup_service_tests;
