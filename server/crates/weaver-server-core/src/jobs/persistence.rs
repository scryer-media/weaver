// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::jobs::model::{FieldUpdate, JobUpdate};
use crate::jobs::record::{ActiveFileIdentity, ActiveJob};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{
    POSTGRES_BATCH_BIND_LIMIT, SQLITE_BATCH_BIND_LIMIT, SqlArg, SqlEngine, SqlRuntime, SqlTx,
    max_rows_for_tx,
};
use sqlx::{Postgres, QueryBuilder, Sqlite};

// Submission values are always plaintext, even when they resemble our stored
// envelope prefix. Encrypt them once at the database boundary.
pub(super) fn encrypt_archive_password(
    key: Option<&crate::persistence::encryption::EncryptionKey>,
    password: Option<&str>,
) -> Result<Option<String>, StateError> {
    let Some(password) = password else {
        return Ok(None);
    };
    if password.is_empty() {
        return Ok(Some(String::new()));
    }
    let key = key.ok_or_else(|| {
        StateError::Database("encryption key is required to store archive passwords".into())
    })?;
    crate::persistence::encryption::encrypt_value(key, password)
        .map(Some)
        .map_err(StateError::Database)
}

// A job's supplied archive passwords, sealed as one value the way its
// password is: a JSON list of `[source label, password]` pairs. An empty list
// stores nothing.
pub(super) fn encrypt_supplied_passwords(
    key: Option<&crate::persistence::encryption::EncryptionKey>,
    candidates: &[crate::jobs::ArchivePasswordCandidate],
) -> Result<Option<String>, StateError> {
    if candidates.is_empty() {
        return Ok(None);
    }
    let pairs: Vec<(&str, &str)> = candidates
        .iter()
        .map(|candidate| (candidate.source().as_str(), candidate.value()))
        .collect();
    let json = serde_json::to_string(&pairs)
        .map_err(|error| StateError::Database(format!("invalid archive passwords: {error}")))?;
    encrypt_archive_password(key, Some(&json))
}

pub(crate) fn decrypt_supplied_passwords(
    key: Option<&crate::persistence::encryption::EncryptionKey>,
    stored: Option<String>,
) -> Result<Vec<crate::jobs::ArchivePasswordCandidate>, StateError> {
    let json = match decrypt_archive_password(key, stored)? {
        Some(json) if !json.is_empty() => json,
        _ => return Ok(Vec::new()),
    };
    let pairs: Vec<(String, String)> = serde_json::from_str(&json)
        .map_err(|error| StateError::Database(format!("invalid archive passwords: {error}")))?;
    pairs
        .into_iter()
        .map(|(label, value)| {
            crate::jobs::ArchivePasswordSource::from_label(&label)
                .map(|source| crate::jobs::ArchivePasswordCandidate::new(source, value))
                .ok_or_else(|| {
                    StateError::Database(format!("unknown archive password source {label}"))
                })
        })
        .collect()
}

pub(crate) fn decrypt_archive_password(
    key: Option<&crate::persistence::encryption::EncryptionKey>,
    password: Option<String>,
) -> Result<Option<String>, StateError> {
    let Some(password) = password else {
        return Ok(None);
    };
    if !crate::persistence::encryption::is_encrypted(&password) {
        return Ok(Some(password));
    }
    let key = key.ok_or_else(|| {
        StateError::Database("encryption key is required to restore archive passwords".into())
    })?;
    crate::persistence::encryption::decrypt_value(key, &password)
        .map(Some)
        .map_err(StateError::Database)
}

async fn active_job_exists_tx(tx: &mut SqlTx<'_>, job_id: JobId) -> Result<bool, StateError> {
    let sql = match tx {
        SqlTx::Postgres(_) => "SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE",
        SqlTx::Sqlite(_) => "SELECT 1 FROM active_jobs WHERE job_id = {} LIMIT 1",
    };
    Ok(tx
        .fetch_optional(sql, &[SqlArg::I64(job_id.0 as i64)])
        .await?
        .is_some())
}

pub(super) async fn lock_active_job_for_write_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
) -> Result<(), StateError> {
    if matches!(tx, SqlTx::Postgres(_)) {
        tx.fetch_optional(
            "SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn lock_active_job_for_delete_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
) -> Result<(), StateError> {
    if matches!(tx, SqlTx::Postgres(_)) {
        tx.fetch_optional(
            "SELECT 1 FROM active_jobs WHERE job_id = {} FOR UPDATE",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?;
    }
    Ok(())
}

fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

pub(crate) fn metadata_json(metadata: &[(String, String)]) -> Result<Option<String>, StateError> {
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(metadata).map(Some).map_err(db_err)
    }
}

async fn bulk_upsert_file_identities_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    identities: &[ActiveFileIdentity],
) -> Result<(), StateError> {
    if identities.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 9);
    let conflict_sql = " ON CONFLICT(job_id, file_index) DO UPDATE SET
                            source_filename = excluded.source_filename,
                            current_filename = excluded.current_filename,
                            canonical_filename = excluded.canonical_filename,
                            classification_kind = excluded.classification_kind,
                            classification_set_name = excluded.classification_set_name,
                            classification_volume_index = excluded.classification_volume_index,
                            classification_source = excluded.classification_source";
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in identities.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO active_file_identities
                     (job_id, file_index, source_filename, current_filename, canonical_filename,
                      classification_kind, classification_set_name, classification_volume_index,
                      classification_source) ",
                );
                builder.push_values(chunk, |mut row, identity| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(identity.file_index as i64)
                        .push_bind(identity.source_filename.clone())
                        .push_bind(identity.current_filename.clone())
                        .push_bind(identity.canonical_filename.clone())
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.kind.as_str().to_string()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.set_name.clone()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .and_then(|classification| classification.volume_index)
                                .map(|value| value as i64),
                        )
                        .push_bind(identity.classification_source.as_str().to_string());
                });
                builder.push(conflict_sql);
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in identities.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO active_file_identities
                     (job_id, file_index, source_filename, current_filename, canonical_filename,
                      classification_kind, classification_set_name, classification_volume_index,
                      classification_source) ",
                );
                builder.push_values(chunk, |mut row, identity| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(identity.file_index as i64)
                        .push_bind(identity.source_filename.clone())
                        .push_bind(identity.current_filename.clone())
                        .push_bind(identity.canonical_filename.clone())
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.kind.as_str().to_string()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.set_name.clone()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .and_then(|classification| classification.volume_index)
                                .map(|value| value as i64),
                        )
                        .push_bind(identity.classification_source.as_str().to_string());
                });
                builder.push(conflict_sql);
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
}

// Update `active_files.filename` for each identity's `current_filename`, the
// sibling write [`Database::save_file_identity`] performs alongside the
// identity upsert. Rows whose `active_files` entry does not yet exist (file
// not completed) are simply not matched, exactly as the single-row path. Runs
// inside the caller's transaction: Postgres batches with `UPDATE ... FROM
// (VALUES ...)`, SQLite issues per-row updates (local to the serialized
// writer, so the extra statements are cheap).
async fn bulk_update_active_files_filenames_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    identities: &[ActiveFileIdentity],
) -> Result<(), StateError> {
    if identities.is_empty() {
        return Ok(());
    }

    if matches!(tx, SqlTx::Sqlite(_)) {
        for identity in identities {
            tx.execute(
                "UPDATE active_files SET filename = {}
                 WHERE job_id = {} AND file_index = {}",
                &[
                    SqlArg::Text(identity.current_filename.clone()),
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::I64(identity.file_index as i64),
                ],
            )
            .await?;
        }
        return Ok(());
    }

    if let SqlTx::Postgres(tx) = tx {
        // 3 binds per row (job_id, file_index, filename).
        let chunk_size = (POSTGRES_BATCH_BIND_LIMIT / 3).max(1);
        for chunk in identities.chunks(chunk_size) {
            let mut builder = QueryBuilder::<Postgres>::new(
                "UPDATE active_files SET filename = v.filename FROM (",
            );
            builder.push_values(chunk, |mut row, identity| {
                row.push_bind(job_id.0 as i64)
                    .push_bind(identity.file_index as i64)
                    .push_bind(identity.current_filename.clone());
            });
            builder.push(
                ") AS v(job_id, file_index, filename)
                 WHERE active_files.job_id = v.job_id
                   AND active_files.file_index = v.file_index",
            );
            builder.build().execute(&mut **tx).await.map_err(db_err)?;
        }
    }
    Ok(())
}

// How a persisted completed-file MD5 was obtained. Legacy rows predate this
// distinction (NULL in `active_files.md5_provenance`) and are never trusted
// for quick verification — they can identify a file, not certify it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletedHashProvenance {
    // Folded from the decode pass over the downloaded bytes themselves.
    Streamed,
    // Confirmed by an actual PAR2 verification pass over the file.
    Verified,
}

impl CompletedHashProvenance {
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::Streamed => "streamed",
            Self::Verified => "verified",
        }
    }
}

// Build the Postgres autocommit statement that upserts `row_count` completed
// files guarded by a `FOR KEY SHARE` lock on the owning job, mirroring the
// single-row [`Database::complete_file_with_optional_hash`] shape. `$1` is the
// guard `job_id`; each row then binds `(job_id, file_index, filename, md5)`.
fn build_complete_files_pg_sql(row_count: usize) -> String {
    let mut values = String::new();
    for row in 0..row_count {
        if row > 0 {
            values.push_str(", ");
        }
        if row == 0 {
            values.push_str("({}::bigint, {}::bigint, {}::text, {}::bytea, {}::text)");
        } else {
            values.push_str("({}, {}, {}, {}, {})");
        }
    }
    format!(
        "WITH active_complete_files_active AS (
             SELECT 1 FROM active_jobs WHERE job_id = {{}} FOR KEY SHARE
         )
         INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance)
         SELECT d.job_id, d.file_index, d.filename, d.md5, d.md5_provenance
         FROM (VALUES {values}) AS d(job_id, file_index, filename, md5, md5_provenance),
              active_complete_files_active
         ON CONFLICT(job_id, file_index) DO UPDATE SET
            filename = excluded.filename,
            md5 = excluded.md5,
            md5_provenance = excluded.md5_provenance"
    )
}

impl Database {
    pub fn create_active_job(&self, job: &ActiveJob) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::I64(job.job_id.0 as i64),
            SqlArg::Bytes(job.nzb_hash.to_vec()),
            SqlArg::Text(job.nzb_path.to_str().unwrap_or("").to_string()),
            SqlArg::Bytes(job.nzb_zstd.clone()),
            SqlArg::Text(job.output_dir.to_str().unwrap_or("").to_string()),
            SqlArg::Text(job.status.to_string()),
            SqlArg::Text(job.download_state.to_string()),
            SqlArg::Text(job.post_state.to_string()),
            SqlArg::Text(job.run_state.to_string()),
            SqlArg::I64(job.created_at as i64),
            SqlArg::OptText(job.category.clone()),
            SqlArg::OptText(metadata_json(&job.metadata)?),
            SqlArg::OptText(job.paused_resume_status.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_download_state.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_post_state.map(str::to_string)),
            SqlArg::OptText(encrypt_archive_password(
                self.encryption_key(),
                job.password_override.as_deref(),
            )?),
            SqlArg::OptText(encrypt_supplied_passwords(
                self.encryption_key(),
                &job.supplied_password_candidates,
            )?),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_active_job", |tx| {
                let args = args.clone();
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO active_jobs
                         (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state, password, password_candidates)
                         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                        &args,
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    #[allow(dead_code)] // retained for restoration paths that bypass admission.
    pub(crate) fn create_active_job_with_file_identities(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job.job_id;
        let identities = identities.to_vec();
        let args = vec![
            SqlArg::I64(job.job_id.0 as i64),
            SqlArg::Bytes(job.nzb_hash.to_vec()),
            SqlArg::Text(job.nzb_path.to_str().unwrap_or("").to_string()),
            SqlArg::Bytes(job.nzb_zstd.clone()),
            SqlArg::Text(job.output_dir.to_str().unwrap_or("").to_string()),
            SqlArg::Text(job.status.to_string()),
            SqlArg::Text(job.download_state.to_string()),
            SqlArg::Text(job.post_state.to_string()),
            SqlArg::Text(job.run_state.to_string()),
            SqlArg::I64(job.created_at as i64),
            SqlArg::OptText(job.category.clone()),
            SqlArg::OptText(metadata_json(&job.metadata)?),
            SqlArg::OptText(job.paused_resume_status.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_download_state.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_post_state.map(str::to_string)),
            SqlArg::OptText(encrypt_archive_password(
                self.encryption_key(),
                job.password_override.as_deref(),
            )?),
            SqlArg::OptText(encrypt_supplied_passwords(
                self.encryption_key(),
                &job.supplied_password_candidates,
            )?),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "create_active_job_with_file_identities",
                |tx| {
                    let args = args.clone();
                    let identities = identities.clone();
                    Box::pin(async move {
                        tx.execute(
                            "INSERT INTO active_jobs
                             (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state, password, password_candidates)
                             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            &args,
                        )
                        .await?;
                        bulk_upsert_file_identities_tx(tx, job_id, &identities).await?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    // Performs the inexpensive first half of SCORE materialization before a
    // pipeline creates a work directory. The transactional check below remains
    // authoritative; this keeps a known-stale submit from allocating anything.
    pub(crate) fn semantic_materialization_is_current(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 FROM semantic_duplicate_candidates
                 WHERE job_id = {} AND candidate_state = {} AND materialization_generation = {}",
                &[
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::Text("active".to_string()),
                    SqlArg::I64(generation),
                ],
            )
            .await?
            .is_some())
        })
    }

    // Checks an owned promotion lease before the scheduler allocates a
    // working directory. The materialization transaction repeats this CAS.
    pub(crate) fn semantic_promotion_materialization_is_current(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let now = (crate::jobs::epoch_ms_now() / 1_000.0) as i64;
        self.run_sql_blocking(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 FROM semantic_duplicate_candidates
                 WHERE job_id = {} AND candidate_state = {}
                   AND promotion_state = {} AND promotion_generation = {}
                   AND promotion_lease_expires_at > {}",
                &[
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::Text("active".to_string()),
                    SqlArg::Text("claimed".to_string()),
                    SqlArg::I64(generation),
                    SqlArg::I64(now),
                ],
            )
            .await?
            .is_some())
        })
    }

    // Atomically turns a reserved duplicate admission into an active job. A
    // SCORE caller supplies the generation it received at admission; parking
    // or superseding that candidate invalidates the generation before any
    // stale request can create scheduler work.
    pub(crate) fn materialize_admission(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
        semantic_materialization_generation: Option<i64>,
        semantic_promotion_generation: Option<i64>,
        restart: bool,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let job_id = job.job_id;
        let identities = identities.to_vec();
        let now = (crate::jobs::epoch_ms_now() / 1_000.0) as i64;
        let args = vec![
            SqlArg::I64(job.job_id.0 as i64),
            SqlArg::Bytes(job.nzb_hash.to_vec()),
            SqlArg::Text(job.nzb_path.to_str().unwrap_or("").to_string()),
            SqlArg::Bytes(job.nzb_zstd.clone()),
            SqlArg::Text(job.output_dir.to_str().unwrap_or("").to_string()),
            SqlArg::Text(job.status.to_string()),
            SqlArg::Text(job.download_state.to_string()),
            SqlArg::Text(job.post_state.to_string()),
            SqlArg::Text(job.run_state.to_string()),
            SqlArg::I64(job.created_at as i64),
            SqlArg::OptText(job.category.clone()),
            SqlArg::OptText(metadata_json(&job.metadata)?),
            SqlArg::OptText(job.paused_resume_status.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_download_state.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_post_state.map(str::to_string)),
            SqlArg::OptText(encrypt_archive_password(
                self.encryption_key(),
                job.password_override.as_deref(),
            )?),
            SqlArg::OptText(encrypt_supplied_passwords(
                self.encryption_key(),
                &job.supplied_password_candidates,
            )?),
        ];
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "materialize_active_job_with_file_identities",
                |tx| {
                    let args = args.clone();
                    let identities = identities.clone();
                    Box::pin(async move {
                        if restart {
                            let deleted = crate::history::persistence::delete_job_history_bundles_tx(tx, &[job_id]).await?;
                            if deleted.is_empty() {
                                return Err(StateError::Database("restart history row disappeared".into()));
                            }
                        }
                        if let Some(generation) = semantic_materialization_generation {
                            let lock = match tx {
                                SqlTx::Postgres(_) => " FOR UPDATE",
                                SqlTx::Sqlite(_) => "",
                            };
                            let Some(group) = tx
                                .fetch_optional(
                                    "SELECT group_id FROM semantic_duplicate_candidates WHERE job_id = {}",
                                    &[SqlArg::I64(job_id.0 as i64)],
                                )
                                .await?
                            else {
                                return Ok(false);
                            };
                            tx.fetch_optional(
                                &format!(
                                    "SELECT group_id FROM semantic_duplicate_groups WHERE group_id = {{}}{lock}"
                                ),
                                &[SqlArg::I64(group.i64("group_id")?)],
                            )
                            .await?
                            .ok_or_else(|| {
                                StateError::Database(
                                    "semantic duplicate group disappeared during materialization"
                                        .to_string(),
                                )
                            })?;
                            let claimed = tx
                                .execute(
                                    "UPDATE semantic_duplicate_candidates
                                     SET updated_at = {}
                                     WHERE job_id = {} AND candidate_state = {}
                                       AND materialization_generation = {}",
                                    &[
                                        SqlArg::I64(now),
                                        SqlArg::I64(job_id.0 as i64),
                                        SqlArg::Text("active".to_string()),
                                        SqlArg::I64(generation),
                                    ],
                                )
                                .await?;
                            if claimed != 1 {
                                return Ok(false);
                            }
                        }
                        if let Some(generation) = semantic_promotion_generation {
                            let lock = match tx {
                                SqlTx::Postgres(_) => " FOR UPDATE",
                                SqlTx::Sqlite(_) => "",
                            };
                            let Some(group) = tx
                                .fetch_optional(
                                    "SELECT group_id FROM semantic_duplicate_candidates WHERE job_id = {}",
                                    &[SqlArg::I64(job_id.0 as i64)],
                                )
                                .await?
                            else {
                                return Ok(false);
                            };
                            tx.fetch_optional(
                                &format!(
                                    "SELECT group_id FROM semantic_duplicate_groups WHERE group_id = {{}}{lock}"
                                ),
                                &[SqlArg::I64(group.i64("group_id")?)],
                            )
                            .await?
                            .ok_or_else(|| {
                                StateError::Database(
                                    "semantic duplicate group disappeared during promotion"
                                        .to_string(),
                                )
                            })?;
                            let claimed = tx
                                .execute(
                                    "UPDATE semantic_duplicate_candidates
                                     SET updated_at = {}
                                     WHERE job_id = {} AND candidate_state = {}
                                       AND promotion_state = {} AND promotion_generation = {}
                                       AND promotion_lease_expires_at > {}",
                                    &[
                                        SqlArg::I64(now),
                                        SqlArg::I64(job_id.0 as i64),
                                        SqlArg::Text("active".to_string()),
                                        SqlArg::Text("claimed".to_string()),
                                        SqlArg::I64(generation),
                                        SqlArg::I64(now),
                                    ],
                                )
                                .await?;
                            if claimed != 1 {
                                return Ok(false);
                            }
                        }
                        tx.execute(
                            "UPDATE duplicate_job_snapshots
                             SET lifecycle = {}, reservation_expires_at = NULL, updated_at = {}
                             WHERE job_id = {}",
                            &[
                                SqlArg::Text("active".to_string()),
                                SqlArg::I64(now),
                                SqlArg::I64(job_id.0 as i64),
                            ],
                        )
                        .await?;
                        tx.execute(
                            "INSERT INTO active_jobs
                             (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state, password, password_candidates)
                             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            &args,
                        )
                        .await?;
                        bulk_upsert_file_identities_tx(tx, job_id, &identities).await?;
                        Ok(true)
                    })
                },
            )
            .await
        });
        if restart && matches!(result, Ok(true)) {
            self.forget_script_effects_for(&[job_id.0]);
            self.invalidate_job_history_cache_for(&[job_id.0]);
            self.notify_script_events_changed();
        }
        result
    }

    pub(crate) fn refresh_script_job_inputs(
        &self,
        job_id: u64,
        facts: &mut crate::post_processing::runner::CompatibilityFacts,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let row = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT metadata, password FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await
        })?;
        if let Some(row) = row {
            facts.parameters = row
                .opt_text("metadata")?
                .map(|text| serde_json::from_str(&text).map_err(db_err))
                .transpose()?
                .unwrap_or_default();
            facts.password =
                decrypt_archive_password(self.encryption_key(), row.opt_text("password")?)?;
        }
        Ok(())
    }

    pub fn update_active_job(&self, job_id: JobId, update: &JobUpdate) -> Result<(), StateError> {
        let datastore = self.datastore();
        let mut update = update.clone();
        if let FieldUpdate::Set(password) = &mut update.password {
            *password = encrypt_archive_password(self.encryption_key(), Some(password))?
                .expect("supplied password");
        }
        let _effects_writer = self.lock_script_effects_writer();
        let rewritten_effects = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_active_job", |tx| {
                let update = update.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1",
                        &[],
                    )
                    .await?;
                    let mut rewritten_effects = None;
                    match &update.category {
                        FieldUpdate::Unchanged => {}
                        FieldUpdate::Clear => {
                            tx.execute(
                                "UPDATE active_jobs SET category = NULL WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                        FieldUpdate::Set(category) => {
                            tx.execute(
                                "UPDATE active_jobs SET category = {} WHERE job_id = {}",
                                &[SqlArg::Text(category.clone()), SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                    }

                    if (!matches!(update.metadata, FieldUpdate::Unchanged)
                        || !update.metadata_edits.is_empty()
                        || update.name.is_some())
                        && let Some(active) = tx
                            .fetch_optional(
                                "SELECT metadata FROM active_jobs WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?
                    {
                        let mut metadata: Vec<(String, String)> = active
                            .opt_text("metadata")?
                            .map(|text| serde_json::from_str(&text).map_err(db_err))
                            .transpose()?
                            .unwrap_or_default();
                        update.apply_to_metadata(&mut metadata);
                        if let Some(name) = &update.name {
                            crate::ingest::set_renamed_title(&mut metadata, name);
                        }
                        if let Some(row) = tx
                            .fetch_optional(
                                "SELECT state FROM script_job_state WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?
                        {
                            let mut effects: crate::post_processing::effects::JobScriptEffects =
                                serde_json::from_str(&row.text("state")?)
                                    .map_err(|error| StateError::Database(error.to_string()))?;
                            if !matches!(update.metadata, FieldUpdate::Unchanged) {
                                for value in effects.parameters.values_mut() {
                                    value.clear();
                                }
                                effects.parameters.extend(metadata.clone());
                            } else {
                                let renamed = update.name.as_ref().map(|name| {
                                    (
                                        crate::ingest::RENAMED_TITLE_METADATA_KEY.to_string(),
                                        Some(name.clone()),
                                    )
                                });
                                for (key, value) in
                                    update.metadata_edits.iter().chain(renamed.iter())
                                {
                                    for (existing, previous) in &mut effects.parameters {
                                        if existing.eq_ignore_ascii_case(key) {
                                            previous.clear();
                                        }
                                    }
                                    effects
                                        .parameters
                                        .insert(key.clone(), value.clone().unwrap_or_default());
                                }
                            }
                            tx.execute(
                                "UPDATE script_job_state SET state = {} WHERE job_id = {}",
                                &[
                                    SqlArg::Text(serde_json::to_string(&effects).map_err(
                                        |error| StateError::Database(error.to_string()),
                                    )?),
                                    SqlArg::I64(job_id.0 as i64),
                                ],
                            )
                            .await?;
                            rewritten_effects = Some(effects);
                        }
                        tx.execute(
                            "UPDATE active_jobs SET metadata = {} WHERE job_id = {}",
                            &[
                                SqlArg::OptText(metadata_json(&metadata)?),
                                SqlArg::I64(job_id.0 as i64),
                            ],
                        )
                        .await?;
                    }

                    // Password override column: NULL = no override (restore
                    // keeps the NZB-derived password), '' = explicitly no
                    // password, anything else = the encrypted override.
                    match update.password {
                        FieldUpdate::Unchanged => {}
                        FieldUpdate::Clear => {
                            tx.execute(
                                "UPDATE active_jobs SET password = {} WHERE job_id = {}",
                                &[SqlArg::Text(String::new()), SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                        FieldUpdate::Set(password) => {
                            tx.execute(
                                "UPDATE active_jobs SET password = {} WHERE job_id = {}",
                                &[SqlArg::Text(password), SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                    }
                    Ok(rewritten_effects)
                })
            })
            .await
        })?;
        if let Some(effects) = rewritten_effects {
            self.cache_script_effects(job_id.0, effects);
        }
        Ok(())
    }

    pub fn update_active_job_output_dir(
        &self,
        job_id: JobId,
        output_dir: &std::path::Path,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let output_dir = output_dir.to_string_lossy().into_owned();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_active_job_output_dir", |tx| {
                let output_dir = output_dir.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE active_jobs SET output_dir = {} WHERE job_id = {}",
                        &[SqlArg::Text(output_dir), SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    // Persist the manual queue order as one transaction. Positions are the
    // full current order (small: one row per queued job) so restores sort by
    // `queue_position` and land in the exact user-arranged sequence.
    pub fn update_active_job_queue_positions(
        &self,
        positions: &[(JobId, i64)],
    ) -> Result<(), StateError> {
        if positions.is_empty() {
            return Ok(());
        }
        // One statement instead of one UPDATE per job: a CASE expression maps
        // each job id to its new position. Ids/positions are internal
        // integers, inlined as decimals directly in the SQL text (no user
        // strings), the same way `get_job_event_stage_bounds` inlines trusted
        // integer ids. Plain CASE/WHEN/END and WHERE IN are portable across
        // both sqlite and postgres.
        let case_arms = positions
            .iter()
            .map(|(job_id, position)| format!("WHEN {} THEN {}", job_id.0, position))
            .collect::<Vec<_>>()
            .join(" ");
        let ids = positions
            .iter()
            .map(|(job_id, _)| job_id.0.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE active_jobs
                SET queue_position = CASE job_id {case_arms} END
              WHERE job_id IN ({ids})"
        );
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::execute(datastore.read_exec(), &sql, &[]).await?;
            Ok(())
        })
    }

    pub fn set_active_job_status(
        &self,
        job_id: JobId,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), StateError> {
        self.set_active_job_runtime(
            job_id, status, None, None, None, error, None, None, None, None, None,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "active job runtime is persisted as one atomic SQL update"
    )]
    pub fn set_active_job_runtime(
        &self,
        job_id: JobId,
        status: &str,
        download_state: Option<&str>,
        post_state: Option<&str>,
        run_state: Option<&str>,
        error: Option<&str>,
        queued_repair_at_epoch_ms: Option<f64>,
        queued_extract_at_epoch_ms: Option<f64>,
        paused_resume_status: Option<&str>,
        paused_resume_download_state: Option<&str>,
        paused_resume_post_state: Option<&str>,
    ) -> Result<(), StateError> {
        self.set_active_job_runtime_at(
            job_id,
            None,
            status,
            download_state,
            post_state,
            run_state,
            error,
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status,
            paused_resume_download_state,
            paused_resume_post_state,
        )
    }

    // [`Self::set_active_job_runtime`] that also moves the job's recorded
    // output directory in the same statement, so a restart can never read the
    // new runtime against the old location or the other way round. `None`
    // keeps the directory already recorded.
    #[expect(
        clippy::too_many_arguments,
        reason = "active job runtime is persisted as one atomic SQL update"
    )]
    pub fn set_active_job_runtime_at(
        &self,
        job_id: JobId,
        output_dir: Option<&str>,
        status: &str,
        download_state: Option<&str>,
        post_state: Option<&str>,
        run_state: Option<&str>,
        error: Option<&str>,
        queued_repair_at_epoch_ms: Option<f64>,
        queued_extract_at_epoch_ms: Option<f64>,
        paused_resume_status: Option<&str>,
        paused_resume_download_state: Option<&str>,
        paused_resume_post_state: Option<&str>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::OptText(output_dir.map(str::to_string)),
            SqlArg::Text(status.to_string()),
            SqlArg::OptText(download_state.map(str::to_string)),
            SqlArg::OptText(post_state.map(str::to_string)),
            SqlArg::OptText(run_state.map(str::to_string)),
            SqlArg::OptText(error.map(str::to_string)),
            SqlArg::OptF64(queued_repair_at_epoch_ms),
            SqlArg::OptF64(queued_extract_at_epoch_ms),
            SqlArg::OptText(paused_resume_status.map(str::to_string)),
            SqlArg::OptText(paused_resume_download_state.map(str::to_string)),
            SqlArg::OptText(paused_resume_post_state.map(str::to_string)),
            SqlArg::I64(job_id.0 as i64),
        ];
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "set_active_job_runtime", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            tx.execute(
                                "UPDATE active_jobs
                                 SET output_dir = COALESCE({}, output_dir),
                                     status = {},
                                     download_state = {},
                                     post_state = {},
                                     run_state = {},
                                     error = {},
                                     queued_repair_at_epoch_ms = {},
                                     queued_extract_at_epoch_ms = {},
                                     paused_resume_status = {},
                                     paused_resume_download_state = {},
                                     paused_resume_post_state = {}
                                 WHERE job_id = {}",
                                &args,
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                SqlEngine::Postgres => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_jobs
                         SET output_dir = COALESCE({}, output_dir),
                             status = {},
                             download_state = {},
                             post_state = {},
                             run_state = {},
                             error = {},
                             queued_repair_at_epoch_ms = {},
                             queued_extract_at_epoch_ms = {},
                             paused_resume_status = {},
                             paused_resume_download_state = {},
                             paused_resume_post_state = {}
                         WHERE job_id = {}",
                        &args,
                    )
                    .await?;
                    Ok(())
                }
            }
        })
    }

    pub fn complete_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: &[u8; 16],
    ) -> Result<(), StateError> {
        self.complete_file_with_optional_hash(job_id, file_index, filename, Some(md5))
    }

    pub fn complete_file_with_optional_hash(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: Option<&[u8; 16]>,
    ) -> Result<(), StateError> {
        self.complete_file_with_provenance(
            job_id,
            file_index,
            filename,
            md5,
            CompletedHashProvenance::Streamed,
        )
    }

    pub fn complete_file_with_provenance(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: Option<&[u8; 16]>,
        provenance: CompletedHashProvenance,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let filename = filename.to_string();
        let md5 = md5.map(|md5| md5.to_vec());
        let provenance = md5.is_some().then(|| provenance.as_sql().to_string());
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "complete_file", |tx| {
                        let filename = filename.clone();
                        let md5 = md5.clone();
                        let provenance = provenance.clone();
                        Box::pin(async move {
                            if active_job_exists_tx(tx, job_id).await? {
                                tx.execute(
                                    "INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance)
                                     VALUES ({}, {}, {}, {}, {})
                                     ON CONFLICT(job_id, file_index) DO UPDATE SET
                                        filename = excluded.filename,
                                        md5 = excluded.md5,
                                        md5_provenance = excluded.md5_provenance",
                                    &[
                                        SqlArg::I64(job_id.0 as i64),
                                        SqlArg::I64(file_index as i64),
                                        SqlArg::Text(filename),
                                        SqlArg::OptBytes(md5),
                                        SqlArg::OptText(provenance),
                                    ],
                                )
                                .await?;
                            }
                            Ok(())
                        })
                    })
                    .await
                }
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "WITH active_file_complete_active AS (
                             SELECT 1
                             FROM active_jobs
                             WHERE job_id = {}
                             FOR KEY SHARE
                         )
                             INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance)
                             SELECT {}, {}, {}, {}, {}
                             FROM active_file_complete_active
                             ON CONFLICT(job_id, file_index) DO UPDATE SET
                                filename = excluded.filename,
                                md5 = excluded.md5,
                                md5_provenance = excluded.md5_provenance",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(file_index as i64),
                            SqlArg::Text(filename),
                            SqlArg::OptBytes(md5),
                            SqlArg::OptText(provenance),
                        ],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "complete_file",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    // Complete a batch of files for a single job, the bulk counterpart to
    // [`Self::complete_file_with_optional_hash`]. Each entry is
    // `(file_index, filename, optional md5)`; the guard semantics match the
    // single-file path (an absent `active_jobs` row inserts nothing). Postgres
    // runs one autocommit `FOR KEY SHARE`-guarded multi-row upsert per chunk;
    // SQLite guards once inside a transaction then bulk-upserts in chunks.
    pub fn complete_files(
        &self,
        job_id: JobId,
        entries: &[(u32, String, Option<[u8; 16]>)],
        provenance: CompletedHashProvenance,
    ) -> Result<(), StateError> {
        // 5 value binds per row (job_id, file_index, filename, md5,
        // md5_provenance) plus the single guard bind reused across the chunk.
        // Batched completions carry decode-streamed digests, so a present md5
        // records 'streamed' provenance.
        const BINDS_PER_ROW: usize = 5;

        if entries.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let entries = entries.to_vec();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    let chunk_size = (SQLITE_BATCH_BIND_LIMIT / BINDS_PER_ROW).max(1);
                    SqlRuntime::run_in_transaction(&datastore, "complete_files", |tx| {
                        let entries = entries.clone();
                        Box::pin(async move {
                            if !active_job_exists_tx(tx, job_id).await? {
                                return Ok(());
                            }
                            let SqlTx::Sqlite(tx) = tx else {
                                return Ok(());
                            };
                            for chunk in entries.chunks(chunk_size) {
                                let mut builder = QueryBuilder::<Sqlite>::new(
                                    "INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance) ",
                                );
                                builder.push_values(
                                    chunk,
                                    |mut row, (file_index, filename, md5)| {
                                        row.push_bind(job_id.0 as i64)
                                            .push_bind(*file_index as i64)
                                            .push_bind(filename.clone())
                                            .push_bind(md5.map(|md5| md5.to_vec()))
                                            .push_bind(md5.map(|_| provenance.as_sql()));
                                    },
                                );
                                builder.push(
                                    " ON CONFLICT(job_id, file_index) DO UPDATE SET
                                        filename = excluded.filename,
                                        md5 = excluded.md5,
                                        md5_provenance = excluded.md5_provenance",
                                );
                                builder.build().execute(&mut **tx).await.map_err(db_err)?;
                            }
                            Ok(())
                        })
                    })
                    .await
                }
                SqlEngine::Postgres => {
                    let chunk_size = (POSTGRES_BATCH_BIND_LIMIT / BINDS_PER_ROW).max(1);
                    let started = std::time::Instant::now();
                    for chunk in entries.chunks(chunk_size) {
                        let sql = build_complete_files_pg_sql(chunk.len());
                        let mut args = Vec::with_capacity(1 + chunk.len() * BINDS_PER_ROW);
                        args.push(SqlArg::I64(job_id.0 as i64));
                        for (file_index, filename, md5) in chunk {
                            args.push(SqlArg::I64(job_id.0 as i64));
                            args.push(SqlArg::I64(*file_index as i64));
                            args.push(SqlArg::Text(filename.clone()));
                            args.push(SqlArg::OptBytes(md5.map(|md5| md5.to_vec())));
                            args.push(SqlArg::OptText(
                                md5.map(|_| provenance.as_sql().to_string()),
                            ));
                        }
                        SqlRuntime::execute(datastore.read_exec(), &sql, &args).await?;
                    }
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "complete_files",
                        started.elapsed(),
                    );
                    Ok(())
                }
            }
        })
    }

    pub fn save_file_identity(
        &self,
        job_id: JobId,
        identity: &ActiveFileIdentity,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let identity = identity.clone();
        self.run_sql_blocking(async move {
            let classification_kind = identity
                .classification
                .as_ref()
                .map(|classification| classification.kind.as_str().to_string());
            let classification_set_name = identity
                .classification
                .as_ref()
                .map(|classification| classification.set_name.clone());
            let classification_volume_index = identity
                .classification
                .as_ref()
                .and_then(|classification| classification.volume_index)
                .map(|value| value as i64);
            let classification_source = identity.classification_source.as_str().to_string();
            let active_file_index = identity.file_index;
            let active_filename = identity.current_filename.clone();
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "save_file_identity", |tx| {
                        let identity = identity.clone();
                        let classification_kind = classification_kind.clone();
                        let classification_set_name = classification_set_name.clone();
                        let classification_source = classification_source.clone();
                        let active_filename = active_filename.clone();
                        Box::pin(async move {
                            tx.execute(
                                "INSERT INTO active_file_identities
                                 (job_id, file_index, source_filename, current_filename, canonical_filename,
                                  classification_kind, classification_set_name, classification_volume_index,
                                  classification_source)
                                 SELECT {}, {}, {}, {}, {}, {}, {}, {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id, file_index) DO UPDATE SET
                                    source_filename = excluded.source_filename,
                                    current_filename = excluded.current_filename,
                                    canonical_filename = excluded.canonical_filename,
                                    classification_kind = excluded.classification_kind,
                                    classification_set_name = excluded.classification_set_name,
                                    classification_volume_index = excluded.classification_volume_index,
                                    classification_source = excluded.classification_source",
                                &[
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::I64(active_file_index as i64),
                                    SqlArg::Text(identity.source_filename),
                                    SqlArg::Text(identity.current_filename),
                                    SqlArg::OptText(identity.canonical_filename),
                                    SqlArg::OptText(classification_kind),
                                    SqlArg::OptText(classification_set_name),
                                    SqlArg::OptI64(classification_volume_index),
                                    SqlArg::Text(classification_source),
                                    SqlArg::I64(job_id.0 as i64),
                                ],
                            )
                            .await?;
                            tx.execute(
                                "UPDATE active_files
                                 SET filename = {}
                                 WHERE job_id = {} AND file_index = {}",
                                &[
                                    SqlArg::Text(active_filename),
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::I64(active_file_index as i64),
                                ],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Both writes ride one autocommit statement: the identity upsert
                // and the sibling filename update are data-modifying CTEs behind
                // a shared FOR KEY SHARE guard, so the statement no-ops when the
                // parent job row is gone and blocks a concurrent archive/delete
                // for its duration. Callers sit on the pipeline task, so the
                // round trips a BEGIN/COMMIT pair would add are latency the
                // actor itself pays.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "WITH active_file_identity_parent AS (
                            SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE
                         ),
                         identity_upsert AS (
                            INSERT INTO active_file_identities
                            (job_id, file_index, source_filename, current_filename, canonical_filename,
                             classification_kind, classification_set_name, classification_volume_index,
                             classification_source)
                            SELECT {}, {}, {}, {}, {}, {}, {}, {}, {}
                            FROM active_file_identity_parent
                            ON CONFLICT(job_id, file_index) DO UPDATE SET
                               source_filename = excluded.source_filename,
                               current_filename = excluded.current_filename,
                               canonical_filename = excluded.canonical_filename,
                               classification_kind = excluded.classification_kind,
                               classification_set_name = excluded.classification_set_name,
                               classification_volume_index = excluded.classification_volume_index,
                               classification_source = excluded.classification_source
                         )
                         UPDATE active_files
                            SET filename = {}
                           FROM active_file_identity_parent
                          WHERE active_files.job_id = {} AND active_files.file_index = {}",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(active_file_index as i64),
                            SqlArg::Text(identity.source_filename),
                            SqlArg::Text(identity.current_filename),
                            SqlArg::OptText(identity.canonical_filename),
                            SqlArg::OptText(classification_kind),
                            SqlArg::OptText(classification_set_name),
                            SqlArg::OptI64(classification_volume_index),
                            SqlArg::Text(classification_source),
                            SqlArg::Text(active_filename),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(active_file_index as i64),
                        ],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "save_file_identity",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    // Persist a batch of file identities for a single job in ONE transaction.
    //
    // This is the bulk counterpart to [`Self::save_file_identity`]: it upserts
    // every `active_file_identities` row via [`bulk_upsert_file_identities_tx`]
    // AND performs the sibling `active_files.filename` update that the bulk tx
    // helper omits, so callers converting a per-identity loop keep identical
    // semantics. Postgres runs one `UPDATE ... FROM (VALUES ...)` per chunk;
    // SQLite issues per-row `UPDATE`s inside the same transaction (cheap and
    // local to the serialized writer).
    pub fn save_file_identities(
        &self,
        job_id: JobId,
        identities: &[ActiveFileIdentity],
    ) -> Result<(), StateError> {
        if identities.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let identities = identities.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_file_identities", |tx| {
                let identities = identities.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    bulk_upsert_file_identities_tx(tx, job_id, &identities).await?;
                    bulk_update_active_files_filenames_tx(tx, job_id, &identities).await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub(crate) fn set_nzb_pause_release(
        &self,
        job_id: JobId,
        pending: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzb_pause_release", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "UPDATE active_jobs SET nzb_release_pending = {} WHERE job_id = {}",
                        &[
                            SqlArg::I64(i64::from(pending)),
                            SqlArg::I64(job_id.0 as i64),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub(crate) fn set_nzb_admitted(&self, job_id: JobId, admitted: bool) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzb_admitted", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    if admitted && !active_job_exists_tx(tx, job_id).await? {
                        return Err(StateError::Database(format!(
                            "cannot admit missing active NZB {}",
                            job_id.0
                        )));
                    }
                    tx.execute(
                        "UPDATE active_jobs SET nzb_admitted = {} WHERE job_id = {}",
                        &[
                            SqlArg::I64(i64::from(admitted)),
                            SqlArg::I64(job_id.0 as i64),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sample_active_job(id: u64) -> ActiveJob {
        ActiveJob {
            job_id: JobId(id),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from(format!("/tmp/queue_order_test_{id}.nzb")),
            nzb_zstd: Vec::new(),
            output_dir: PathBuf::from(format!("/tmp/queue_order_test_out_{id}")),
            created_at: 1_700_000_000 + id,
            category: None,
            metadata: vec![],
            status: "queued",
            download_state: "queued",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            password_override: None,
            supplied_password_candidates: Vec::new(),
        }
    }

    fn raw_archive_password(db: &Database, id: JobId) -> Option<String> {
        let datastore = db.datastore();
        db.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT password FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(id.0 as i64)],
            )
            .await?
            .unwrap()
            .opt_text("password")
        })
        .unwrap()
    }

    fn replace_stored_archive_password(db: &Database, id: JobId, value: &str) {
        let datastore = db.datastore();
        let value = value.to_string();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE active_jobs SET password = {} WHERE job_id = {}",
                &[SqlArg::Text(value), SqlArg::I64(id.0 as i64)],
            )
            .await
            .map(|_| ())
        })
        .unwrap();
    }

    #[test]
    fn archive_password_is_encrypted_on_every_active_admission_path() {
        let db = Database::open_in_memory().unwrap();
        // A user's literal password may resemble a stored envelope. It must
        // still be encrypted, then restored exactly rather than interpreted.
        let plaintext = "enc:v1:literal archive password";
        for id in 1..=3 {
            let mut job = sample_active_job(id);
            job.password_override = Some(plaintext.into());
            match id {
                1 => db.create_active_job(&job).unwrap(),
                2 => db
                    .create_active_job_with_file_identities(&job, &[])
                    .unwrap(),
                _ => assert!(
                    db.materialize_admission(&job, &[], None, None, false)
                        .unwrap()
                ),
            }
            let stored = raw_archive_password(&db, job.job_id).unwrap();
            assert!(crate::persistence::encryption::is_encrypted(&stored));
            assert_ne!(stored, plaintext);
            assert_eq!(
                crate::persistence::encryption::decrypt_value(
                    db.encryption_key().unwrap(),
                    &stored
                )
                .unwrap(),
                plaintext
            );
            assert_eq!(
                db.load_active_jobs().unwrap()[&job.job_id]
                    .password_override
                    .as_deref(),
                Some(plaintext)
            );
        }
        assert!(db.has_encrypted_credentials().unwrap());
        db.validate_encrypted_credentials(db.encryption_key().unwrap())
            .unwrap();
    }

    #[test]
    fn metadata_edits_preserve_newer_script_parameters_and_refresh_effects() {
        use crate::post_processing::effects::JobScriptEffects;

        let db = Database::open_in_memory().unwrap();
        let mut job = sample_active_job(41);
        job.metadata = vec![("untouched".into(), "original".into())];
        db.create_active_job(&job).unwrap();
        let initial = JobScriptEffects {
            parameters: [
                ("Priority".into(), "NORMAL".into()),
                ("obsolete".into(), "remove me".into()),
                ("chain".into(), "preserve me".into()),
            ]
            .into(),
            directory: Some(PathBuf::from("fixture-output")),
            ..Default::default()
        };
        db.save_job_script_effects(job.job_id.0, &initial).unwrap();
        let update = JobUpdate {
            metadata_edits: vec![
                ("priority".into(), Some("HIGH".into())),
                ("OBSOLETE".into(), None),
            ],
            name: Some("renamed fixture".into()),
            ..Default::default()
        };
        // A directive commits after the intent was built but before its writer
        // runs. The edit must patch this current row rather than an old snapshot.
        db.save_job_script_effects(
            job.job_id.0,
            &JobScriptEffects {
                parameters: [("later".into(), "new directive".into())].into(),
                ..Default::default()
            },
        )
        .unwrap();
        db.update_active_job(job.job_id, &update).unwrap();

        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        for expected in [
            ("untouched", "original"),
            ("chain", "preserve me"),
            ("later", "new directive"),
            ("priority", "HIGH"),
        ] {
            assert!(
                metadata
                    .iter()
                    .any(|(key, value)| key == expected.0 && value == expected.1)
            );
        }
        assert!(
            !metadata
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case("obsolete"))
        );
        assert_eq!(
            metadata
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("priority"))
                .count(),
            1
        );
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("renamed fixture")
        );
        let effects = db.job_script_effects(job.job_id.0).unwrap();
        assert_eq!(effects.directory, initial.directory);
        assert_eq!(effects.parameters["chain"], "preserve me");
        assert_eq!(effects.parameters["later"], "new directive");
        assert_eq!(effects.parameters["Priority"], "");
        assert_eq!(effects.parameters["priority"], "HIGH");
        let mut replayed = metadata.clone();
        effects.merge_parameters(&mut replayed);
        assert!(
            !replayed
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case("obsolete"))
        );
        assert_eq!(
            replayed
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("priority"))
                .count(),
            1
        );

        db.save_job_script_effects(
            job.job_id.0,
            &JobScriptEffects {
                parameters: [("next".into(), "another directive".into())].into(),
                ..Default::default()
            },
        )
        .unwrap();
        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        assert!(metadata.contains(&("later".into(), "new directive".into())));
        assert!(metadata.contains(&("priority".into(), "HIGH".into())));
        assert!(metadata.contains(&("next".into(), "another directive".into())));
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("renamed fixture")
        );
    }

    #[test]
    fn rename_only_persists_metadata_without_replacing_other_fields() {
        let db = Database::open_in_memory().unwrap();
        let mut job = sample_active_job(42);
        job.metadata = vec![("preserved".into(), "value".into())];
        db.create_active_job(&job).unwrap();
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                name: Some("new title".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        assert!(metadata.contains(&("preserved".into(), "value".into())));
        assert_eq!(crate::ingest::renamed_title(&metadata), Some("new title"));
    }

    #[test]
    fn metadata_replacement_and_clear_apply_edits_after_the_replacement() {
        use crate::post_processing::effects::JobScriptEffects;

        let db = Database::open_in_memory().unwrap();
        let job = sample_active_job(43);
        db.create_active_job(&job).unwrap();
        db.save_job_script_effects(
            job.job_id.0,
            &JobScriptEffects {
                parameters: [("chain".into(), "old directive".into())].into(),
                ..Default::default()
            },
        )
        .unwrap();
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                metadata: FieldUpdate::Set(vec![("replacement".into(), "initial".into())]),
                metadata_edits: vec![("REPLACEMENT".into(), Some("final".into()))],
                name: Some("replacement title".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        assert_eq!(metadata.len(), 2);
        assert!(metadata.contains(&("REPLACEMENT".into(), "final".into())));
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("replacement title")
        );
        assert_eq!(
            db.job_script_effects(job.job_id.0).unwrap().parameters["chain"],
            ""
        );
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                metadata: FieldUpdate::Clear,
                metadata_edits: vec![("after-clear".into(), Some("value".into()))],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id].metadata,
            vec![("after-clear".into(), "value".into())]
        );
    }

    #[test]
    fn archive_password_updates_preserve_null_empty_and_encrypted_overrides() {
        let db = Database::open_in_memory().unwrap();
        let job = sample_active_job(1);
        db.create_active_job(&job).unwrap();
        assert_eq!(raw_archive_password(&db, job.job_id), None);
        for password in ["first-password", "enc:v1:another-literal-password"] {
            db.update_active_job(
                job.job_id,
                &JobUpdate {
                    password: FieldUpdate::Set(password.into()),
                    ..JobUpdate::default()
                },
            )
            .unwrap();
            assert_ne!(raw_archive_password(&db, job.job_id).unwrap(), password);
            assert_eq!(
                db.load_active_jobs().unwrap()[&job.job_id]
                    .password_override
                    .as_deref(),
                Some(password)
            );
        }
        let encrypted = raw_archive_password(&db, job.job_id);
        db.update_active_job(job.job_id, &JobUpdate::default())
            .unwrap();
        assert_eq!(raw_archive_password(&db, job.job_id), encrypted);
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                password: FieldUpdate::Clear,
                ..JobUpdate::default()
            },
        )
        .unwrap();
        assert_eq!(raw_archive_password(&db, job.job_id).as_deref(), Some(""));
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id]
                .password_override
                .as_deref(),
            Some("")
        );
    }

    #[test]
    fn archive_password_rejects_missing_wrong_and_corrupt_keys_or_values() {
        use crate::persistence::encryption::EncryptionKey;
        assert!(encrypt_archive_password(None, Some("secret")).is_err());
        let mut db = Database::open_in_memory().unwrap();
        let key = db.encryption_key().unwrap().clone();
        let mut job = sample_active_job(1);
        job.password_override = Some("secret".into());
        db.create_active_job(&job).unwrap();
        let stored = raw_archive_password(&db, job.job_id).unwrap();
        assert!(decrypt_archive_password(None, Some(stored)).is_err());
        let wrong_key = EncryptionKey::generate();
        assert!(db.validate_encrypted_credentials(&wrong_key).is_err());
        db.set_encryption_key(wrong_key);
        assert!(db.load_active_jobs().is_err());
        db.set_encryption_key(key.clone());
        replace_stored_archive_password(&db, job.job_id, "enc:v1:corrupted");
        assert!(db.load_active_jobs().is_err());
        assert!(db.validate_encrypted_credentials(&key).is_err());
    }

    #[test]
    fn par3_content_identity_and_completed_name_survive_reopen() {
        use crate::jobs::ids::NzbFileId;
        use crate::jobs::record::FileIdentitySource;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("content-identity.db");
        let job = sample_active_job(1);
        let identity = ActiveFileIdentity {
            file_index: 0,
            source_filename: "obfuscated.dat".into(),
            current_filename: "archive.zip".into(),
            canonical_filename: Some("archive.zip".into()),
            classification: None,
            classification_source: FileIdentitySource::Par3,
        };
        {
            let db = Database::open(&path).unwrap();
            db.create_active_job(&job).unwrap();
            db.complete_files(
                job.job_id,
                &[(0, "obfuscated.dat".into(), None)],
                CompletedHashProvenance::Verified,
            )
            .unwrap();
            let mut intent = identity.clone();
            intent.current_filename = intent.source_filename.clone();
            intent.classification_source = FileIdentitySource::Par3Pending;
            db.save_file_identity(job.job_id, &intent).unwrap();
        }
        {
            let db = Database::open(&path).unwrap();
            let restored = db.load_active_jobs().unwrap();
            let intent = &restored[&job.job_id].file_identities[&0];
            assert_eq!(
                intent.classification_source,
                FileIdentitySource::Par3Pending
            );
            assert_eq!(intent.current_filename, "obfuscated.dat");
            assert_eq!(intent.canonical_filename.as_deref(), Some("archive.zip"));
            db.save_file_identity(job.job_id, &identity).unwrap();
        }
        let db = Database::open(&path).unwrap();
        let jobs = db.load_active_jobs().unwrap();
        let restored = &jobs[&job.job_id];
        assert_eq!(restored.file_identities[&0], identity);
        assert!(restored.complete_files.contains(&NzbFileId {
            job_id: job.job_id,
            file_index: 0
        }));
        let datastore = db.datastore();
        let name = db
            .run_sql_blocking_read(async move {
                SqlRuntime::fetch_optional(
                    datastore.read_exec(),
                    "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
                    &[SqlArg::I64(1), SqlArg::I64(0)],
                )
                .await?
                .unwrap()
                .opt_text("filename")
            })
            .unwrap();
        assert_eq!(name.as_deref(), Some("archive.zip"));
    }

    #[test]
    fn archive_password_persistence_requires_the_same_key_after_reopen() {
        use crate::persistence::encryption::EncryptionKey;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("archive-password.db");
        let key = EncryptionKey::generate();
        let mut job = sample_active_job(1);
        job.password_override = Some("restart-password".into());
        {
            let mut db = Database::open(&path).unwrap();
            assert!(db.create_active_job(&job).is_err());
            assert!(db.load_active_jobs().unwrap().is_empty());
            db.set_encryption_key(key.clone());
            db.create_active_job(&job).unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        assert!(db.has_encrypted_credentials().unwrap());
        assert!(db.load_active_jobs().is_err());
        db.validate_encrypted_credentials(&key).unwrap();
        db.set_encryption_key(key);
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id]
                .password_override
                .as_deref(),
            Some("restart-password")
        );
    }

    #[test]
    fn legacy_archive_password_migration_is_encrypted_and_idempotent() {
        let db = Database::open_in_memory().unwrap();
        let job = sample_active_job(1);
        db.create_active_job(&job).unwrap();
        replace_stored_archive_password(&db, job.job_id, "legacy-password");
        assert!(!db.has_encrypted_credentials().unwrap());
        db.migrate_plaintext_credentials().unwrap();
        let stored = raw_archive_password(&db, job.job_id).unwrap();
        assert_ne!(stored, "legacy-password");
        assert!(db.has_encrypted_credentials().unwrap());
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id]
                .password_override
                .as_deref(),
            Some("legacy-password")
        );
        db.migrate_plaintext_credentials().unwrap();
        assert_eq!(
            raw_archive_password(&db, job.job_id).as_deref(),
            Some(stored.as_str())
        );
    }

    #[test]
    fn update_active_job_queue_positions_applies_case_update_to_all_rows() {
        let db = Database::open_in_memory().unwrap();
        for id in [1_u64, 2, 3] {
            db.create_active_job(&sample_active_job(id)).unwrap();
        }

        db.update_active_job_queue_positions(&[(JobId(1), 2), (JobId(2), 0), (JobId(3), 1)])
            .unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].queue_position, Some(2));
        assert_eq!(jobs[&JobId(2)].queue_position, Some(0));
        assert_eq!(jobs[&JobId(3)].queue_position, Some(1));
    }

    #[test]
    fn update_active_job_queue_positions_overwrites_previous_positions_in_one_statement() {
        let db = Database::open_in_memory().unwrap();
        for id in [1_u64, 2] {
            db.create_active_job(&sample_active_job(id)).unwrap();
        }
        db.update_active_job_queue_positions(&[(JobId(1), 0), (JobId(2), 1)])
            .unwrap();

        // A second batch (the full re-derived order) must fully overwrite the
        // positions from the first, in one statement.
        db.update_active_job_queue_positions(&[(JobId(1), 1), (JobId(2), 0)])
            .unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].queue_position, Some(1));
        assert_eq!(jobs[&JobId(2)].queue_position, Some(0));
    }

    #[test]
    fn update_active_job_queue_positions_empty_slice_is_noop() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_active_job(1)).unwrap();

        db.update_active_job_queue_positions(&[]).unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].queue_position, None);
    }
}
