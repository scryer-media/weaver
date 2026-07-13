use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::StateError;
use crate::jobs::assembly::DetectedArchiveIdentity;
use crate::jobs::ids::JobId;
use crate::jobs::model::{FieldUpdate, JobUpdate};
use crate::jobs::record::{
    ActiveExtractionChunk, ActiveFileIdentity, ActiveFileProgress, ActiveJob, ExtractionChunk,
};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRuntime, SqlTx};
use sqlx::{Postgres, QueryBuilder, Sqlite};

const SQLITE_BATCH_BIND_LIMIT: usize = 900;
const POSTGRES_BATCH_BIND_LIMIT: usize = 16_000;

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

fn metadata_json(metadata: &[(String, String)]) -> Result<Option<String>, StateError> {
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(metadata).map(Some).map_err(db_err)
    }
}

fn max_rows_for_tx(tx: &SqlTx<'_>, binds_per_row: usize) -> usize {
    let bind_limit = match tx {
        SqlTx::Sqlite(_) => SQLITE_BATCH_BIND_LIMIT,
        SqlTx::Postgres(_) => POSTGRES_BATCH_BIND_LIMIT,
    };
    (bind_limit / binds_per_row.max(1)).max(1)
}

async fn active_job_ids_tx(
    tx: &mut SqlTx<'_>,
    job_ids: &HashSet<JobId>,
) -> Result<HashSet<JobId>, StateError> {
    if job_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let ids = job_ids
        .iter()
        .map(|job_id| job_id.0 as i64)
        .collect::<Vec<_>>();
    let chunk_size = max_rows_for_tx(tx, 1);
    let mut active = HashSet::new();
    for chunk in ids.chunks(chunk_size) {
        let placeholders = vec!["{}"; chunk.len()].join(", ");
        let sql = match tx {
            SqlTx::Postgres(_) => {
                format!(
                    "SELECT job_id FROM active_jobs WHERE job_id IN ({placeholders}) FOR KEY SHARE"
                )
            }
            SqlTx::Sqlite(_) => {
                format!("SELECT job_id FROM active_jobs WHERE job_id IN ({placeholders})")
            }
        };
        let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
        for row in tx.fetch_all(&sql, &args).await? {
            let id = row.i64("job_id")?;
            if let Ok(id) = u64::try_from(id) {
                active.insert(JobId(id));
            }
        }
    }
    Ok(active)
}

async fn bulk_upsert_file_progress_tx(
    tx: &mut SqlTx<'_>,
    progress: &[ActiveFileProgress],
) -> Result<(), StateError> {
    if progress.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 3);
    let conflict_sql = " ON CONFLICT(job_id, file_index)
                         DO UPDATE SET contiguous_bytes_written =
                            CASE
                                WHEN active_file_progress.contiguous_bytes_written > excluded.contiguous_bytes_written
                                THEN active_file_progress.contiguous_bytes_written
                                ELSE excluded.contiguous_bytes_written
                            END";
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in progress.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO active_file_progress
                     (job_id, file_index, contiguous_bytes_written) ",
                );
                builder.push_values(chunk, |mut row, entry| {
                    row.push_bind(entry.job_id.0 as i64)
                        .push_bind(entry.file_index as i64)
                        .push_bind(entry.contiguous_bytes_written as i64);
                });
                builder.push(conflict_sql);
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in progress.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO active_file_progress
                     (job_id, file_index, contiguous_bytes_written) ",
                );
                builder.push_values(chunk, |mut row, entry| {
                    row.push_bind(entry.job_id.0 as i64)
                        .push_bind(entry.file_index as i64)
                        .push_bind(entry.contiguous_bytes_written as i64);
                });
                builder.push(conflict_sql);
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
}

async fn bulk_insert_failed_extractions_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    members: &[String],
) -> Result<(), StateError> {
    if members.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 2);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in members.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO active_failed_extractions (job_id, member_name) ",
                );
                builder.push_values(chunk, |mut row, member_name| {
                    row.push_bind(job_id.0 as i64).push_bind(member_name);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in members.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO active_failed_extractions (job_id, member_name) ",
                );
                builder.push_values(chunk, |mut row, member_name| {
                    row.push_bind(job_id.0 as i64).push_bind(member_name);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
}

async fn bulk_insert_verified_suspect_volumes_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    set_name: &str,
    volumes: &[u32],
) -> Result<(), StateError> {
    if volumes.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 3);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in volumes.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO active_rar_verified_suspect
                     (job_id, set_name, volume_index) ",
                );
                builder.push_values(chunk, |mut row, volume_index| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(set_name)
                        .push_bind(*volume_index as i64);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in volumes.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO active_rar_verified_suspect
                     (job_id, set_name, volume_index) ",
                );
                builder.push_values(chunk, |mut row, volume_index| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(set_name)
                        .push_bind(*volume_index as i64);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
}

async fn bulk_insert_member_chunks_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    set_name: &str,
    chunks: &[ExtractionChunk],
) -> Result<(), StateError> {
    if chunks.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 10);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in chunks.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO active_extraction_chunks
                     (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
                      start_offset, end_offset, verified, appended) ",
                );
                builder.push_values(chunk, |mut row, chunk| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(set_name)
                        .push_bind(&chunk.member_name)
                        .push_bind(chunk.volume_index as i64)
                        .push_bind(chunk.bytes_written as i64)
                        .push_bind(&chunk.temp_path)
                        .push_bind(chunk.start_offset as i64)
                        .push_bind(chunk.end_offset as i64)
                        .push_bind(chunk.verified)
                        .push_bind(chunk.appended);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in chunks.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO active_extraction_chunks
                     (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
                      start_offset, end_offset, verified, appended) ",
                );
                builder.push_values(chunk, |mut row, chunk| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(set_name)
                        .push_bind(&chunk.member_name)
                        .push_bind(chunk.volume_index as i64)
                        .push_bind(chunk.bytes_written as i64)
                        .push_bind(&chunk.temp_path)
                        .push_bind(chunk.start_offset as i64)
                        .push_bind(chunk.end_offset as i64)
                        .push_bind(chunk.verified)
                        .push_bind(chunk.appended);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
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

/// Update `active_files.filename` for each identity's `current_filename`, the
/// sibling write [`Database::save_file_identity`] performs alongside the
/// identity upsert. Rows whose `active_files` entry does not yet exist (file
/// not completed) are simply not matched, exactly as the single-row path. Runs
/// inside the caller's transaction: Postgres batches with `UPDATE ... FROM
/// (VALUES ...)`, SQLite issues per-row updates (local to the serialized
/// writer, so the extra statements are cheap).
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

/// Build the Postgres autocommit statement that inserts `row_count` extracted
/// members guarded by a `FOR KEY SHARE` lock on the owning job. `$1` is the
/// guard `job_id`; each row then binds `(job_id, member_name, output_path,
/// output_size)`. Explicit casts on the first VALUES row give Postgres the
/// column types it cannot infer from all-parameter rows.
fn build_extracted_members_pg_sql(row_count: usize) -> String {
    let mut values = String::new();
    for row in 0..row_count {
        if row > 0 {
            values.push_str(", ");
        }
        if row == 0 {
            values.push_str("({}::bigint, {}::text, {}::text, {}::bigint)");
        } else {
            values.push_str("({}, {}, {}, {})");
        }
    }
    format!(
        "WITH active_extracted_members_active AS (
             SELECT 1 FROM active_jobs WHERE job_id = {{}} FOR KEY SHARE
         )
         INSERT INTO active_extracted (job_id, member_name, output_path, output_size)
         SELECT d.job_id, d.member_name, d.output_path, d.output_size
         FROM (VALUES {values}) AS d(job_id, member_name, output_path, output_size),
              active_extracted_members_active
         ON CONFLICT(job_id, member_name) DO NOTHING"
    )
}

/// Build the Postgres autocommit statement that upserts `row_count` completed
/// files guarded by a `FOR KEY SHARE` lock on the owning job, mirroring the
/// single-row [`Database::complete_file_with_optional_hash`] shape. `$1` is the
/// guard `job_id`; each row then binds `(job_id, file_index, filename, md5)`.
fn build_complete_files_pg_sql(row_count: usize) -> String {
    let mut values = String::new();
    for row in 0..row_count {
        if row > 0 {
            values.push_str(", ");
        }
        if row == 0 {
            values.push_str("({}::bigint, {}::bigint, {}::text, {}::bytea)");
        } else {
            values.push_str("({}, {}, {}, {})");
        }
    }
    format!(
        "WITH active_complete_files_active AS (
             SELECT 1 FROM active_jobs WHERE job_id = {{}} FOR KEY SHARE
         )
         INSERT INTO active_files (job_id, file_index, filename, md5)
         SELECT d.job_id, d.file_index, d.filename, d.md5
         FROM (VALUES {values}) AS d(job_id, file_index, filename, md5),
              active_complete_files_active
         ON CONFLICT(job_id, file_index) DO UPDATE SET
            filename = excluded.filename,
            md5 = excluded.md5"
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
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_active_job", |tx| {
                let args = args.clone();
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO active_jobs
                         (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state)
                         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
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
                             (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state)
                             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
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

    /// Performs the inexpensive first half of SCORE materialization before a
    /// pipeline creates a work directory. The transactional check below remains
    /// authoritative; this keeps a known-stale submit from allocating anything.
    pub(crate) fn semantic_materialization_is_current(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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

    /// Checks an owned promotion lease before the scheduler allocates a
    /// working directory. The materialization transaction repeats this CAS.
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

    /// Atomically turns a reserved duplicate admission into an active job. A
    /// SCORE caller supplies the generation it received at admission; parking
    /// or superseding that candidate invalidates the generation before any
    /// stale request can create scheduler work.
    pub(crate) fn materialize_active_job_with_file_identities(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
        semantic_materialization_generation: Option<i64>,
        semantic_promotion_generation: Option<i64>,
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
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "materialize_active_job_with_file_identities",
                |tx| {
                    let args = args.clone();
                    let identities = identities.clone();
                    Box::pin(async move {
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
                             (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state)
                             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            &args,
                        )
                        .await?;
                        bulk_upsert_file_identities_tx(tx, job_id, &identities).await?;
                        Ok(true)
                    })
                },
            )
            .await
        })
    }

    pub fn update_active_job(&self, job_id: JobId, update: &JobUpdate) -> Result<(), StateError> {
        let datastore = self.datastore();
        let update = update.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_active_job", |tx| {
                let update = update.clone();
                Box::pin(async move {
                    match update.category {
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
                                &[SqlArg::Text(category), SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                    }

                    match update.metadata {
                        FieldUpdate::Unchanged => {}
                        FieldUpdate::Clear => {
                            tx.execute(
                                "UPDATE active_jobs SET metadata = NULL WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                        FieldUpdate::Set(metadata) => {
                            tx.execute(
                                "UPDATE active_jobs SET metadata = {} WHERE job_id = {}",
                                &[
                                    SqlArg::OptText(metadata_json(&metadata)?),
                                    SqlArg::I64(job_id.0 as i64),
                                ],
                            )
                            .await?;
                        }
                    }

                    // Password override column: NULL = no override (restore
                    // keeps the NZB-derived password), '' = explicitly no
                    // password, anything else = the override itself.
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
                    Ok(())
                })
            })
            .await
        })
    }

    /// Persist the manual queue order as one transaction. Positions are the
    /// full current order (small: one row per queued job) so restores sort by
    /// `queue_position` and land in the exact user-arranged sequence.
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
        self.run_sql_blocking(async move {
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
        let datastore = self.datastore();
        let args = vec![
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
                                 SET status = {},
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
                         SET status = {},
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

    pub fn upsert_file_progress_batch(
        &self,
        progress: &[ActiveFileProgress],
    ) -> Result<(), StateError> {
        if progress.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let mut deduped = HashMap::<(JobId, u32), ActiveFileProgress>::new();
        for entry in progress {
            deduped
                .entry((entry.job_id, entry.file_index))
                .and_modify(|existing| {
                    if entry.contiguous_bytes_written > existing.contiguous_bytes_written {
                        *existing = entry.clone();
                    }
                })
                .or_insert_with(|| entry.clone());
        }
        let progress = deduped.into_values().collect::<Vec<_>>();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "upsert_file_progress_batch", |tx| {
                let progress = progress.clone();
                Box::pin(async move {
                    let job_ids = progress
                        .iter()
                        .map(|entry| entry.job_id)
                        .collect::<HashSet<_>>();
                    let active_job_ids = active_job_ids_tx(tx, &job_ids).await?;
                    let active_progress = progress
                        .into_iter()
                        .filter(|entry| active_job_ids.contains(&entry.job_id))
                        .collect::<Vec<_>>();
                    bulk_upsert_file_progress_tx(tx, &active_progress).await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn clear_file_progress(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "clear_file_progress", |tx| {
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "DELETE FROM active_file_progress WHERE job_id = {} AND file_index = {}",
                                &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Deleting child rows of a gone job already deletes nothing, and a
                // racing archive's cascade delete is idempotent with this one, so
                // the FOR KEY SHARE guard adds nothing: run a plain autocommit
                // DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_file_progress WHERE job_id = {} AND file_index = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_file_progress",
                        started.elapsed(),
                    );
                    result
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
        let datastore = self.datastore();
        let filename = filename.to_string();
        let md5 = md5.map(|md5| md5.to_vec());
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "complete_file", |tx| {
                        let filename = filename.clone();
                        let md5 = md5.clone();
                        Box::pin(async move {
                            if active_job_exists_tx(tx, job_id).await? {
                                tx.execute(
                                    "INSERT INTO active_files (job_id, file_index, filename, md5)
                                     VALUES ({}, {}, {}, {})
                                     ON CONFLICT(job_id, file_index) DO UPDATE SET
                                        filename = excluded.filename,
                                        md5 = excluded.md5",
                                    &[
                                        SqlArg::I64(job_id.0 as i64),
                                        SqlArg::I64(file_index as i64),
                                        SqlArg::Text(filename),
                                        SqlArg::OptBytes(md5),
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
                             INSERT INTO active_files (job_id, file_index, filename, md5)
                             SELECT {}, {}, {}, {}
                             FROM active_file_complete_active
                             ON CONFLICT(job_id, file_index) DO UPDATE SET
                                filename = excluded.filename,
                                md5 = excluded.md5",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(file_index as i64),
                            SqlArg::Text(filename),
                            SqlArg::OptBytes(md5),
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

    /// Complete a batch of files for a single job, the bulk counterpart to
    /// [`Self::complete_file_with_optional_hash`]. Each entry is
    /// `(file_index, filename, optional md5)`; the guard semantics match the
    /// single-file path (an absent `active_jobs` row inserts nothing). Postgres
    /// runs one autocommit `FOR KEY SHARE`-guarded multi-row upsert per chunk;
    /// SQLite guards once inside a transaction then bulk-upserts in chunks.
    pub fn complete_files(
        &self,
        job_id: JobId,
        entries: &[(u32, String, Option<[u8; 16]>)],
    ) -> Result<(), StateError> {
        // 4 value binds per row (job_id, file_index, filename, md5) plus the
        // single guard bind reused across the chunk.
        const BINDS_PER_ROW: usize = 4;

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
                                    "INSERT INTO active_files (job_id, file_index, filename, md5) ",
                                );
                                builder.push_values(
                                    chunk,
                                    |mut row, (file_index, filename, md5)| {
                                        row.push_bind(job_id.0 as i64)
                                            .push_bind(*file_index as i64)
                                            .push_bind(filename.clone())
                                            .push_bind(md5.map(|md5| md5.to_vec()));
                                    },
                                );
                                builder.push(
                                    " ON CONFLICT(job_id, file_index) DO UPDATE SET
                                        filename = excluded.filename,
                                        md5 = excluded.md5",
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

    pub fn mark_file_incomplete(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "mark_file_incomplete", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    for table in [
                        "active_file_progress",
                        "active_files",
                        "active_detected_archives",
                    ] {
                        tx.execute(
                            &format!(
                                "DELETE FROM {table} WHERE job_id = {{}} AND file_index = {{}}"
                            ),
                            &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
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
            SqlRuntime::run_in_transaction(&datastore, "save_file_identity", |tx| {
                let identity = identity.clone();
                Box::pin(async move {
                    let sql = match tx {
                        SqlTx::Postgres(_) => {
                            "INSERT INTO active_file_identities
                         (job_id, file_index, source_filename, current_filename, canonical_filename,
                          classification_kind, classification_set_name, classification_volume_index,
                          classification_source)
                         SELECT {}, {}, {}, {}, {}, {}, {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_file_identity_parent
                         ON CONFLICT(job_id, file_index) DO UPDATE SET
                            source_filename = excluded.source_filename,
                            current_filename = excluded.current_filename,
                            canonical_filename = excluded.canonical_filename,
                            classification_kind = excluded.classification_kind,
                            classification_set_name = excluded.classification_set_name,
                            classification_volume_index = excluded.classification_volume_index,
                            classification_source = excluded.classification_source"
                        }
                        SqlTx::Sqlite(_) => {
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
                            classification_source = excluded.classification_source"
                        }
                    };
                    let active_file_index = identity.file_index;
                    let active_filename = identity.current_filename.clone();
                    tx.execute(
                        sql,
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(active_file_index as i64),
                            SqlArg::Text(identity.source_filename),
                            SqlArg::Text(identity.current_filename),
                            SqlArg::OptText(identity.canonical_filename),
                            SqlArg::OptText(
                                identity
                                    .classification
                                    .as_ref()
                                    .map(|classification| classification.kind.as_str().to_string()),
                            ),
                            SqlArg::OptText(
                                identity
                                    .classification
                                    .as_ref()
                                    .map(|classification| classification.set_name.clone()),
                            ),
                            SqlArg::OptI64(
                                identity
                                    .classification
                                    .as_ref()
                                    .and_then(|classification| classification.volume_index)
                                    .map(|value| value as i64),
                            ),
                            SqlArg::Text(identity.classification_source.as_str().to_string()),
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
        })
    }

    /// Persist a batch of file identities for a single job in ONE transaction.
    ///
    /// This is the bulk counterpart to [`Self::save_file_identity`]: it upserts
    /// every `active_file_identities` row via [`bulk_upsert_file_identities_tx`]
    /// AND performs the sibling `active_files.filename` update that the bulk tx
    /// helper omits, so callers converting a per-identity loop keep identical
    /// semantics. Postgres runs one `UPDATE ... FROM (VALUES ...)` per chunk;
    /// SQLite issues per-row `UPDATE`s inside the same transaction (cheap and
    /// local to the serialized writer).
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

    pub fn delete_file_identity(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "delete_file_identity", |tx| {
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "DELETE FROM active_file_identities WHERE job_id = {} AND file_index = {}",
                                &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_file_identities WHERE job_id = {} AND file_index = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "delete_file_identity",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn set_par2_metadata(
        &self,
        job_id: JobId,
        slice_size: u64,
        recovery_block_count: u32,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let args = [
                SqlArg::I64(job_id.0 as i64),
                SqlArg::I64(slice_size as i64),
                SqlArg::I64(recovery_block_count as i64),
                SqlArg::I64(job_id.0 as i64),
            ];
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "set_par2_metadata", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "INSERT INTO active_par2 (job_id, slice_size, recovery_block_count)
                                 SELECT {}, {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id) DO UPDATE SET
                                    slice_size = excluded.slice_size,
                                    recovery_block_count = excluded.recovery_block_count",
                                &args,
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // The FOR KEY SHARE existence guard folds into the statement as a
                // CTE the INSERT selects FROM: it no-ops the write when the job
                // row is gone and blocks a concurrent archive/delete of the parent
                // for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_par2 (job_id, slice_size, recovery_block_count)
                         SELECT {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_par2_parent
                         ON CONFLICT(job_id) DO UPDATE SET
                            slice_size = excluded.slice_size,
                            recovery_block_count = excluded.recovery_block_count",
                        &args,
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "set_par2_metadata",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn upsert_par2_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        recovery_block_count: u32,
        promoted: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let filename = filename.to_string();
        self.run_sql_blocking(async move {
            let args = [
                SqlArg::I64(job_id.0 as i64),
                SqlArg::I64(file_index as i64),
                SqlArg::Text(filename),
                SqlArg::I64(recovery_block_count as i64),
                SqlArg::Bool(promoted),
                SqlArg::I64(job_id.0 as i64),
            ];
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "upsert_par2_file", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "INSERT INTO active_par2_files
                                 (job_id, file_index, filename, recovery_block_count, promoted)
                                 SELECT {}, {}, {}, {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id, file_index) DO UPDATE SET
                                    filename = excluded.filename,
                                    recovery_block_count = excluded.recovery_block_count,
                                    promoted = excluded.promoted",
                                &args,
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Guard folded into a FOR KEY SHARE CTE the INSERT selects FROM:
                // no-ops when the job row is gone and blocks a concurrent
                // archive/delete of the parent for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_par2_files
                         (job_id, file_index, filename, recovery_block_count, promoted)
                         SELECT {}, {}, {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_par2_files_parent
                         ON CONFLICT(job_id, file_index) DO UPDATE SET
                            filename = excluded.filename,
                            recovery_block_count = excluded.recovery_block_count,
                            promoted = excluded.promoted",
                        &args,
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "upsert_par2_file",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn set_par2_file_promotion(
        &self,
        job_id: JobId,
        file_index: u32,
        promoted: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "set_par2_file_promotion", |tx| {
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "UPDATE active_par2_files SET promoted = {} WHERE job_id = {} AND file_index = {}",
                                &[
                                    SqlArg::Bool(promoted),
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::I64(file_index as i64),
                                ],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Fold the FOR KEY SHARE guard into the UPDATE via a FROM join: the
                // join both no-ops the write when the parent job row is gone AND
                // blocks a concurrent archive/delete of it for the statement's
                // duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_par2_files SET promoted = {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_par2_files_parent
                         WHERE active_par2_files.job_id = {} AND active_par2_files.file_index = {}",
                        &[
                            SqlArg::Bool(promoted),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(file_index as i64),
                        ],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "set_par2_file_promotion",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn replace_failed_extractions(
        &self,
        job_id: JobId,
        members: &HashSet<String>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let members = members.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "replace_failed_extractions", |tx| {
                let members = members.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    tx.execute(
                        "DELETE FROM active_failed_extractions WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    let members = members.into_iter().collect::<Vec<_>>();
                    bulk_insert_failed_extractions_tx(tx, job_id, &members).await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn add_failed_extraction(
        &self,
        job_id: JobId,
        member_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let member_name = member_name.to_string();
        self.run_sql_blocking(async move {
            let args = [
                SqlArg::I64(job_id.0 as i64),
                SqlArg::Text(member_name),
                SqlArg::I64(job_id.0 as i64),
            ];
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "add_failed_extraction", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "INSERT INTO active_failed_extractions (job_id, member_name)
                                 SELECT {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id, member_name) DO NOTHING",
                                &args,
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Guard folded into a FOR KEY SHARE CTE the INSERT selects FROM:
                // no-ops when the job row is gone and blocks a concurrent
                // archive/delete of the parent for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_failed_extractions (job_id, member_name)
                         SELECT {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_failed_extractions_parent
                         ON CONFLICT(job_id, member_name) DO NOTHING",
                        &args,
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "add_failed_extraction",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn remove_failed_extraction(
        &self,
        job_id: JobId,
        member_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let member_name = member_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "remove_failed_extraction", |tx| {
                        let member_name = member_name.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "DELETE FROM active_failed_extractions
                                 WHERE job_id = {} AND member_name = {}",
                                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(member_name)],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_failed_extractions
                         WHERE job_id = {} AND member_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(member_name)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "remove_failed_extraction",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn set_active_job_normalization_retried(
        &self,
        job_id: JobId,
        normalization_retried: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let args = [
                SqlArg::Bool(normalization_retried),
                SqlArg::I64(job_id.0 as i64),
            ];
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "set_active_job_normalization_retried",
                        |tx| {
                            let args = args.clone();
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "UPDATE active_jobs SET normalization_retried = {} WHERE job_id = {}",
                                    &args,
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // The UPDATE targets the active_jobs row itself, so it already
                // takes that row's lock for its duration and no-ops when the row
                // is gone: the separate guard is redundant here (mirrors
                // `set_active_job_runtime`). Run a plain autocommit UPDATE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_jobs SET normalization_retried = {} WHERE job_id = {}",
                        &args,
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "set_active_job_normalization_retried",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn replace_verified_suspect_volumes(
        &self,
        job_id: JobId,
        set_name: &str,
        volumes: &HashSet<u32>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        let volumes = volumes.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "replace_verified_suspect_volumes", |tx| {
                let set_name = set_name.clone();
                let volumes = volumes.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    tx.execute(
                        "DELETE FROM active_rar_verified_suspect
                         WHERE job_id = {} AND set_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name.clone())],
                    )
                    .await?;
                    let volumes = volumes.into_iter().collect::<Vec<_>>();
                    bulk_insert_verified_suspect_volumes_tx(tx, job_id, &set_name, &volumes)
                        .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn clear_verified_suspect_volumes(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "clear_verified_suspect_volumes",
                        |tx| {
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_rar_verified_suspect WHERE job_id = {}",
                                    &[SqlArg::I64(job_id.0 as i64)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_rar_verified_suspect WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_verified_suspect_volumes",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn add_extracted_member(
        &self,
        job_id: JobId,
        member_name: &str,
        output_path: &Path,
    ) -> Result<(), StateError> {
        self.add_extracted_members(
            job_id,
            std::slice::from_ref(&(member_name.to_string(), output_path.to_path_buf())),
        )
    }

    /// Persist a batch of extracted archive members for a single job.
    ///
    /// Each `output_path` is stat'd (blocking IO — callers run this via
    /// `spawn_blocking`) to record its on-disk size; an entry whose stat fails
    /// is skipped and logged rather than failing the whole batch, matching the
    /// per-member error tolerance of the RAR extraction scheduler. Surviving
    /// rows are inserted multi-row in bind-budget-sized chunks: Postgres runs
    /// one autocommit statement per chunk guarded by a `FOR KEY SHARE` CTE on
    /// `active_jobs`; SQLite guards once inside a transaction then bulk-inserts,
    /// mirroring [`Self::replace_verified_suspect_volumes`].
    pub fn add_extracted_members(
        &self,
        job_id: JobId,
        entries: &[(String, std::path::PathBuf)],
    ) -> Result<(), StateError> {
        // 4 value binds per row (job_id, member_name, output_path, output_size)
        // plus the single guard bind ($1) reused across every row in a chunk.
        const BINDS_PER_ROW: usize = 4;

        if entries.is_empty() {
            return Ok(());
        }

        // Stat each output before touching the DB; tolerate individual failures
        // exactly as the previous per-member loop did.
        let mut rows: Vec<(String, String, i64)> = Vec::with_capacity(entries.len());
        for (member_name, output_path) in entries {
            let output_size = match std::fs::metadata(output_path) {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    tracing::warn!(
                        job_id = job_id.0,
                        member = %member_name,
                        path = %output_path.display(),
                        error = %error,
                        "skipping extracted member whose output could not be stat'd"
                    );
                    continue;
                }
            };
            let output_size = match i64::try_from(output_size) {
                Ok(value) => value,
                Err(_) => {
                    tracing::warn!(
                        job_id = job_id.0,
                        member = %member_name,
                        path = %output_path.display(),
                        "skipping extracted member whose output is too large to persist"
                    );
                    continue;
                }
            };
            rows.push((
                member_name.clone(),
                output_path.to_string_lossy().to_string(),
                output_size,
            ));
        }

        if rows.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    let chunk_size = (SQLITE_BATCH_BIND_LIMIT / BINDS_PER_ROW).max(1);
                    SqlRuntime::run_in_transaction(&datastore, "add_extracted_members", |tx| {
                        let rows = rows.clone();
                        Box::pin(async move {
                            if !active_job_exists_tx(tx, job_id).await? {
                                return Ok(());
                            }
                            let SqlTx::Sqlite(tx) = tx else {
                                return Ok(());
                            };
                            for chunk in rows.chunks(chunk_size) {
                                let mut builder = QueryBuilder::<Sqlite>::new(
                                    "INSERT INTO active_extracted
                                     (job_id, member_name, output_path, output_size) ",
                                );
                                builder.push_values(chunk, |mut row, entry| {
                                    row.push_bind(job_id.0 as i64)
                                        .push_bind(entry.0.clone())
                                        .push_bind(entry.1.clone())
                                        .push_bind(entry.2);
                                });
                                builder.push(" ON CONFLICT(job_id, member_name) DO NOTHING");
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
                    for chunk in rows.chunks(chunk_size) {
                        let sql = build_extracted_members_pg_sql(chunk.len());
                        let mut args = Vec::with_capacity(1 + chunk.len() * BINDS_PER_ROW);
                        args.push(SqlArg::I64(job_id.0 as i64));
                        for (member_name, output_path, output_size) in chunk {
                            args.push(SqlArg::I64(job_id.0 as i64));
                            args.push(SqlArg::Text(member_name.clone()));
                            args.push(SqlArg::Text(output_path.clone()));
                            args.push(SqlArg::I64(*output_size));
                        }
                        SqlRuntime::execute(datastore.read_exec(), &sql, &args).await?;
                    }
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "add_extracted_members",
                        started.elapsed(),
                    );
                    Ok(())
                }
            }
        })
    }

    pub fn clear_extracted_members(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "clear_extracted_members", |tx| {
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "DELETE FROM active_extracted WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_extracted WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_extracted_members",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn insert_extraction_chunk(&self, chunk: &ActiveExtractionChunk) -> Result<(), StateError> {
        let datastore = self.datastore();
        let chunk = chunk.clone();
        self.run_sql_blocking(async move {
            let job_id = chunk.job_id;
            let args = [
                SqlArg::I64(chunk.job_id.0 as i64),
                SqlArg::Text(chunk.set_name),
                SqlArg::Text(chunk.member_name),
                SqlArg::I64(chunk.volume_index as i64),
                SqlArg::I64(chunk.bytes_written as i64),
                SqlArg::Text(chunk.temp_path),
                SqlArg::I64(chunk.start_offset as i64),
                SqlArg::I64(chunk.end_offset as i64),
                SqlArg::I64(chunk.job_id.0 as i64),
            ];
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "insert_extraction_chunk", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "INSERT INTO active_extraction_chunks
                                 (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
                                  start_offset, end_offset)
                                 SELECT {}, {}, {}, {}, {}, {}, {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id, set_name, member_name, volume_index) DO UPDATE SET
                                    bytes_written = excluded.bytes_written,
                                    temp_path = excluded.temp_path,
                                    start_offset = excluded.start_offset,
                                    end_offset = excluded.end_offset",
                                &args,
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Guard folded into a FOR KEY SHARE CTE the INSERT selects FROM:
                // no-ops when the job row is gone and blocks a concurrent
                // archive/delete of the parent for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_extraction_chunks
                         (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
                          start_offset, end_offset)
                         SELECT {}, {}, {}, {}, {}, {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_extraction_chunks_parent
                         ON CONFLICT(job_id, set_name, member_name, volume_index) DO UPDATE SET
                            bytes_written = excluded.bytes_written,
                            temp_path = excluded.temp_path,
                            start_offset = excluded.start_offset,
                            end_offset = excluded.end_offset",
                        &args,
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "insert_extraction_chunk",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn mark_chunk_verified(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        volume_index: u32,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        let member_name = member_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "mark_chunk_verified", |tx| {
                        let set_name = set_name.clone();
                        let member_name = member_name.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "UPDATE active_extraction_chunks SET verified = TRUE
                                 WHERE job_id = {} AND set_name = {} AND member_name = {} AND volume_index = {}",
                                &[
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::Text(set_name),
                                    SqlArg::Text(member_name),
                                    SqlArg::I64(volume_index as i64),
                                ],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Fold the FOR KEY SHARE guard into the UPDATE via a FROM join: it
                // no-ops the write when the parent job row is gone AND blocks a
                // concurrent archive/delete of it for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_extraction_chunks SET verified = TRUE
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_extraction_chunks_parent
                         WHERE active_extraction_chunks.job_id = {}
                           AND active_extraction_chunks.set_name = {}
                           AND active_extraction_chunks.member_name = {}
                           AND active_extraction_chunks.volume_index = {}",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(set_name),
                            SqlArg::Text(member_name),
                            SqlArg::I64(volume_index as i64),
                        ],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "mark_chunk_verified",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn replace_member_chunks(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        chunks: &[ExtractionChunk],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        let member_name = member_name.to_string();
        let chunks = chunks.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "replace_member_chunks", |tx| {
                let set_name = set_name.clone();
                let member_name = member_name.clone();
                let chunks = chunks.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    tx.execute(
                        "DELETE FROM active_extraction_chunks
                         WHERE job_id = {} AND set_name = {} AND member_name = {}",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(set_name.clone()),
                            SqlArg::Text(member_name.clone()),
                        ],
                    )
                    .await?;

                    bulk_insert_member_chunks_tx(tx, job_id, &set_name, &chunks).await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn mark_chunk_appended(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        volume_index: u32,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        let member_name = member_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "mark_chunk_appended", |tx| {
                        let set_name = set_name.clone();
                        let member_name = member_name.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "UPDATE active_extraction_chunks SET appended = TRUE
                                 WHERE job_id = {} AND set_name = {} AND member_name = {} AND volume_index = {}",
                                &[
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::Text(set_name),
                                    SqlArg::Text(member_name),
                                    SqlArg::I64(volume_index as i64),
                                ],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Fold the FOR KEY SHARE guard into the UPDATE via a FROM join: it
                // no-ops the write when the parent job row is gone AND blocks a
                // concurrent archive/delete of it for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_extraction_chunks SET appended = TRUE
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_extraction_chunks_parent
                         WHERE active_extraction_chunks.job_id = {}
                           AND active_extraction_chunks.set_name = {}
                           AND active_extraction_chunks.member_name = {}
                           AND active_extraction_chunks.volume_index = {}",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(set_name),
                            SqlArg::Text(member_name),
                            SqlArg::I64(volume_index as i64),
                        ],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "mark_chunk_appended",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn clear_member_chunks(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        let member_name = member_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "clear_member_chunks", |tx| {
                        let set_name = set_name.clone();
                        let member_name = member_name.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "DELETE FROM active_extraction_chunks
                                 WHERE job_id = {} AND set_name = {} AND member_name = {}",
                                &[
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::Text(set_name),
                                    SqlArg::Text(member_name),
                                ],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_extraction_chunks
                         WHERE job_id = {} AND set_name = {} AND member_name = {}",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(set_name),
                            SqlArg::Text(member_name),
                        ],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_member_chunks",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn clear_member_chunks_for_all_sets(
        &self,
        job_id: JobId,
        member_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let member_name = member_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "clear_member_chunks_for_all_sets",
                        |tx| {
                            let member_name = member_name.clone();
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_extraction_chunks
                                     WHERE job_id = {} AND member_name = {}",
                                    &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(member_name)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_extraction_chunks
                         WHERE job_id = {} AND member_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(member_name)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_member_chunks_for_all_sets",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn save_archive_headers(
        &self,
        job_id: JobId,
        set_name: &str,
        headers: &[u8],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        let headers = headers.to_vec();
        self.run_sql_blocking(async move {
            let args = [
                SqlArg::I64(job_id.0 as i64),
                SqlArg::Text(set_name),
                SqlArg::Bytes(headers),
                SqlArg::I64(job_id.0 as i64),
            ];
            let affected = match datastore.engine() {
                // The statement already guards itself with FOR KEY SHARE, so run
                // it as a single autocommit statement instead of wrapping it in a
                // redundant BEGIN/COMMIT round-trip.
                SqlEngine::Postgres => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_archive_headers (job_id, set_name, headers)
                         SELECT {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_archive_headers_parent
                         ON CONFLICT(job_id, set_name) DO UPDATE SET headers = excluded.headers
                         WHERE active_archive_headers.headers <> excluded.headers",
                        &args,
                    )
                    .await?
                }
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "save_archive_headers", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            tx.execute(
                                "INSERT INTO active_archive_headers (job_id, set_name, headers)
                                 SELECT {}, {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id, set_name) DO UPDATE SET headers = excluded.headers
                                 WHERE active_archive_headers.headers <> excluded.headers",
                                &args,
                            )
                            .await
                        })
                    })
                    .await?
                }
            };
            let label = if affected == 0 {
                "persistence.archive_headers.upsert.unchanged"
            } else {
                "persistence.archive_headers.upsert.changed"
            };
            crate::runtime::perf_probe::record(label, std::time::Duration::ZERO);
            Ok(())
        })
    }

    pub fn delete_archive_headers(&self, job_id: JobId, set_name: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "delete_archive_headers", |tx| {
                        let set_name = set_name.clone();
                        Box::pin(async move {
                            lock_active_job_for_write_tx(tx, job_id).await?;
                            tx.execute(
                                "DELETE FROM active_archive_headers WHERE job_id = {} AND set_name = {}",
                                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_archive_headers WHERE job_id = {} AND set_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "delete_archive_headers",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn save_rar_volume_facts(
        &self,
        job_id: JobId,
        set_name: &str,
        volume_index: u32,
        facts_blob: &[u8],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        let facts_blob = facts_blob.to_vec();
        self.run_sql_blocking(async move {
            let args = [
                SqlArg::I64(job_id.0 as i64),
                SqlArg::Text(set_name),
                SqlArg::I64(volume_index as i64),
                SqlArg::Bytes(facts_blob),
                SqlArg::I64(job_id.0 as i64),
            ];
            let affected = match datastore.engine() {
                SqlEngine::Postgres => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_rar_volume_facts
                         (job_id, set_name, volume_index, facts_blob)
                         SELECT {}, {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_rar_volume_facts_parent
                         ON CONFLICT(job_id, set_name, volume_index) DO UPDATE SET facts_blob = excluded.facts_blob
                         WHERE active_rar_volume_facts.facts_blob <> excluded.facts_blob",
                        &args,
                    )
                    .await?
                }
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "save_rar_volume_facts", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            tx.execute(
                                "INSERT INTO active_rar_volume_facts
                                 (job_id, set_name, volume_index, facts_blob)
                                 SELECT {}, {}, {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id, set_name, volume_index) DO UPDATE SET facts_blob = excluded.facts_blob
                                 WHERE active_rar_volume_facts.facts_blob <> excluded.facts_blob",
                                &args,
                            )
                            .await
                        })
                    })
                    .await?
                }
            };
            let label = if affected == 0 {
                "persistence.rar_volume_facts.upsert.unchanged"
            } else {
                "persistence.rar_volume_facts.upsert.changed"
            };
            crate::runtime::perf_probe::record(label, std::time::Duration::ZERO);
            Ok(())
        })
    }

    pub fn save_detected_archive_identity(
        &self,
        job_id: JobId,
        file_index: u32,
        detected: &DetectedArchiveIdentity,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let detected = detected.clone();
        self.run_sql_blocking(async move {
            let args = [
                SqlArg::I64(job_id.0 as i64),
                SqlArg::I64(file_index as i64),
                SqlArg::Text(detected.kind.as_str().to_string()),
                SqlArg::Text(detected.set_name),
                SqlArg::OptI64(detected.volume_index.map(|value| value as i64)),
                SqlArg::I64(job_id.0 as i64),
            ];
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "save_detected_archive_identity",
                        |tx| {
                            let args = args.clone();
                            Box::pin(async move {
                                tx.execute(
                                    "INSERT INTO active_detected_archives
                                     (job_id, file_index, kind, set_name, volume_index)
                                     SELECT {}, {}, {}, {}, {}
                                     WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                     ON CONFLICT(job_id, file_index) DO UPDATE SET
                                        kind = excluded.kind,
                                        set_name = excluded.set_name,
                                        volume_index = excluded.volume_index",
                                    &args,
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // Guard folded into a FOR KEY SHARE CTE the INSERT selects FROM:
                // no-ops when the job row is gone and blocks a concurrent
                // archive/delete of the parent for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_detected_archives
                         (job_id, file_index, kind, set_name, volume_index)
                         SELECT {}, {}, {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_detected_archive_parent
                         ON CONFLICT(job_id, file_index) DO UPDATE SET
                            kind = excluded.kind,
                            set_name = excluded.set_name,
                            volume_index = excluded.volume_index",
                        &args,
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "save_detected_archive_identity",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn delete_detected_archive_identity(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "delete_detected_archive_identity",
                        |tx| {
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_detected_archives WHERE job_id = {} AND file_index = {}",
                                    &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_detected_archives WHERE job_id = {} AND file_index = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "delete_detected_archive_identity",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn delete_all_rar_volume_facts(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "delete_all_rar_volume_facts",
                        |tx| {
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_rar_volume_facts WHERE job_id = {}",
                                    &[SqlArg::I64(job_id.0 as i64)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_rar_volume_facts WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "delete_all_rar_volume_facts",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn delete_rar_volume_facts_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "delete_rar_volume_facts_for_set",
                        |tx| {
                            let set_name = set_name.clone();
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_rar_volume_facts WHERE job_id = {} AND set_name = {}",
                                    &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_rar_volume_facts WHERE job_id = {} AND set_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "delete_rar_volume_facts_for_set",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn set_volume_status(
        &self,
        job_id: JobId,
        set_name: &str,
        volume_index: u32,
        extracted: bool,
        par2_clean: bool,
        deleted: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "set_volume_status", |tx| {
                        let set_name = set_name.clone();
                        Box::pin(async move {
                            if !active_job_exists_tx(tx, job_id).await? {
                                return Ok(());
                            }
                            tx.execute(
                                "INSERT INTO active_volume_status
                                 (job_id, set_name, volume_index, extracted, par2_clean, deleted)
                                 VALUES ({}, {}, {}, {}, {}, {})
                                 ON CONFLICT(job_id, set_name, volume_index) DO UPDATE SET
                                    extracted = excluded.extracted,
                                    par2_clean = excluded.par2_clean,
                                    deleted = excluded.deleted",
                                &[
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::Text(set_name),
                                    SqlArg::I64(volume_index as i64),
                                    SqlArg::Bool(extracted),
                                    SqlArg::Bool(par2_clean),
                                    SqlArg::Bool(deleted),
                                ],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Guard folded into a FOR KEY SHARE CTE the INSERT selects FROM:
                // no-ops when the job row is gone and blocks a concurrent
                // archive/delete of the parent for the statement's duration.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO active_volume_status
                         (job_id, set_name, volume_index, extracted, par2_clean, deleted)
                         SELECT {}, {}, {}, {}, {}, {}
                         FROM (SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE) active_volume_status_parent
                         ON CONFLICT(job_id, set_name, volume_index) DO UPDATE SET
                            extracted = excluded.extracted,
                            par2_clean = excluded.par2_clean,
                            deleted = excluded.deleted",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(set_name),
                            SqlArg::I64(volume_index as i64),
                            SqlArg::Bool(extracted),
                            SqlArg::Bool(par2_clean),
                            SqlArg::Bool(deleted),
                            SqlArg::I64(job_id.0 as i64),
                        ],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "set_volume_status",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn clear_volume_status_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "clear_volume_status_for_set",
                        |tx| {
                            let set_name = set_name.clone();
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_volume_status WHERE job_id = {} AND set_name = {}",
                                    &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_volume_status WHERE job_id = {} AND set_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_volume_status_for_set",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn clear_verified_suspect_volumes_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "clear_verified_suspect_volumes_for_set",
                        |tx| {
                            let set_name = set_name.clone();
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_rar_verified_suspect WHERE job_id = {} AND set_name = {}",
                                    &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_rar_verified_suspect WHERE job_id = {} AND set_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_verified_suspect_volumes_for_set",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    pub fn clear_extraction_chunks_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(
                        &datastore,
                        "clear_extraction_chunks_for_set",
                        |tx| {
                            let set_name = set_name.clone();
                            Box::pin(async move {
                                lock_active_job_for_write_tx(tx, job_id).await?;
                                tx.execute(
                                    "DELETE FROM active_extraction_chunks WHERE job_id = {} AND set_name = {}",
                                    &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                                )
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                }
                // DELETE of a gone job's child rows is already a no-op and is
                // idempotent with a racing archive cascade, so the lock adds
                // nothing: plain autocommit DELETE.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM active_extraction_chunks WHERE job_id = {} AND set_name = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
                    )
                    .await
                    .map(|_| ());
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "clear_extraction_chunks_for_set",
                        started.elapsed(),
                    );
                    result
                }
            }
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
        }
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
