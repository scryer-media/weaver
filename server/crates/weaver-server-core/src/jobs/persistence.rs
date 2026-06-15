use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::StateError;
use crate::jobs::assembly::DetectedArchiveIdentity;
use crate::jobs::ids::JobId;
use crate::jobs::model::{FieldUpdate, JobUpdate};
use crate::jobs::record::{
    ActiveExtractionChunk, ActiveFileIdentity, ActiveFileProgress, ActiveJob, CommittedSegment,
    ExtractionChunk,
};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRuntime, SqlTx};
use sqlx::{Postgres, QueryBuilder, Sqlite};

const SQLITE_BATCH_BIND_LIMIT: usize = 900;
const POSTGRES_BATCH_BIND_LIMIT: usize = 16_000;

async fn active_job_exists_tx(tx: &mut SqlTx<'_>, job_id: JobId) -> Result<bool, StateError> {
    lock_active_job_for_write_tx(tx, job_id).await?;
    Ok(tx
        .fetch_optional(
            "SELECT 1 FROM active_jobs WHERE job_id = {} LIMIT 1",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?
        .is_some())
}

pub(super) async fn lock_active_job_for_write_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
) -> Result<(), StateError> {
    if matches!(tx, SqlTx::Postgres(_)) {
        tx.execute(
            "SELECT pg_advisory_xact_lock({})",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn lock_active_jobs_for_write_tx(
    tx: &mut SqlTx<'_>,
    job_ids: &HashSet<JobId>,
) -> Result<(), StateError> {
    let mut job_ids = job_ids.iter().copied().collect::<Vec<_>>();
    job_ids.sort_unstable();
    for job_id in job_ids {
        lock_active_job_for_write_tx(tx, job_id).await?;
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
    lock_active_jobs_for_write_tx(tx, job_ids).await?;
    let chunk_size = max_rows_for_tx(tx, 1);
    let mut active = HashSet::new();
    for chunk in ids.chunks(chunk_size) {
        let placeholders = vec!["{}"; chunk.len()].join(", ");
        let sql = format!("SELECT job_id FROM active_jobs WHERE job_id IN ({placeholders})");
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

async fn bulk_insert_segments_tx(
    tx: &mut SqlTx<'_>,
    segments: &[CommittedSegment],
) -> Result<(), StateError> {
    if segments.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 6);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in segments.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO active_segments
                     (job_id, file_index, segment_number, file_offset, decoded_size, crc32) ",
                );
                builder.push_values(chunk, |mut row, seg| {
                    row.push_bind(seg.job_id.0 as i64)
                        .push_bind(seg.file_index as i64)
                        .push_bind(seg.segment_number as i64)
                        .push_bind(seg.file_offset as i64)
                        .push_bind(seg.decoded_size as i64)
                        .push_bind(seg.crc32 as i64);
                });
                builder.push(" ON CONFLICT(job_id, file_index, segment_number) DO NOTHING");
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in segments.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO active_segments
                     (job_id, file_index, segment_number, file_offset, decoded_size, crc32) ",
                );
                builder.push_values(chunk, |mut row, seg| {
                    row.push_bind(seg.job_id.0 as i64)
                        .push_bind(seg.file_index as i64)
                        .push_bind(seg.segment_number as i64)
                        .push_bind(seg.file_offset as i64)
                        .push_bind(seg.decoded_size as i64)
                        .push_bind(seg.crc32 as i64);
                });
                builder.push(" ON CONFLICT(job_id, file_index, segment_number) DO NOTHING");
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
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

impl Database {
    pub fn create_active_job(&self, job: &ActiveJob) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job.job_id;
        let args = vec![
            SqlArg::I64(job.job_id.0 as i64),
            SqlArg::Bytes(job.nzb_hash.to_vec()),
            SqlArg::Text(job.nzb_path.to_str().unwrap_or("").to_string()),
            SqlArg::Bytes(job.nzb_zstd.clone()),
            SqlArg::Text(job.output_dir.to_str().unwrap_or("").to_string()),
            SqlArg::I64(job.created_at as i64),
            SqlArg::OptText(job.category.clone()),
            SqlArg::OptText(metadata_json(&job.metadata)?),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_active_job", |tx| {
                let args = args.clone();
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "INSERT INTO active_jobs
                         (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata)
                         VALUES ({}, {}, {}, {}, {}, 'queued', 'queued', 'idle', 'active', {}, {}, {})",
                        &args,
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

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
            SqlArg::I64(job.created_at as i64),
            SqlArg::OptText(job.category.clone()),
            SqlArg::OptText(metadata_json(&job.metadata)?),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "create_active_job_with_file_identities",
                |tx| {
                    let args = args.clone();
                    let identities = identities.clone();
                    Box::pin(async move {
                        lock_active_job_for_write_tx(tx, job_id).await?;
                        tx.execute(
                            "INSERT INTO active_jobs
                             (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata)
                             VALUES ({}, {}, {}, {}, {}, 'queued', 'queued', 'idle', 'active', {}, {}, {})",
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

    pub fn update_active_job(&self, job_id: JobId, update: &JobUpdate) -> Result<(), StateError> {
        let datastore = self.datastore();
        let update = update.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_active_job", |tx| {
                let update = update.clone();
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
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
                    Ok(())
                })
            })
            .await
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
                            lock_active_job_for_write_tx(tx, job_id).await?;
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
                    let mut pg_args = Vec::with_capacity(args.len() + 1);
                    pg_args.push(SqlArg::I64(job_id.0 as i64));
                    pg_args.extend(args);
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "WITH active_job_lock AS (
                             SELECT pg_advisory_xact_lock({})
                         )
                         UPDATE active_jobs
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
                         FROM active_job_lock
                         WHERE job_id = {}",
                        &pg_args,
                    )
                    .await?;
                    Ok(())
                }
            }
        })
    }

    pub fn commit_segments(&self, segments: &[CommittedSegment]) -> Result<(), StateError> {
        if segments.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let segments = segments.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "commit_segments", |tx| {
                let segments = segments.clone();
                Box::pin(async move {
                    let job_ids = segments
                        .iter()
                        .map(|segment| segment.job_id)
                        .collect::<HashSet<_>>();
                    let active_job_ids = active_job_ids_tx(tx, &job_ids).await?;
                    let active_segments = segments
                        .into_iter()
                        .filter(|segment| active_job_ids.contains(&segment.job_id))
                        .collect::<Vec<_>>();
                    bulk_insert_segments_tx(tx, &active_segments).await?;
                    Ok(())
                })
            })
            .await
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
            deduped.insert((entry.job_id, entry.file_index), entry.clone());
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
        })
    }

    pub fn complete_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: &[u8; 16],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let filename = filename.to_string();
        let md5 = md5.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "complete_file", |tx| {
                let filename = filename.clone();
                let md5 = md5.clone();
                Box::pin(async move {
                    if active_job_exists_tx(tx, job_id).await? {
                        tx.execute(
                            "INSERT INTO active_files (job_id, file_index, filename, md5)
                             VALUES ({}, {}, {}, {})
                             ON CONFLICT(job_id, file_index) DO NOTHING",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::I64(file_index as i64),
                                SqlArg::Text(filename),
                                SqlArg::Bytes(md5),
                            ],
                        )
                        .await?;
                    }
                    tx.execute(
                        "DELETE FROM active_file_progress WHERE job_id = {} AND file_index = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn mark_file_incomplete(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "mark_file_incomplete", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    for table in [
                        "active_segments",
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
                    lock_active_job_for_write_tx(tx, job_id).await?;
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
                            SqlArg::I64(identity.file_index as i64),
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
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn delete_file_identity(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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
            SqlRuntime::run_in_transaction(&datastore, "set_par2_metadata", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "INSERT INTO active_par2 (job_id, slice_size, recovery_block_count)
                         SELECT {}, {}, {}
                         WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                         ON CONFLICT(job_id) DO UPDATE SET
                            slice_size = excluded.slice_size,
                            recovery_block_count = excluded.recovery_block_count",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(slice_size as i64),
                            SqlArg::I64(recovery_block_count as i64),
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
            SqlRuntime::run_in_transaction(&datastore, "upsert_par2_file", |tx| {
                let filename = filename.clone();
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
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(file_index as i64),
                            SqlArg::Text(filename),
                            SqlArg::I64(recovery_block_count as i64),
                            SqlArg::Bool(promoted),
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

    pub fn set_par2_file_promotion(
        &self,
        job_id: JobId,
        file_index: u32,
        promoted: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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
            SqlRuntime::run_in_transaction(&datastore, "add_failed_extraction", |tx| {
                let member_name = member_name.clone();
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "INSERT INTO active_failed_extractions (job_id, member_name)
                         SELECT {}, {}
                         WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                         ON CONFLICT(job_id, member_name) DO NOTHING",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(member_name),
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

    pub fn remove_failed_extraction(
        &self,
        job_id: JobId,
        member_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let member_name = member_name.to_string();
        self.run_sql_blocking(async move {
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
        })
    }

    pub fn set_active_job_normalization_retried(
        &self,
        job_id: JobId,
        normalization_retried: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "set_active_job_normalization_retried",
                |tx| {
                    Box::pin(async move {
                        lock_active_job_for_write_tx(tx, job_id).await?;
                        tx.execute(
                            "UPDATE active_jobs SET normalization_retried = {} WHERE job_id = {}",
                            &[
                                SqlArg::Bool(normalization_retried),
                                SqlArg::I64(job_id.0 as i64),
                            ],
                        )
                        .await?;
                        Ok(())
                    })
                },
            )
            .await
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
            SqlRuntime::run_in_transaction(&datastore, "clear_verified_suspect_volumes", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "DELETE FROM active_rar_verified_suspect WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn add_extracted_member(
        &self,
        job_id: JobId,
        member_name: &str,
        output_path: &Path,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let member_name = member_name.to_string();
        let output_size = std::fs::metadata(output_path)?.len();
        let output_size = i64::try_from(output_size).map_err(|_| {
            StateError::Database(format!(
                "extracted member output is too large to persist: {}",
                output_path.display()
            ))
        })?;
        let output_path = output_path.to_string_lossy().to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "add_extracted_member", |tx| {
                let member_name = member_name.clone();
                let output_path = output_path.clone();
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "INSERT INTO active_extracted (job_id, member_name, output_path, output_size)
                         SELECT {}, {}, {}, {}
                         WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                         ON CONFLICT(job_id, member_name) DO NOTHING",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(member_name),
                            SqlArg::Text(output_path),
                            SqlArg::I64(output_size),
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

    pub fn clear_extracted_members(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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
        })
    }

    pub fn insert_extraction_chunk(&self, chunk: &ActiveExtractionChunk) -> Result<(), StateError> {
        let datastore = self.datastore();
        let chunk = chunk.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_extraction_chunk", |tx| {
                let chunk = chunk.clone();
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, chunk.job_id).await?;
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
                        &[
                            SqlArg::I64(chunk.job_id.0 as i64),
                            SqlArg::Text(chunk.set_name),
                            SqlArg::Text(chunk.member_name),
                            SqlArg::I64(chunk.volume_index as i64),
                            SqlArg::I64(chunk.bytes_written as i64),
                            SqlArg::Text(chunk.temp_path),
                            SqlArg::I64(chunk.start_offset as i64),
                            SqlArg::I64(chunk.end_offset as i64),
                            SqlArg::I64(chunk.job_id.0 as i64),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
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
            SqlRuntime::run_in_transaction(&datastore, "save_archive_headers", |tx| {
                let set_name = set_name.clone();
                let headers = headers.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    tx.execute(
                        "INSERT INTO active_archive_headers (job_id, set_name, headers)
                         VALUES ({}, {}, {})
                         ON CONFLICT(job_id, set_name) DO UPDATE SET headers = excluded.headers",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(set_name),
                            SqlArg::Bytes(headers),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn delete_archive_headers(&self, job_id: JobId, set_name: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
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
            SqlRuntime::run_in_transaction(&datastore, "save_rar_volume_facts", |tx| {
                let set_name = set_name.clone();
                let facts_blob = facts_blob.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    tx.execute(
                        "INSERT INTO active_rar_volume_facts
                         (job_id, set_name, volume_index, facts_blob)
                         VALUES ({}, {}, {}, {})
                         ON CONFLICT(job_id, set_name, volume_index) DO UPDATE SET facts_blob = excluded.facts_blob",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(set_name),
                            SqlArg::I64(volume_index as i64),
                            SqlArg::Bytes(facts_blob),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
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
            SqlRuntime::run_in_transaction(&datastore, "save_detected_archive_identity", |tx| {
                let detected = detected.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    tx.execute(
                        "INSERT INTO active_detected_archives
                         (job_id, file_index, kind, set_name, volume_index)
                         VALUES ({}, {}, {}, {}, {})
                         ON CONFLICT(job_id, file_index) DO UPDATE SET
                            kind = excluded.kind,
                            set_name = excluded.set_name,
                            volume_index = excluded.volume_index",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(file_index as i64),
                            SqlArg::Text(detected.kind.as_str().to_string()),
                            SqlArg::Text(detected.set_name),
                            SqlArg::OptI64(detected.volume_index.map(|value| value as i64)),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn delete_detected_archive_identity(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_detected_archive_identity", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "DELETE FROM active_detected_archives WHERE job_id = {} AND file_index = {}",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn delete_all_rar_volume_facts(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_all_rar_volume_facts", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "DELETE FROM active_rar_volume_facts WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
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
            SqlRuntime::run_in_transaction(&datastore, "delete_rar_volume_facts_for_set", |tx| {
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
            })
            .await
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
            SqlRuntime::run_in_transaction(&datastore, "clear_volume_status_for_set", |tx| {
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
            })
            .await
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
            SqlRuntime::run_in_transaction(&datastore, "clear_extraction_chunks_for_set", |tx| {
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
            })
            .await
        })
    }
}
