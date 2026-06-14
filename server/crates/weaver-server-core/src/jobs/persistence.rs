use std::collections::HashSet;
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
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, StoreDatastore};

async fn active_job_exists(datastore: &StoreDatastore, job_id: JobId) -> Result<bool, StateError> {
    Ok(SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT 1 FROM active_jobs WHERE job_id = {} LIMIT 1",
        &[SqlArg::I64(job_id.0 as i64)],
    )
    .await?
    .is_some())
}

async fn active_job_exists_tx(tx: &mut SqlTx<'_>, job_id: JobId) -> Result<bool, StateError> {
    Ok(tx
        .fetch_optional(
            "SELECT 1 FROM active_jobs WHERE job_id = {} LIMIT 1",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?
        .is_some())
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

impl Database {
    pub fn create_active_job(&self, job: &ActiveJob) -> Result<(), StateError> {
        let datastore = self.datastore();
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
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO active_jobs
                 (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, created_at, category, metadata)
                 VALUES ({}, {}, {}, {}, {}, 'queued', 'queued', 'idle', 'active', {}, {}, {})",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn update_active_job(&self, job_id: JobId, update: &JobUpdate) -> Result<(), StateError> {
        let datastore = self.datastore();
        let update = update.clone();
        self.run_sql_blocking(async move {
            match update.category {
                FieldUpdate::Unchanged => {}
                FieldUpdate::Clear => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_jobs SET category = NULL WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                }
                FieldUpdate::Set(category) => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_jobs SET category = {} WHERE job_id = {}",
                        &[SqlArg::Text(category), SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                }
            }

            match update.metadata {
                FieldUpdate::Unchanged => {}
                FieldUpdate::Clear => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE active_jobs SET metadata = NULL WHERE job_id = {}",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                }
                FieldUpdate::Set(metadata) => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
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
                    let mut known_active_jobs = HashSet::new();
                    let mut missing_jobs = HashSet::new();
                    for seg in segments {
                        if missing_jobs.contains(&seg.job_id) {
                            continue;
                        }
                        if !known_active_jobs.contains(&seg.job_id) {
                            if active_job_exists_tx(tx, seg.job_id).await? {
                                known_active_jobs.insert(seg.job_id);
                            } else {
                                missing_jobs.insert(seg.job_id);
                                continue;
                            }
                        }
                        tx.execute(
                            "INSERT INTO active_segments
                             (job_id, file_index, segment_number, file_offset, decoded_size, crc32)
                             VALUES ({}, {}, {}, {}, {}, {})
                             ON CONFLICT(job_id, file_index, segment_number) DO NOTHING",
                            &[
                                SqlArg::I64(seg.job_id.0 as i64),
                                SqlArg::I64(seg.file_index as i64),
                                SqlArg::I64(seg.segment_number as i64),
                                SqlArg::I64(seg.file_offset as i64),
                                SqlArg::I64(seg.decoded_size as i64),
                                SqlArg::I64(seg.crc32 as i64),
                            ],
                        )
                        .await?;
                    }
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
        let progress = progress.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "upsert_file_progress_batch", |tx| {
                let progress = progress.clone();
                Box::pin(async move {
                    let mut known_active_jobs = HashSet::new();
                    let mut missing_jobs = HashSet::new();
                    for entry in progress {
                        if missing_jobs.contains(&entry.job_id) {
                            continue;
                        }
                        if !known_active_jobs.contains(&entry.job_id) {
                            if active_job_exists_tx(tx, entry.job_id).await? {
                                known_active_jobs.insert(entry.job_id);
                            } else {
                                missing_jobs.insert(entry.job_id);
                                continue;
                            }
                        }
                        tx.execute(
                            "INSERT INTO active_file_progress
                             (job_id, file_index, contiguous_bytes_written)
                             VALUES ({}, {}, {})
                             ON CONFLICT(job_id, file_index)
                             DO UPDATE SET contiguous_bytes_written =
                                CASE
                                    WHEN active_file_progress.contiguous_bytes_written > excluded.contiguous_bytes_written
                                    THEN active_file_progress.contiguous_bytes_written
                                    ELSE excluded.contiguous_bytes_written
                                END",
                            &[
                                SqlArg::I64(entry.job_id.0 as i64),
                                SqlArg::I64(entry.file_index as i64),
                                SqlArg::I64(entry.contiguous_bytes_written as i64),
                            ],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn clear_file_progress(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_file_progress WHERE job_id = {} AND file_index = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
            )
            .await?;
            Ok(())
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO active_file_identities
                 (job_id, file_index, source_filename, current_filename, canonical_filename,
                  classification_kind, classification_set_name, classification_volume_index,
                  classification_source)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})
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
                ],
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_file_identity(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_file_identities WHERE job_id = {} AND file_index = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
            )
            .await?;
            Ok(())
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO active_par2 (job_id, slice_size, recovery_block_count)
                 VALUES ({}, {}, {})
                 ON CONFLICT(job_id) DO UPDATE SET
                    slice_size = excluded.slice_size,
                    recovery_block_count = excluded.recovery_block_count",
                &[
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::I64(slice_size as i64),
                    SqlArg::I64(recovery_block_count as i64),
                ],
            )
            .await?;
            Ok(())
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO active_par2_files
                 (job_id, file_index, filename, recovery_block_count, promoted)
                 VALUES ({}, {}, {}, {}, {})
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
                ],
            )
            .await?;
            Ok(())
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
            SqlRuntime::execute(
                datastore.read_exec(),
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
                    for member_name in members {
                        tx.execute(
                            "INSERT INTO active_failed_extractions (job_id, member_name)
                             VALUES ({}, {})",
                            &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(member_name)],
                        )
                        .await?;
                    }
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO active_failed_extractions (job_id, member_name)
                 VALUES ({}, {})
                 ON CONFLICT(job_id, member_name) DO NOTHING",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(member_name)],
            )
            .await?;
            Ok(())
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
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_failed_extractions
                 WHERE job_id = {} AND member_name = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(member_name)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn set_active_job_normalization_retried(
        &self,
        job_id: JobId,
        normalization_retried: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE active_jobs SET normalization_retried = {} WHERE job_id = {}",
                &[
                    SqlArg::Bool(normalization_retried),
                    SqlArg::I64(job_id.0 as i64),
                ],
            )
            .await?;
            Ok(())
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
                    for volume_index in volumes {
                        tx.execute(
                            "INSERT INTO active_rar_verified_suspect
                             (job_id, set_name, volume_index)
                             VALUES ({}, {}, {})",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(set_name.clone()),
                                SqlArg::I64(volume_index as i64),
                            ],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn clear_verified_suspect_volumes(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_rar_verified_suspect WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(())
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
        let output_path = output_path.to_str().unwrap_or("").to_string();
        self.run_sql_blocking(async move {
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO active_extracted (job_id, member_name, output_path)
                 VALUES ({}, {}, {})
                 ON CONFLICT(job_id, member_name) DO NOTHING",
                &[
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::Text(member_name),
                    SqlArg::Text(output_path),
                ],
            )
            .await?;
            Ok(())
        })
    }

    pub fn clear_extracted_members(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_extracted WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn insert_extraction_chunk(&self, chunk: &ActiveExtractionChunk) -> Result<(), StateError> {
        let datastore = self.datastore();
        let chunk = chunk.clone();
        self.run_sql_blocking(async move {
            if !active_job_exists(&datastore, chunk.job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO active_extraction_chunks
                 (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
                  start_offset, end_offset)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {})
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
                ],
            )
            .await?;
            Ok(())
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
            SqlRuntime::execute(
                datastore.read_exec(),
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

                    for chunk in chunks {
                        tx.execute(
                            "INSERT INTO active_extraction_chunks
                             (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
                              start_offset, end_offset, verified, appended)
                             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(set_name.clone()),
                                SqlArg::Text(chunk.member_name),
                                SqlArg::I64(chunk.volume_index as i64),
                                SqlArg::I64(chunk.bytes_written as i64),
                                SqlArg::Text(chunk.temp_path),
                                SqlArg::I64(chunk.start_offset as i64),
                                SqlArg::I64(chunk.end_offset as i64),
                                SqlArg::Bool(chunk.verified),
                                SqlArg::Bool(chunk.appended),
                            ],
                        )
                        .await?;
                    }
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
            SqlRuntime::execute(
                datastore.read_exec(),
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
            SqlRuntime::execute(
                datastore.read_exec(),
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
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
    }

    pub fn delete_archive_headers(&self, job_id: JobId, set_name: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_archive_headers WHERE job_id = {} AND set_name = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
            )
            .await?;
            Ok(())
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
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
    }

    pub fn delete_detected_archive_identity(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_detected_archives WHERE job_id = {} AND file_index = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(file_index as i64)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_all_rar_volume_facts(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_rar_volume_facts WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(())
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
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_rar_volume_facts WHERE job_id = {} AND set_name = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
            )
            .await?;
            Ok(())
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
            if !active_job_exists(&datastore, job_id).await? {
                return Ok(());
            }
            SqlRuntime::execute(
                datastore.read_exec(),
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
    }

    pub fn clear_volume_status_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_volume_status WHERE job_id = {} AND set_name = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
            )
            .await?;
            Ok(())
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
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_rar_verified_suspect WHERE job_id = {} AND set_name = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
            )
            .await?;
            Ok(())
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
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM active_extraction_chunks WHERE job_id = {} AND set_name = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
            )
            .await?;
            Ok(())
        })
    }
}
