use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::StateError;
use crate::jobs::assembly::{DetectedArchiveIdentity, DetectedArchiveKind};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::record::{
    ActiveFileIdentity, ActivePar2File, ExtractionChunk, FileIdentitySource, RarVolumeFactsBySet,
    RecoveredJob,
};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};

type PersistedNzbRecord = (PathBuf, Option<Vec<u8>>);

fn md5_from_bytes(bytes: Vec<u8>) -> Result<[u8; 16], StateError> {
    bytes.try_into().map_err(|value: Vec<u8>| {
        StateError::Database(format!("invalid MD5 length: {}", value.len()))
    })
}

fn nzb_hash_from_bytes(bytes: Vec<u8>) -> [u8; 32] {
    let mut hash = [0u8; 32];
    if bytes.len() == hash.len() {
        hash.copy_from_slice(&bytes);
    }
    hash
}

fn metadata_from_json(raw: Option<String>) -> Vec<(String, String)> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

fn extracted_member_marker_valid(
    job_id: JobId,
    member_name: &str,
    output_path: &str,
    output_size: i64,
) -> bool {
    if output_size < 0 || output_path.is_empty() {
        tracing::debug!(
            job_id = job_id.0,
            member = %member_name,
            output_path = %output_path,
            output_size,
            "discarding unvalidated extracted member marker"
        );
        return false;
    }

    match std::fs::metadata(Path::new(output_path)) {
        Ok(metadata) if metadata.is_file() && metadata.len() == output_size as u64 => true,
        Ok(metadata) => {
            tracing::debug!(
                job_id = job_id.0,
                member = %member_name,
                output_path = %output_path,
                output_size,
                actual_size = metadata.len(),
                is_file = metadata.is_file(),
                "discarding stale extracted member marker"
            );
            false
        }
        Err(error) => {
            tracing::debug!(
                job_id = job_id.0,
                member = %member_name,
                output_path = %output_path,
                output_size,
                error = %error,
                "discarding missing extracted member marker"
            );
            false
        }
    }
}

fn detected_from_row(
    kind: String,
    set_name: String,
    volume_index: Option<i64>,
) -> Option<DetectedArchiveIdentity> {
    DetectedArchiveKind::parse(&kind).map(|kind| DetectedArchiveIdentity {
        kind,
        set_name,
        volume_index: volume_index.map(|value| value as u32),
    })
}

fn extraction_chunk_from_row(row: SqlRow) -> Result<ExtractionChunk, StateError> {
    Ok(ExtractionChunk {
        member_name: row.text("member_name")?,
        volume_index: row.i64("volume_index")? as u32,
        bytes_written: row.i64("bytes_written")? as u64,
        temp_path: row.text("temp_path")?,
        start_offset: row.i64("start_offset")? as u64,
        end_offset: row.i64("end_offset")? as u64,
        verified: row.bool("verified")?,
        appended: row.bool("appended")?,
    })
}

impl Database {
    pub fn load_complete_file_hashes(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, [u8; 16]>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT file_index, md5
                 FROM active_files
                 WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;

            let mut hashes = HashMap::new();
            for row in rows {
                hashes.insert(
                    row.i64("file_index")? as u32,
                    md5_from_bytes(row.bytes("md5")?)?,
                );
            }
            Ok(hashes)
        })
    }

    pub fn load_par2_files(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, ActivePar2File>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT file_index, filename, recovery_block_count, promoted
                 FROM active_par2_files
                 WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut files = HashMap::new();
            for row in rows {
                let file = ActivePar2File {
                    file_index: row.i64("file_index")? as u32,
                    filename: row.text("filename")?,
                    recovery_block_count: row.i64("recovery_block_count")? as u32,
                    promoted: row.bool("promoted")?,
                };
                files.insert(file.file_index, file);
            }
            Ok(files)
        })
    }

    pub fn load_failed_extractions(&self, job_id: JobId) -> Result<HashSet<String>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT member_name FROM active_failed_extractions
                 WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            rows.into_iter()
                .map(|row| row.text("member_name"))
                .collect::<Result<HashSet<_>, _>>()
        })
    }

    pub fn load_active_job_normalization_retried(&self, job_id: JobId) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT normalization_retried FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            row.map(|row| row.bool("normalization_retried"))
                .transpose()
                .map(|value| value.unwrap_or(false))
        })
    }

    pub fn load_verified_suspect_volumes(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<String, HashSet<u32>>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT set_name, volume_index
                 FROM active_rar_verified_suspect
                 WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut result = HashMap::<String, HashSet<u32>>::new();
            for row in rows {
                result
                    .entry(row.text("set_name")?)
                    .or_default()
                    .insert(row.i64("volume_index")? as u32);
            }
            Ok(result)
        })
    }

    pub fn load_active_jobs(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let mut jobs = HashMap::new();

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, error,
                        nzb_hash,
                        created_at, queued_repair_at_epoch_ms,
                        queued_extract_at_epoch_ms, paused_resume_status,
                        paused_resume_download_state, paused_resume_post_state,
                        category, metadata
                 FROM active_jobs",
                &[],
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                let job = RecoveredJob {
                    job_id,
                    nzb_hash: nzb_hash_from_bytes(row.bytes("nzb_hash")?),
                    nzb_path: PathBuf::from(row.text("nzb_path")?),
                    nzb_zstd: row.opt_bytes("nzb_zstd")?,
                    output_dir: PathBuf::from(row.text("output_dir")?),
                    committed_segments: HashSet::new(),
                    file_progress: HashMap::new(),
                    detected_archives: HashMap::new(),
                    file_identities: HashMap::new(),
                    complete_files: HashSet::new(),
                    extracted_members: HashSet::new(),
                    status: row.text("status")?,
                    download_state: row.opt_text("download_state")?,
                    post_state: row.opt_text("post_state")?,
                    run_state: row.opt_text("run_state")?,
                    error: row.opt_text("error")?,
                    created_at: row.i64("created_at")? as u64,
                    queued_repair_at_epoch_ms: row.opt_f64("queued_repair_at_epoch_ms")?,
                    queued_extract_at_epoch_ms: row.opt_f64("queued_extract_at_epoch_ms")?,
                    paused_resume_status: row.opt_text("paused_resume_status")?,
                    paused_resume_download_state: row.opt_text("paused_resume_download_state")?,
                    paused_resume_post_state: row.opt_text("paused_resume_post_state")?,
                    category: row.opt_text("category")?,
                    metadata: metadata_from_json(row.opt_text("metadata")?),
                };
                jobs.insert(job_id, job);
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, file_index, segment_number FROM active_segments",
                &[],
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                let file_index = row.i64("file_index")? as u32;
                let segment_number = row.i64("segment_number")? as u32;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.committed_segments.insert(SegmentId {
                        file_id: NzbFileId { job_id, file_index },
                        segment_number,
                    });
                }
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, file_index, contiguous_bytes_written
                 FROM active_file_progress",
                &[],
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.file_progress.insert(
                        row.i64("file_index")? as u32,
                        row.i64("contiguous_bytes_written")? as u64,
                    );
                }
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, file_index, source_filename, current_filename,
                        canonical_filename, classification_kind, classification_set_name,
                        classification_volume_index, classification_source
                 FROM active_file_identities",
                &[],
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                let identity = ActiveFileIdentity {
                    file_index: row.i64("file_index")? as u32,
                    source_filename: row.text("source_filename")?,
                    current_filename: row.text("current_filename")?,
                    canonical_filename: row.opt_text("canonical_filename")?,
                    classification: match (
                        row.opt_text("classification_kind")?,
                        row.opt_text("classification_set_name")?,
                    ) {
                        (Some(kind), Some(set_name)) => detected_from_row(
                            kind,
                            set_name,
                            row.opt_i64("classification_volume_index")?,
                        ),
                        _ => None,
                    },
                    classification_source: FileIdentitySource::parse(
                        &row.text("classification_source")?,
                    )
                    .unwrap_or(FileIdentitySource::Declared),
                };
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.file_identities.insert(identity.file_index, identity);
                }
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, file_index, kind, set_name, volume_index
                 FROM active_detected_archives",
                &[],
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                let Some(detected) = detected_from_row(
                    row.text("kind")?,
                    row.text("set_name")?,
                    row.opt_i64("volume_index")?,
                ) else {
                    continue;
                };
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.detected_archives
                        .insert(row.i64("file_index")? as u32, detected);
                }
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, file_index FROM active_files",
                &[],
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                let file_index = row.i64("file_index")? as u32;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.complete_files.insert(NzbFileId { job_id, file_index });
                }
            }

            for job in jobs.values_mut() {
                let completed_indices = job
                    .complete_files
                    .iter()
                    .map(|file_id| file_id.file_index)
                    .collect::<HashSet<_>>();
                let before_detected = job.detected_archives.len();
                job.detected_archives
                    .retain(|file_index, _| completed_indices.contains(file_index));
                for identity in job.file_identities.values_mut() {
                    if !completed_indices.contains(&identity.file_index) {
                        identity.classification = None;
                    }
                }
                let discarded_detected = before_detected.saturating_sub(job.detected_archives.len());
                if discarded_detected > 0 {
                    tracing::debug!(
                        job_id = job.job_id.0,
                        discarded_detected,
                        "discarded stale archive discovery cache entries for incomplete files"
                    );
                }
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, member_name, output_path, output_size FROM active_extracted",
                &[],
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                if let Some(job) = jobs.get_mut(&job_id) {
                    let member_name = row.text("member_name")?;
                    let output_path = row.text("output_path")?;
                    let output_size = row.i64("output_size")?;
                    if extracted_member_marker_valid(
                        job_id,
                        &member_name,
                        &output_path,
                        output_size,
                    ) {
                        job.extracted_members.insert(member_name);
                    }
                }
            }

            Ok(jobs)
        })
    }

    pub fn load_active_job_persisted_nzb(
        &self,
        job_id: JobId,
    ) -> Result<Option<PersistedNzbRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT nzb_path, nzb_zstd
                 FROM active_jobs
                 WHERE job_id = {}
                 LIMIT 1",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .map(|row| {
                Ok((
                    PathBuf::from(row.text("nzb_path")?),
                    row.opt_bytes("nzb_zstd")?,
                ))
            })
            .transpose()
        })
    }

    pub fn load_detected_archive_identities(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, DetectedArchiveIdentity>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT file_index, kind, set_name, volume_index
                 FROM active_detected_archives
                 WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut detected = HashMap::new();
            for row in rows {
                let Some(identity) = detected_from_row(
                    row.text("kind")?,
                    row.text("set_name")?,
                    row.opt_i64("volume_index")?,
                ) else {
                    continue;
                };
                detected.insert(row.i64("file_index")? as u32, identity);
            }
            Ok(detected)
        })
    }

    pub fn max_job_id_all(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM (
                     SELECT MAX(job_id) AS id FROM active_jobs
                     UNION ALL
                     SELECT MAX(job_id) AS id FROM job_history
                 ) ids",
                &[],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_i64("id"))
                .transpose()?
                .flatten()
                .unwrap_or(0) as u64)
        })
    }

    pub fn get_extraction_chunks(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<Vec<ExtractionChunk>, StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT member_name, volume_index, bytes_written, temp_path,
                        start_offset, end_offset, verified, appended
                 FROM active_extraction_chunks
                 WHERE job_id = {} AND set_name = {}
                 ORDER BY member_name, volume_index",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
            )
            .await?;
            rows.into_iter().map(extraction_chunk_from_row).collect()
        })
    }

    pub fn load_archive_headers(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<Option<Vec<u8>>, StateError> {
        let datastore = self.datastore();
        let set_name = set_name.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT headers FROM active_archive_headers
                 WHERE job_id = {} AND set_name = {}",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Text(set_name)],
            )
            .await?
            .map(|row| row.bytes("headers"))
            .transpose()
        })
    }

    pub fn load_all_archive_headers(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<String, Vec<u8>>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT set_name, headers FROM active_archive_headers
                 WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut headers = HashMap::new();
            for row in rows {
                headers.insert(row.text("set_name")?, row.bytes("headers")?);
            }
            Ok(headers)
        })
    }

    pub fn load_all_rar_volume_facts(
        &self,
        job_id: JobId,
    ) -> Result<RarVolumeFactsBySet, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT set_name, volume_index, facts_blob
                 FROM active_rar_volume_facts
                 WHERE job_id = {}
                 ORDER BY set_name, volume_index",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut grouped: RarVolumeFactsBySet = HashMap::new();
            for row in rows {
                grouped
                    .entry(row.text("set_name")?)
                    .or_default()
                    .push((row.i64("volume_index")? as u32, row.bytes("facts_blob")?));
            }
            Ok(grouped)
        })
    }

    pub fn load_deleted_volume_statuses(
        &self,
        job_id: JobId,
    ) -> Result<Vec<(String, u32)>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT set_name, volume_index
                 FROM active_volume_status
                 WHERE job_id = {} AND deleted = {}
                 ORDER BY set_name, volume_index",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::Bool(true)],
            )
            .await?;
            rows.into_iter()
                .map(|row| Ok((row.text("set_name")?, row.i64("volume_index")? as u32)))
                .collect()
        })
    }
}
