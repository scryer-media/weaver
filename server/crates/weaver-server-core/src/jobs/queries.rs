// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::StateError;
use crate::jobs::assembly::{DetectedArchiveIdentity, DetectedArchiveKind, FileCoverage};
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource, RecoveredJob};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

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

impl Database {
    // Every persisted completed-file digest regardless of provenance,
    // including legacy rows of unknown origin. Identity hints only — never
    // verification evidence.
    pub fn load_complete_file_hashes_any(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, [u8; 16]>, StateError> {
        self.load_complete_file_hashes_inner(job_id, false)
    }

    fn load_complete_file_hashes_inner(
        &self,
        job_id: JobId,
        trusted_only: bool,
    ) -> Result<HashMap<u32, [u8; 16]>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            // Explicit allowlist rather than IS NOT NULL: a future provenance
            // label is untrusted until the code that writes it also teaches
            // this filter what it means.
            let sql = if trusted_only {
                "SELECT file_index, md5
                 FROM active_files
                 WHERE job_id = {} AND md5_provenance IN ('streamed', 'verified')"
            } else {
                "SELECT file_index, md5
                 FROM active_files
                 WHERE job_id = {}"
            };
            let rows =
                SqlRuntime::fetch_all(datastore.read_exec(), sql, &[SqlArg::I64(job_id.0 as i64)])
                    .await?;

            let mut hashes = HashMap::new();
            for row in rows {
                let Some(md5) = row.opt_bytes("md5")? else {
                    continue;
                };
                hashes.insert(row.i64("file_index")? as u32, md5_from_bytes(md5)?);
            }
            Ok(hashes)
        })
    }

    // The root phase of a graph job, which is its status while it runs its
    // tree.
    pub fn save_active_job_root_phase(
        &self,
        job_id: JobId,
        phase: &'static str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE active_jobs SET root_phase = {} WHERE job_id = {}",
                &[
                    SqlArg::Text(phase.to_string()),
                    SqlArg::I64(job_id.0 as i64),
                ],
            )
            .await?;
            Ok(())
        })
    }

    pub fn load_active_job_root_phase(&self, job_id: JobId) -> Result<Option<String>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT root_phase FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_text("root_phase"))
                .transpose()?
                .flatten())
        })
    }

    pub(crate) fn load_nzb_pause_releases(&self) -> Result<HashSet<JobId>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id FROM active_jobs WHERE nzb_release_pending = 1",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.i64("job_id").map(|id| JobId(id as u64)))
            .collect()
        })
    }

    pub(crate) fn load_nzb_admissions(&self) -> Result<HashSet<JobId>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id FROM active_jobs WHERE nzb_admitted = 1",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.i64("job_id").map(|id| JobId(id as u64)))
            .collect()
        })
    }

    pub fn load_active_jobs(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking_read(async move {
            let mut jobs = HashMap::new();

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, nzb_path, nzb_zstd, output_dir, status, download_state, post_state, run_state, error,
                        nzb_hash,
                        created_at, queued_repair_at_epoch_ms,
                        queued_extract_at_epoch_ms, paused_resume_status,
                        paused_resume_download_state, paused_resume_post_state,
                        category, metadata, queue_position, password
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
                    detected_archives: HashMap::new(),
                    file_identities: HashMap::new(),
                    complete_files: HashSet::new(),
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
                    queue_position: row.opt_i64("queue_position")?,
                    password_override: super::persistence::decrypt_archive_password(
                        key.as_ref(),
                        row.opt_text("password")?,
                    )?,
                };
                jobs.insert(job_id, job);
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

            Ok(jobs)
        })
    }

    pub fn load_active_job_persisted_nzb(
        &self,
        job_id: JobId,
    ) -> Result<Option<PersistedNzbRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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

    // Everything the archive-password harvest reads from an active job's row:
    // the persisted NZB, and the passwords supplied with the job (decrypted).
    // A failed read is `Read`; a value of a type this build does not read is
    // `Rows`; the passwords' own split is `decrypt_supplied_passwords`'.
    pub fn load_active_job_password_sources(
        &self,
        job_id: JobId,
    ) -> Result<
        Option<(
            PersistedNzbRecord,
            Vec<crate::jobs::ArchivePasswordCandidate>,
        )>,
        crate::LoadError,
    > {
        use crate::LoadError;
        let datastore = self.datastore();
        let row = self
            .run_sql_blocking_read(async move {
                SqlRuntime::fetch_optional(
                    datastore.read_exec(),
                    "SELECT nzb_path, nzb_zstd, password_candidates
                     FROM active_jobs
                     WHERE job_id = {}
                     LIMIT 1",
                    &[SqlArg::I64(job_id.0 as i64)],
                )
                .await
            })
            .map_err(LoadError::Read)?;
        row.map(|row| {
            let typed = || -> Result<_, StateError> {
                Ok((
                    (
                        PathBuf::from(row.text("nzb_path")?),
                        row.opt_bytes("nzb_zstd")?,
                    ),
                    row.opt_text("password_candidates")?,
                ))
            };
            let (nzb, stored) = typed().map_err(LoadError::Rows)?;
            let supplied = crate::jobs::persistence::decrypt_supplied_passwords(
                self.encryption_key(),
                stored,
            )?;
            Ok((nzb, supplied))
        })
        .transpose()
    }

    // Load the per-file runtime view for one active job: the bytes each
    // incomplete file's coverage checkpoint holds, plus the set of fully
    // completed file indices. Used by integration facades that expose per-file listings
    // without loading the full recovery snapshot.
    pub fn load_active_file_runtime(
        &self,
        job_id: JobId,
    ) -> Result<(HashMap<u32, u64>, HashSet<u32>), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT file_index, coverage
                 FROM active_file_progress
                 WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut progress = HashMap::with_capacity(rows.len());
            for row in rows {
                let covered = FileCoverage::decode(&row.bytes("coverage")?)
                    .map_or(0, |coverage| coverage.covered_bytes());
                progress.insert(row.i64("file_index")? as u32, covered);
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT file_index FROM active_files WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut complete = HashSet::with_capacity(rows.len());
            for row in rows {
                complete.insert(row.i64("file_index")? as u32);
            }

            Ok((progress, complete))
        })
    }

    pub fn max_job_id_all(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM (
                     SELECT MAX(job_id) AS id FROM active_jobs
                     UNION ALL
                     SELECT MAX(job_id) AS id FROM job_history
                     UNION ALL
                     SELECT MAX(job_id) AS id FROM duplicate_job_snapshots
                     UNION ALL
                     SELECT MAX(job_id) AS id FROM forgotten_duplicate_identities
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
}
