// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::StateError;
use crate::jobs::assembly::{DetectedArchiveIdentity, DetectedArchiveKind, FileCoverage};
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource, RecoveredJob};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use weaver_server_base::jobs::ids::{JobId, NzbFileId};

type PersistedNzbRecord = (PathBuf, Option<Vec<u8>>);

use weaver_server_jobs::jobs::queries as stored_jobs;
pub(crate) use weaver_server_jobs::jobs::queries::InstallRowFacts;

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

pub trait JobsQueries {
    fn load_complete_file_hashes_any(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, [u8; 16]>, StateError>;
    fn save_active_job_root_phase(
        &self,
        job_id: JobId,
        phase: &'static str,
    ) -> Result<(), StateError>;
    fn load_active_job_root_phase(&self, job_id: JobId) -> Result<Option<String>, StateError>;
    fn load_active_jobs(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError>;
    fn load_active_job_rows(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError>;
    fn load_active_job(&self, job_id: JobId) -> Result<Option<RecoveredJob>, StateError>;
    fn load_active_job_row(&self, job_id: JobId) -> Result<Option<RecoveredJob>, StateError>;
    fn load_active_job_persisted_nzb(
        &self,
        job_id: JobId,
    ) -> Result<Option<PersistedNzbRecord>, StateError>;
    fn load_active_job_password_sources(
        &self,
        job_id: JobId,
    ) -> Result<
        Option<(
            PersistedNzbRecord,
            Vec<crate::jobs::ArchivePasswordCandidate>,
        )>,
        crate::LoadError,
    >;
    fn load_active_file_runtime(
        &self,
        job_id: JobId,
    ) -> Result<(HashMap<u32, u64>, HashSet<u32>), StateError>;
    fn max_job_id_all(&self) -> Result<u64, StateError>;
}

impl JobsQueries for Database {
    // Every persisted completed-file digest regardless of provenance,
    // including legacy rows of unknown origin. Identity hints only — never
    // verification evidence.
    fn load_complete_file_hashes_any(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, [u8; 16]>, StateError> {
        self.load_complete_file_hashes_inner(job_id, false)
    }

    fn save_active_job_root_phase(
        &self,
        job_id: JobId,
        phase: &'static str,
    ) -> Result<(), StateError> {
        stored_jobs::save_active_job_root_phase(self, job_id, phase)
    }

    fn load_active_job_root_phase(&self, job_id: JobId) -> Result<Option<String>, StateError> {
        stored_jobs::load_active_job_root_phase(self, job_id)
    }

    // Every active job's row, without its stored NZB: a restart reads each
    // NZB on its own, so the queue's NZBs are never all in memory at once.
    fn load_active_jobs(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        self.load_active_jobs_where(None, true)
    }

    // Every active job's row alone: no file identities, detected archives or
    // completed files. A restart reads those for each job as it installs it.
    fn load_active_job_rows(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        self.load_active_jobs_where(None, false)
    }

    // One active job's row, without its stored NZB.
    fn load_active_job(&self, job_id: JobId) -> Result<Option<RecoveredJob>, StateError> {
        Ok(self
            .load_active_jobs_where(Some(&[job_id]), true)?
            .remove(&job_id))
    }

    // One active job's row alone, as `load_active_job_rows` reads each.
    fn load_active_job_row(&self, job_id: JobId) -> Result<Option<RecoveredJob>, StateError> {
        Ok(self
            .load_active_jobs_where(Some(&[job_id]), false)?
            .remove(&job_id))
    }

    fn load_active_job_persisted_nzb(
        &self,
        job_id: JobId,
    ) -> Result<Option<PersistedNzbRecord>, StateError> {
        stored_jobs::load_active_job_persisted_nzb(self, job_id)
    }

    // Everything the archive-password harvest reads from an active job's row:
    // the persisted NZB, and the passwords supplied with the job (decrypted).
    // A failed read is `Read`; a value of a type this build does not read is
    // `Rows`; the passwords' own split is `decrypt_supplied_passwords`'.
    fn load_active_job_password_sources(
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
                    "SELECT a.nzb_path, n.nzb_zstd, a.password_candidates
                     FROM active_jobs AS a
                     LEFT JOIN job_nzbs AS n ON n.job_id = a.job_id
                     WHERE a.job_id = {}
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
    fn load_active_file_runtime(
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

    fn max_job_id_all(&self) -> Result<u64, StateError> {
        stored_jobs::max_job_id_all(self)
    }
}

trait JobsLocalQueries {
    fn load_complete_file_hashes_inner(
        &self,
        job_id: JobId,
        trusted_only: bool,
    ) -> Result<HashMap<u32, [u8; 16]>, StateError>;
    fn load_active_jobs_where(
        &self,
        only: Option<&[JobId]>,
        with_files: bool,
    ) -> Result<HashMap<JobId, RecoveredJob>, StateError>;
    fn read_active_jobs_chunk(
        datastore: &crate::persistence::sql_runtime::StoreDatastore,
        key: Option<&crate::persistence::encryption::EncryptionKey>,
        filter: &str,
        args: &[SqlArg],
        with_files: bool,
        out: &mut HashMap<JobId, RecoveredJob>,
    ) -> impl std::future::Future<Output = Result<(), StateError>> + Send;
}

impl JobsLocalQueries for Database {
    fn load_complete_file_hashes_inner(
        &self,
        job_id: JobId,
        trusted_only: bool,
    ) -> Result<HashMap<u32, [u8; 16]>, StateError> {
        stored_jobs::load_complete_file_hashes_inner(self, job_id, trusted_only)
    }

    fn load_active_jobs_where(
        &self,
        only: Option<&[JobId]>,
        with_files: bool,
    ) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        let chunks: Vec<Vec<SqlArg>> = match only {
            Some(ids) => ids
                .chunks(crate::persistence::sql_runtime::max_rows_for_engine(
                    datastore.engine(),
                    1,
                ))
                .map(|chunk| chunk.iter().map(|id| SqlArg::I64(id.0 as i64)).collect())
                .collect(),
            None => vec![Vec::new()],
        };
        let filtered = only.is_some();
        self.run_sql_blocking_read(async move {
            let mut jobs = HashMap::new();
            for args in chunks {
                let filter = if filtered {
                    format!(" WHERE job_id IN ({})", vec!["{}"; args.len()].join(", "))
                } else {
                    String::new()
                };
                Self::read_active_jobs_chunk(
                    &datastore,
                    key.as_ref(),
                    &filter,
                    &args,
                    with_files,
                    &mut jobs,
                )
                .await?;
            }
            Ok(jobs)
        })
    }

    async fn read_active_jobs_chunk(
        datastore: &crate::persistence::sql_runtime::StoreDatastore,
        key: Option<&crate::persistence::encryption::EncryptionKey>,
        filter: &str,
        args: &[SqlArg],
        with_files: bool,
        out: &mut HashMap<JobId, RecoveredJob>,
    ) -> Result<(), StateError> {
        {
            let mut jobs = HashMap::new();

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!(
                    "SELECT job_id, nzb_path, output_dir, status, download_state, post_state, run_state, error,
                        nzb_hash,
                        created_at, queued_repair_at_epoch_ms,
                        queued_extract_at_epoch_ms, paused_resume_status,
                        paused_resume_download_state, paused_resume_post_state,
                        category, metadata, queue_position, password
                 FROM active_jobs{filter}"
                ),
                args,
            )
            .await?;
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                let job = RecoveredJob {
                    job_id,
                    nzb_hash: nzb_hash_from_bytes(row.bytes("nzb_hash")?),
                    nzb_path: PathBuf::from(row.text("nzb_path")?),
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
                        key,
                        row.opt_text("password")?,
                    )?,
                };
                jobs.insert(job_id, job);
            }
            if !with_files {
                out.extend(jobs);
                return Ok(());
            }

            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!(
                    "SELECT job_id, file_index, source_filename, current_filename,
                        canonical_filename, classification_kind, classification_set_name,
                        classification_volume_index, classification_source
                 FROM active_file_identities{filter}"
                ),
                args,
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
                &format!(
                    "SELECT job_id, file_index, kind, set_name, volume_index
                 FROM active_detected_archives{filter}"
                ),
                args,
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
                &format!("SELECT job_id, file_index FROM active_files{filter}"),
                args,
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
                let discarded_detected =
                    before_detected.saturating_sub(job.detected_archives.len());
                if discarded_detected > 0 {
                    tracing::debug!(
                        job_id = job.job_id.0,
                        discarded_detected,
                        "discarded stale archive discovery cache entries for incomplete files"
                    );
                }
            }

            out.extend(jobs);
            Ok(())
        }
    }
}

pub(crate) trait JobsInternalQueries {
    fn load_nzb_pause_releases(&self) -> Result<HashSet<JobId>, StateError>;
    fn load_nzb_admissions(&self) -> Result<HashSet<JobId>, StateError>;
    fn load_active_jobs_by_id(
        &self,
        job_ids: &[JobId],
    ) -> Result<HashMap<JobId, RecoveredJob>, StateError>;
    fn load_install_row_facts(
        &self,
        job_ids: &[JobId],
    ) -> Result<HashMap<JobId, InstallRowFacts>, StateError>;
    #[cfg(test)]
    fn load_active_job_staging_dir(&self, job_id: JobId) -> Result<Option<PathBuf>, StateError>;
}

impl JobsInternalQueries for Database {
    fn load_nzb_pause_releases(&self) -> Result<HashSet<JobId>, StateError> {
        stored_jobs::load_nzb_pause_releases(self)
    }

    fn load_nzb_admissions(&self) -> Result<HashSet<JobId>, StateError> {
        stored_jobs::load_nzb_admissions(self)
    }

    // As `load_active_job`, for every job of `job_ids` at once: the same
    // statements, each over a bind-sized chunk of the ids.
    fn load_active_jobs_by_id(
        &self,
        job_ids: &[JobId],
    ) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }
        self.load_active_jobs_where(Some(job_ids), true)
    }

    fn load_install_row_facts(
        &self,
        job_ids: &[JobId],
    ) -> Result<HashMap<JobId, InstallRowFacts>, StateError> {
        stored_jobs::load_install_row_facts(self, job_ids)
    }

    // Where the job's row says it stages the files it delivers: None when
    // the row names no staging directory, or there is no row.
    #[cfg(test)]
    fn load_active_job_staging_dir(&self, job_id: JobId) -> Result<Option<PathBuf>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT staging_dir FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .map(|row| row.opt_text("staging_dir"))
            .transpose()?
            .flatten()
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from))
        })
    }
}
