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

// What `Database::load_install_row_facts` reads for one job.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct InstallRowFacts {
    pub(crate) staging_dir: Option<PathBuf>,
    pub(crate) server_attribution: Option<String>,
    pub(crate) support_facts: Option<String>,
    pub(crate) has_checkpoint: bool,
}

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
                datastore.write_exec(),
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

    // Every active job's row, without its stored NZB: a restart reads each
    // NZB on its own, so the queue's NZBs are never all in memory at once.
    pub fn load_active_jobs(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        self.load_active_jobs_where(None, true)
    }

    // Every active job's row alone: no file identities, detected archives or
    // completed files. A restart reads those for each job as it installs it.
    pub fn load_active_job_rows(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        self.load_active_jobs_where(None, false)
    }

    // One active job's row, without its stored NZB.
    pub fn load_active_job(&self, job_id: JobId) -> Result<Option<RecoveredJob>, StateError> {
        Ok(self
            .load_active_jobs_where(Some(&[job_id]), true)?
            .remove(&job_id))
    }

    // As `load_active_job`, for every job of `job_ids` at once: the same
    // statements, each over a bind-sized chunk of the ids.
    pub(crate) fn load_active_jobs_by_id(
        &self,
        job_ids: &[JobId],
    ) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }
        self.load_active_jobs_where(Some(job_ids), true)
    }

    // One active job's row alone, as `load_active_job_rows` reads each.
    pub fn load_active_job_row(&self, job_id: JobId) -> Result<Option<RecoveredJob>, StateError> {
        Ok(self
            .load_active_jobs_where(Some(&[job_id]), false)?
            .remove(&job_id))
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

    // What an install reads of each job's row beside its runtime, for every
    // job of `job_ids` in one statement per bind-sized chunk: the staging
    // directory, the server attribution and support facts as stored, and
    // whether a checkpoint is stored. A job with no active row reads its
    // support facts from its history row, as `load_job_support_facts` does.
    pub(crate) fn load_install_row_facts(
        &self,
        job_ids: &[JobId],
    ) -> Result<HashMap<JobId, InstallRowFacts>, StateError> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let datastore = self.datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        self.run_sql_blocking_read(async move {
            let mut facts = HashMap::with_capacity(ids.len());
            let chunk_len =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
            for chunk in ids.chunks(chunk_len) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT a.job_id, a.staging_dir, a.server_attribution,
                            COALESCE(a.support_facts,
                                (SELECT h.support_facts FROM job_history AS h
                                 WHERE h.job_id = a.job_id)) AS support_facts,
                            EXISTS (SELECT 1 FROM graph_nodes AS g WHERE g.job_id = a.job_id)
                                AS has_checkpoint
                         FROM active_jobs AS a WHERE a.job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    facts.insert(
                        JobId(row.i64("job_id")? as u64),
                        InstallRowFacts {
                            staging_dir: row
                                .opt_text("staging_dir")?
                                .filter(|dir| !dir.is_empty())
                                .map(PathBuf::from),
                            server_attribution: row.opt_text("server_attribution")?,
                            support_facts: row.opt_text("support_facts")?,
                            has_checkpoint: row.bool("has_checkpoint")?,
                        },
                    );
                }
                let missing = chunk
                    .iter()
                    .copied()
                    .filter(|id| !facts.contains_key(&JobId(*id as u64)))
                    .collect::<Vec<_>>();
                if missing.is_empty() {
                    continue;
                }
                let placeholders = vec!["{}"; missing.len()].join(", ");
                let args = missing.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, support_facts FROM job_history WHERE job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    facts.entry(JobId(row.i64("job_id")? as u64)).or_default().support_facts =
                        row.opt_text("support_facts")?;
                }
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT DISTINCT job_id FROM graph_nodes WHERE job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    facts.entry(JobId(row.i64("job_id")? as u64)).or_default().has_checkpoint = true;
                }
            }
            Ok(facts)
        })
    }

    // Where the job's row says it stages the files it delivers: None when
    // the row names no staging directory, or there is no row.
    #[cfg(test)]
    pub(crate) fn load_active_job_staging_dir(
        &self,
        job_id: JobId,
    ) -> Result<Option<PathBuf>, StateError> {
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

    pub fn load_active_job_persisted_nzb(
        &self,
        job_id: JobId,
    ) -> Result<Option<PersistedNzbRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT a.nzb_path, n.nzb_zstd
                 FROM active_jobs AS a
                 LEFT JOIN job_nzbs AS n ON n.job_id = a.job_id
                 WHERE a.job_id = {}
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
