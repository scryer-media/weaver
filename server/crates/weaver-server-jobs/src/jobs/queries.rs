// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::{
    StateError,
    connection::Database,
    sql_runtime::{SqlArg, SqlRuntime, max_rows_for_engine},
};

type PersistedNzbRecord = (PathBuf, Option<Vec<u8>>);

// What `Database::load_install_row_facts` reads for one job.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct InstallRowFacts {
    pub staging_dir: Option<PathBuf>,
    pub server_attribution: Option<String>,
    pub support_facts: Option<String>,
    pub has_checkpoint: bool,
}

fn md5_from_bytes(bytes: Vec<u8>) -> Result<[u8; 16], StateError> {
    bytes.try_into().map_err(|value: Vec<u8>| {
        StateError::Database(format!("invalid MD5 length: {}", value.len()))
    })
}

// The root phase of a graph job, which is its status while it runs its
// tree.
pub fn save_active_job_root_phase(
    db: &Database,
    job_id: JobId,
    phase: &'static str,
) -> Result<(), StateError> {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
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

pub fn load_active_job_root_phase(
    db: &Database,
    job_id: JobId,
) -> Result<Option<String>, StateError> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
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

pub fn load_active_job_persisted_nzb(
    db: &Database,
    job_id: JobId,
) -> Result<Option<PersistedNzbRecord>, StateError> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
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

pub fn max_job_id_all(db: &Database) -> Result<u64, StateError> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
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

pub fn load_complete_file_hashes_inner(
    db: &Database,
    job_id: JobId,
    trusted_only: bool,
) -> Result<HashMap<u32, [u8; 16]>, StateError> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
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

pub fn load_nzb_pause_releases(db: &Database) -> Result<HashSet<JobId>, StateError> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
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

pub fn load_nzb_admissions(db: &Database) -> Result<HashSet<JobId>, StateError> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
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

// What an install reads of each job's row beside its runtime, for every
// job of `job_ids` in one statement per bind-sized chunk: the staging
// directory, the server attribution and support facts as stored, and
// whether a checkpoint is stored. A job with no active row reads its
// support facts from its history row, as `load_job_support_facts` does.
pub fn load_install_row_facts(
    db: &Database,
    job_ids: &[JobId],
) -> Result<HashMap<JobId, InstallRowFacts>, StateError> {
    if job_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let datastore = db.datastore();
    let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
    db.run_sql_blocking_read(async move {
        let mut facts = HashMap::with_capacity(ids.len());
        let chunk_len = max_rows_for_engine(datastore.engine(), 1);
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
                facts
                    .entry(JobId(row.i64("job_id")? as u64))
                    .or_default()
                    .support_facts = row.opt_text("support_facts")?;
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
                facts
                    .entry(JobId(row.i64("job_id")? as u64))
                    .or_default()
                    .has_checkpoint = true;
            }
        }
        Ok(facts)
    })
}
