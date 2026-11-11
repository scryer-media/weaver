// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's NZB outline in the database: one `active_job_nzb_files` row per
// posted file, and the NZB's title, first password and file count on its
// active row. The rows go with the job's other active rows when it leaves.

use std::collections::HashMap;

use sqlx::{Postgres, QueryBuilder, Sqlite};

use crate::StateError;
use crate::ingest::outline::{NzbOutline, OutlineFile};
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{
    SqlArg, SqlRuntime, SqlTx, db_err, max_rows_for_engine, max_rows_for_tx,
};

// The outline's columns on the active row, the password sealed.
#[derive(Clone)]
pub(crate) struct OutlineHead {
    title: Option<String>,
    password: Option<String>,
    files: i64,
}

impl OutlineHead {
    pub(crate) fn of(
        key: Option<&crate::persistence::encryption::EncryptionKey>,
        outline: &NzbOutline,
    ) -> Result<Self, StateError> {
        Ok(Self {
            title: outline.title.clone(),
            password: super::persistence::encrypt_archive_password(
                key,
                outline.password.as_deref(),
            )?,
            files: outline.files.len() as i64,
        })
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
)]
// Write the outline of a job whose active row exists, in the caller's
// transaction: its file rows in bind-safe chunks, then its head. A job with
// no active row is left with no row here.
pub(crate) async fn write_outline_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    head: &OutlineHead,
    files: &[OutlineFile],
) -> Result<(), StateError> {
    let id = job_id.0 as i64;
    let updated = tx
        .execute(
            "UPDATE active_jobs SET nzb_title = {}, nzb_password = {}, nzb_files = {}
             WHERE job_id = {}",
            &[
                SqlArg::OptText(head.title.clone()),
                SqlArg::OptText(head.password.clone()),
                SqlArg::I64(head.files),
                SqlArg::I64(id),
            ],
        )
        .await?;
    if updated == 0 {
        return Ok(());
    }
    tx.execute(
        "DELETE FROM active_job_nzb_files WHERE job_id = {}",
        &[SqlArg::I64(id)],
    )
    .await?;
    let rows: Vec<(i64, &OutlineFile, String)> = files
        .iter()
        .zip(0i64..)
        .map(|(file, index)| {
            let groups = serde_json::to_string(&file.groups).map_err(|error| {
                StateError::Database(format!("invalid NZB file groups: {error}"))
            })?;
            Ok((index, file, groups))
        })
        .collect::<Result<_, StateError>>()?;
    let chunk_size = max_rows_for_tx(tx, 7);
    const INSERT: &str = "INSERT INTO active_job_nzb_files
        (job_id, file_index, full_name, groups, posted_at, articles, bytes) ";
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in rows.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(INSERT);
                builder.push_values(chunk, |mut row, (index, file, groups)| {
                    row.push_bind(id)
                        .push_bind(*index)
                        .push_bind(file.full_name.clone())
                        .push_bind(groups.clone())
                        .push_bind(file.date as i64)
                        .push_bind(i64::from(file.articles))
                        .push_bind(file.bytes as i64);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in rows.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(INSERT);
                builder.push_values(chunk, |mut row, (index, file, groups)| {
                    row.push_bind(id)
                        .push_bind(*index)
                        .push_bind(file.full_name.clone())
                        .push_bind(groups.clone())
                        .push_bind(file.date as i64)
                        .push_bind(i64::from(file.articles))
                        .push_bind(file.bytes as i64);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
}

impl Database {
    // Write a job's outline in one transaction of its own.
    pub(crate) fn write_job_outline(
        &self,
        job_id: JobId,
        outline: &NzbOutline,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let head = OutlineHead::of(self.encryption_key(), outline)?;
        let files = outline.files.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "write_job_outline", |tx| {
                let head = head.clone();
                let files = files.clone();
                Box::pin(async move { write_outline_tx(tx, job_id, &head, &files).await })
            })
            .await
        })
    }

    // The stored outline of every active job that has one, or of each of the
    // jobs named, those read in bind-safe chunks. An outline whose file rows
    // do not match its count is not one.
    pub(crate) fn load_job_outlines(
        &self,
        only: Option<&[JobId]>,
    ) -> Result<HashMap<JobId, NzbOutline>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        let chunks: Vec<Vec<i64>> = match only {
            None => vec![Vec::new()],
            Some(ids) => {
                let mut ids: Vec<i64> = ids.iter().map(|id| id.0 as i64).collect();
                ids.sort_unstable();
                ids.dedup();
                let size = max_rows_for_engine(datastore.engine(), 1);
                ids.chunks(size).map(<[i64]>::to_vec).collect()
            }
        };
        let filtered = only.is_some();
        self.run_sql_blocking_read(async move {
            let mut outlines = HashMap::new();
            let mut counts = HashMap::new();
            for chunk in chunks {
                if filtered && chunk.is_empty() {
                    continue;
                }
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let (head_filter, file_filter) = if filtered {
                    (
                        format!(" AND job_id IN ({placeholders})"),
                        format!(" WHERE job_id IN ({placeholders})"),
                    )
                } else {
                    (String::new(), String::new())
                };
                let args: Vec<SqlArg> = chunk.iter().copied().map(SqlArg::I64).collect();
                let heads = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, nzb_title, nzb_password, nzb_files FROM active_jobs
                         WHERE nzb_files IS NOT NULL{head_filter}"
                    ),
                    &args,
                )
                .await?;
                if heads.is_empty() {
                    continue;
                }
                for row in heads {
                    let job_id = JobId(row.i64("job_id")? as u64);
                    let password = super::persistence::decrypt_archive_password(
                        key.as_ref(),
                        row.opt_text("nzb_password")?,
                    )?;
                    let files = row.i64("nzb_files")?;
                    counts.insert(job_id, usize::try_from(files).unwrap_or(usize::MAX));
                    outlines.insert(
                        job_id,
                        NzbOutline {
                            title: row.opt_text("nzb_title")?,
                            password,
                            files: Vec::with_capacity(usize::try_from(files).unwrap_or(0)),
                        },
                    );
                }
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, file_index, full_name, groups, posted_at, articles, bytes
                         FROM active_job_nzb_files{file_filter} ORDER BY job_id, file_index"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    let job_id = JobId(row.i64("job_id")? as u64);
                    let Some(outline) = outlines.get_mut(&job_id) else {
                        continue;
                    };
                    // A row out of place leaves the outline short, so it is
                    // dropped below.
                    if row.i64("file_index")? != outline.files.len() as i64 {
                        continue;
                    }
                    let groups = serde_json::from_str(&row.text("groups")?).map_err(|error| {
                        StateError::Database(format!("invalid NZB file groups: {error}"))
                    })?;
                    outline.files.push(OutlineFile {
                        full_name: row.text("full_name")?,
                        groups,
                        date: row.i64("posted_at")? as u64,
                        articles: u32::try_from(row.i64("articles")?).unwrap_or(u32::MAX),
                        bytes: row.i64("bytes")? as u64,
                    });
                }
            }
            outlines.retain(|job_id, outline| counts.get(job_id) == Some(&outline.files.len()));
            Ok(outlines)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::ActiveJob;

    fn active_job(id: u64) -> ActiveJob {
        ActiveJob {
            job_id: JobId(id),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from(format!("job-{id}.nzb")),
            nzb_zstd: Vec::new(),
            output_dir: PathBuf::from(format!("job-{id}")),
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

    // More files than one statement can bind.
    fn outline(files: usize) -> NzbOutline {
        NzbOutline {
            title: Some("Title".to_string()),
            password: Some("secret".to_string()),
            files: (0..files)
                .map(|index| OutlineFile {
                    full_name: format!("file.{index}.bin"),
                    groups: vec!["alt.binaries.a".to_string(), format!("g{index}")],
                    date: index as u64,
                    articles: index as u32 + 1,
                    bytes: index as u64 * 700,
                })
                .collect(),
        }
    }

    fn file_rows(db: &Database) -> i64 {
        let datastore = db.datastore();
        db.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) FROM active_job_nzb_files",
                &[],
            )
            .await?
            .expect("a count")
            .i64_at(0)
        })
        .unwrap()
    }

    // An outline written with its job's admission reads back whole, past one
    // statement's binds, and is written again in place. A job with no active
    // row gets no outline.
    #[test]
    fn an_outline_reads_back_as_written() {
        let db = Database::open_in_memory().unwrap();
        let large = outline(6_000);
        assert!(
            db.materialize_admission_with_outline(
                &active_job(1),
                &[],
                Some(&large),
                None,
                None,
                false
            )
            .unwrap()
        );
        db.create_active_job(&active_job(2)).unwrap();
        let small = outline(2);
        db.write_job_outline(JobId(2), &small).unwrap();
        db.write_job_outline(JobId(2), &small).unwrap();
        db.write_job_outline(JobId(3), &small).unwrap();
        let outlines = db.load_job_outlines(None).unwrap();
        assert_eq!(outlines.len(), 2);
        assert_eq!(outlines[&JobId(1)], large);
        assert_eq!(outlines[&JobId(2)], small);
        let one = db.load_job_outlines(Some(&[JobId(2), JobId(3)])).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[&JobId(2)], small);
        assert_eq!(file_rows(&db), 6_002);
    }

    // The outlines of more jobs than one statement can bind read back in one
    // call, each job's own, and a job named twice or with none is read once
    // or not at all.
    #[test]
    fn the_outlines_of_a_long_id_list_read_back_whole() {
        let db = Database::open_in_memory().unwrap();
        let jobs = max_rows_for_engine(db.datastore().engine(), 1) as u64 + 50;
        for id in 1..=jobs {
            db.create_active_job(&active_job(id)).unwrap();
            db.write_job_outline(JobId(id), &outline(id as usize % 3 + 1))
                .unwrap();
        }
        let mut wanted: Vec<JobId> = (1..=jobs).map(JobId).collect();
        wanted.push(JobId(1));
        wanted.push(JobId(jobs + 1));
        let outlines = db.load_job_outlines(Some(&wanted)).unwrap();
        assert_eq!(outlines, db.load_job_outlines(None).unwrap());
        assert_eq!(outlines.len(), jobs as usize);
        for id in 1..=jobs {
            assert_eq!(
                outlines[&JobId(id)],
                outline(id as usize % 3 + 1),
                "job {id}"
            );
        }
    }

    // The outline goes with the job's active rows, archived or deleted.
    #[test]
    fn an_outline_leaves_with_its_job() {
        let db = Database::open_in_memory().unwrap();
        for id in [1, 2] {
            db.create_active_job(&active_job(id)).unwrap();
            db.write_job_outline(JobId(id), &outline(3)).unwrap();
        }
        db.delete_active_job(JobId(1)).unwrap();
        db.archive_job(
            JobId(2),
            &crate::JobHistoryRow {
                job_id: 2,
                job_hash: None,
                name: "job-2".into(),
                status: "failed".into(),
                error_message: None,
                total_bytes: 0,
                downloaded_bytes: 0,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: 0,
                category: None,
                output_dir: None,
                nzb_path: None,
                created_at: 1,
                completed_at: 2,
                metadata: None,
                server_attribution: None,
            },
        )
        .unwrap();
        assert!(db.load_job_outlines(None).unwrap().is_empty());
        assert_eq!(file_rows(&db), 0);
    }
}
