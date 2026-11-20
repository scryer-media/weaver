// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's NZB outline in the database: the NZB's title, first password and
// file count on its active row, and every posted file of it as one
// compressed blob, one `active_job_outlines` row per job. The row goes with
// the job's other active rows when it leaves.
//
use std::collections::HashMap;

use rayon::prelude::*;

use super::outline::NzbOutline;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database as SqlDatabase;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, max_rows_for_engine};
use weaver_server_jobs::jobs::outline_persistence::{decode_outline_files, encode_outline_files};

// The outline's columns on the active row, the password sealed, and its
// files' stored blob.
#[derive(Clone)]
pub struct OutlineHead {
    title: Option<String>,
    password: Option<String>,
    files: i64,
    blob: Vec<u8>,
}

impl OutlineHead {
    pub fn of(
        key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
        outline: &NzbOutline,
    ) -> Result<Self, StateError> {
        Ok(Self {
            title: outline.title.clone(),
            password: weaver_server_jobs::jobs::stored_passwords::encrypt_archive_password(
                key,
                outline.password.as_deref(),
            )?,
            files: outline.files.len() as i64,
            blob: encode_outline_files(&outline.files)?,
        })
    }
}

// Write the outline of a job whose active row exists, in the caller's
// transaction: its head, then its blob. A job with no active row is left
// with no row here.
pub async fn write_outline_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    head: &OutlineHead,
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
        "INSERT INTO active_job_outlines (job_id, outline) VALUES ({}, {})
         ON CONFLICT(job_id) DO UPDATE SET outline = excluded.outline",
        &[SqlArg::I64(id), SqlArg::Bytes(head.blob.clone())],
    )
    .await?;
    Ok(())
}

// One stored outline as read, before its blob is opened.
struct StoredOutline {
    job_id: JobId,
    title: Option<String>,
    password: Option<String>,
    files: i64,
    blob: Option<Vec<u8>>,
}

pub trait JobsOutlinePersistenceInternalQueries {
    fn write_job_outline(&self, job_id: JobId, outline: &NzbOutline) -> Result<(), StateError>;
    fn load_job_outlines(
        &self,
        only: Option<&[JobId]>,
    ) -> Result<HashMap<JobId, NzbOutline>, StateError>;
}

impl JobsOutlinePersistenceInternalQueries for SqlDatabase {
    // Write a job's outline in one transaction of its own.
    fn write_job_outline(&self, job_id: JobId, outline: &NzbOutline) -> Result<(), StateError> {
        let datastore = self.datastore();
        let head = OutlineHead::of(self.encryption_key(), outline)?;
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "write_job_outline", |tx| {
                let head = head.clone();
                Box::pin(async move { write_outline_tx(tx, job_id, &head).await })
            })
            .await
        })
    }

    // The stored outline of every active job that has one, or of each of the
    // jobs named, those read in bind-safe chunks. An outline whose files do
    // not match its count, or whose blob does not open, is not one. The blobs
    // are opened after the read, off the SQL runtime.
    fn load_job_outlines(
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
        let stored = self.run_sql_blocking_read(async move {
            let mut stored = Vec::new();
            for chunk in chunks {
                if filtered && chunk.is_empty() {
                    continue;
                }
                let filter = if filtered {
                    let placeholders = vec!["{}"; chunk.len()].join(", ");
                    format!(" AND a.job_id IN ({placeholders})")
                } else {
                    String::new()
                };
                let args: Vec<SqlArg> = chunk.iter().copied().map(SqlArg::I64).collect();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT a.job_id, a.nzb_title, a.nzb_password, a.nzb_files, o.outline
                         FROM active_jobs AS a
                         LEFT JOIN active_job_outlines AS o ON o.job_id = a.job_id
                         WHERE a.nzb_files IS NOT NULL{filter}"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    stored.push(StoredOutline {
                        job_id: JobId(row.i64("job_id")? as u64),
                        title: row.opt_text("nzb_title")?,
                        password:
                            weaver_server_jobs::jobs::stored_passwords::decrypt_archive_password(
                                key.as_ref(),
                                row.opt_text("nzb_password")?,
                            )?,
                        files: row.i64("nzb_files")?,
                        blob: row.opt_bytes("outline")?,
                    });
                }
            }
            Ok(stored)
        })?;
        Ok(stored
            .into_par_iter()
            .filter_map(|stored| {
                // No blob: no file was stored for the job.
                let files = match stored.blob.as_deref().map(decode_outline_files) {
                    None => Vec::new(),
                    Some(Ok(files)) => files,
                    Some(Err(error)) => {
                        tracing::warn!(
                            job_id = stored.job_id.0,
                            %error,
                            "a stored NZB outline could not be read; the job reads its stored NZB"
                        );
                        return None;
                    }
                };
                if usize::try_from(stored.files).ok() != Some(files.len()) {
                    return None;
                }
                Some((
                    stored.job_id,
                    NzbOutline {
                        title: stored.title,
                        password: stored.password,
                        files,
                    },
                ))
            })
            .collect())
    }
}
