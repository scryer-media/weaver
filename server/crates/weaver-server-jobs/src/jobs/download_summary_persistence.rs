// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::download_summary::DownloadSummary;
use super::ids::JobId;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime};
use weaver_server_db::persistence::{StateError, connection::Database};

pub trait JobsDownloadSummaryPersistenceQueries {
    fn save_active_download_summary(&self, job_id: JobId, json: String) -> Result<(), StateError>;
    fn history_download_summary(
        &self,
        job_id: JobId,
    ) -> Result<Option<DownloadSummary>, StateError>;
}

impl<T: std::borrow::Borrow<Database> + ?Sized> JobsDownloadSummaryPersistenceQueries for T {
    // Put a finishing job's summary on its active row. The history archive,
    // queued after this write, copies it from there.
    fn save_active_download_summary(&self, job_id: JobId, json: String) -> Result<(), StateError> {
        let db = self.borrow();
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "active_download_summary", |tx| {
                let json = json.clone();
                Box::pin(async move {
                    // UPDATE cannot recreate a cancelled or archived job.
                    tx.execute(
                        "UPDATE active_jobs SET download_summary = {} WHERE job_id = {}",
                        &[SqlArg::Text(json), SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    // A finished job's summary: one column of its history row, by its key.
    // `None` for a job still in the queue, one that finished before
    // summaries were kept, or one whose summary this build cannot read.
    fn history_download_summary(
        &self,
        job_id: JobId,
    ) -> Result<Option<DownloadSummary>, StateError> {
        let db = self.borrow();
        let datastore = db.datastore();
        db.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT download_summary FROM job_history WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_text("download_summary"))
                .transpose()?
                .flatten()
                .and_then(|json| DownloadSummary::from_storage_json(&json)))
        })
    }
}
