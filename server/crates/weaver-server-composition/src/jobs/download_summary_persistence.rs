// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_jobs::jobs::download_summary_persistence::JobsDownloadSummaryPersistenceQueries;

#[cfg(test)]
use super::download_summary::DownloadSummary;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
use super::ids::JobId;
#[cfg(test)]
use crate::Database;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
use crate::StateError;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
use weaver_server_db::persistence::connection::Database as SqlDatabase;

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub trait JobsDownloadSummaryPersistenceInternalQueries {
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    fn history_download_summary_text(&self, job_id: JobId) -> Result<Option<String>, StateError>;
}

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
impl JobsDownloadSummaryPersistenceInternalQueries for SqlDatabase {
    // The stored text of a history row's summary, as written.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    fn history_download_summary_text(&self, job_id: JobId) -> Result<Option<String>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT download_summary FROM job_history WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_text("download_summary"))
                .transpose()?
                .flatten())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobHistoryRow;
    use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
    use crate::history::queries::HistoryQueries;
    use crate::jobs::download_summary::{DOWNLOAD_SUMMARY_VERSION, Layout};

    fn history_row(id: u64) -> JobHistoryRow {
        JobHistoryRow {
            job_id: id,
            job_hash: None,
            name: "lantern".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 10,
            downloaded_bytes: 10,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1_767_000_000,
            completed_at: 1_767_000_090,
            metadata: None,
            server_attribution: None,
            archived_at: None,
            protocol: crate::jobs::Protocol::Nzb,
        }
    }

    fn put_history_summary(db: &Database, job_id: u64, json: String) {
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "test_history_summary", |tx| {
                let json = json.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE job_history SET download_summary = {} WHERE job_id = {}",
                        &[SqlArg::Text(json), SqlArg::I64(job_id as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
        .unwrap();
    }

    #[test]
    fn no_history_read_decodes_the_summary_column() {
        for select in crate::history::queries::JOB_HISTORY_SELECTS {
            assert!(!select.contains("download_summary"), "{select}");
        }
    }

    // A legacy row reads as none; a stored one reads back; deleting the
    // entry takes its summary with it.
    #[test]
    fn a_history_summary_reads_back_and_goes_with_its_entry() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history_row(41)).unwrap();
        assert_eq!(
            JobsDownloadSummaryPersistenceQueries::history_download_summary(&db, JobId(41))
                .unwrap(),
            None
        );

        let summary = DownloadSummary {
            version: DOWNLOAD_SUMMARY_VERSION,
            layout: Some(Layout::Plain),
            ..DownloadSummary::default()
        };
        put_history_summary(&db, 41, summary.to_storage_json().unwrap());
        assert_eq!(
            db.history_download_summary(JobId(41)).unwrap(),
            Some(summary)
        );
        assert!(db.get_job_history(41).unwrap().is_some());

        assert!(db.delete_job_history(41).unwrap());
        assert_eq!(db.history_download_summary_text(JobId(41)).unwrap(), None);
        db.insert_job_history(&history_row(41)).unwrap();
        assert_eq!(
            db.history_download_summary_text(JobId(41)).unwrap(),
            None,
            "a new entry of the same id starts with no summary"
        );
    }

    // The summary is copied from the active row as the job is archived, so
    // a job still queued has none on its history side.
    #[test]
    fn an_active_summary_is_not_a_history_summary() {
        let db = Database::open_in_memory().unwrap();
        db.save_active_download_summary(JobId(42), "{}".into())
            .unwrap();
        assert_eq!(db.history_download_summary_text(JobId(42)).unwrap(), None);
    }
}
