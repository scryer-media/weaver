use super::{ids::JobId, support_facts::JobSupportFacts};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::{Database, StateError};

impl Database {
    // Checkpoint live jobs' support facts onto their active rows. The history
    // archive copies the column from there, so the last checkpoint before it
    // is what a finished job keeps.
    pub fn save_active_support_facts(
        &self,
        snapshots: Vec<(JobId, Option<String>)>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "active_support_facts", |tx| {
                let snapshots = snapshots.clone();
                Box::pin(async move {
                    for (job_id, json) in snapshots {
                        // UPDATE cannot recreate a cancelled or archived job.
                        tx.execute(
                            "UPDATE active_jobs SET support_facts = {} WHERE job_id = {}",
                            &[SqlArg::OptText(json), SqlArg::I64(job_id.0 as i64)],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // A job's support facts from its active row, or from its history row once
    // it has finished. A job with neither reads as having none.
    pub fn load_job_support_facts(&self, job_id: JobId) -> Result<JobSupportFacts, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COALESCE(
                    (SELECT support_facts FROM active_jobs WHERE job_id = {}),
                    (SELECT support_facts FROM job_history WHERE job_id = {})
                 ) AS support_facts",
                &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let json = row
                .map(|row| row.opt_text("support_facts"))
                .transpose()?
                .flatten();
            Ok(JobSupportFacts::from_storage(json.as_deref()))
        })
    }
}
