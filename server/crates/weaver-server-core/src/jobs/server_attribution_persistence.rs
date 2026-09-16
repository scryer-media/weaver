use super::{ids::JobId, server_attribution::JobServerAttribution};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::{Database, StateError};

impl Database {
    pub(crate) fn save_active_server_attribution(
        &self,
        snapshots: Vec<(JobId, String)>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "active_server_attribution", |tx| {
                let snapshots = snapshots.clone();
                Box::pin(async move {
                    for (job_id, json) in snapshots {
                        // UPDATE cannot recreate a cancelled or archived job.
                        tx.execute(
                            "UPDATE active_jobs SET server_attribution = {} WHERE job_id = {}",
                            &[SqlArg::Text(json), SqlArg::I64(job_id.0 as i64)],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    pub(crate) fn load_active_server_attribution(
        &self,
        job_id: JobId,
    ) -> Result<JobServerAttribution, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT server_attribution FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let json = row
                .map(|row| row.opt_text("server_attribution"))
                .transpose()?
                .flatten();
            Ok(json
                .as_deref()
                .map(JobServerAttribution::from_storage_json)
                .unwrap_or_default())
        })
    }
}
