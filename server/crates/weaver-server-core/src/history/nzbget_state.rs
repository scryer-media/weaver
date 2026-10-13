// History state that only the NZBGet-compatible API reads or writes. It lives
// on the history row, so erasing the row erases it too, and the web UI never
// sees it.

use std::collections::HashMap;

use crate::StateError;
use crate::history::HistoryFailureKind;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NzbgetHistoryState {
    // Removed from the client's default history listing by HistoryDelete.
    pub hidden: bool,
    // Why the job failed, when it failed and weaver kept the kind.
    pub failure_kind: Option<HistoryFailureKind>,
}

impl Database {
    // Hide or unhide a history row from the NZBGet-compatible API. Returns
    // false when no history row exists for the job.
    pub fn set_nzbget_history_hidden(&self, job_id: u64, hidden: bool) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzbget_history_hidden", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE job_history SET nzbget_hidden = {} WHERE job_id = {}",
                            &[SqlArg::I64(i64::from(hidden)), SqlArg::I64(job_id)],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // Record why a history row's job failed, or clear it. Returns false when
    // no history row exists for the job.
    pub fn set_history_failure_kind(
        &self,
        job_id: u64,
        kind: Option<HistoryFailureKind>,
    ) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_history_failure_kind", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE job_history SET failure_kind = {} WHERE job_id = {}",
                            &[
                                SqlArg::OptText(kind.map(|kind| kind.as_str().to_string())),
                                SqlArg::I64(job_id),
                            ],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // The facade-only state for each listed history row. Rows absent from the
    // map have no history row.
    pub fn nzbget_history_states(
        &self,
        job_ids: &[u64],
    ) -> Result<HashMap<u64, NzbgetHistoryState>, StateError> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }
        // Internal u64 job ids, inlined as decimal integers.
        let ids = job_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT job_id, nzbget_hidden, failure_kind FROM job_history WHERE job_id IN ({ids})"
        );
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &[]).await?;
            let mut states = HashMap::with_capacity(rows.len());
            for row in rows {
                states.insert(
                    row.i64("job_id")? as u64,
                    NzbgetHistoryState {
                        hidden: row.i64("nzbget_hidden")? != 0,
                        failure_kind: row
                            .opt_text("failure_kind")?
                            .as_deref()
                            .and_then(HistoryFailureKind::from_persisted),
                    },
                );
            }
            Ok(states)
        })
    }
}
