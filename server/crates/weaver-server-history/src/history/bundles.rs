// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Deleting whole history bundles: the history row and every row and stored
//! NZB that belongs to it, in one transaction.

use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlTx, max_rows_for_tx};

pub async fn delete_job_history_bundles_tx(
    tx: &mut SqlTx<'_>,
    job_ids: &[JobId],
) -> Result<Vec<JobId>, StateError> {
    if job_ids.is_empty() {
        return Ok(Vec::new());
    }
    tx.execute(
        "UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1",
        &[],
    )
    .await?;
    let ids = job_ids
        .iter()
        .map(|job_id| {
            i64::try_from(job_id.0).map_err(|_| StateError::Database("job id is too large".into()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut deleted = Vec::new();
    for chunk in ids.chunks(max_rows_for_tx(tx, 1)) {
        let placeholders = vec!["{}"; chunk.len()].join(", ");
        let lock_sql = match tx {
            SqlTx::Postgres(_) => {
                format!(
                    "SELECT job_id FROM job_history WHERE job_id IN ({placeholders}) FOR UPDATE"
                )
            }
            SqlTx::Sqlite(_) => {
                format!("SELECT job_id FROM job_history WHERE job_id IN ({placeholders})")
            }
        };
        let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
        let present = tx
            .fetch_all(&lock_sql, &args)
            .await?
            .iter()
            .map(|row| row.i64("job_id"))
            .collect::<Result<Vec<_>, _>>()?;
        if present.is_empty() {
            continue;
        }
        let placeholders = vec!["{}"; present.len()].join(", ");
        let args = present.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
        for table in [
            "job_events",
            "job_history_attributes",
            "job_history_archived_files",
            "job_history_files",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE job_id IN ({placeholders})"),
                &args,
            )
            .await?;
        }
        weaver_server_jobs::jobs::child_rows::delete_script_state_rows_tx(tx, &present).await?;
        tx.execute(
            &format!("DELETE FROM job_history WHERE job_id IN ({placeholders})"),
            &args,
        )
        .await?;
        // The stored NZB goes with the history row, unless the job is in the
        // queue again.
        weaver_server_jobs::jobs::nzb_persistence::delete_unowned_job_nzbs_tx(tx, &present).await?;
        deleted.extend(present.into_iter().map(|id| JobId(id as u64)));
    }
    Ok(deleted)
}
