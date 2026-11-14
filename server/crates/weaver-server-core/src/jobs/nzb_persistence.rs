// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's stored NZB in the database: its compressed bytes, one `job_nzbs`
// row per job, apart from the job's active or history row so a read of that
// row never walks them. The row is written as the job is admitted and stays
// as the job finishes, so its history reads the same bytes; it goes once the
// job is in neither the queue nor the history, in the transaction that
// removes the last of those rows.

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::sql_runtime::{SqlArg, SqlTx, max_rows_for_tx};

// Store a job's NZB in the caller's transaction, beside the active row it
// writes. Bytes an earlier job of the same id left are replaced.
pub(crate) async fn write_active_job_nzb_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    nzb_zstd: &[u8],
) -> Result<(), StateError> {
    tx.execute(
        "INSERT INTO job_nzbs (job_id, nzb_zstd) VALUES ({}, {})
         ON CONFLICT(job_id) DO UPDATE SET nzb_zstd = excluded.nzb_zstd",
        &[
            SqlArg::I64(job_id.0 as i64),
            SqlArg::Bytes(nzb_zstd.to_vec()),
        ],
    )
    .await?;
    Ok(())
}

// Remove the stored NZB of each of these jobs that is now in neither the
// queue nor the history, in the caller's transaction, after it removed the
// job's rows. One statement per chunk of ids.
pub(crate) async fn delete_unowned_job_nzbs_tx(
    tx: &mut SqlTx<'_>,
    job_ids: &[i64],
) -> Result<usize, StateError> {
    let mut deleted = 0;
    for chunk in job_ids.chunks(max_rows_for_tx(tx, 1)) {
        let placeholders = vec!["{}"; chunk.len()].join(", ");
        let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
        deleted += tx
            .execute(
                &format!(
                    "DELETE FROM job_nzbs
                      WHERE job_id IN ({placeholders})
                        AND NOT EXISTS (
                            SELECT 1 FROM active_jobs WHERE active_jobs.job_id = job_nzbs.job_id
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM job_history WHERE job_history.job_id = job_nzbs.job_id
                        )"
                ),
                &args,
            )
            .await? as usize;
    }
    Ok(deleted)
}
