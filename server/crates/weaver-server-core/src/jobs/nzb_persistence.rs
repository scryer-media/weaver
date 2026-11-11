// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's stored NZB in the database: its compressed bytes, one
// `active_job_nzbs` row per job, apart from the active row so a read of that
// row never walks them. The row goes with the job's other active rows.

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::sql_runtime::{SqlArg, SqlTx};

// Store a job's NZB in the caller's transaction, beside the active row it
// writes. Bytes an earlier job of the same id left are replaced.
pub(crate) async fn write_active_job_nzb_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    nzb_zstd: &[u8],
) -> Result<(), StateError> {
    tx.execute(
        "INSERT INTO active_job_nzbs (job_id, nzb_zstd) VALUES ({}, {})
         ON CONFLICT(job_id) DO UPDATE SET nzb_zstd = excluded.nzb_zstd",
        &[
            SqlArg::I64(job_id.0 as i64),
            SqlArg::Bytes(nzb_zstd.to_vec()),
        ],
    )
    .await?;
    Ok(())
}
