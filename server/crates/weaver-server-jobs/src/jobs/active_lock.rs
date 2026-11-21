// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The Postgres row locks a write or delete of a job's rows takes on its
// active row first. SQLite has one writer, so it takes none.

use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlTx};

pub async fn lock_active_job_for_write_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
) -> Result<(), StateError> {
    if matches!(tx, SqlTx::Postgres(_)) {
        tx.fetch_optional(
            "SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?;
    }
    Ok(())
}

pub async fn lock_active_job_for_delete_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
) -> Result<(), StateError> {
    if matches!(tx, SqlTx::Postgres(_)) {
        tx.fetch_optional(
            "SELECT 1 FROM active_jobs WHERE job_id = {} FOR UPDATE",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?;
    }
    Ok(())
}
