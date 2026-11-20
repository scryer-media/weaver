// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tables that hang off an active job row, and their per-job delete.

use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlTx};

// The stored NZB is not among them: it stays while the job's history row
// does, and goes by `delete_unowned_job_nzbs_tx`.
pub const ACTIVE_JOB_CHILD_TABLES: [&str; 15] = [
    "active_file_progress",
    "active_files",
    "active_file_identities",
    "active_extracted",
    "active_detected_archives",
    "active_job_outlines",
    "graph_nodes",
    "graph_members",
    "graph_coverage",
    "graph_envelopes",
    "graph_crypt",
    "graph_recovery",
    "graph_holds",
    "graph_facts_parts",
    "graph_held_parts",
];

// Deletes one job's rows from each table, in order. PostgreSQL takes them in
// one statement.
pub async fn delete_job_rows(
    tx: &mut SqlTx<'_>,
    tables: &[&str],
    id: i64,
) -> Result<(), StateError> {
    if matches!(&*tx, SqlTx::Postgres(_)) {
        let mut sql = String::from("WITH ");
        for (idx, table) in tables.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "d{idx} AS (DELETE FROM {table} WHERE job_id = {{}})"
            ));
        }
        sql.push_str(" SELECT 1");
        let args = vec![SqlArg::I64(id); tables.len()];
        tx.execute(&sql, &args).await?;
        return Ok(());
    }

    for table in tables {
        tx.execute(
            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
            &[SqlArg::I64(id)],
        )
        .await?;
    }
    Ok(())
}

// Clears what an earlier run of this job id left in the child tables, so a
// job written again under the same id starts with none of it. Its stored NZB
// is written over by the caller.
pub async fn delete_active_job_child_rows_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
) -> Result<(), StateError> {
    delete_job_rows(tx, &ACTIVE_JOB_CHILD_TABLES, job_id.0 as i64).await
}
