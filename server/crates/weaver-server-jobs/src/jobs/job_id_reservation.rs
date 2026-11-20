// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The next job id, reserved in the settings table: above every id any job
// table holds and never below 10,000.

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlTx};

pub const NEXT_JOB_ID_SETTING_KEY: &str = "next_job_id";

async fn max_seen_job_id_floor_tx(tx: &mut SqlTx<'_>) -> Result<u64, StateError> {
    let row = tx
        .fetch_optional(
            "SELECT MAX(id) AS id FROM (
                 SELECT MAX(job_id) AS id FROM active_jobs
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM job_history
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM duplicate_job_snapshots
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM forgotten_duplicate_identities
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM job_tags
             ) ids",
            &[],
        )
        .await?;
    let max_seen = row
        .map(|row| row.opt_i64("id"))
        .transpose()?
        .flatten()
        .unwrap_or(0);

    Ok((max_seen as u64 + 1).max(10_000))
}

pub async fn reserve_next_job_id_tx(tx: &mut SqlTx<'_>) -> Result<u64, StateError> {
    let floor = max_seen_job_id_floor_tx(tx).await?;
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {})
         ON CONFLICT(key) DO NOTHING",
        &[
            SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
            SqlArg::Text(floor.to_string()),
        ],
    )
    .await?;

    let select_sql = match tx {
        SqlTx::Postgres(_) => "SELECT value FROM settings WHERE key = {} FOR UPDATE",
        SqlTx::Sqlite(_) => "SELECT value FROM settings WHERE key = {}",
    };
    let persisted = tx
        .fetch_optional(
            select_sql,
            &[SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string())],
        )
        .await?
        .and_then(|row| row.text("value").ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(floor);
    let next_job_id = persisted.max(floor).max(10_000);
    tx.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[
            SqlArg::Text((next_job_id + 1).to_string()),
            SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
        ],
    )
    .await?;

    Ok(next_job_id)
}
