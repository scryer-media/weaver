// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::sql_runtime::{SqlArg, SqlRuntime};
use super::{Database, StateError};
use std::collections::HashSet;
#[cfg(test)]
use weaver_server_db::persistence::maintenance::*;

impl Database {
    // Every job with a record, live, a torrent, or in the history: the jobs
    // whose per-job directories are still theirs.
    pub fn recorded_job_ids(&self) -> Result<HashSet<u64>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id FROM active_jobs UNION SELECT job_id FROM torrents \
                 UNION SELECT job_id FROM job_history",
                &[],
            )
            .await?;
            let mut ids = HashSet::new();
            for row in rows {
                let id = row.i64("job_id")?;
                if let Ok(id) = u64::try_from(id) {
                    ids.insert(id);
                }
            }
            Ok(ids)
        })
    }

    // Which of `job_ids` a queue, torrent or history row records, as
    // `recorded_job_ids` reads them all: one statement per chunk that fits
    // the bind limit, so the read is the size of the ids asked about, never
    // of the whole queue, torrent list and history. A torrent's row counts
    // whether or not the server runs torrents, and while a removal has left
    // it retiring: what carries its job's marker is its own until the row
    // goes.
    pub fn recorded_job_ids_among(&self, job_ids: &[u64]) -> Result<HashSet<u64>, StateError> {
        if job_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let datastore = self.datastore();
        let ids = job_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
        self.run_sql_blocking(async move {
            let mut recorded = HashSet::new();
            // Each id is bound three times: once for each table.
            for chunk in ids.chunks(crate::persistence::sql_runtime::max_rows_for_engine(
                datastore.engine(),
                3,
            )) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk
                    .iter()
                    .chain(chunk)
                    .chain(chunk)
                    .copied()
                    .map(SqlArg::I64)
                    .collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id FROM active_jobs WHERE job_id IN ({placeholders}) \
                         UNION SELECT job_id FROM torrents WHERE job_id IN ({placeholders}) \
                         UNION SELECT job_id FROM job_history WHERE job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    if let Ok(id) = u64::try_from(row.i64("job_id")?) {
                        recorded.insert(id);
                    }
                }
            }
            Ok(recorded)
        })
    }
}

#[cfg(test)]
mod tests;
