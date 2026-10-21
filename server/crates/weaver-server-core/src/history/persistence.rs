// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::history::{parse_history_metadata, public_history_attributes};
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{
    POSTGRES_BATCH_BIND_LIMIT, SqlArg, SqlEngine, SqlRuntime, SqlTx, max_rows_for_tx,
};
use sqlx::{Postgres, QueryBuilder, Sqlite};

fn build_integration_events_pg_sql(row_count: usize) -> String {
    let mut values = String::new();
    for row in 0..row_count {
        if row > 0 {
            values.push_str(", ");
        }
        if row == 0 {
            values.push_str("({}::bigint, {}::text, {}::bigint, {}::text)");
        } else {
            values.push_str("({}, {}, {}, {})");
        }
    }
    format!(
        "INSERT INTO integration_events (timestamp, kind, item_id, payload_json) VALUES {values}"
    )
}

async fn bulk_insert_integration_events_tx(
    tx: &mut SqlTx<'_>,
    events: &[IntegrationEventRow],
) -> Result<(), StateError> {
    if events.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 4);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in events.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO integration_events (timestamp, kind, item_id, payload_json) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(event.item_id.map(|value| value as i64))
                        .push_bind(&event.payload_json);
                });
                builder
                    .build()
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| StateError::Database(error.to_string()))?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in events.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO integration_events (timestamp, kind, item_id, payload_json) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(event.item_id.map(|value| value as i64))
                        .push_bind(&event.payload_json);
                });
                builder
                    .build()
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| StateError::Database(error.to_string()))?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn replace_job_history_attributes_tx(
    tx: &mut SqlTx<'_>,
    entry: &JobHistoryRow,
) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM job_history_attributes WHERE job_id = {}",
        &[SqlArg::I64(entry.job_id as i64)],
    )
    .await?;

    let metadata = parse_history_metadata(entry.metadata.as_deref());
    for (key, value) in public_history_attributes(&metadata) {
        tx.execute(
            "INSERT INTO job_history_attributes (job_id, key, value, completed_at)
             VALUES ({}, {}, {}, {})
             ON CONFLICT(job_id, key, value) DO UPDATE SET
                completed_at = excluded.completed_at",
            &[
                SqlArg::I64(entry.job_id as i64),
                SqlArg::Text(key),
                SqlArg::Text(value),
                SqlArg::I64(entry.completed_at),
            ],
        )
        .await?;
    }

    Ok(())
}

impl Database {
    pub fn insert_job_history(&self, entry: &JobHistoryRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let cache_entry = entry.clone();
        // Capture the cache generation before the write so a delete racing this
        // insert (invalidating after its commit) makes the re-cache a no-op.
        let observed_generation = self.job_history_cache_generation();
        let args = job_history_args(entry);
        let attribute_entry = entry.clone();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_job_history", |tx| {
                let args = args.clone();
                let attribute_entry = attribute_entry.clone();
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO job_history
                            (job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                             optional_recovery_bytes, optional_recovery_downloaded_bytes,
                             failed_bytes, health, category, output_dir, nzb_path, nzb_zstd,
                             created_at, completed_at, metadata, server_attribution)
                         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                         ON CONFLICT(job_id) DO UPDATE SET
                            job_hash = excluded.job_hash,
                            name = excluded.name,
                            status = excluded.status,
                            error_message = excluded.error_message,
                            total_bytes = excluded.total_bytes,
                            downloaded_bytes = excluded.downloaded_bytes,
                            optional_recovery_bytes = excluded.optional_recovery_bytes,
                            optional_recovery_downloaded_bytes = excluded.optional_recovery_downloaded_bytes,
                            failed_bytes = excluded.failed_bytes,
                            health = excluded.health,
                            category = excluded.category,
                            output_dir = excluded.output_dir,
                            nzb_path = excluded.nzb_path,
                            nzb_zstd = COALESCE(excluded.nzb_zstd, job_history.nzb_zstd),
                            created_at = excluded.created_at,
                            completed_at = excluded.completed_at,
                            metadata = excluded.metadata,
                            server_attribution = COALESCE(excluded.server_attribution, job_history.server_attribution)",
                        &args,
                    )
                    .await?;
                    replace_job_history_attributes_tx(tx, &attribute_entry).await?;
                    crate::jobs::duplicate_persistence::transition_duplicate_snapshot_for_history_tx(
                        tx,
                        crate::JobId(attribute_entry.job_id),
                        &attribute_entry.status,
                        attribute_entry.error_message.as_deref(),
                        None,
                        attribute_entry.health,
                        attribute_entry.failed_bytes,
                        attribute_entry.completed_at,
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        });
        if result.is_ok() {
            self.cache_job_history_at(cache_entry, observed_generation);
        }
        result
    }

    pub fn delete_job_history(&self, job_id: u64) -> Result<bool, StateError> {
        self.delete_job_history_bundles(&[JobId(job_id)], false)
            .map(|deleted| !deleted.is_empty())
    }

    // Permanently removes visible history and the duplicate identity it owns.
    // Unlike scheduler-internal history cleanup, this records a tombstone so a
    // stale backfill cannot restore the identity after the delete commits.
    pub fn delete_job_history_and_forget_duplicate_identity(
        &self,
        job_id: u64,
    ) -> Result<bool, StateError> {
        self.delete_job_histories_and_forget_duplicate_identities(&[JobId(job_id)])
            .map(|deleted| !deleted.is_empty())
    }

    // The batched form of `delete_job_history_and_forget_duplicate_identity`:
    // every id's history bundle and duplicate identity go in one transaction.
    // Returns the ids that had a history row; an id without one is skipped.
    pub fn delete_job_histories_and_forget_duplicate_identities(
        &self,
        job_ids: &[JobId],
    ) -> Result<Vec<JobId>, StateError> {
        self.delete_job_history_bundles(job_ids, true)
    }

    fn delete_job_history_bundles(
        &self,
        job_ids: &[JobId],
        forget_duplicate_identities: bool,
    ) -> Result<Vec<JobId>, StateError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let datastore = self.datastore();
        // Deduped so no chunk or tombstone insert names an id twice.
        let job_ids = job_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_job_history_bundles", |tx| {
                let job_ids = job_ids.clone();
                Box::pin(async move {
                    let deleted = delete_job_history_bundles_tx(tx, &job_ids).await?;
                    if forget_duplicate_identities {
                        crate::jobs::duplicate_persistence::forget_duplicate_identities_for_history_delete_tx(
                            tx, &deleted,
                        )
                        .await?;
                    }
                    Ok(deleted)
                })
            })
            .await
        });
        if let Ok(deleted) = &result
            && !deleted.is_empty()
        {
            let ids = deleted.iter().map(|id| id.0).collect::<Vec<_>>();
            self.forget_script_effects_for(&ids);
            self.invalidate_job_history_cache_for(&ids);
            self.notify_script_events_changed();
        }
        result
    }

    pub fn insert_integration_events(
        &self,
        events: &[IntegrationEventRow],
    ) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let events = events.to_vec();
        self.run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "insert_integration_events", |tx| {
                        let events = events.clone();
                        Box::pin(async move {
                            bulk_insert_integration_events_tx(tx, &events).await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Integration events are append-only rows with no parent guard,
                // so each chunk rides one autocommit statement instead of
                // paying a BEGIN/COMMIT pair around it.
                SqlEngine::Postgres => {
                    const BINDS_PER_ROW: usize = 4;
                    let chunk_size = (POSTGRES_BATCH_BIND_LIMIT / BINDS_PER_ROW).max(1);
                    let started = std::time::Instant::now();
                    for chunk in events.chunks(chunk_size) {
                        let sql = build_integration_events_pg_sql(chunk.len());
                        let mut args = Vec::with_capacity(chunk.len() * BINDS_PER_ROW);
                        for event in chunk {
                            args.push(SqlArg::I64(event.timestamp));
                            args.push(SqlArg::Text(event.kind.clone()));
                            args.push(SqlArg::OptI64(event.item_id.map(|value| value as i64)));
                            args.push(SqlArg::Text(event.payload_json.clone()));
                        }
                        SqlRuntime::execute(datastore.read_exec(), &sql, &args).await?;
                    }
                    crate::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "insert_integration_events",
                        started.elapsed(),
                    );
                    Ok(())
                }
            }
        })
    }

    pub fn delete_all_integration_events(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(datastore.read_exec(), "DELETE FROM integration_events", &[])
                .await?;
            Ok(())
        })
    }
}

// Deletes the history bundle of every id that has a history row, and
// returns those ids. Each chunk runs one `IN (...)` statement per table; the
// `script_output_state` lock is taken once, first, in the same order as
// insertion takes it.
async fn delete_job_history_bundles_tx(
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
        for table in ["job_events", "job_history_attributes"] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE job_id IN ({placeholders})"),
                &args,
            )
            .await?;
        }
        crate::post_processing::output::delete_script_state_rows_tx(tx, &present).await?;
        tx.execute(
            &format!("DELETE FROM job_history WHERE job_id IN ({placeholders})"),
            &args,
        )
        .await?;
        deleted.extend(present.into_iter().map(|id| JobId(id as u64)));
    }
    Ok(deleted)
}

fn job_history_args(entry: &JobHistoryRow) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(entry.job_id as i64),
        SqlArg::OptBytes(entry.job_hash.clone()),
        SqlArg::Text(entry.name.clone()),
        SqlArg::Text(entry.status.clone()),
        SqlArg::OptText(entry.error_message.clone()),
        SqlArg::I64(entry.total_bytes as i64),
        SqlArg::I64(entry.downloaded_bytes as i64),
        SqlArg::I64(entry.optional_recovery_bytes as i64),
        SqlArg::I64(entry.optional_recovery_downloaded_bytes as i64),
        SqlArg::I64(entry.failed_bytes as i64),
        SqlArg::I64(i64::from(entry.health)),
        SqlArg::OptText(entry.category.clone()),
        SqlArg::OptText(entry.output_dir.clone()),
        SqlArg::OptText(entry.nzb_path.clone()),
        SqlArg::OptBytes(None),
        SqlArg::I64(entry.created_at),
        SqlArg::I64(entry.completed_at),
        SqlArg::OptText(entry.metadata.clone()),
        SqlArg::OptText(entry.server_attribution.clone()),
    ]
}
