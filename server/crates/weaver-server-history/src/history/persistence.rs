// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::history::attributes::{parse_history_metadata, public_history_attributes};
use crate::history::bundles::delete_job_history_bundles_tx;
use crate::history::cache::HistoryCacheQueries;
use crate::history::database::HistoryDatabase;
use crate::history::output_files::OutputFileListing;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use sqlx::{Postgres, QueryBuilder, Sqlite};
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{
    POSTGRES_BATCH_BIND_LIMIT, SqlArg, SqlEngine, SqlRuntime, SqlTx, max_rows_for_tx,
};

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

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
)]
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

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
)]
pub async fn replace_job_history_attributes_tx(
    tx: &mut SqlTx<'_>,
    entry: &JobHistoryRow,
) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM job_history_attributes WHERE job_id = {}",
        &[SqlArg::I64(entry.job_id as i64)],
    )
    .await?;

    let metadata = parse_history_metadata(entry.metadata.as_deref());
    // One row per pair: a statement may not update the same row twice.
    let attributes: Vec<(String, String)> = public_history_attributes(&metadata)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if attributes.is_empty() {
        return Ok(());
    }

    const INSERT: &str = "INSERT INTO job_history_attributes (job_id, key, value, completed_at) ";
    const ON_CONFLICT: &str = " ON CONFLICT(job_id, key, value) DO UPDATE SET
                completed_at = excluded.completed_at";
    let job_id = entry.job_id as i64;
    let completed_at = entry.completed_at;
    let chunk_size = max_rows_for_tx(tx, 4);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in attributes.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(INSERT);
                builder.push_values(chunk, |mut row, (key, value)| {
                    row.push_bind(job_id)
                        .push_bind(key)
                        .push_bind(value)
                        .push_bind(completed_at);
                });
                builder.push(ON_CONFLICT);
                builder
                    .build()
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| StateError::Database(error.to_string()))?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in attributes.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(INSERT);
                builder.push_values(chunk, |mut row, (key, value)| {
                    row.push_bind(job_id)
                        .push_bind(key)
                        .push_bind(value)
                        .push_bind(completed_at);
                });
                builder.push(ON_CONFLICT);
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

// The columns of a history row as `job_history_args` binds them.
const HISTORY_INSERT: &str = "INSERT INTO job_history
    (job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
     optional_recovery_bytes, optional_recovery_downloaded_bytes,
     failed_bytes, health, category, output_dir, nzb_path,
     created_at, completed_at, metadata, server_attribution, protocol,
     release_name, search_key, archived_at)
 VALUES ";
const HISTORY_ROW: &str =
    "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})";
const HISTORY_ARGS: usize = 22;
const HISTORY_UPSERT: &str = " ON CONFLICT(job_id) DO UPDATE SET
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
    created_at = excluded.created_at,
    completed_at = excluded.completed_at,
    metadata = excluded.metadata,
    release_name = excluded.release_name,
    search_key = excluded.search_key,
    archived_at = excluded.archived_at,
    archived_file_count = CASE WHEN excluded.archived_at IS NULL
        THEN NULL ELSE job_history.archived_file_count END,
    archived_file_bytes = CASE WHEN excluded.archived_at IS NULL
        THEN NULL ELSE job_history.archived_file_bytes END,
    -- A rewritten entry is swept again.
    files_swept_at = NULL,
    server_attribution = COALESCE(excluded.server_attribution, job_history.server_attribution),
    protocol = excluded.protocol";

// The job's history row, its attributes and its duplicate identity's move
// to history, written in `tx`.
pub async fn insert_job_history_tx(
    tx: &mut SqlTx<'_>,
    entry: &JobHistoryRow,
) -> Result<(), StateError> {
    tx.execute(
        &format!("{HISTORY_INSERT}{HISTORY_ROW}{HISTORY_UPSERT}"),
        &job_history_args(entry),
    )
    .await?;
    // An entry written with its files keeps no record of
    // removed ones.
    if entry.archived_at.is_none() {
        clear_archived_history_files_tx(tx, entry.job_id as i64).await?;
    }
    replace_job_history_attributes_tx(tx, entry).await?;
    weaver_server_jobs::jobs::duplicate_persistence::transition_duplicate_snapshot_for_history_tx(
        tx,
        JobId(entry.job_id),
        &entry.status,
        entry.error_message.as_deref(),
        None,
        entry.health,
        entry.failed_bytes,
        entry.completed_at,
    )
    .await?;
    Ok(())
}

/// `insert_job_history_tx` of each of `entries`, in order, in few
/// statements: the rows, the archive records they clear, their attributes
/// and their duplicate identities' moves each go in bind-sized chunks. A
/// job written twice is written in order: the batch splits before it.
pub async fn insert_job_histories_tx(
    tx: &mut SqlTx<'_>,
    entries: &[&JobHistoryRow],
) -> Result<(), StateError> {
    for run in unique_runs(entries, |entry| entry.job_id) {
        insert_unique_job_histories_tx(tx, run).await?;
    }
    Ok(())
}

/// `items` split, in order, into runs in which no key appears twice: a
/// batch that writes each run at once writes as one item at a time would.
pub fn unique_runs<T, K: Ord>(items: &[T], key: impl Fn(&T) -> K) -> Vec<&[T]> {
    let mut runs = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut start = 0;
    for (at, item) in items.iter().enumerate() {
        if !seen.insert(key(item)) {
            runs.push(&items[start..at]);
            start = at;
            seen.clear();
            seen.insert(key(item));
        }
    }
    if start < items.len() {
        runs.push(&items[start..]);
    }
    runs
}

async fn insert_unique_job_histories_tx(
    tx: &mut SqlTx<'_>,
    entries: &[&JobHistoryRow],
) -> Result<(), StateError> {
    if entries.is_empty() {
        return Ok(());
    }
    for chunk in entries.chunks(max_rows_for_tx(tx, HISTORY_ARGS)) {
        let rows = vec![HISTORY_ROW; chunk.len()].join(", ");
        let args: Vec<SqlArg> = chunk
            .iter()
            .flat_map(|entry| job_history_args(entry))
            .collect();
        tx.execute(&format!("{HISTORY_INSERT}{rows}{HISTORY_UPSERT}"), &args)
            .await?;
    }
    // An entry written with its files keeps no record of removed ones.
    let cleared: Vec<i64> = entries
        .iter()
        .filter(|entry| entry.archived_at.is_none())
        .map(|entry| entry.job_id as i64)
        .collect();
    delete_by_job_tx(tx, "job_history_archived_files", &cleared).await?;
    let ids: Vec<i64> = entries.iter().map(|entry| entry.job_id as i64).collect();
    delete_by_job_tx(tx, "job_history_attributes", &ids).await?;
    let mut attributes = Vec::new();
    for entry in entries {
        let metadata = parse_history_metadata(entry.metadata.as_deref());
        // One row per pair: a statement may not update the same row twice.
        let pairs: std::collections::BTreeSet<(String, String)> =
            public_history_attributes(&metadata).into_iter().collect();
        attributes.extend(
            pairs
                .into_iter()
                .map(|(key, value)| (entry.job_id as i64, key, value, entry.completed_at)),
        );
    }
    for chunk in attributes.chunks(max_rows_for_tx(tx, 4)) {
        let rows = vec!["({}, {}, {}, {})"; chunk.len()].join(", ");
        let mut args = Vec::with_capacity(chunk.len() * 4);
        for (job_id, key, value, completed_at) in chunk {
            args.push(SqlArg::I64(*job_id));
            args.push(SqlArg::Text(key.clone()));
            args.push(SqlArg::Text(value.clone()));
            args.push(SqlArg::I64(*completed_at));
        }
        tx.execute(
            &format!(
                "INSERT INTO job_history_attributes (job_id, key, value, completed_at) VALUES {rows}
                 ON CONFLICT(job_id, key, value) DO UPDATE SET completed_at = excluded.completed_at"
            ),
            &args,
        )
        .await?;
    }
    let moves: Vec<_> = entries
        .iter()
        .map(
            |entry| weaver_server_jobs::jobs::duplicate_persistence::HistoryTransition {
                job_id: JobId(entry.job_id),
                history_status: &entry.status,
                raw_error: entry.error_message.as_deref(),
                typed_terminal_cause: None,
                health: entry.health,
                failed_bytes: entry.failed_bytes,
                now: entry.completed_at,
            },
        )
        .collect();
    weaver_server_jobs::jobs::duplicate_persistence::transition_duplicate_snapshots_for_history_tx(
        tx, &moves,
    )
    .await?;
    Ok(())
}

// The rows of `table` of each of `job_ids`, gone: one statement per
// bind-sized chunk.
pub async fn delete_by_job_tx(
    tx: &mut SqlTx<'_>,
    table: &str,
    job_ids: &[i64],
) -> Result<u64, StateError> {
    let mut changed = 0;
    for chunk in job_ids.chunks(max_rows_for_tx(tx, 1)) {
        let ids = vec!["{}"; chunk.len()].join(", ");
        let args: Vec<SqlArg> = chunk.iter().copied().map(SqlArg::I64).collect();
        changed += tx
            .execute(
                &format!("DELETE FROM {table} WHERE job_id IN ({ids})"),
                &args,
            )
            .await?;
    }
    Ok(changed)
}

pub trait HistoryPersistenceInsertJobHistoryQueries {
    fn insert_job_history(&self, entry: &JobHistoryRow) -> Result<(), StateError>;
    fn delete_job_history(&self, job_id: u64) -> Result<bool, StateError>;
    fn delete_job_history_and_forget_duplicate_identity(
        &self,
        job_id: u64,
    ) -> Result<bool, StateError>;
    fn delete_job_histories_and_forget_duplicate_identities(
        &self,
        job_ids: &[JobId],
    ) -> Result<Vec<JobId>, StateError>;
    fn insert_integration_events(&self, events: &[IntegrationEventRow]) -> Result<(), StateError>;
    fn delete_all_integration_events(&self) -> Result<(), StateError>;
}

impl<D: HistoryDatabase> HistoryPersistenceInsertJobHistoryQueries for D {
    fn insert_job_history(&self, entry: &JobHistoryRow) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let cache_entry = entry.clone();
        // Capture the cache generation before the write so a delete racing this
        // insert (invalidating after its commit) makes the re-cache a no-op.
        let observed_generation = self.job_history_cache_generation();
        let entry_in_tx = entry.clone();
        let result = self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_job_history", |tx| {
                let entry = entry_in_tx.clone();
                Box::pin(async move { insert_job_history_tx(tx, &entry).await })
            })
            .await
        });
        if result.is_ok() {
            self.cache_job_history_at(cache_entry, observed_generation);
        }
        result
    }

    fn delete_job_history(&self, job_id: u64) -> Result<bool, StateError> {
        self.delete_job_history_bundles(&[JobId(job_id)], false)
            .map(|deleted| !deleted.is_empty())
    }

    // Permanently removes visible history and the duplicate identity it owns.
    // Unlike scheduler-internal history cleanup, this records a tombstone so a
    // stale backfill cannot restore the identity after the delete commits.
    fn delete_job_history_and_forget_duplicate_identity(
        &self,
        job_id: u64,
    ) -> Result<bool, StateError> {
        self.delete_job_histories_and_forget_duplicate_identities(&[JobId(job_id)])
            .map(|deleted| !deleted.is_empty())
    }

    // The batched form of `delete_job_history_and_forget_duplicate_identity`:
    // every id's history bundle and duplicate identity go in one transaction.
    // Returns the ids that had a history row; an id without one is skipped.
    fn delete_job_histories_and_forget_duplicate_identities(
        &self,
        job_ids: &[JobId],
    ) -> Result<Vec<JobId>, StateError> {
        self.delete_job_history_bundles(job_ids, true)
    }

    fn insert_integration_events(&self, events: &[IntegrationEventRow]) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.borrow().datastore();
        let events = events.to_vec();
        self.borrow().run_sql_blocking(async move {
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
                        SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
                    }
                    weaver_server_base::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "insert_integration_events",
                        started.elapsed(),
                    );
                    Ok(())
                }
            }
        })
    }

    fn delete_all_integration_events(&self) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "DELETE FROM integration_events",
                &[],
            )
            .await?;
            Ok(())
        })
    }
}

trait HistoryPersistenceDeleteJobHistoryBundlesLocalQueries {
    fn delete_job_history_bundles(
        &self,
        job_ids: &[JobId],
        forget_duplicate_identities: bool,
    ) -> Result<Vec<JobId>, StateError>;
}

impl<D: HistoryDatabase> HistoryPersistenceDeleteJobHistoryBundlesLocalQueries for D {
    fn delete_job_history_bundles(
        &self,
        job_ids: &[JobId],
        forget_duplicate_identities: bool,
    ) -> Result<Vec<JobId>, StateError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let _tags = self.lock_tag_writer();
        let datastore = self.borrow().datastore();
        // Deduped so no chunk or tombstone insert names an id twice.
        let job_ids = job_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let result = self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_job_history_bundles", |tx| {
                let job_ids = job_ids.clone();
                Box::pin(async move {
                    let deleted = delete_job_history_bundles_tx(tx, &job_ids).await?;
                    // A job deleted from history takes its tags with it. A
                    // restart keeps them: it reuses the bundle delete above,
                    // not this.
                    let ids = deleted.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
                    weaver_server_settings::tags::delete_job_tags_tx(tx, &ids).await?;
                    if forget_duplicate_identities {
                        weaver_server_jobs::jobs::duplicate_persistence::forget_duplicate_identities_for_history_delete_tx(
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
            self.forget_job_script_effects(&ids);
            self.invalidate_job_history_cache_for(&ids);
            self.job_history_bundles_deleted(&ids);
        }
        result
    }
}

// Deletes the history bundle of every id that has a history row, and
// returns those ids. Each chunk runs one `IN (...)` statement per table; the
// `script_output_state` lock is taken once, first, in the same order as
// insertion takes it.
pub trait HistoryPersistenceRecordArchivedHistoryFilesInternalQueries {
    fn record_archived_history_files(
        &self,
        records: &[(JobId, OutputFileListing)],
    ) -> Result<(), StateError>;
    fn stamp_history_archived(&self, job_ids: &[JobId], archived_at: i64)
    -> Result<(), StateError>;
    fn stamp_history_swept(
        &self,
        job_ids: &[JobId],
        swept_at: i64,
        finished_by: i64,
    ) -> Result<(), StateError>;
}

impl<D: HistoryDatabase> HistoryPersistenceRecordArchivedHistoryFilesInternalQueries for D {
    // Keeps the record of what each entry's output folder held, read before
    // its archive removes it: the totals and the first files, in one
    // transaction. Only an entry with neither a stamp nor a record takes
    // one, so a retry after an archive that failed part way keeps the record
    // of the whole folder.
    // The record is shown only once the entry is stamped.
    fn record_archived_history_files(
        &self,
        records: &[(JobId, OutputFileListing)],
    ) -> Result<(), StateError> {
        if records.is_empty() {
            return Ok(());
        }
        let records = records
            .iter()
            .map(|(job_id, listing)| {
                i64::try_from(job_id.0)
                    .map(|id| (id, listing.clone()))
                    .map_err(|_| StateError::Database("job id is too large".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let job_ids = records.iter().map(|(id, _)| *id as u64).collect::<Vec<_>>();
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "record_archived_history_files", |tx| {
                let records = records.clone();
                Box::pin(async move { record_archived_history_files_tx(tx, &records).await })
            })
            .await
        })?;
        self.invalidate_job_history_cache_for(&job_ids);
        Ok(())
    }

    // Says that each entry was archived at `archived_at`, epoch seconds, in
    // one transaction. An entry that already says so keeps its first time;
    // the record kept before the archive is left as it is.
    fn stamp_history_archived(
        &self,
        job_ids: &[JobId],
        archived_at: i64,
    ) -> Result<(), StateError> {
        if job_ids.is_empty() {
            return Ok(());
        }
        let ids = job_ids
            .iter()
            .map(|job_id| {
                i64::try_from(job_id.0)
                    .map_err(|_| StateError::Database("job id is too large".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "stamp_history_archived", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    for chunk in ids.chunks(max_rows_for_tx(tx, 1).saturating_sub(2).max(1)) {
                        let placeholders = vec!["{}"; chunk.len()].join(", ");
                        let mut args = Vec::with_capacity(chunk.len() + 1);
                        args.push(SqlArg::I64(archived_at));
                        args.extend(chunk.iter().map(|id| SqlArg::I64(*id)));
                        tx.execute(
                            &format!(
                                "UPDATE job_history SET archived_at = {{}}
                                  WHERE job_id IN ({placeholders})
                                    AND archived_at IS NULL"
                            ),
                            &args,
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })?;
        let job_ids = job_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        self.invalidate_job_history_cache_for(&job_ids);
        Ok(())
    }

    // Says that housekeeping swept each failed entry at `swept_at`, epoch
    // seconds, and has nothing more to do for it, in one transaction.
    // Internal: no view of the entry reads it, so no cached entry changes. An
    // entry already stamped keeps its first time, and one that is no longer
    // the failure the sweep listed, as after a retry, is not stamped.
    fn stamp_history_swept(
        &self,
        job_ids: &[JobId],
        swept_at: i64,
        finished_by: i64,
    ) -> Result<(), StateError> {
        if job_ids.is_empty() {
            return Ok(());
        }
        let ids = job_ids
            .iter()
            .map(|job_id| {
                i64::try_from(job_id.0)
                    .map_err(|_| StateError::Database("job id is too large".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "stamp_history_swept", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    for chunk in ids.chunks(max_rows_for_tx(tx, 1).saturating_sub(2).max(1)) {
                        let placeholders = vec!["{}"; chunk.len()].join(", ");
                        let mut args = Vec::with_capacity(chunk.len() + 2);
                        args.push(SqlArg::I64(swept_at));
                        args.push(SqlArg::I64(finished_by));
                        args.extend(chunk.iter().map(|id| SqlArg::I64(*id)));
                        tx.execute(
                            &format!(
                                "UPDATE job_history SET files_swept_at = {{}}
                                  WHERE status = 'failed' AND files_swept_at IS NULL
                                    AND completed_at <= {{}}
                                    AND job_id IN ({placeholders})"
                            ),
                            &args,
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }
}

// The totals and the kept files of every entry with neither a stamp nor a
// record: one update per chunk of entries, then one insert per chunk of
// files, only for the entries the update took.
async fn record_archived_history_files_tx(
    tx: &mut SqlTx<'_>,
    records: &[(i64, OutputFileListing)],
) -> Result<(), StateError> {
    let mut recorded = std::collections::BTreeSet::new();
    for chunk in records.chunks(max_rows_for_tx(tx, 3).max(1)) {
        let values =
            vec!["(CAST({} AS BIGINT), CAST({} AS BIGINT), CAST({} AS BIGINT))"; chunk.len()]
                .join(", ");
        let mut args = Vec::with_capacity(chunk.len() * 3);
        for (job_id, listing) in chunk {
            args.push(SqlArg::I64(*job_id));
            args.push(SqlArg::I64(
                i64::try_from(listing.total_count).unwrap_or(i64::MAX),
            ));
            args.push(SqlArg::I64(
                i64::try_from(listing.total_bytes).unwrap_or(i64::MAX),
            ));
        }
        let rows = tx
            .fetch_all(
                &format!(
                    "WITH archived (archived_id, file_count, file_bytes) AS (VALUES {values})
                     UPDATE job_history
                        SET archived_file_count = archived.file_count,
                            archived_file_bytes = archived.file_bytes
                       FROM archived
                      WHERE job_history.job_id = archived.archived_id
                        AND job_history.archived_at IS NULL
                        AND job_history.archived_file_count IS NULL
                  RETURNING job_id"
                ),
                &args,
            )
            .await?;
        for row in rows {
            recorded.insert(row.i64("job_id")?);
        }
    }

    let files = records
        .iter()
        .filter(|(job_id, _)| recorded.contains(job_id))
        .flat_map(|(job_id, listing)| {
            listing
                .files
                .iter()
                .enumerate()
                .map(move |(ordinal, file)| (*job_id, ordinal as i64, file))
        })
        .collect::<Vec<_>>();
    for chunk in files.chunks(max_rows_for_tx(tx, 4)) {
        let values = vec!["({}, {}, {}, {})"; chunk.len()].join(", ");
        let mut args = Vec::with_capacity(chunk.len() * 4);
        for (job_id, ordinal, file) in chunk {
            args.push(SqlArg::I64(*job_id));
            args.push(SqlArg::I64(*ordinal));
            args.push(SqlArg::Text(file.rel_path.clone()));
            args.push(SqlArg::I64(
                i64::try_from(file.size_bytes).unwrap_or(i64::MAX),
            ));
        }
        tx.execute(
            &format!(
                "INSERT INTO job_history_archived_files (job_id, ordinal, rel_path, size_bytes)
                 VALUES {values}"
            ),
            &args,
        )
        .await?;
    }
    Ok(())
}

// Forgets the archive record of `job_id`: its kept files and its totals. A
// job that finishes again has its files again.
pub async fn clear_archived_history_files_tx(
    tx: &mut SqlTx<'_>,
    job_id: i64,
) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM job_history_archived_files WHERE job_id = {}",
        &[SqlArg::I64(job_id)],
    )
    .await?;
    Ok(())
}

fn job_history_args(entry: &JobHistoryRow) -> Vec<SqlArg> {
    let (release_name, search_key) =
        crate::history::attributes::history_search_columns(&entry.name, entry.metadata.as_deref());
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
        SqlArg::I64(entry.created_at),
        SqlArg::I64(entry.completed_at),
        SqlArg::OptText(entry.metadata.clone()),
        SqlArg::OptText(entry.server_attribution.clone()),
        SqlArg::Text(entry.protocol.as_str().to_string()),
        SqlArg::Text(release_name),
        SqlArg::Text(search_key),
        SqlArg::OptI64(entry.archived_at),
    ]
}
