// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::persistence::sql_runtime::{
    POSTGRES_BATCH_BIND_LIMIT, SqlArg, SqlEngine, SqlRuntime, SqlTx, StoreDatastore,
    max_rows_for_engine, max_rows_for_tx,
};
use crate::persistence::{Database, DatabaseWriterExecutor};
use sqlx::{Postgres, QueryBuilder, Sqlite};

// A persisted job event.
#[derive(Debug, Clone)]
pub struct JobEvent {
    pub job_id: u64,
    pub timestamp: i64,
    pub kind: String,
    pub message: String,
    pub file_id: Option<String>,
}

// A job event read from durable storage together with its stable row id.
#[derive(Debug, Clone)]
pub struct JobEventRecord {
    pub id: u64,
    pub event: JobEvent,
}

impl std::ops::Deref for JobEventRecord {
    type Target = JobEvent;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

pub const JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER: &str = "__timeline:finalizing-download";

// Per-job (kind, first_ts, last_ts) event bounds keyed by job id.
pub type JobEventStageBounds = std::collections::HashMap<u64, Vec<(String, i64, i64)>>;

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
)]
async fn bulk_insert_job_events_tx(
    tx: &mut SqlTx<'_>,
    events: &[JobEvent],
) -> Result<(), StateError> {
    if events.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 5);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in events.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO job_events (job_id, timestamp, kind, message, file_id) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.job_id as i64)
                        .push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(&event.message)
                        .push_bind(&event.file_id);
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
                    "INSERT INTO job_events (job_id, timestamp, kind, message, file_id) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.job_id as i64)
                        .push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(&event.message)
                        .push_bind(&event.file_id);
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

fn build_job_events_pg_sql(row_count: usize) -> String {
    let mut values = String::new();
    for row in 0..row_count {
        if row > 0 {
            values.push_str(", ");
        }
        if row == 0 {
            values.push_str("({}::bigint, {}::bigint, {}::text, {}::text, {}::text)");
        } else {
            values.push_str("({}, {}, {}, {}, {})");
        }
    }
    format!("INSERT INTO job_events (job_id, timestamp, kind, message, file_id) VALUES {values}")
}

async fn insert_job_events_sql(
    datastore: StoreDatastore,
    events: Vec<JobEvent>,
) -> Result<(), StateError> {
    match datastore.engine() {
        SqlEngine::Sqlite => {
            SqlRuntime::run_in_transaction(&datastore, "insert_job_events", |tx| {
                let events = events.clone();
                Box::pin(async move {
                    bulk_insert_job_events_tx(tx, &events).await?;
                    Ok(())
                })
            })
            .await
        }
        // Timeline rows are append-only diagnostics with no parent guard, so
        // there is nothing a surrounding transaction protects: each chunk is
        // one autocommit statement, and the BEGIN/COMMIT round trips the old
        // wrapper paid per call are gone.
        SqlEngine::Postgres => {
            const BINDS_PER_ROW: usize = 5;
            let chunk_size = (POSTGRES_BATCH_BIND_LIMIT / BINDS_PER_ROW).max(1);
            let started = std::time::Instant::now();
            for chunk in events.chunks(chunk_size) {
                let sql = build_job_events_pg_sql(chunk.len());
                let mut args = Vec::with_capacity(chunk.len() * BINDS_PER_ROW);
                for event in chunk {
                    args.push(SqlArg::I64(event.job_id as i64));
                    args.push(SqlArg::I64(event.timestamp));
                    args.push(SqlArg::Text(event.kind.clone()));
                    args.push(SqlArg::Text(event.message.clone()));
                    args.push(SqlArg::OptText(event.file_id.clone()));
                }
                SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
            }
            crate::runtime::perf_probe::record_sql_op(
                "postgres",
                "insert_job_events",
                started.elapsed(),
            );
            Ok(())
        }
    }
}

impl Database {
    // Insert a single job event.
    pub fn insert_job_event(
        &self,
        job_id: u64,
        timestamp: i64,
        kind: &str,
        message: &str,
        file_id: Option<&str>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let kind = kind.to_string();
        let message = message.to_string();
        let file_id = file_id.map(str::to_string);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "INSERT INTO job_events (job_id, timestamp, kind, message, file_id)
                 VALUES ({}, {}, {}, {}, {})",
                &[
                    SqlArg::I64(job_id as i64),
                    SqlArg::I64(timestamp),
                    SqlArg::Text(kind),
                    SqlArg::Text(message),
                    SqlArg::OptText(file_id),
                ],
            )
            .await?;
            Ok(())
        })
    }

    // Batch-insert job events in a single transaction.
    pub fn insert_job_events(&self, events: &[JobEvent]) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let events = events.to_vec();
        self.run_sql_blocking(insert_job_events_sql(datastore, events))
    }

    // Load all events for a specific job, ordered by insertion order.
    pub fn get_job_events(&self, job_id: u64) -> Result<Vec<JobEvent>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, timestamp, kind, message, file_id
                   FROM job_events
                  WHERE job_id = {}
                  ORDER BY id ASC",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(JobEvent {
                        job_id: row.i64("job_id")? as u64,
                        timestamp: row.i64("timestamp")?,
                        kind: row.text("kind")?,
                        message: row.text("message")?,
                        file_id: row.opt_text("file_id")?,
                    })
                })
                .collect()
        })
    }

    // Load the most recent events for a job, ordered oldest-first, capped at
    // `cap` rows.
    //
    // Unlike [`Database::get_job_events`], which scans a job's entire event log,
    // this reads only the newest `cap` rows (`ORDER BY id DESC LIMIT cap`) and
    // reverses them in Rust so the returned slice is still oldest-first. It is
    // intended for read-only views that re-poll frequently and only render the
    // tail of a potentially huge log (large RAR jobs write several rows per
    // extracted member).
    pub fn get_job_events_latest(
        &self,
        job_id: u64,
        cap: u32,
    ) -> Result<Vec<JobEvent>, StateError> {
        self.get_job_events_window(job_id, cap, 0)
    }

    // Up to `limit` of a job's events, oldest-first, ending `skip_newest`
    // events before its newest one.
    pub fn get_job_events_window(
        &self,
        job_id: u64,
        limit: u32,
        skip_newest: u32,
    ) -> Result<Vec<JobEvent>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, timestamp, kind, message, file_id
                   FROM job_events
                  WHERE job_id = {}
                  ORDER BY id DESC
                  LIMIT {} OFFSET {}",
                &[
                    SqlArg::I64(job_id as i64),
                    SqlArg::I64(i64::from(limit)),
                    SqlArg::I64(i64::from(skip_newest)),
                ],
            )
            .await?;
            // Restore ascending (oldest-first) order after the descending fetch.
            rows.reverse();
            rows.into_iter()
                .map(|row| {
                    Ok(JobEvent {
                        job_id: row.i64("job_id")? as u64,
                        timestamp: row.i64("timestamp")?,
                        kind: row.text("kind")?,
                        message: row.text("message")?,
                        file_id: row.opt_text("file_id")?,
                    })
                })
                .collect()
        })
    }

    // The newest of a job's events of `kinds`, leaving out a `DownloadFinished`
    // that does not carry `download_finished_file_id`.
    pub fn get_last_job_event_of_kinds(
        &self,
        job_id: u64,
        kinds: &[&str],
        download_finished_file_id: &str,
    ) -> Result<Option<JobEvent>, StateError> {
        if kinds.is_empty() {
            return Ok(None);
        }
        let mut args = vec![SqlArg::I64(job_id as i64)];
        args.extend(kinds.iter().map(|kind| SqlArg::Text((*kind).to_string())));
        args.push(SqlArg::Text(download_finished_file_id.to_string()));
        let sql = format!(
            "SELECT job_id, timestamp, kind, message, file_id
               FROM job_events
              WHERE job_id = {{}}
                AND kind IN ({})
                AND (kind <> 'DownloadFinished' OR file_id = {{}})
              ORDER BY id DESC
              LIMIT 1",
            vec!["{}"; kinds.len()].join(", ")
        );
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args).await?;
            row.map(|row| {
                Ok(JobEvent {
                    job_id: row.i64("job_id")? as u64,
                    timestamp: row.i64("timestamp")?,
                    kind: row.text("kind")?,
                    message: row.text("message")?,
                    file_id: row.opt_text("file_id")?,
                })
            })
            .transpose()
        })
    }

    // How many events of each kind a job has, with the id of the newest of
    // each, ordered by that id.
    pub fn get_job_event_kind_counts(
        &self,
        job_id: u64,
    ) -> Result<Vec<(String, u64, u64)>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT kind, COUNT(*) AS count, MAX(id) AS last_id
                   FROM job_events
                  WHERE job_id = {}
                  GROUP BY kind
                  ORDER BY last_id ASC",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    Ok((
                        row.text("kind")?,
                        row.i64("count")? as u64,
                        row.i64("last_id")? as u64,
                    ))
                })
                .collect()
        })
    }

    // Load the most recent events for a job with their stable database ids.
    // Results are returned oldest-first after the capped newest-row query.
    pub fn get_job_event_records_latest(
        &self,
        job_id: u64,
        cap: u32,
    ) -> Result<Vec<JobEventRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, job_id, timestamp, kind, message, file_id
                   FROM job_events
                  WHERE job_id = {}
                  ORDER BY id DESC
                  LIMIT {}",
                &[SqlArg::I64(job_id as i64), SqlArg::I64(i64::from(cap))],
            )
            .await?;
            rows.reverse();
            rows.into_iter()
                .map(|row| {
                    Ok(JobEventRecord {
                        id: row.i64("id")? as u64,
                        event: JobEvent {
                            job_id: row.i64("job_id")? as u64,
                            timestamp: row.i64("timestamp")?,
                            kind: row.text("kind")?,
                            message: row.text("message")?,
                            file_id: row.opt_text("file_id")?,
                        },
                    })
                })
                .collect()
        })
    }

    // Batched min/max event timestamps per (job, kind) for stage-duration
    // reporting. One query regardless of how many jobs are listed; only the
    // stage-boundary kinds in `kinds` are scanned.
    pub fn get_job_event_stage_bounds(
        &self,
        job_ids: &[u64],
        kinds: &[&str],
    ) -> Result<JobEventStageBounds, StateError> {
        if job_ids.is_empty() || kinds.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        // Each job's ids sit in one chunk, so no job's groups are split.
        let mut job_ids = job_ids.to_vec();
        job_ids.sort_unstable();
        job_ids.dedup();
        let kinds: Vec<String> = kinds.iter().map(|kind| kind.to_string()).collect();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let chunk_size = max_rows_for_engine(datastore.engine(), 1)
                .saturating_sub(kinds.len())
                .max(1);
            let kind_placeholders = vec!["{}"; kinds.len()].join(", ");
            let mut bounds: std::collections::HashMap<u64, Vec<(String, i64, i64)>> =
                std::collections::HashMap::new();
            for chunk in job_ids.chunks(chunk_size) {
                let id_placeholders = vec!["{}"; chunk.len()].join(", ");
                let sql = format!(
                    "SELECT job_id, kind, MIN(timestamp) AS first_ts, MAX(timestamp) AS last_ts
                       FROM job_events
                      WHERE job_id IN ({id_placeholders}) AND kind IN ({kind_placeholders})
                      GROUP BY job_id, kind"
                );
                let args: Vec<SqlArg> = chunk
                    .iter()
                    .map(|id| SqlArg::I64(*id as i64))
                    .chain(kinds.iter().cloned().map(SqlArg::Text))
                    .collect();
                for row in SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await? {
                    bounds.entry(row.i64("job_id")? as u64).or_default().push((
                        row.text("kind")?,
                        row.i64("first_ts")?,
                        row.i64("last_ts")?,
                    ));
                }
            }
            Ok(bounds)
        })
    }

    // Batched newest-first event read across multiple jobs in a single
    // query. Floored at the stable database row `min_id` and capped at
    // `limit` rows total (not per job).
    //
    // Unlike [`Database::get_job_events_latest`], the result is returned in
    // the raw `ORDER BY id DESC` order (newest first) and is not reversed.
    pub fn get_recent_job_events_multi(
        &self,
        job_ids: &[u64],
        min_id: u64,
        limit: usize,
    ) -> Result<Vec<JobEventRecord>, StateError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut job_ids = job_ids.to_vec();
        job_ids.sort_unstable();
        job_ids.dedup();
        let min_id = min_id.min(i64::MAX as u64) as i64;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            // Each chunk gives its newest `limit`, and the newest `limit` of
            // those is the newest `limit` over every listed job.
            let chunk_size = max_rows_for_engine(datastore.engine(), 1)
                .saturating_sub(1)
                .max(1);
            let chunks: Vec<&[u64]> = job_ids.chunks(chunk_size).collect();
            let mut events = Vec::new();
            for chunk in &chunks {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let sql = format!(
                    "SELECT id, job_id, timestamp, kind, message, file_id
                       FROM job_events
                      WHERE job_id IN ({placeholders}) AND id >= {{}}
                      ORDER BY id DESC
                      LIMIT {limit}"
                );
                let args: Vec<SqlArg> = chunk
                    .iter()
                    .map(|id| SqlArg::I64(*id as i64))
                    .chain(std::iter::once(SqlArg::I64(min_id)))
                    .collect();
                for row in SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await? {
                    events.push(JobEventRecord {
                        id: row.i64("id")? as u64,
                        event: JobEvent {
                            job_id: row.i64("job_id")? as u64,
                            timestamp: row.i64("timestamp")?,
                            kind: row.text("kind")?,
                            message: row.text("message")?,
                            file_id: row.opt_text("file_id")?,
                        },
                    });
                }
            }
            if chunks.len() > 1 {
                events.sort_by_key(|event| std::cmp::Reverse(event.id));
                events.truncate(limit);
            }
            Ok(events)
        })
    }

    // Delete all events for a job.
    pub fn delete_job_events(&self, job_id: u64) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "DELETE FROM job_events WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            Ok(())
        })
    }

    // Delete all job events across all jobs.
    pub fn delete_all_job_events(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(datastore.write_exec(), "DELETE FROM job_events", &[]).await?;
            Ok(())
        })
    }
}

impl DatabaseWriterExecutor {
    pub(crate) fn insert_job_events(&self, events: &[JobEvent]) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let events = events.to_vec();
        self.run_sql_blocking(insert_job_events_sql(datastore, events))
    }
}

#[cfg(test)]
mod tests;

// Inline (rather than added to `tests.rs`, which is owned by another change in
// flight against this crate): covers the new batched multi-job event read.
#[cfg(test)]
mod event_window_tests {
    use super::*;

    fn kinds(events: &[JobEvent]) -> Vec<String> {
        events.iter().map(|event| event.kind.clone()).collect()
    }

    #[test]
    fn a_window_counts_back_from_the_newest_event() {
        let db = Database::open_in_memory().unwrap();
        for at in 0..10 {
            db.insert_job_event(1, at, &format!("K{at}"), "", None)
                .unwrap();
            db.insert_job_event(2, at, "OTHER", "", None).unwrap();
        }
        let window = db.get_job_events_window(1, 3, 0).unwrap();
        assert_eq!(kinds(&window), ["K7", "K8", "K9"]);
        assert_eq!(
            kinds(&window),
            kinds(&db.get_job_events_latest(1, 3).unwrap())
        );
        let window = db.get_job_events_window(1, 3, 2).unwrap();
        assert_eq!(kinds(&window), ["K5", "K6", "K7"]);
        let window = db.get_job_events_window(1, 5, 8).unwrap();
        assert_eq!(kinds(&window), ["K0", "K1"]);
        assert!(db.get_job_events_window(1, 5, 10).unwrap().is_empty());
    }

    // An id list past one statement's bind budget reads the same rows as a
    // list that fits.
    #[test]
    fn a_long_job_list_reads_what_a_short_one_reads() {
        let db = Database::open_in_memory().unwrap();
        let far = 39_999u64;
        for at in 0..6 {
            db.insert_job_event(3, 100 + at, "JobCreated", &format!("near {at}"), None)
                .unwrap();
            db.insert_job_event(far, 200 + at, "JobCompleted", &format!("far {at}"), None)
                .unwrap();
        }
        let short = [3, far];
        let long: Vec<u64> = (1..=40_000).collect();
        assert!(long.len() > max_rows_for_engine(db.datastore().engine(), 1));

        let ids =
            |events: Vec<JobEventRecord>| events.iter().map(|event| event.id).collect::<Vec<_>>();
        for (min_id, limit) in [(0, 4), (0, 100), (5, 3)] {
            assert_eq!(
                ids(db
                    .get_recent_job_events_multi(&long, min_id, limit)
                    .unwrap()),
                ids(db
                    .get_recent_job_events_multi(&short, min_id, limit)
                    .unwrap()),
                "min_id {min_id}, limit {limit}"
            );
        }

        let kinds = ["JobCreated", "JobCompleted"];
        let sorted = |mut bounds: JobEventStageBounds| {
            bounds.values_mut().for_each(|entries| entries.sort());
            let mut bounds: Vec<_> = bounds.into_iter().collect();
            bounds.sort();
            bounds
        };
        let long_bounds = sorted(db.get_job_event_stage_bounds(&long, &kinds).unwrap());
        assert_eq!(
            long_bounds,
            sorted(db.get_job_event_stage_bounds(&short, &kinds).unwrap())
        );
        assert_eq!(
            long_bounds,
            vec![
                (3, vec![("JobCreated".to_string(), 100, 105)]),
                (far, vec![("JobCompleted".to_string(), 200, 205)]),
            ]
        );
    }

    // Reading only the newest finalization event decides a restore exactly
    // as replaying the whole log does.
    #[test]
    fn the_newest_finalization_event_decides_like_the_whole_log() {
        let marker = Some(JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER);
        let sequences: [&[(&str, Option<&str>)]; 7] = [
            &[],
            &[("JobCreated", None), ("DownloadFinished", marker)],
            &[
                ("DownloadFinished", marker),
                ("DownloadFinished", Some("file-1")),
                ("DownloadFinished", None),
                ("FileComplete", None),
            ],
            &[
                ("DownloadFinished", marker),
                ("DownloadPipelineDrained", None),
            ],
            &[
                ("DownloadFinished", marker),
                ("JobFailed", None),
                ("ExtractionComplete", None),
            ],
            &[
                ("JobCompleted", None),
                ("JobCreated", None),
                ("DownloadFinished", marker),
                ("RepairStarted", None),
            ],
            &[("DownloadFinished", Some("file-2")), ("JobCancelled", None)],
        ];
        let db = Database::open_in_memory().unwrap();
        for (index, sequence) in sequences.iter().enumerate() {
            let job_id = 40 + index as u64;
            for (at, (kind, file_id)) in sequence.iter().enumerate() {
                db.insert_job_event(job_id, at as i64, kind, "", *file_id)
                    .unwrap();
            }
            let whole = crate::Pipeline::download_finalization_open(
                crate::JobId(job_id),
                db.get_job_events(job_id),
            );
            let newest = crate::Pipeline::download_finalization_open(
                crate::JobId(job_id),
                db.get_last_job_event_of_kinds(
                    job_id,
                    crate::Pipeline::DOWNLOAD_FINALIZATION_KINDS,
                    JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER,
                )
                .map(|event| event.into_iter().collect()),
            );
            assert_eq!(newest, whole, "{sequence:?}");
        }
    }
}

#[cfg(test)]
mod get_recent_job_events_multi_tests {
    use super::*;

    #[test]
    fn returns_newest_first_across_jobs_respecting_min_id_and_limit() {
        let db = Database::open_in_memory().unwrap();
        // Row id 1 is below the requested IDFrom floor.
        db.insert_job_event(1, 500, "OLD", "too old", None).unwrap();
        db.insert_job_event(1, 1000, "A", "job1-a", None).unwrap();
        db.insert_job_event(2, 1001, "B", "job2-a", None).unwrap();
        db.insert_job_event(1, 1002, "C", "job1-b", None).unwrap();
        db.insert_job_event(2, 1003, "D", "job2-b", None).unwrap();
        // Unrelated job must not leak into the batch.
        db.insert_job_event(3, 2000, "OTHER", "other job", None)
            .unwrap();

        let events = db.get_recent_job_events_multi(&[1, 2], 2, 10).unwrap();
        // Newest first (ORDER BY id DESC): D, C, B, A.
        let messages: Vec<&str> = events.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, vec!["job2-b", "job1-b", "job2-a", "job1-a"]);
        assert_eq!(
            events.iter().map(|event| event.id).collect::<Vec<_>>(),
            vec![5, 4, 3, 2]
        );

        // `limit` caps the total row count across jobs while staying
        // newest-first.
        let limited = db.get_recent_job_events_multi(&[1, 2], 2, 2).unwrap();
        let limited_messages: Vec<&str> = limited.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(limited_messages, vec!["job2-b", "job1-b"]);

        let latest = db.get_job_event_records_latest(1, 2).unwrap();
        assert_eq!(
            latest.iter().map(|event| event.id).collect::<Vec<_>>(),
            vec![2, 4]
        );
    }

    #[test]
    fn empty_job_ids_returns_empty_without_querying() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_event(1, 1000, "A", "job1-a", None).unwrap();
        assert!(
            db.get_recent_job_events_multi(&[], 0, 10)
                .unwrap()
                .is_empty()
        );
    }
}
