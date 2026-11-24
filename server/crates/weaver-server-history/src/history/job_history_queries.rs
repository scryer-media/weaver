// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::cache::HistoryCacheQueries;
use super::database::HistoryDatabase;
use std::path::PathBuf;

use super::model::{HistoryEditFields, HistoryFilter, HistoryOrder, HistoryOrderKey};
use super::record::{IntegrationEventRow, JobHistoryRow};
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime, max_rows_for_engine};

pub use super::queries::HistoryFileState;
use super::queries::{
    HistoryIdSplit, append_history_from_clause, append_history_pagination,
    append_history_row_filter_predicates, archived_history_files_sql, count_history_rows,
    count_history_rows_by_category, count_history_rows_by_protocol, history_order_sql,
    split_history_ids,
};

pub use super::retention::HistoryInternalQueries;

type PersistedNzbRecord = (PathBuf, Option<Vec<u8>>);

const JOB_HISTORY_SELECT: &str =
    "SELECT job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
        optional_recovery_bytes, optional_recovery_downloaded_bytes,
        failed_bytes, health, category, output_dir, nzb_path,
        created_at, completed_at, metadata, server_attribution, protocol, archived_at
   FROM job_history";

const JOB_HISTORY_SELECT_ALIASED: &str =
    "SELECT h.job_id, h.job_hash, h.name, h.status, h.error_message, h.total_bytes, h.downloaded_bytes,
        h.optional_recovery_bytes, h.optional_recovery_downloaded_bytes,
        h.failed_bytes, h.health, h.category, h.output_dir, h.nzb_path,
        h.created_at, h.completed_at, h.metadata, h.server_attribution, h.protocol,
        h.archived_at
   FROM ";

// The column lists every history read decodes, for a test that keeps the
// download summary out of them.
#[cfg(any(test, feature = "test-support"))]
pub const JOB_HISTORY_SELECTS: [&str; 2] = [JOB_HISTORY_SELECT, JOB_HISTORY_SELECT_ALIASED];

pub trait HistoryQueries {
    fn get_job_history(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError>;
    fn get_job_history_profiled(
        &self,
        job_id: u64,
        label: &'static str,
    ) -> Result<Option<JobHistoryRow>, StateError>;
    fn list_job_history_output_dirs(
        &self,
        job_ids: &[weaver_server_base::jobs::ids::JobId],
    ) -> Result<
        std::collections::BTreeMap<weaver_server_base::jobs::ids::JobId, Option<String>>,
        StateError,
    >;
    fn shared_output_dirs(
        &self,
        dirs: &[String],
    ) -> Result<std::collections::BTreeSet<String>, StateError>;
    fn list_job_history_file_states(
        &self,
        job_ids: &[weaver_server_base::jobs::ids::JobId],
    ) -> Result<
        std::collections::BTreeMap<weaver_server_base::jobs::ids::JobId, HistoryFileState>,
        StateError,
    >;
    fn list_archived_history_files(
        &self,
        job_id: u64,
        from: u64,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, StateError>;
    fn has_archived_history_file(&self, job_id: u64, ordinal: u64) -> Result<bool, StateError>;
    fn list_job_history(&self, filter: &HistoryFilter) -> Result<Vec<JobHistoryRow>, StateError>;
    fn count_job_history(&self, filter: &HistoryFilter) -> Result<u32, StateError>;
    fn count_job_history_by_category(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<(String, u32)>, StateError>;
    fn count_job_history_by_protocol(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<(weaver_server_jobs::jobs::model::Protocol, u32)>, StateError>;
    fn job_history_ids_among(&self, job_ids: &[u64]) -> Result<Vec<u64>, StateError>;
    fn job_history_edit_fields(&self, job_id: u64)
    -> Result<Option<HistoryEditFields>, StateError>;
    fn list_integration_events_after(
        &self,
        after_id: Option<i64>,
        item_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<Vec<IntegrationEventRow>, StateError>;
    fn latest_integration_event_id(&self) -> Result<Option<i64>, StateError>;
    fn max_job_id(&self) -> Result<u64, StateError>;
    fn load_history_job_persisted_nzb(
        &self,
        job_id: u64,
    ) -> Result<Option<PersistedNzbRecord>, StateError>;
}

impl<D: HistoryDatabase> HistoryQueries for D {
    fn get_job_history(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError> {
        self.get_job_history_inner(job_id)
    }

    fn get_job_history_profiled(
        &self,
        job_id: u64,
        label: &'static str,
    ) -> Result<Option<JobHistoryRow>, StateError> {
        let _probe = weaver_server_base::runtime::perf_probe::scope(label);
        self.get_job_history_inner(job_id)
    }

    // The recorded output directory of every id that has a history row, read
    // in bind-sized chunks. An id without a row is absent from the map.
    fn list_job_history_output_dirs(
        &self,
        job_ids: &[weaver_server_base::jobs::ids::JobId],
    ) -> Result<
        std::collections::BTreeMap<weaver_server_base::jobs::ids::JobId, Option<String>>,
        StateError,
    > {
        if job_ids.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.borrow().datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        self.borrow().run_sql_blocking_read(async move {
            let mut dirs = std::collections::BTreeMap::new();
            for chunk in ids.chunks(max_rows_for_engine(datastore.engine(), 1)) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, output_dir FROM job_history WHERE job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    dirs.insert(
                        weaver_server_base::jobs::ids::JobId(row.i64("job_id")? as u64),
                        row.opt_text("output_dir")?,
                    );
                }
            }
            Ok(dirs)
        })
    }

    // Which of `dirs` more than one job records as its output folder, across
    // history and the queue: one statement per bind-sized chunk, served by
    // the output-folder indexes.
    fn shared_output_dirs(
        &self,
        dirs: &[String],
    ) -> Result<std::collections::BTreeSet<String>, StateError> {
        if dirs.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.borrow().datastore();
        let dirs = dirs
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        self.borrow().run_sql_blocking_read(async move {
            let mut shared = std::collections::BTreeSet::new();
            for chunk in dirs.chunks(max_rows_for_engine(datastore.engine(), 2)) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk
                    .iter()
                    .chain(chunk.iter())
                    .cloned()
                    .map(SqlArg::Text)
                    .collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT output_dir FROM (
                             SELECT output_dir, job_id FROM job_history
                              WHERE output_dir IN ({placeholders})
                             UNION ALL
                             SELECT output_dir, job_id FROM active_jobs
                              WHERE output_dir IN ({placeholders})
                         ) recorded
                         GROUP BY output_dir
                         HAVING COUNT(DISTINCT job_id) > 1"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    shared.insert(row.text("output_dir")?);
                }
            }
            Ok(shared)
        })
    }

    // What an archive reads of each id that has a history row: the
    // recorded output directory, when it was archived and the totals of the
    // folder its archive removed, read in bind-sized chunks. An id without a row
    // is absent from the map.
    fn list_job_history_file_states(
        &self,
        job_ids: &[weaver_server_base::jobs::ids::JobId],
    ) -> Result<
        std::collections::BTreeMap<weaver_server_base::jobs::ids::JobId, HistoryFileState>,
        StateError,
    > {
        if job_ids.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.borrow().datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        self.borrow().run_sql_blocking_read(async move {
            let mut states = std::collections::BTreeMap::new();
            for chunk in ids.chunks(max_rows_for_engine(datastore.engine(), 1)) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, output_dir, archived_at, archived_file_count,
                                archived_file_bytes
                           FROM job_history
                          WHERE job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    states.insert(
                        weaver_server_base::jobs::ids::JobId(row.i64("job_id")? as u64),
                        HistoryFileState {
                            output_dir: row.opt_text("output_dir")?,
                            archived_at: row.opt_i64("archived_at")?,
                            archived_file_count: row
                                .opt_i64("archived_file_count")?
                                .map(|count| count.max(0) as u64),
                            archived_file_bytes: row
                                .opt_i64("archived_file_bytes")?
                                .map(|bytes| bytes.max(0) as u64),
                        },
                    );
                }
            }
            Ok(states)
        })
    }

    // One page of the files a history entry recorded when its files were
    // removed, from position `from` on, in listing order, as
    // `(relative path, bytes)`. A range of the primary key.
    fn list_archived_history_files(
        &self,
        job_id: u64,
        from: u64,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, StateError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            let (sql, args) = archived_history_files_sql(job_id, from, limit);
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter()
                .map(|row| Ok((row.text("rel_path")?, row.i64("size_bytes")?.max(0) as u64)))
                .collect()
        })
    }

    // Whether the record of an archive holds a file at position
    // `ordinal`: the record keeps fewer files than the totals count once the
    // folder was larger than the cap. One primary-key lookup.
    fn has_archived_history_file(&self, job_id: u64, ordinal: u64) -> Result<bool, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 AS present FROM job_history_archived_files
                  WHERE job_id = {} AND ordinal = {}",
                &[SqlArg::I64(job_id as i64), SqlArg::I64(ordinal as i64)],
            )
            .await?;
            Ok(row.is_some())
        })
    }

    // The visibility predicate is applied in SQL, before pagination.
    fn list_job_history(&self, filter: &HistoryFilter) -> Result<Vec<JobHistoryRow>, StateError> {
        let datastore = self.borrow().datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.borrow().run_sql_blocking_read(async move {
            let (offset, limit) = (
                base.offset.unwrap_or(0) as usize,
                base.limit.map(|limit| limit as usize),
            );
            // A split list is read whole up to the page's end in each chunk,
            // and the page is cut from the merged rows in the same order.
            let through = |extra: usize| HistoryFilter {
                limit: limit.map(|limit| (limit + offset + extra).min(u32::MAX as usize) as u32),
                offset: None,
                ..base.clone()
            };
            let mut page = match &ids {
                HistoryIdSplit::Whole => {
                    return fetch_history_rows(&datastore, &base).await;
                }
                HistoryIdSplit::In(chunks) => {
                    let mut page = Vec::new();
                    for chunk in chunks {
                        let filter = HistoryFilter {
                            item_ids: Some(chunk.clone()),
                            ..through(0)
                        };
                        page.extend(fetch_history_rows(&datastore, &filter).await?);
                    }
                    sort_history_rows(base.order, &mut page);
                    page
                }
                HistoryIdSplit::NotIn(excluded, _) => {
                    let mut page = fetch_history_rows(&datastore, &through(excluded.len())).await?;
                    page.retain(|row| excluded.binary_search(&row.job_id).is_err());
                    page
                }
            };
            page.drain(..offset.min(page.len()));
            if let Some(limit) = limit {
                page.truncate(limit);
            }
            Ok(page)
        })
    }

    fn count_job_history(&self, filter: &HistoryFilter) -> Result<u32, StateError> {
        let datastore = self.borrow().datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.borrow().run_sql_blocking_read(async move {
            let mut count = 0i64;
            for (filter, sign) in ids.statements(&base) {
                count += sign * count_history_rows(&datastore, &filter).await?;
            }
            Ok(count.max(0) as u32)
        })
    }

    // How many history rows each category holds, as `(category, count)`.
    //
    // A row with no category comes back under the empty string, which is what
    // the column holds for it. The aggregate runs in SQL so the caller can put
    // a number beside a facet without reading the table it is counting: the
    // whole point of the facet list is that it describes rows no page is
    // showing.
    fn count_job_history_by_category(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<(String, u32)>, StateError> {
        let datastore = self.borrow().datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.borrow().run_sql_blocking_read(async move {
            let mut counts = std::collections::BTreeMap::<String, i64>::new();
            for (filter, sign) in ids.statements(&base) {
                for (category, count) in count_history_rows_by_category(&datastore, &filter).await?
                {
                    *counts.entry(category).or_default() += sign * count;
                }
            }
            Ok(counts
                .into_iter()
                .filter(|(_, count)| *count > 0)
                .map(|(category, count)| (category, count as u32))
                .collect())
        })
    }

    // How many history rows each class of job holds, counted in SQL as the
    // categories are. A stored key no class answers to is left out.
    fn count_job_history_by_protocol(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<(weaver_server_jobs::jobs::model::Protocol, u32)>, StateError> {
        use weaver_server_jobs::jobs::model::Protocol;
        let datastore = self.borrow().datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.borrow().run_sql_blocking_read(async move {
            let mut counts = std::collections::BTreeMap::<Protocol, i64>::new();
            for (filter, sign) in ids.statements(&base) {
                for (key, count) in count_history_rows_by_protocol(&datastore, &filter).await? {
                    if let Some(protocol) = Protocol::parse(&key) {
                        *counts.entry(protocol).or_default() += sign * count;
                    }
                }
            }
            Ok(counts
                .into_iter()
                .filter(|(_, count)| *count > 0)
                .map(|(protocol, count)| (protocol, count as u32))
                .collect())
        })
    }

    // The ids among `job_ids` that have a history row, hidden or not, in no
    // particular order. Reads only the id column, through the primary key.
    fn job_history_ids_among(&self, job_ids: &[u64]) -> Result<Vec<u64>, StateError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let datastore = self.borrow().datastore();
        let job_ids = job_ids.to_vec();
        self.borrow().run_sql_blocking_read(async move {
            let mut found = Vec::new();
            for chunk in job_ids.chunks(max_rows_for_engine(datastore.engine(), 1)) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let sql =
                    format!("SELECT job_id FROM job_history WHERE job_id IN ({placeholders})");
                let args: Vec<SqlArg> = chunk.iter().map(|id| SqlArg::I64(*id as i64)).collect();
                for row in SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await? {
                    found.push(row.i64("job_id")? as u64);
                }
            }
            Ok(found)
        })
    }

    // The name, category and metadata of `job_id`'s history row, the columns
    // a history edit changes, and its archive time, or `None` when it
    // has no row.
    fn job_history_edit_fields(
        &self,
        job_id: u64,
    ) -> Result<Option<HistoryEditFields>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT name, category, metadata, archived_at FROM job_history
                  WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            row.map(|row| {
                Ok(HistoryEditFields {
                    name: row.text("name")?,
                    category: row.opt_text("category")?,
                    metadata: row.opt_text("metadata")?,
                    archived_at: row.opt_i64("archived_at")?,
                })
            })
            .transpose()
        })
    }

    fn list_integration_events_after(
        &self,
        after_id: Option<i64>,
        item_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<Vec<IntegrationEventRow>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            let mut sql = String::from(
                "SELECT id, timestamp, kind, item_id, payload_json
                   FROM integration_events
                  WHERE 1=1",
            );
            let mut args = Vec::new();

            if let Some(after_id) = after_id {
                sql.push_str(" AND id > {}");
                args.push(SqlArg::I64(after_id));
            }

            if let Some(item_id) = item_id {
                sql.push_str(" AND item_id = {}");
                args.push(SqlArg::I64(item_id as i64));
            }

            sql.push_str(" ORDER BY id ASC");

            if let Some(limit) = limit {
                sql.push_str(" LIMIT {}");
                args.push(SqlArg::I64(i64::from(limit)));
            }

            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter()
                .map(|row| {
                    Ok(IntegrationEventRow {
                        id: row.i64("id")?,
                        timestamp: row.i64("timestamp")?,
                        kind: row.text("kind")?,
                        item_id: row.opt_i64("item_id")?.map(|value| value as u64),
                        payload_json: row.text("payload_json")?,
                    })
                })
                .collect()
        })
    }

    fn latest_integration_event_id(&self) -> Result<Option<i64>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM integration_events",
                &[],
            )
            .await?
            .map(|row| row.opt_i64("id"))
            .transpose()
            .map(Option::flatten)
        })
    }

    fn max_job_id(&self) -> Result<u64, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            let max = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(job_id) AS job_id FROM job_history",
                &[],
            )
            .await?
            .map(|row| row.opt_i64("job_id"))
            .transpose()?
            .flatten()
            .unwrap_or(0);
            Ok(max as u64)
        })
    }

    fn load_history_job_persisted_nzb(
        &self,
        job_id: u64,
    ) -> Result<Option<PersistedNzbRecord>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT h.nzb_path, n.nzb_zstd
                   FROM job_history AS h
                   LEFT JOIN job_nzbs AS n ON n.job_id = h.job_id
                  WHERE h.job_id = {}
                  LIMIT 1",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?
            .map(|row| {
                let path = row
                    .opt_text("nzb_path")?
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(format!("job-{job_id}.nzb")));
                Ok((path, row.opt_bytes("nzb_zstd")?))
            })
            .transpose()
        })
    }
}

trait HistoryLocalQueries {
    fn get_job_history_inner(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError>;
}

impl<D: HistoryDatabase> HistoryLocalQueries for D {
    fn get_job_history_inner(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError> {
        if let Some(row) = self.get_cached_job_history(job_id) {
            weaver_server_base::runtime::perf_probe::record(
                "db.get_job_history.cache_hit",
                std::time::Duration::ZERO,
            );
            return Ok(Some(row));
        }
        weaver_server_base::runtime::perf_probe::record(
            "db.get_job_history.cache_miss",
            std::time::Duration::ZERO,
        );
        // Snapshot the cache generation before the read so a delete that races
        // this read (invalidating after its own commit) makes the re-cache a
        // no-op instead of resurrecting the deleted row from a stale read.
        let observed_generation = self.job_history_cache_generation();
        self.history_row_read();
        let datastore = self.borrow().datastore();
        let row = self.borrow().run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("{JOB_HISTORY_SELECT} WHERE job_id = {{}} LIMIT 1"),
                &[SqlArg::I64(job_id as i64)],
            )
            .await?
            .map(job_history_row_from_sql)
            .transpose()
        })?;
        if let Some(row) = &row {
            self.cache_job_history_at(row.clone(), observed_generation);
        }
        Ok(row)
    }
}

#[cfg(any(test, feature = "test-support"))]
pub trait HistoryUnpagedQueries {
    fn list_job_history_ids_by_status(&self, statuses: &[&str]) -> Result<Vec<u64>, StateError>;
}

#[cfg(any(test, feature = "test-support"))]
impl<D: HistoryDatabase> HistoryUnpagedQueries for D {
    // The ids of every history row, hidden ones too, whose status is one of
    // `statuses`, in the order `list_job_history` returns them by default:
    // newest first. The unpaged read the pages replace.
    fn list_job_history_ids_by_status(&self, statuses: &[&str]) -> Result<Vec<u64>, StateError> {
        if statuses.is_empty() {
            return Ok(Vec::new());
        }
        let datastore = self.borrow().datastore();
        let placeholders = vec!["{}"; statuses.len()].join(", ");
        let args = statuses
            .iter()
            .map(|status| SqlArg::Text((*status).to_owned()))
            .collect::<Vec<_>>();
        self.borrow().run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!(
                    "SELECT job_id FROM job_history WHERE status IN ({placeholders})
                      ORDER BY completed_at DESC, job_id DESC"
                ),
                &args,
            )
            .await?;
            rows.into_iter()
                .map(|row| Ok(row.i64("job_id")? as u64))
                .collect()
        })
    }
}

// A row's place among the states `HistoryOrderKey::State` orders by.
fn history_state_rank(status: &str) -> i64 {
    match status.to_ascii_lowercase().as_str() {
        "complete" => 0,
        "paused" => 2,
        _ => 1,
    }
}

// Sorts `rows` into `order`, comparing the stored column values the way the
// database orders them. Each row's key is built once, not per comparison.
pub fn sort_history_rows(order: HistoryOrder, rows: &mut Vec<JobHistoryRow>) {
    let value = |row: &JobHistoryRow| -> (i64, String) {
        match order.key {
            HistoryOrderKey::CompletedAt => (row.completed_at, String::new()),
            HistoryOrderKey::TotalBytes => (row.total_bytes as i64, String::new()),
            HistoryOrderKey::Health => (i64::from(row.health as i32), String::new()),
            HistoryOrderKey::State => (history_state_rank(&row.status), String::new()),
            HistoryOrderKey::Name => (
                0,
                super::attributes::history_release_name(&row.name, row.metadata.as_deref())
                    .to_ascii_lowercase(),
            ),
            HistoryOrderKey::Category => (
                0,
                row.category
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
            ),
        }
    };
    let mut keyed = rows
        .drain(..)
        .map(|row| (value(&row), row))
        .collect::<Vec<_>>();
    keyed.sort_by(|(a_key, a), (b_key, b)| {
        let ascending = a_key.cmp(b_key).then_with(|| a.job_id.cmp(&b.job_id));
        if order.descending {
            ascending.reverse()
        } else {
            ascending
        }
    });
    rows.extend(keyed.into_iter().map(|(_, row)| row));
}

async fn fetch_history_rows(
    datastore: &weaver_server_db::persistence::sql_runtime::StoreDatastore,
    filter: &HistoryFilter,
) -> Result<Vec<JobHistoryRow>, StateError> {
    let mut sql = String::from(JOB_HISTORY_SELECT_ALIASED);
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter, datastore.engine());
    sql.push_str(&history_order_sql(filter.order, datastore.engine()));

    append_history_pagination(&mut sql, &mut args, filter);

    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
    rows.into_iter().map(job_history_row_from_sql).collect()
}

pub fn job_history_row_from_sql(
    row: weaver_server_db::persistence::sql_runtime::SqlRow,
) -> Result<JobHistoryRow, StateError> {
    Ok(JobHistoryRow {
        job_id: row.i64("job_id")? as u64,
        job_hash: row.opt_bytes("job_hash")?,
        name: row.text("name")?,
        status: row.text("status")?,
        error_message: row.opt_text("error_message")?,
        total_bytes: row.i64("total_bytes")? as u64,
        downloaded_bytes: row.i64("downloaded_bytes")? as u64,
        optional_recovery_bytes: row.i64("optional_recovery_bytes")? as u64,
        optional_recovery_downloaded_bytes: row.i64("optional_recovery_downloaded_bytes")? as u64,
        failed_bytes: row.i64("failed_bytes")? as u64,
        health: row.i32("health")? as u32,
        category: row.opt_text("category")?,
        output_dir: row.opt_text("output_dir")?,
        nzb_path: row.opt_text("nzb_path")?,
        created_at: row.i64("created_at")?,
        completed_at: row.i64("completed_at")?,
        metadata: row.opt_text("metadata")?,
        server_attribution: row.opt_text("server_attribution")?,
        protocol: {
            let protocol = row.text("protocol")?;
            weaver_server_jobs::jobs::model::Protocol::parse(&protocol)
                .ok_or_else(|| StateError::Database(format!("unknown job protocol: {protocol}")))?
        },
        archived_at: row.opt_i64("archived_at")?,
    })
}
