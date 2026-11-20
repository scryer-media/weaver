// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::history::cache::HistoryCacheQueries;
use crate::torrents::queries::TorrentsInternalQueries;
use std::path::PathBuf;

use crate::StateError;
use crate::history::model::{HistoryEditFields, HistoryFilter, HistoryOrder, HistoryOrderKey};
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, max_rows_for_engine};

#[cfg(test)]
use crate::persistence::sql_runtime::SqlEngine;
pub use weaver_server_history::history::queries::HistoryFileState;
use weaver_server_history::history::queries::{
    HistoryIdSplit, append_history_from_clause, append_history_pagination,
    append_history_row_filter_predicates, archived_history_files_sql, count_history_rows,
    count_history_rows_by_category, history_order_sql, split_history_ids,
};

#[cfg(test)]
use weaver_server_history::history::queries::{history_completed_by_sql, history_status_ids_sql};
pub(crate) use weaver_server_history::history::retention::HistoryInternalQueries;

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
#[cfg(test)]
pub(crate) const JOB_HISTORY_SELECTS: [&str; 2] = [JOB_HISTORY_SELECT, JOB_HISTORY_SELECT_ALIASED];

pub trait HistoryQueries {
    fn get_job_history(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError>;
    fn get_job_history_profiled(
        &self,
        job_id: u64,
        label: &'static str,
    ) -> Result<Option<JobHistoryRow>, StateError>;
    fn list_job_history_output_dirs(
        &self,
        job_ids: &[crate::JobId],
    ) -> Result<std::collections::BTreeMap<crate::JobId, Option<String>>, StateError>;
    fn shared_output_dirs(
        &self,
        dirs: &[String],
    ) -> Result<std::collections::BTreeSet<String>, StateError>;
    fn list_job_history_file_states(
        &self,
        job_ids: &[crate::JobId],
    ) -> Result<std::collections::BTreeMap<crate::JobId, HistoryFileState>, StateError>;
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

impl HistoryQueries for Database {
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
        job_ids: &[crate::JobId],
    ) -> Result<std::collections::BTreeMap<crate::JobId, Option<String>>, StateError> {
        if job_ids.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        self.run_sql_blocking_read(async move {
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
                        crate::JobId(row.i64("job_id")? as u64),
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
        let datastore = self.datastore();
        let dirs = dirs
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        self.run_sql_blocking_read(async move {
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
        job_ids: &[crate::JobId],
    ) -> Result<std::collections::BTreeMap<crate::JobId, HistoryFileState>, StateError> {
        if job_ids.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        self.run_sql_blocking_read(async move {
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
                        crate::JobId(row.i64("job_id")? as u64),
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.run_sql_blocking_read(async move {
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

    // The ids among `job_ids` that have a history row, hidden or not, in no
    // particular order. Reads only the id column, through the primary key.
    fn job_history_ids_among(&self, job_ids: &[u64]) -> Result<Vec<u64>, StateError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let datastore = self.datastore();
        let job_ids = job_ids.to_vec();
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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

impl HistoryLocalQueries for Database {
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
        self.torrent_read();
        let datastore = self.datastore();
        let row = self.run_sql_blocking_read(async move {
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

#[cfg(test)]
pub(crate) trait HistoryUnpagedQueries {
    fn list_job_history_ids_by_status(&self, statuses: &[&str]) -> Result<Vec<u64>, StateError>;
}

#[cfg(test)]
impl HistoryUnpagedQueries for Database {
    // The ids of every history row, hidden ones too, whose status is one of
    // `statuses`, in the order `list_job_history` returns them by default:
    // newest first. The unpaged read the pages replace.
    fn list_job_history_ids_by_status(&self, statuses: &[&str]) -> Result<Vec<u64>, StateError> {
        if statuses.is_empty() {
            return Ok(Vec::new());
        }
        let datastore = self.datastore();
        let placeholders = vec!["{}"; statuses.len()].join(", ");
        let args = statuses
            .iter()
            .map(|status| SqlArg::Text((*status).to_owned()))
            .collect::<Vec<_>>();
        self.run_sql_blocking_read(async move {
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
fn sort_history_rows(order: HistoryOrder, rows: &mut Vec<JobHistoryRow>) {
    let value = |row: &JobHistoryRow| -> (i64, String) {
        match order.key {
            HistoryOrderKey::CompletedAt => (row.completed_at, String::new()),
            HistoryOrderKey::TotalBytes => (row.total_bytes as i64, String::new()),
            HistoryOrderKey::Health => (i64::from(row.health as i32), String::new()),
            HistoryOrderKey::State => (history_state_rank(&row.status), String::new()),
            HistoryOrderKey::Name => (
                0,
                super::history_release_name(&row.name, row.metadata.as_deref())
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
    datastore: &crate::persistence::sql_runtime::StoreDatastore,
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

pub(crate) fn job_history_row_from_sql(
    row: crate::persistence::sql_runtime::SqlRow,
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
            crate::jobs::Protocol::parse(&protocol)
                .ok_or_else(|| StateError::Database(format!("unknown job protocol: {protocol}")))?
        },
        archived_at: row.opt_i64("archived_at")?,
    })
}

#[cfg(test)]
mod order_index_tests {
    use super::*;
    use crate::history::nzbget_state::HistoryNzbgetStateQueries;
    use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;

    // The retention read is a range of a completed-at index, with and
    // without its cursor, and sorts nothing.
    #[test]
    fn the_retention_read_is_served_by_the_completed_index() {
        let db = Database::open_in_memory().unwrap();
        for after in [None, Some((1_700_000_000, 9))] {
            let (sql, args) = history_completed_by_sql(1_800_000_000, after, 1000);
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            let datastore = db.datastore();
            let detail = db
                .run_sql_blocking_read(async move {
                    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                    rows.iter()
                        .map(|row| row.text("detail"))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .join("; ");
            println!("retention read plan, cursor {after:?}: {detail}");
            // Either completed-at index holds `(completed_at, job_id)` in
            // order: `job_id` is the rowid, which every SQLite index carries.
            assert!(
                detail.contains("USING COVERING INDEX idx_job_history_completed_at")
                    || detail.contains("USING COVERING INDEX idx_job_history_completed_job"),
                "{detail}"
            );
            assert!(!detail.contains("TEMP B-TREE"), "{detail}");
        }
    }

    // The scheduled retention read of one status is a range of the status
    // index, with and without its cursor, and sorts nothing.
    #[test]
    fn the_status_retention_read_is_served_by_the_status_index() {
        let db = Database::open_in_memory().unwrap();
        for status in ["failed", "complete"] {
            for after in [None, Some((1_700_000_000, 9))] {
                let (sql, args) = history_status_ids_sql(status, after, 1000);
                let sql = format!("EXPLAIN QUERY PLAN {sql}");
                let datastore = db.datastore();
                let detail = db
                    .run_sql_blocking_read(async move {
                        let rows =
                            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                        rows.iter()
                            .map(|row| row.text("detail"))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .unwrap()
                    .join("; ");
                println!("status retention plan, {status}, cursor {after:?}: {detail}");
                assert!(
                    detail.contains("USING COVERING INDEX idx_job_history_status_completed_job"),
                    "{detail}"
                );
                assert!(!detail.contains("TEMP B-TREE"), "{detail}");
            }
        }
    }

    // The archived-files page is a range of the table's primary key, read in
    // its order, so it sorts nothing.
    #[test]
    fn the_archived_files_page_is_served_by_the_primary_key() {
        let db = Database::open_in_memory().unwrap();
        for from in [0, 4_000] {
            let (sql, args) = archived_history_files_sql(7, from, 100);
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            let datastore = db.datastore();
            let detail = db
                .run_sql_blocking_read(async move {
                    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                    rows.iter()
                        .map(|row| row.text("detail"))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .join("; ");
            println!("archived files page plan, from {from}: {detail}");
            assert!(
                detail.contains(
                    "SEARCH job_history_archived_files USING PRIMARY KEY (job_id=? AND ordinal>?)"
                ),
                "{detail}"
            );
            assert!(!detail.contains("TEMP B-TREE"), "{detail}");
        }
    }

    // The id-only status read returns the ids, in the order, that decoding
    // whole rows did.
    #[test]
    fn the_status_id_read_matches_the_full_row_read() {
        let db = Database::open_in_memory().unwrap();
        for (job_id, status, completed_at) in [
            (1, "complete", 50),
            (2, "failed", 50),
            (3, "partial", 10),
            (4, "cancelled", 70),
            (5, "complete", 90),
            (6, "failed", 5),
        ] {
            db.insert_job_history(&JobHistoryRow {
                job_id,
                job_hash: None,
                name: format!("status-{job_id}"),
                status: status.into(),
                error_message: None,
                total_bytes: 1,
                downloaded_bytes: 1,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: 1000,
                category: None,
                output_dir: None,
                nzb_path: None,
                created_at: 1,
                completed_at,
                metadata: None,
                server_attribution: None,
                archived_at: None,
                protocol: crate::jobs::Protocol::Nzb,
            })
            .unwrap();
        }
        db.set_nzbget_history_hidden(2, true).unwrap();
        for statuses in [
            vec!["failed", "partial"],
            vec!["complete"],
            vec!["failed", "partial", "complete", "cancelled"],
        ] {
            let full = db
                .list_job_history(&HistoryFilter {
                    statuses: Some(statuses.iter().map(|status| status.to_string()).collect()),
                    ..Default::default()
                })
                .unwrap()
                .into_iter()
                .map(|row| row.job_id)
                .collect::<Vec<_>>();
            assert!(!full.is_empty());
            assert_eq!(db.list_job_history_ids_by_status(&statuses).unwrap(), full);
        }
    }

    // Every History page order is read through its index: SQLite walks the
    // index in order instead of sorting the table in a temporary b-tree.
    #[test]
    fn every_history_order_is_served_by_an_index() {
        let db = Database::open_in_memory().unwrap();
        let keys = [
            HistoryOrderKey::CompletedAt,
            HistoryOrderKey::TotalBytes,
            HistoryOrderKey::Health,
            HistoryOrderKey::Name,
            HistoryOrderKey::State,
            HistoryOrderKey::Category,
        ];
        // A search is a substring of the name, which no index seeks, so a
        // searched page walks its order's index and stops at the page. The
        // same holds for Weaver's listing, which reads every row, and for the
        // NZBGet facade's, which leaves out the rows it hid.
        for ((key, search), nzbget_shown_only) in keys
            .into_iter()
            .flat_map(|key| [(key, None), (key, Some("release".to_string()))])
            .flat_map(|pair| [(pair.clone(), false), (pair, true)])
        {
            for descending in [true, false] {
                let order = HistoryOrder { key, descending };
                let filter = HistoryFilter {
                    name_contains: search.clone(),
                    nzbget_shown_only,
                    ..Default::default()
                };
                let mut sql =
                    String::from("EXPLAIN QUERY PLAN SELECT h.job_id FROM job_history h WHERE 1=1");
                let mut args = Vec::new();
                append_history_row_filter_predicates(
                    &mut sql,
                    &mut args,
                    &filter,
                    SqlEngine::Sqlite,
                );
                sql.push_str(&history_order_sql(order, SqlEngine::Sqlite));
                sql.push_str(" LIMIT 25");
                let datastore = db.datastore();
                let detail = db
                    .run_sql_blocking_read(async move {
                        let rows =
                            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                        rows.iter()
                            .map(|row| row.text("detail"))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .unwrap()
                    .join("; ");
                assert!(
                    detail.contains("USING INDEX") || detail.contains("USING COVERING INDEX"),
                    "{order:?} {search:?} {nzbget_shown_only}: {detail}"
                );
                assert!(
                    !detail.contains("TEMP B-TREE"),
                    "{order:?} {search:?} {nzbget_shown_only}: {detail}"
                );
            }
        }
    }

    // The order the database returns is the order `sort_history_rows`
    // gives, so pages merged from several statements stay in order.
    #[test]
    fn sort_history_rows_matches_the_database_order() {
        let db = Database::open_in_memory().unwrap();
        let names = [
            "b.Release",
            "B.release",
            "a.Release",
            "Ä.release",
            "é",
            "Z",
            "z",
            "",
        ];
        let statuses = [
            "complete", "Paused", "failed", "COMPLETE", "partial", "weird",
        ];
        let categories = [
            None,
            Some(""),
            Some("TV"),
            Some("tv"),
            Some("Ébc"),
            Some("anime"),
        ];
        for job_id in 1..=48u64 {
            db.insert_job_history(&JobHistoryRow {
                job_id,
                job_hash: None,
                name: names[(job_id % 8) as usize].to_string(),
                status: statuses[(job_id % 6) as usize].to_string(),
                error_message: None,
                total_bytes: job_id % 5,
                downloaded_bytes: 0,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: (job_id % 3) as u32,
                category: categories[(job_id % 6) as usize].map(str::to_string),
                output_dir: None,
                nzb_path: None,
                created_at: 1,
                completed_at: (job_id % 7) as i64,
                // The name sort is by the full release name, which the
                // original or renamed title in the metadata can replace.
                metadata: match job_id % 5 {
                    1 => Some(r#"[["weaver.original_title","A.Full.Release.1080p"]]"#.into()),
                    2 => Some(
                        r#"[["weaver.original_title","Y.Full"],["weaver.renamed_title","m.Renamed"]]"#
                            .into(),
                    ),
                    _ => None,
                },
                server_attribution: None,
                archived_at: None,
                protocol: crate::jobs::Protocol::Nzb,
            })
            .unwrap();
        }
        let keys = [
            HistoryOrderKey::CompletedAt,
            HistoryOrderKey::TotalBytes,
            HistoryOrderKey::Health,
            HistoryOrderKey::Name,
            HistoryOrderKey::State,
            HistoryOrderKey::Category,
        ];
        for key in keys {
            for descending in [true, false] {
                let order = HistoryOrder { key, descending };
                let filter = HistoryFilter {
                    order,
                    ..Default::default()
                };
                let read = db.list_job_history(&filter).unwrap();
                let mut sorted = read.clone();
                sort_history_rows(order, &mut sorted);
                let ids =
                    |rows: &[JobHistoryRow]| rows.iter().map(|row| row.job_id).collect::<Vec<_>>();
                assert_eq!(ids(&read), ids(&sorted), "{order:?}");
            }
        }
    }
}
