// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;

use crate::StateError;
use crate::history::model::HistoryFilter;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, max_rows_for_engine};

type PersistedNzbRecord = (PathBuf, Option<Vec<u8>>);

const JOB_HISTORY_SELECT: &str =
    "SELECT job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
        optional_recovery_bytes, optional_recovery_downloaded_bytes,
        failed_bytes, health, category, output_dir, nzb_path,
        created_at, completed_at, metadata, server_attribution
   FROM job_history";

const JOB_HISTORY_SELECT_ALIASED: &str =
    "SELECT h.job_id, h.job_hash, h.name, h.status, h.error_message, h.total_bytes, h.downloaded_bytes,
        h.optional_recovery_bytes, h.optional_recovery_downloaded_bytes,
        h.failed_bytes, h.health, h.category, h.output_dir, h.nzb_path,
        h.created_at, h.completed_at, h.metadata, h.server_attribution
   FROM ";

impl Database {
    pub fn get_job_history(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError> {
        self.get_job_history_inner(job_id)
    }

    pub fn get_job_history_profiled(
        &self,
        job_id: u64,
        label: &'static str,
    ) -> Result<Option<JobHistoryRow>, StateError> {
        let _probe = crate::runtime::perf_probe::scope(label);
        self.get_job_history_inner(job_id)
    }

    fn get_job_history_inner(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError> {
        if let Some(row) = self.get_cached_job_history(job_id) {
            crate::runtime::perf_probe::record(
                "db.get_job_history.cache_hit",
                std::time::Duration::ZERO,
            );
            return Ok(Some(row));
        }
        crate::runtime::perf_probe::record(
            "db.get_job_history.cache_miss",
            std::time::Duration::ZERO,
        );
        // Snapshot the cache generation before the read so a delete that races
        // this read (invalidating after its own commit) makes the re-cache a
        // no-op instead of resurrecting the deleted row from a stale read.
        let observed_generation = self.job_history_cache_generation();
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

    // The recorded output directory of every id that has a history row, read
    // in bind-sized chunks. An id without a row is absent from the map.
    pub fn list_job_history_output_dirs(
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

    pub fn list_job_history(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<JobHistoryRow>, StateError> {
        self.list_job_history_with_visibility(filter, true)
    }

    // The facade's visibility predicate must be applied before pagination.
    pub fn list_nzbget_history(
        &self,
        filter: &HistoryFilter,
        include_hidden: bool,
    ) -> Result<Vec<JobHistoryRow>, StateError> {
        self.list_job_history_with_visibility(filter, include_hidden)
    }

    fn list_job_history_with_visibility(
        &self,
        filter: &HistoryFilter,
        include_hidden: bool,
    ) -> Result<Vec<JobHistoryRow>, StateError> {
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
            let mut rows = match &ids {
                HistoryIdSplit::Whole => {
                    return fetch_history_rows(&datastore, &base, include_hidden).await;
                }
                HistoryIdSplit::In(chunks) => {
                    let mut rows = Vec::new();
                    for chunk in chunks {
                        let filter = HistoryFilter {
                            item_ids: Some(chunk.clone()),
                            ..through(0)
                        };
                        rows.extend(fetch_history_rows(&datastore, &filter, include_hidden).await?);
                    }
                    rows.sort_by(|a, b| {
                        (b.completed_at, b.job_id).cmp(&(a.completed_at, a.job_id))
                    });
                    rows
                }
                HistoryIdSplit::NotIn(excluded, _) => {
                    let mut rows =
                        fetch_history_rows(&datastore, &through(excluded.len()), include_hidden)
                            .await?;
                    rows.retain(|row| excluded.binary_search(&row.job_id).is_err());
                    rows
                }
            };
            rows.drain(..offset.min(rows.len()));
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            Ok(rows)
        })
    }

    pub fn count_job_history(&self, filter: &HistoryFilter) -> Result<u32, StateError> {
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
    pub fn count_job_history_by_category(
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

    pub fn list_integration_events_after(
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

    pub fn latest_integration_event_id(&self) -> Result<Option<i64>, StateError> {
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

    pub fn max_job_id(&self) -> Result<u64, StateError> {
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

    pub fn load_history_job_persisted_nzb(
        &self,
        job_id: u64,
    ) -> Result<Option<PersistedNzbRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT nzb_path, nzb_zstd
                   FROM job_history
                  WHERE job_id = {}
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

fn append_history_from_clause(sql: &mut String, args: &mut Vec<SqlArg>, filter: &HistoryFilter) {
    match (&filter.metadata_equals, &filter.metadata_has_key) {
        (Some(metadata_equals), maybe_key) => {
            sql.push_str(
                "job_history_attributes a
                 JOIN job_history h ON h.job_id = a.job_id
                 WHERE a.key = {} AND a.value = {}",
            );
            args.push(SqlArg::Text(metadata_equals.key.clone()));
            args.push(SqlArg::Text(metadata_equals.value.clone()));
            if let Some(key) = maybe_key
                && key != &metadata_equals.key
            {
                sql.push_str(
                    " AND EXISTS (
                        SELECT 1
                          FROM job_history_attributes ak
                         WHERE ak.job_id = h.job_id
                           AND ak.key = {}
                     )",
                );
                args.push(SqlArg::Text(key.clone()));
            }
        }
        (None, Some(key)) => {
            sql.push_str(
                "(SELECT DISTINCT job_id
                    FROM job_history_attributes
                   WHERE key = {}) a
                 JOIN job_history h ON h.job_id = a.job_id
                 WHERE 1=1",
            );
            args.push(SqlArg::Text(key.clone()));
        }
        (None, None) => {
            sql.push_str("job_history h WHERE 1=1");
        }
    }
}

fn append_history_row_filter_predicates(
    sql: &mut String,
    args: &mut Vec<SqlArg>,
    filter: &HistoryFilter,
) {
    if let Some(statuses) = &filter.statuses {
        append_in_i64_or_text_filter(
            sql,
            args,
            "h.status",
            statuses.iter().cloned().map(SqlArg::Text).collect(),
        );
    }
    if let Some(status_not_in) = &filter.status_not_in {
        append_not_in_filter(
            sql,
            args,
            "h.status",
            status_not_in.iter().cloned().map(SqlArg::Text).collect(),
        );
    }
    if let Some(item_ids) = &filter.item_ids {
        append_in_i64_or_text_filter(
            sql,
            args,
            "h.job_id",
            item_ids
                .iter()
                .copied()
                .map(|value| SqlArg::I64(value as i64))
                .collect(),
        );
    }
    if let Some(excluded_ids) = &filter.item_ids_not_in {
        append_not_in_filter(
            sql,
            args,
            "h.job_id",
            excluded_ids
                .iter()
                .copied()
                .map(|value| SqlArg::I64(value as i64))
                .collect(),
        );
    }
    if let Some(category) = &filter.category {
        sql.push_str(" AND h.category = {}");
        args.push(SqlArg::Text(category.clone()));
    }
}

fn append_in_i64_or_text_filter(
    sql: &mut String,
    args: &mut Vec<SqlArg>,
    column: &str,
    values: Vec<SqlArg>,
) {
    if values.is_empty() {
        sql.push_str(" AND 1=0");
        return;
    }

    sql.push_str(" AND ");
    sql.push_str(column);
    sql.push_str(" IN (");
    sql.push_str(
        &std::iter::repeat_n("{}", values.len())
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push(')');
    args.extend(values);
}

fn append_not_in_filter(
    sql: &mut String,
    args: &mut Vec<SqlArg>,
    column: &str,
    values: Vec<SqlArg>,
) {
    // An empty exclusion list excludes nothing, so the predicate is a no-op.
    if values.is_empty() {
        return;
    }

    sql.push_str(" AND ");
    sql.push_str(column);
    sql.push_str(" NOT IN (");
    sql.push_str(
        &std::iter::repeat_n("{}", values.len())
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push(')');
    args.extend(values);
}

// A job-id list longer than the engine binds in one statement, split into
// chunks it does bind. Ids are deduplicated first, so the chunks are disjoint
// and their counts add up.
enum HistoryIdSplit {
    // The filter's id lists fit one statement.
    Whole,
    // `item_ids`, in chunks.
    In(Vec<Vec<u64>>),
    // `item_ids_not_in`, sorted, and in chunks: the rows left out are counted
    // through the chunks and subtracted.
    NotIn(Vec<u64>, Vec<Vec<u64>>),
}

impl HistoryIdSplit {
    // The statements whose signed counts sum to the filter's count.
    fn statements(&self, base: &HistoryFilter) -> Vec<(HistoryFilter, i64)> {
        let with_ids = |chunk: &Vec<u64>| HistoryFilter {
            item_ids: Some(chunk.clone()),
            ..base.clone()
        };
        match self {
            Self::Whole => vec![(base.clone(), 1)],
            Self::In(chunks) => chunks.iter().map(|chunk| (with_ids(chunk), 1)).collect(),
            Self::NotIn(_, chunks) => std::iter::once((base.clone(), 1))
                .chain(chunks.iter().map(|chunk| (with_ids(chunk), -1)))
                .collect(),
        }
    }
}

// Each id list is bound one parameter per id, and an engine takes only so
// many in one statement (SQLite stops at 32,766), so a list past the batch
// budget is split. The base filter keeps everything but the split list.
fn split_history_ids(
    engine: crate::persistence::sql_runtime::SqlEngine,
    filter: &HistoryFilter,
) -> (HistoryFilter, HistoryIdSplit) {
    let mut base = filter.clone();
    let mut excluded = base.item_ids_not_in.take().unwrap_or_default();
    excluded.sort_unstable();
    excluded.dedup();
    if let Some(ids) = &mut base.item_ids {
        ids.sort_unstable();
        ids.dedup();
        // Leaving out of a list is taking from it: one list, no exclusion.
        ids.retain(|id| excluded.binary_search(id).is_err());
        excluded.clear();
    }
    // Room for the filter's other parameters beside the list.
    let budget = max_rows_for_engine(engine, 1).saturating_sub(64).max(1);
    if base.item_ids.as_ref().is_some_and(|ids| ids.len() > budget) {
        let ids = base.item_ids.take().unwrap_or_default();
        let chunks = ids.chunks(budget).map(<[u64]>::to_vec).collect();
        return (base, HistoryIdSplit::In(chunks));
    }
    if excluded.len() > budget {
        let chunks = excluded.chunks(budget).map(<[u64]>::to_vec).collect();
        return (base, HistoryIdSplit::NotIn(excluded, chunks));
    }
    if !excluded.is_empty() {
        base.item_ids_not_in = Some(excluded);
    }
    (base, HistoryIdSplit::Whole)
}

async fn fetch_history_rows(
    datastore: &crate::persistence::sql_runtime::StoreDatastore,
    filter: &HistoryFilter,
    include_hidden: bool,
) -> Result<Vec<JobHistoryRow>, StateError> {
    let mut sql = String::from(JOB_HISTORY_SELECT_ALIASED);
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter);
    if !include_hidden {
        sql.push_str(" AND h.nzbget_hidden = 0");
    }
    sql.push_str(" ORDER BY h.completed_at DESC, h.job_id DESC");

    append_history_pagination(&mut sql, &mut args, filter);

    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
    rows.into_iter().map(job_history_row_from_sql).collect()
}

async fn count_history_rows(
    datastore: &crate::persistence::sql_runtime::StoreDatastore,
    filter: &HistoryFilter,
) -> Result<i64, StateError> {
    let mut sql = String::from("SELECT COUNT(*) AS count FROM ");
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter);

    Ok(
        SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args)
            .await?
            .map(|row| row.i64("count"))
            .transpose()?
            .unwrap_or(0),
    )
}

async fn count_history_rows_by_category(
    datastore: &crate::persistence::sql_runtime::StoreDatastore,
    filter: &HistoryFilter,
) -> Result<Vec<(String, i64)>, StateError> {
    let mut sql =
        String::from("SELECT COALESCE(h.category, '') AS category, COUNT(*) AS count FROM ");
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter);

    sql.push_str(" GROUP BY COALESCE(h.category, '')");

    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
    rows.into_iter()
        .map(|row| Ok((row.text("category")?, row.i64("count")?)))
        .collect()
}

fn append_history_pagination(sql: &mut String, args: &mut Vec<SqlArg>, filter: &HistoryFilter) {
    match (filter.limit, filter.offset) {
        (Some(limit), Some(offset)) => {
            sql.push_str(" LIMIT {} OFFSET {}");
            args.push(SqlArg::I64(i64::from(limit)));
            args.push(SqlArg::I64(i64::from(offset)));
        }
        (Some(limit), None) => {
            sql.push_str(" LIMIT {}");
            args.push(SqlArg::I64(i64::from(limit)));
        }
        (None, Some(offset)) => {
            sql.push_str(" LIMIT {} OFFSET {}");
            args.push(SqlArg::I64(i64::from(u32::MAX)));
            args.push(SqlArg::I64(i64::from(offset)));
        }
        (None, None) => {}
    }
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
    })
}
