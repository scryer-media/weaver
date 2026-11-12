// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;

use crate::StateError;
use crate::history::model::{
    HistoryEditFields, HistoryFilter, HistoryKeyRow, HistoryOrder, HistoryOrderKey, HistoryRows,
};
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRuntime, max_rows_for_engine};

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

    // Every failed job whose record says it finished at or before
    // `finished_by`, in epoch seconds, with the output directory it records.
    pub(crate) fn failed_job_history_finished_by(
        &self,
        finished_by: i64,
    ) -> Result<std::collections::BTreeMap<crate::JobId, Option<String>>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, output_dir FROM job_history WHERE status = 'failed' AND completed_at <= {}",
                &[SqlArg::I64(finished_by)],
            )
            .await?;
            let mut dirs = std::collections::BTreeMap::new();
            for row in rows {
                dirs.insert(
                    crate::JobId(row.i64("job_id")? as u64),
                    row.opt_text("output_dir")?,
                );
            }
            Ok(dirs)
        })
    }

    // The visibility predicate is applied in SQL, before pagination.
    pub fn list_job_history(
        &self,
        filter: &HistoryFilter,
        rows: HistoryRows,
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
            let mut page = match &ids {
                HistoryIdSplit::Whole => {
                    return fetch_history_rows(&datastore, &base, rows).await;
                }
                HistoryIdSplit::In(chunks) => {
                    let mut page = Vec::new();
                    for chunk in chunks {
                        let filter = HistoryFilter {
                            item_ids: Some(chunk.clone()),
                            ..through(0)
                        };
                        page.extend(fetch_history_rows(&datastore, &filter, rows).await?);
                    }
                    page.sort_by(|a, b| history_order_cmp(base.order, a, b));
                    page
                }
                HistoryIdSplit::NotIn(excluded, _) => {
                    let mut page =
                        fetch_history_rows(&datastore, &through(excluded.len()), rows).await?;
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

    pub fn count_job_history(
        &self,
        filter: &HistoryFilter,
        rows: HistoryRows,
    ) -> Result<u32, StateError> {
        let datastore = self.datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.run_sql_blocking_read(async move {
            let mut count = 0i64;
            for (filter, sign) in ids.statements(&base) {
                count += sign * count_history_rows(&datastore, &filter, rows).await?;
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
        rows: HistoryRows,
    ) -> Result<Vec<(String, u32)>, StateError> {
        let datastore = self.datastore();
        let (base, ids) = split_history_ids(datastore.engine(), filter);
        self.run_sql_blocking_read(async move {
            let mut counts = std::collections::BTreeMap::<String, i64>::new();
            for (filter, sign) in ids.statements(&base) {
                for (category, count) in
                    count_history_rows_by_category(&datastore, &filter, rows).await?
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
    pub fn job_history_ids_among(&self, job_ids: &[u64]) -> Result<Vec<u64>, StateError> {
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
    // a history edit changes, or `None` when it has no row.
    pub fn job_history_edit_fields(
        &self,
        job_id: u64,
    ) -> Result<Option<HistoryEditFields>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT name, category, metadata FROM job_history WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            row.map(|row| {
                Ok(HistoryEditFields {
                    name: row.text("name")?,
                    category: row.opt_text("category")?,
                    metadata: row.opt_text("metadata")?,
                })
            })
            .transpose()
        })
    }

    // The match-and-order columns of every history row `rows` shows, in no
    // particular order. `with_titles` adds the name and metadata a title is
    // built from; without it both come back empty.
    pub fn list_job_history_keys(
        &self,
        rows: HistoryRows,
        with_titles: bool,
    ) -> Result<Vec<HistoryKeyRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut sql = String::from(if with_titles {
                "SELECT h.job_id, h.name, h.status, h.total_bytes, h.health, h.category,
                        h.completed_at, h.metadata
                   FROM job_history h WHERE 1=1"
            } else {
                "SELECT h.job_id, h.status, h.total_bytes, h.health, h.category, h.completed_at
                   FROM job_history h WHERE 1=1"
            });
            append_history_visibility(&mut sql, rows);
            let found = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &[]).await?;
            found
                .into_iter()
                .map(|row| {
                    Ok(HistoryKeyRow {
                        job_id: row.i64("job_id")? as u64,
                        name: if with_titles {
                            row.text("name")?
                        } else {
                            String::new()
                        },
                        status: row.text("status")?,
                        total_bytes: row.i64("total_bytes")? as u64,
                        health: row.i32("health")? as u32,
                        category: row.opt_text("category")?,
                        completed_at: row.i64("completed_at")?,
                        metadata: if with_titles {
                            row.opt_text("metadata")?
                        } else {
                            None
                        },
                    })
                })
                .collect()
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
    if let Some(categories) = &filter.categories_any {
        append_in_i64_or_text_filter(
            sql,
            args,
            "COALESCE(h.category, '')",
            categories.iter().cloned().map(SqlArg::Text).collect(),
        );
    }
}

// ASCII upper and lower case, for Postgres's `translate`. SQLite's LOWER
// folds ASCII letters alone, so both engines fold exactly these, and the
// `C` collation makes Postgres compare bytes as SQLite does.
const PG_ASCII_UPPER: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const PG_ASCII_LOWER: &str = "abcdefghijklmnopqrstuvwxyz";

// The expression `key` orders rows by. Each is the expression of its history
// index in migration 86, character for character, so the index serves it.
fn history_order_expression(key: HistoryOrderKey, engine: SqlEngine) -> String {
    let fold = |column: &str| match engine {
        SqlEngine::Sqlite => format!("LOWER({column})"),
        SqlEngine::Postgres => {
            format!("translate({column}, '{PG_ASCII_UPPER}', '{PG_ASCII_LOWER}') COLLATE \"C\"")
        }
    };
    match key {
        HistoryOrderKey::CompletedAt => "h.completed_at".to_string(),
        HistoryOrderKey::TotalBytes => "h.total_bytes".to_string(),
        HistoryOrderKey::Health => "h.health".to_string(),
        HistoryOrderKey::Name => fold("h.name"),
        HistoryOrderKey::Category => fold("COALESCE(h.category, '')"),
        HistoryOrderKey::State => {
            let status = match engine {
                SqlEngine::Sqlite => "LOWER(h.status)".to_string(),
                SqlEngine::Postgres => {
                    format!("translate(h.status, '{PG_ASCII_UPPER}', '{PG_ASCII_LOWER}')")
                }
            };
            format!("(CASE {status} WHEN 'complete' THEN 0 WHEN 'paused' THEN 2 ELSE 1 END)")
        }
    }
}

// The ORDER BY clause of `order`, served by the history index on its key.
fn history_order_sql(order: HistoryOrder, engine: SqlEngine) -> String {
    let direction = if order.descending { "DESC" } else { "ASC" };
    format!(
        " ORDER BY {} {direction}, h.job_id {direction}",
        history_order_expression(order.key, engine)
    )
}

// A row's place among the states `HistoryOrderKey::State` orders by.
fn history_state_rank(status: &str) -> i64 {
    match status.to_ascii_lowercase().as_str() {
        "complete" => 0,
        "paused" => 2,
        _ => 1,
    }
}

// Two rows in `order`, comparing the stored column values the way the
// database orders them.
fn history_order_cmp(
    order: HistoryOrder,
    a: &JobHistoryRow,
    b: &JobHistoryRow,
) -> std::cmp::Ordering {
    let value = |row: &JobHistoryRow| -> (i64, String) {
        match order.key {
            HistoryOrderKey::CompletedAt => (row.completed_at, String::new()),
            HistoryOrderKey::TotalBytes => (row.total_bytes as i64, String::new()),
            HistoryOrderKey::Health => (i64::from(row.health as i32), String::new()),
            HistoryOrderKey::State => (history_state_rank(&row.status), String::new()),
            HistoryOrderKey::Name => (0, row.name.to_ascii_lowercase()),
            HistoryOrderKey::Category => (
                0,
                row.category
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
            ),
        }
    };
    let ascending = value(a)
        .cmp(&value(b))
        .then_with(|| a.job_id.cmp(&b.job_id));
    if order.descending {
        ascending.reverse()
    } else {
        ascending
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
    rows: HistoryRows,
) -> Result<Vec<JobHistoryRow>, StateError> {
    let mut sql = String::from(JOB_HISTORY_SELECT_ALIASED);
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter);
    append_history_visibility(&mut sql, rows);
    sql.push_str(&history_order_sql(filter.order, datastore.engine()));

    append_history_pagination(&mut sql, &mut args, filter);

    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
    rows.into_iter().map(job_history_row_from_sql).collect()
}

async fn count_history_rows(
    datastore: &crate::persistence::sql_runtime::StoreDatastore,
    filter: &HistoryFilter,
    rows: HistoryRows,
) -> Result<i64, StateError> {
    let mut sql = String::from("SELECT COUNT(*) AS count FROM ");
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter);
    append_history_visibility(&mut sql, rows);

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
    rows: HistoryRows,
) -> Result<Vec<(String, i64)>, StateError> {
    let mut sql =
        String::from("SELECT COALESCE(h.category, '') AS category, COUNT(*) AS count FROM ");
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter);
    append_history_visibility(&mut sql, rows);

    sql.push_str(" GROUP BY COALESCE(h.category, '')");

    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
    rows.into_iter()
        .map(|row| Ok((row.text("category")?, row.i64("count")?)))
        .collect()
}

// The one visibility predicate every history listing and count shares.
fn append_history_visibility(sql: &mut String, rows: HistoryRows) {
    if rows == HistoryRows::Shown {
        sql.push_str(" AND h.nzbget_hidden = 0");
    }
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

#[cfg(test)]
mod order_index_tests {
    use super::*;

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
        for key in keys {
            for descending in [true, false] {
                let order = HistoryOrder { key, descending };
                let sql = format!(
                    "EXPLAIN QUERY PLAN SELECT h.job_id FROM job_history h WHERE 1=1{} LIMIT 25",
                    history_order_sql(order, SqlEngine::Sqlite)
                );
                let datastore = db.datastore();
                let detail = db
                    .run_sql_blocking_read(async move {
                        let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &[]).await?;
                        rows.iter()
                            .map(|row| row.text("detail"))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .unwrap()
                    .join("; ");
                assert!(
                    detail.contains("USING INDEX") || detail.contains("USING COVERING INDEX"),
                    "{order:?}: {detail}"
                );
                assert!(!detail.contains("TEMP B-TREE"), "{order:?}: {detail}");
            }
        }
    }

    // The order the database returns is the order `history_order_cmp`
    // gives, so pages merged from several statements stay in order.
    #[test]
    fn history_order_cmp_matches_the_database_order() {
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
                metadata: None,
                server_attribution: None,
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
                let read = db.list_job_history(&filter, HistoryRows::All).unwrap();
                let mut sorted = read.clone();
                sorted.sort_by(|a, b| history_order_cmp(order, a, b));
                let ids =
                    |rows: &[JobHistoryRow]| rows.iter().map(|row| row.job_id).collect::<Vec<_>>();
                assert_eq!(ids(&read), ids(&sorted), "{order:?}");
            }
        }
    }
}
