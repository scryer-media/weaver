// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::model::{HistoryFilter, HistoryOrder, HistoryOrderKey};
use weaver_server_db::persistence::{
    StateError,
    sql_runtime::{SqlArg, SqlEngine, SqlRuntime, StoreDatastore, max_rows_for_engine},
};

pub fn append_history_from_clause(
    sql: &mut String,
    args: &mut Vec<SqlArg>,
    filter: &HistoryFilter,
) {
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

pub fn append_history_row_filter_predicates(
    sql: &mut String,
    args: &mut Vec<SqlArg>,
    filter: &HistoryFilter,
    engine: SqlEngine,
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
    // A tag is looked up through `idx_job_tags_tag`; no tag at all, through
    // the table's key, which starts with the job id.
    if let Some(tag) = &filter.tag {
        let tag = tag.trim();
        if tag.is_empty() {
            sql.push_str(" AND NOT EXISTS (SELECT 1 FROM job_tags jt WHERE jt.job_id = h.job_id)");
        } else {
            sql.push_str(
                " AND EXISTS (SELECT 1 FROM job_tags jt WHERE jt.tag = {} AND jt.job_id = h.job_id)",
            );
            args.push(SqlArg::Text(tag.to_owned()));
        }
    }
    if let Some(since) = filter.completed_since {
        sql.push_str(" AND h.completed_at >= {} AND LOWER(h.status) <> 'paused'");
        args.push(SqlArg::I64(since));
    }
    if let Some(categories) = &filter.categories_any {
        append_in_i64_or_text_filter(
            sql,
            args,
            "COALESCE(h.category, '')",
            categories.iter().cloned().map(SqlArg::Text).collect(),
        );
    }
    if let Some(protocol) = filter.protocol {
        sql.push_str(" AND h.protocol = {}");
        args.push(SqlArg::Text(protocol.as_str().to_owned()));
    }
    if let Some(needle) = &filter.name_contains {
        // The search, normalized as each row's `search_key` is, found in it
        // by LIKE: accents, case and separators aside, in any of the row's
        // titles. A `%`, `_` or `\` in the search is escaped, so it matches
        // only itself. Postgres also takes a close spelling through pg_trgm
        // where migration 88 could install it.
        let needle = super::attributes::normalize_history_search_text(needle);
        match engine {
            SqlEngine::Sqlite => {
                sql.push_str(" AND h.search_key LIKE {} ESCAPE '\\'");
                args.push(SqlArg::Text(history_like_pattern(&needle)));
            }
            SqlEngine::Postgres => {
                sql.push_str(
                    " AND (h.search_key LIKE {} ESCAPE '\\' OR weaver_history_fuzzy({}, h.search_key))",
                );
                args.push(SqlArg::Text(history_like_pattern(&needle)));
                args.push(SqlArg::Text(needle));
            }
        }
    }
    if filter.nzbget_shown_only {
        sql.push_str(" AND h.nzbget_hidden = 0");
    }
}

// The LIKE pattern that finds `needle` anywhere in a name.
fn history_like_pattern(needle: &str) -> String {
    let mut pattern = String::with_capacity(needle.len() + 2);
    pattern.push('%');
    for c in needle.chars() {
        if matches!(c, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    pattern.push('%');
    pattern
}

// ASCII upper and lower case, for Postgres's `translate`. SQLite's LOWER
// folds ASCII letters alone, so both engines fold exactly these, and the
// `C` collation makes Postgres compare bytes as SQLite does.
const PG_ASCII_UPPER: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const PG_ASCII_LOWER: &str = "abcdefghijklmnopqrstuvwxyz";

// The expression `key` orders rows by. Each is the expression of its history
// index in migration 86, or 87 for the name, character for character, so the
// index serves it. The name is the full release name.
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
        HistoryOrderKey::Name => fold("h.release_name"),
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
pub fn history_order_sql(order: HistoryOrder, engine: SqlEngine) -> String {
    let direction = if order.descending { "DESC" } else { "ASC" };
    format!(
        " ORDER BY {} {direction}, h.job_id {direction}",
        history_order_expression(order.key, engine)
    )
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
pub enum HistoryIdSplit {
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
    pub fn statements(&self, base: &HistoryFilter) -> Vec<(HistoryFilter, i64)> {
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
pub fn split_history_ids(
    engine: SqlEngine,
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

pub async fn count_history_rows(
    datastore: &StoreDatastore,
    filter: &HistoryFilter,
) -> Result<i64, StateError> {
    let mut sql = String::from("SELECT COUNT(*) AS count FROM ");
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter, datastore.engine());

    Ok(
        SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args)
            .await?
            .map(|row| row.i64("count"))
            .transpose()?
            .unwrap_or(0),
    )
}

pub async fn count_history_rows_by_category(
    datastore: &StoreDatastore,
    filter: &HistoryFilter,
) -> Result<Vec<(String, i64)>, StateError> {
    count_history_rows_grouped(datastore, filter, "COALESCE(h.category, '')").await
}

// How many history rows each class of job holds, as its stored key (`nzb`,
// `torrent`) and count.
pub async fn count_history_rows_by_protocol(
    datastore: &StoreDatastore,
    filter: &HistoryFilter,
) -> Result<Vec<(String, i64)>, StateError> {
    count_history_rows_grouped(datastore, filter, "h.protocol").await
}

async fn count_history_rows_grouped(
    datastore: &StoreDatastore,
    filter: &HistoryFilter,
    key: &str,
) -> Result<Vec<(String, i64)>, StateError> {
    let mut sql = format!("SELECT {key} AS grouped, COUNT(*) AS count FROM ");
    let mut args = Vec::new();

    append_history_from_clause(&mut sql, &mut args, filter);
    append_history_row_filter_predicates(&mut sql, &mut args, filter, datastore.engine());

    sql.push_str(&format!(" GROUP BY {key}"));

    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
    rows.into_iter()
        .map(|row| Ok((row.text("grouped")?, row.i64("count")?)))
        .collect()
}

pub fn append_history_pagination(sql: &mut String, args: &mut Vec<SqlArg>, filter: &HistoryFilter) {
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

// What an archive, and the job page's file list, need of one history
// row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryFileState {
    pub output_dir: Option<String>,
    pub archived_at: Option<i64>,
    // How many files the folder held when the entry was archived, and their
    // bytes; set by an archive that recorded the folder.
    pub archived_file_count: Option<u64>,
    pub archived_file_bytes: Option<u64>,
}

// The archived-files page read: a range of the primary key, in its order.
pub fn archived_history_files_sql(job_id: u64, from: u64, limit: usize) -> (String, Vec<SqlArg>) {
    (
        "SELECT rel_path, size_bytes FROM job_history_archived_files
          WHERE job_id = {} AND ordinal >= {}
          ORDER BY ordinal LIMIT {}"
            .to_string(),
        vec![
            SqlArg::I64(job_id as i64),
            SqlArg::I64(i64::try_from(from).unwrap_or(i64::MAX)),
            SqlArg::I64(i64::try_from(limit).unwrap_or(i64::MAX)),
        ],
    )
}

// The scheduled retention read: ids of one status only, newest first, a
// range of the status index, paged on `(completed_at, job_id)`.
pub fn history_status_ids_sql(
    status: &str,
    after: Option<(i64, u64)>,
    limit: usize,
) -> (String, Vec<SqlArg>) {
    let mut sql = String::from("SELECT job_id, completed_at FROM job_history WHERE status = {}");
    let mut args = vec![SqlArg::Text(status.to_owned())];
    if let Some((completed_at, job_id)) = after {
        // The upper bound keeps the read a range of the index.
        sql.push_str(" AND completed_at <= {} AND (completed_at < {} OR job_id < {})");
        args.extend([
            SqlArg::I64(completed_at),
            SqlArg::I64(completed_at),
            SqlArg::I64(job_id as i64),
        ]);
    }
    sql.push_str(" ORDER BY completed_at DESC, job_id DESC LIMIT {}");
    args.push(SqlArg::I64(limit as i64));
    (sql, args)
}

// The retention read: ids only, a range of a completed-at index, paged on
// `(completed_at, job_id)`.
pub fn history_completed_by_sql(
    cutoff: i64,
    after: Option<(i64, u64)>,
    limit: usize,
) -> (String, Vec<SqlArg>) {
    let mut sql =
        String::from("SELECT job_id, completed_at FROM job_history WHERE completed_at <= {}");
    let mut args = vec![SqlArg::I64(cutoff)];
    if let Some((completed_at, job_id)) = after {
        // The lower bound keeps the read a range of the index.
        sql.push_str(" AND completed_at >= {} AND (completed_at > {} OR job_id > {})");
        args.extend([
            SqlArg::I64(completed_at),
            SqlArg::I64(completed_at),
            SqlArg::I64(job_id as i64),
        ]);
    }
    sql.push_str(" ORDER BY completed_at, job_id LIMIT {}");
    args.push(SqlArg::I64(limit as i64));
    (sql, args)
}
