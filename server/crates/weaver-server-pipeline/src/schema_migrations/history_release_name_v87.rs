// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 87, upgrade step: every history row is given its full release
// name.
//
// The History page sorts and searches by the full release name: the renamed
// title, else the original title, else the stored name. It lives in the
// row's metadata, which SQL cannot order by through an index, so 87 stores
// it in `release_name` and this step fills that column for the rows written
// before it. The rows are read and written a page at a time by job id.

use crate::StateError;
use crate::history::history_release_name;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "fill_history_release_names_v87";
// A restore of an older bundle fills this column with the search key 88
// stores, through `history_search_key_v88`.

// Three binds a row in the update, well under either engine's bind limit.
const PAGE_ROWS: i64 = 1_000;

// Run the step on a connection that is already inside the transaction it
// belongs to. Rows whose stored name is already right are not written.
pub(crate) async fn fill_history_release_names(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let mut after = i64::MIN;
    loop {
        let rows = conn
            .fetch_all(
                "SELECT job_id, name, metadata, release_name
                   FROM job_history
                  WHERE job_id > {}
                  ORDER BY job_id
                  LIMIT {}",
                &[SqlArg::I64(after), SqlArg::I64(PAGE_ROWS)],
            )
            .await?;
        let Some(last) = rows.last() else {
            return Ok(());
        };
        after = last.i64("job_id")?;
        let page_len = rows.len();

        let mut changed = Vec::new();
        for row in rows {
            let release_name =
                history_release_name(&row.text("name")?, row.opt_text("metadata")?.as_deref());
            if row.text("release_name")? != release_name {
                changed.push((row.i64("job_id")?, release_name));
            }
        }
        if !changed.is_empty() {
            let cases = vec!["WHEN {} THEN {}"; changed.len()].join(" ");
            let ids = vec!["{}"; changed.len()].join(", ");
            let sql = format!(
                "UPDATE job_history SET release_name = CASE job_id {cases} END WHERE job_id IN ({ids})"
            );
            let mut args = Vec::with_capacity(changed.len() * 3);
            for (job_id, release_name) in &changed {
                args.push(SqlArg::I64(*job_id));
                args.push(SqlArg::Text(release_name.clone()));
            }
            args.extend(changed.iter().map(|(job_id, _)| SqlArg::I64(*job_id)));
            conn.execute(&sql, &args).await?;
        }
        if page_len < PAGE_ROWS as usize {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use super::*;

    #[tokio::test]
    async fn every_row_takes_its_full_release_name() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE job_history (
                 job_id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL,
                 metadata TEXT,
                 release_name TEXT NOT NULL DEFAULT ''
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        // More rows than one page, so the step crosses a page boundary.
        let mut rows = Vec::new();
        for job_id in 1..=(PAGE_ROWS + 3) {
            let (metadata, expected) = match job_id % 4 {
                0 => (None, format!("Short {job_id}")),
                1 => (
                    Some(format!(
                        r#"[["weaver.original_title","Full.Release.{job_id}.1080p"]]"#
                    )),
                    format!("Full.Release.{job_id}.1080p"),
                ),
                2 => (
                    Some(format!(
                        r#"[["weaver.original_title","Full.{job_id}"],["weaver.renamed_title"," Renamed {job_id} "]]"#
                    )),
                    format!("Renamed {job_id}"),
                ),
                _ => (Some("not json".to_string()), format!("Short {job_id}")),
            };
            rows.push((job_id, metadata, expected));
        }
        for (job_id, metadata, _) in &rows {
            sqlx::query("INSERT INTO job_history (job_id, name, metadata) VALUES (?, ?, ?)")
                .bind(job_id)
                .bind(format!("Short {job_id}"))
                .bind(metadata)
                .execute(&pool)
                .await
                .unwrap();
        }

        let mut conn = pool.acquire().await.unwrap();
        fill_history_release_names(&mut SqlConn::Sqlite(&mut conn))
            .await
            .unwrap();

        let stored: Vec<(i64, String)> =
            sqlx::query_as("SELECT job_id, release_name FROM job_history ORDER BY job_id")
                .fetch_all(&mut *conn)
                .await
                .unwrap();
        let expected: Vec<(i64, String)> = rows
            .into_iter()
            .map(|(job_id, _, expected)| (job_id, expected))
            .collect();
        assert_eq!(stored, expected);
    }
}
