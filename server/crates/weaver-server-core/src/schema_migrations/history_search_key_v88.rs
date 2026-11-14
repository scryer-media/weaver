// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 88, upgrade step: every history row is given its search key,
// and Postgres is given trigram matching for the History search.
//
// The History search finds its text in any of a row's titles, accents and
// case and separators aside (`history_search_columns`). Rust builds that
// key, so 88 stores it in `search_key` and this step fills it, with the full
// release name 87 stores, for the rows written before it. A restore of an
// older bundle runs the same fill.
//
// On Postgres the search also matches a close spelling through pg_trgm.
// Installing the extension needs a privilege a database role may not have,
// so it is tried under a savepoint. `weaver_history_fuzzy` is the one name
// the search calls: the trigram match when the extension is there, and
// false when it is not, so the search falls back to its LIKE alone.

use crate::StateError;
use crate::history::history_search_columns;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "fill_history_search_keys_v88";
// The schema that first stores a history row's search key. A backup taken
// below it restores rows without one.
pub(crate) const SCHEMA_VERSION: i64 = 88;

// Five binds a row in the update, well under either engine's bind limit.
const PAGE_ROWS: i64 = 1_000;

const TRIGRAM_FUZZY: &str =
    "CREATE OR REPLACE FUNCTION weaver_history_fuzzy(needle text, search_key text)
    RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$ SELECT needle <% search_key $$";
const TRIGRAM_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_job_history_search_key_trgm
    ON job_history USING gin (search_key gin_trgm_ops)";
const NO_FUZZY: &str =
    "CREATE OR REPLACE FUNCTION weaver_history_fuzzy(needle text, search_key text)
    RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$ SELECT false $$";

// The migration step: the stored columns, then on Postgres the fuzzy match.
pub(crate) async fn fill_history_search_keys_and_fuzzy(
    conn: &mut SqlConn<'_>,
) -> Result<(), StateError> {
    fill_history_search_columns(conn).await?;
    if matches!(conn, SqlConn::Postgres(_)) {
        install_postgres_fuzzy(conn).await?;
    }
    Ok(())
}

async fn install_postgres_fuzzy(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    conn.execute("SAVEPOINT weaver_pg_trgm", &[]).await?;
    match conn
        .execute("CREATE EXTENSION IF NOT EXISTS pg_trgm", &[])
        .await
    {
        Ok(_) => {
            conn.execute("RELEASE SAVEPOINT weaver_pg_trgm", &[])
                .await?;
            conn.execute(TRIGRAM_FUZZY, &[]).await?;
            conn.execute(TRIGRAM_INDEX, &[]).await?;
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "pg_trgm could not be installed; the History search matches exact text only"
            );
            conn.execute("ROLLBACK TO SAVEPOINT weaver_pg_trgm", &[])
                .await?;
            conn.execute("RELEASE SAVEPOINT weaver_pg_trgm", &[])
                .await?;
            conn.execute(NO_FUZZY, &[]).await?;
        }
    }
    Ok(())
}

// Sets `release_name` and `search_key` on every history row from its name
// and metadata, a page at a time by job id. Rows already right are not
// written. Run on a connection already inside its transaction.
pub(crate) async fn fill_history_search_columns(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let mut after = i64::MIN;
    loop {
        let rows = conn
            .fetch_all(
                "SELECT job_id, name, metadata, release_name, search_key
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
            let (release_name, search_key) =
                history_search_columns(&row.text("name")?, row.opt_text("metadata")?.as_deref());
            if row.text("release_name")? != release_name || row.text("search_key")? != search_key {
                changed.push((row.i64("job_id")?, release_name, search_key));
            }
        }
        if !changed.is_empty() {
            let cases = vec!["WHEN {} THEN {}"; changed.len()].join(" ");
            let ids = vec!["{}"; changed.len()].join(", ");
            let sql = format!(
                "UPDATE job_history
                    SET release_name = CASE job_id {cases} END,
                        search_key = CASE job_id {cases} END
                  WHERE job_id IN ({ids})"
            );
            let mut args = Vec::with_capacity(changed.len() * 5);
            for (job_id, release_name, _) in &changed {
                args.push(SqlArg::I64(*job_id));
                args.push(SqlArg::Text(release_name.clone()));
            }
            for (job_id, _, search_key) in &changed {
                args.push(SqlArg::I64(*job_id));
                args.push(SqlArg::Text(search_key.clone()));
            }
            args.extend(changed.iter().map(|(job_id, _, _)| SqlArg::I64(*job_id)));
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
    async fn every_row_takes_its_release_name_and_search_key() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE job_history (
                 job_id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL,
                 metadata TEXT,
                 release_name TEXT NOT NULL DEFAULT '',
                 search_key TEXT NOT NULL DEFAULT ''
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        // More rows than one page, so the step crosses a page boundary.
        let mut rows = Vec::new();
        for job_id in 1..=(PAGE_ROWS + 3) {
            let metadata = match job_id % 3 {
                0 => None,
                1 => Some(format!(
                    r#"[["weaver.original_title","Été.Release.{job_id}.1080p"]]"#
                )),
                _ => Some("not json".to_string()),
            };
            let name = format!("Short {job_id}");
            let expected = history_search_columns(&name, metadata.as_deref());
            rows.push((job_id, name, metadata, expected));
        }
        for (job_id, name, metadata, _) in &rows {
            sqlx::query("INSERT INTO job_history (job_id, name, metadata) VALUES (?, ?, ?)")
                .bind(job_id)
                .bind(name)
                .bind(metadata)
                .execute(&pool)
                .await
                .unwrap();
        }

        let mut conn = pool.acquire().await.unwrap();
        fill_history_search_keys_and_fuzzy(&mut SqlConn::Sqlite(&mut conn))
            .await
            .unwrap();

        let stored: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT job_id, release_name, search_key FROM job_history ORDER BY job_id",
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();
        let expected: Vec<(i64, String, String)> = rows
            .into_iter()
            .map(|(job_id, _, _, (release_name, key))| (job_id, release_name, key))
            .collect();
        assert_eq!(stored, expected);
        // A row's titles, normalized, one a line.
        let lines: Vec<&str> = stored[0].2.lines().collect();
        assert!(lines.contains(&"ete release 1 1080p"), "{lines:?}");
        assert!(lines.contains(&"short 1"), "{lines:?}");
    }

    #[test]
    fn titles_and_searches_normalize_alike() {
        use crate::history::normalize_history_search_text as norm;
        assert_eq!(norm("  Été.2024_1080p--WEB "), "ete 2024 1080p web");
        assert_eq!(norm("The.Movie"), norm("the movie"));
        assert_eq!(norm("Ünïcödé"), "unicode");
        assert_eq!(norm("..."), "");
    }
}
