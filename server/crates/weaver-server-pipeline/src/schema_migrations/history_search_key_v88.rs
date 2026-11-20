// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::history_search_key_v88::*;

#[cfg(test)]
mod tests {
    use crate::history::history_search_columns;
    use crate::persistence::sql_runtime::SqlConn;
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
