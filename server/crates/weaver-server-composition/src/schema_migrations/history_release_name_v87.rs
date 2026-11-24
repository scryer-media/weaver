// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::history_release_name_v87::*;

#[cfg(test)]
mod tests {
    use crate::persistence::sql_runtime::SqlConn;
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
