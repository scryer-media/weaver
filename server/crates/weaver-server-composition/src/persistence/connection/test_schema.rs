// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::{SystemTime, UNIX_EPOCH};

// A schema of its own for `test_name` on the server WEAVER_TEST_POSTGRES_URL
// names, as (admin pool, schema, URL of the schema); none when it names none.
// The one place a test outside this module opens a PostgreSQL connection.
pub async fn create_postgres_test_schema(
    test_name: &str,
) -> Option<(sqlx::PgPool, String, String)> {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is not set");
        return None;
    };
    if base_url.trim().is_empty() {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is empty");
        return None;
    }

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("weaver_test_{test_name}_{}_{}", std::process::id(), suffix);
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    execute_schema_ddl(
        &admin_pool,
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
    )
    .await;
    execute_schema_ddl(&admin_pool, format!("CREATE SCHEMA {schema}")).await;

    let target_url = postgres_url_for_schema(&base_url, &schema);
    Some((admin_pool, schema, target_url))
}

pub async fn execute_schema_ddl(pool: &sqlx::PgPool, sql: String) {
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap();
}

fn postgres_url_for_schema(base_url: &str, schema: &str) -> String {
    let separator = if base_url.contains('?') { '&' } else { '?' };
    format!("{base_url}{separator}options=-csearch_path%3D{schema}")
}
