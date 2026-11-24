// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::settings::persistence::SettingsPersistenceQueries;

fn execute_sql(db: &Database, sql: &'static str) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.write_exec(), sql, &[]).await?;
        Ok(())
    })
    .unwrap();
}

fn exact_count(db: &Database, table: &str) -> u64 {
    let datastore = db.datastore();
    let sql = format!("SELECT COUNT(*) FROM {}", quoted(table, '"'));
    db.run_sql_blocking_read(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &[])
            .await?
            .expect("a count row");
        Ok(row.i64_at(0)?.max(0) as u64)
    })
    .unwrap()
}

// Each figure bounds the exact count from above, a table without a rowid
// and SQLite's own are left out, and the estimate appears once ANALYZE
// has run.
#[test]
fn every_table_is_bounded_from_above_without_a_count() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("probe.one", "1").unwrap();
    db.set_setting("probe.two", "2").unwrap();
    execute_sql(
        &db,
        "CREATE TABLE probe_rows (id INTEGER PRIMARY KEY, value TEXT)",
    );
    execute_sql(
        &db,
        "INSERT INTO probe_rows (value) VALUES ('a'), ('b'), ('c'), ('d')",
    );
    execute_sql(&db, "DELETE FROM probe_rows WHERE value = 'b'");
    execute_sql(
        &db,
        "CREATE TABLE probe_keys (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID",
    );

    let sizes = db.diagnostic_table_sizes().expect("sqlite").unwrap();
    assert!(sizes.iter().all(|size| size.table != "probe_keys"));
    assert!(sizes.iter().all(|size| !size.table.starts_with("sqlite_")));
    for size in &sizes {
        let exact = exact_count(&db, &size.table);
        assert!(
            size.rows_upper_bound.unwrap_or(0) >= exact,
            "{size:?} under {exact}"
        );
        assert_eq!(size.rows_estimated_at_last_analyze, None);
    }
    let probe = sizes
        .iter()
        .find(|size| size.table == "probe_rows")
        .expect("probe_rows");
    assert_eq!(probe.rows_upper_bound, Some(4));
    assert_eq!(exact_count(&db, "probe_rows"), 3);

    execute_sql(&db, "ANALYZE");
    let sizes = db.diagnostic_table_sizes().expect("sqlite").unwrap();
    let probe = sizes
        .iter()
        .find(|size| size.table == "probe_rows")
        .expect("probe_rows");
    assert_eq!(probe.rows_upper_bound, Some(4));
    assert_eq!(probe.rows_estimated_at_last_analyze, Some(3));
}

#[test]
fn names_are_quoted_with_their_quotes_doubled() {
    assert_eq!(quoted("job\"s", '"'), "\"job\"\"s\"");
    assert_eq!(quoted("it's", '\''), "'it''s'");
    assert_eq!(
        largest_rowids(&["a".to_string(), "b".to_string()]),
        "SELECT 'a' AS tbl, (SELECT MAX(rowid) FROM \"a\") AS max_rowid UNION ALL \
         SELECT 'b' AS tbl, (SELECT MAX(rowid) FROM \"b\") AS max_rowid"
    );
}
