// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How large each table of a SQLite database is, read for the admin
// diagnostics package without counting a row. A count reads every page of a
// table, and the package is asked for when the database may already be slow,
// so each figure here costs a few page reads whatever the table's size:
//
// - `rows_upper_bound`: the table's largest rowid. Rowids are positive and
//   unique, so the table holds at most that many rows; a deleted row still
//   counts toward it, so it overstates a table that rows leave.
// - `rows_estimated_at_last_analyze`: the row count `ANALYZE` stored in
//   `sqlite_stat1`, as of when it last ran; absent when it never ran for the
//   table.
//
// Every read is on the read executor, in one snapshot, and writes nothing.

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database as SqlDatabase;
use weaver_server_db::persistence::sql_runtime::{SqlRuntime, StoreDatastore};

// SQLite refuses a compound statement of more than 500 parts by default, so
// the per-table reads are joined in pages of this many tables.
const TABLES_PER_STATEMENT: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticTableSize {
    pub table: String,
    pub rows_upper_bound: Option<u64>,
    pub rows_estimated_at_last_analyze: Option<u64>,
}

// A name as an SQL identifier or string literal, its quotes doubled.
pub fn quoted(name: &str, quote: char) -> String {
    let doubled = name.replace(quote, &format!("{quote}{quote}"));
    format!("{quote}{doubled}{quote}")
}

// One statement reading the largest rowid of each table named.
pub fn largest_rowids(tables: &[String]) -> String {
    tables
        .iter()
        .map(|table| {
            format!(
                "SELECT {} AS tbl, (SELECT MAX(rowid) FROM {}) AS max_rowid",
                quoted(table, '\''),
                quoted(table, '"'),
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

pub trait JobsDiagnosticTablesQueries {
    fn diagnostic_table_sizes(&self) -> Option<Result<Vec<DiagnosticTableSize>, StateError>>;
}

impl JobsDiagnosticTablesQueries for SqlDatabase {
    // The tables of the database, by name, each with its figures; `None` for
    // an engine that is not SQLite. Tables without a rowid and SQLite's own
    // are left out.
    fn diagnostic_table_sizes(&self) -> Option<Result<Vec<DiagnosticTableSize>, StateError>> {
        let datastore = self.datastore();
        if !matches!(datastore, StoreDatastore::Sqlite { .. }) {
            return None;
        }
        Some(self.run_sql_blocking_read(async move {
            let exec = || datastore.read_exec();
            let schema = SqlRuntime::fetch_all(
                exec(),
                "SELECT name, COALESCE(sql, '') AS sql FROM sqlite_master
                  WHERE type = 'table' ORDER BY name",
                &[],
            )
            .await?;
            let mut tables = Vec::new();
            let mut analyzed = false;
            for row in &schema {
                let name = row.text("name")?;
                if name == "sqlite_stat1" {
                    analyzed = true;
                }
                let without_rowid = row
                    .text("sql")?
                    .to_ascii_lowercase()
                    .contains("without rowid");
                if !name.starts_with("sqlite_") && !without_rowid {
                    tables.push(name);
                }
            }
            let mut sizes: Vec<DiagnosticTableSize> = Vec::with_capacity(tables.len());
            for page in tables.chunks(TABLES_PER_STATEMENT) {
                for row in SqlRuntime::fetch_all(exec(), &largest_rowids(page), &[]).await? {
                    sizes.push(DiagnosticTableSize {
                        table: row.text("tbl")?,
                        rows_upper_bound: row
                            .opt_i64("max_rowid")?
                            .map(|rowid| rowid.max(0) as u64),
                        rows_estimated_at_last_analyze: None,
                    });
                }
            }
            if analyzed {
                // The first number of a stat is the rows of the table or
                // index it describes; the table's rows are the largest.
                let estimates = SqlRuntime::fetch_all(
                    exec(),
                    "SELECT tbl, MAX(CAST(stat AS INTEGER)) AS estimated
                       FROM sqlite_stat1 GROUP BY tbl",
                    &[],
                )
                .await?;
                let mut by_table = std::collections::HashMap::with_capacity(estimates.len());
                for row in &estimates {
                    if let Some(estimated) = row.opt_i64("estimated")? {
                        by_table.insert(row.text("tbl")?, estimated.max(0) as u64);
                    }
                }
                for size in &mut sizes {
                    size.rows_estimated_at_last_analyze = by_table.get(&size.table).copied();
                }
            }
            Ok(sizes)
        }))
    }
}
