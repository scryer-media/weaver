// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `database.json`: how a SQLite database is stored, the sizes of its files
// and how its pages are laid out. The package is asked for when the database
// may be stalled, so the two halves are read apart: the file sizes need no
// database and are always written; the page facts wait on the read executor,
// and only for a short while. A value that cannot be read is `null`, named
// with a reason code in the file's `errors`, and an error entry in the
// manifest; the package never fails for it. Another engine has no such
// facts, and the file is left out.

use std::future::Future;
use std::time::Duration;

use serde::Serialize;

use weaver_server_core::Database;
use weaver_server_core::persistence::connection::{
    SqliteFileSize, SqliteFileSizes, SqlitePageFacts,
};

use super::archive::Component;
use super::bounded::{self, REASON_READ_FAILED, REASON_TIMED_OUT};

pub(super) const FILE: &str = "database.json";

// Why a value is missing, beside `timed_out` and `read_failed`:
// a file's size could not be read.
pub(super) const REASON_UNREADABLE: &str = "unreadable";

pub(super) async fn collect_database_facts(db: &Database) -> Vec<Component> {
    let sizes = {
        let db = db.clone();
        tokio::task::spawn_blocking(move || db.sqlite_file_sizes()).await
    };
    let sizes = match sizes {
        Ok(Some(sizes)) => sizes,
        Ok(None) => return Vec::new(),
        Err(error) => {
            return vec![Component::failed(
                FILE,
                format!("the database file sizes could not be read: {error}"),
            )];
        }
    };
    let db = db.clone();
    let page_facts = bounded::blocking(move || Ok(db.sqlite_page_facts()));
    collect_within(sizes, page_facts, bounded::DATABASE_WAIT).await
}

pub(super) async fn collect_within(
    sizes: SqliteFileSizes,
    page_facts: impl Future<Output = Result<Option<SqlitePageFacts>, String>>,
    wait: Duration,
) -> Vec<Component> {
    let deadline = tokio::time::Instant::now() + wait;
    let pages = match bounded::within(deadline, page_facts).await {
        Ok(Some(facts)) => PageOutcome::Read(facts),
        // The engine was SQLite a moment ago; a page read that now finds
        // another has nothing to report.
        Ok(None) => PageOutcome::Failed("the database engine is not SQLite".to_string()),
        Err(refused) if refused.reason == REASON_TIMED_OUT => PageOutcome::TimedOut(wait),
        Err(refused) => PageOutcome::Failed(refused.message),
    };
    database_components(&sizes, &pages)
}

pub(super) enum PageOutcome {
    Read(SqlitePageFacts),
    Failed(String),
    TimedOut(Duration),
}

#[derive(Serialize)]
struct DatabaseFacts<'a> {
    engine: &'static str,
    files: DatabaseFiles,
    page_size: Option<u64>,
    page_count: Option<u64>,
    freelist_count: Option<u64>,
    journal_mode: Option<&'a str>,
    // The values that could not be read, each by name, with why.
    errors: Vec<ValueError>,
}

#[derive(Serialize)]
struct DatabaseFiles {
    database: FileSize,
    wal: FileSize,
    shm: FileSize,
}

// `state` is `present`, `absent` (no such file: SQLite keeps no `-wal` or
// `-shm` while none is open) or `unreadable`.
#[derive(Serialize)]
struct FileSize {
    state: &'static str,
    bytes: Option<u64>,
}

#[derive(Serialize)]
struct ValueError {
    value: &'static str,
    reason: &'static str,
    error: String,
}

const PAGE_VALUES: [&str; 4] = ["page_size", "page_count", "freelist_count", "journal_mode"];

pub(super) fn database_components(sizes: &SqliteFileSizes, pages: &PageOutcome) -> Vec<Component> {
    let mut errors = Vec::new();
    let files = DatabaseFiles {
        database: file_size(&mut errors, "database_bytes", &sizes.database_file),
        wal: file_size(&mut errors, "wal_bytes", &sizes.wal_file),
        shm: file_size(&mut errors, "shm_bytes", &sizes.shm_file),
    };
    let (page_size, page_count, freelist_count, journal_mode) = match pages {
        PageOutcome::Read(facts) => (
            value_or_error(&mut errors, "page_size", &facts.page_size).copied(),
            value_or_error(&mut errors, "page_count", &facts.page_count).copied(),
            value_or_error(&mut errors, "freelist_count", &facts.freelist_count).copied(),
            value_or_error(&mut errors, "journal_mode", &facts.journal_mode).map(String::as_str),
        ),
        PageOutcome::Failed(error) => {
            for value in PAGE_VALUES {
                errors.push(ValueError {
                    value,
                    reason: REASON_READ_FAILED,
                    error: error.clone(),
                });
            }
            (None, None, None, None)
        }
        PageOutcome::TimedOut(wait) => {
            for value in PAGE_VALUES {
                errors.push(ValueError {
                    value,
                    reason: REASON_TIMED_OUT,
                    error: format!("the database did not answer within {}ms", wait.as_millis()),
                });
            }
            (None, None, None, None)
        }
    };

    let mut components: Vec<Component> = errors
        .iter()
        .map(|failed| {
            Component::failed_because(
                FILE,
                failed.reason,
                format!("{} could not be read: {}", failed.value, failed.error),
            )
        })
        .collect();
    components.push(Component::json(
        FILE,
        &DatabaseFacts {
            engine: "sqlite",
            files,
            page_size,
            page_count,
            freelist_count,
            journal_mode,
            errors,
        },
    ));
    components
}

fn file_size(errors: &mut Vec<ValueError>, value: &'static str, size: &SqliteFileSize) -> FileSize {
    match size {
        SqliteFileSize::Present(bytes) => FileSize {
            state: "present",
            bytes: Some(*bytes),
        },
        SqliteFileSize::Absent => FileSize {
            state: "absent",
            bytes: None,
        },
        SqliteFileSize::Unreadable(error) => {
            errors.push(ValueError {
                value,
                reason: REASON_UNREADABLE,
                error: error.clone(),
            });
            FileSize {
                state: "unreadable",
                bytes: None,
            }
        }
    }
}

fn value_or_error<'a, T>(
    errors: &mut Vec<ValueError>,
    value: &'static str,
    read: &'a Result<T, String>,
) -> Option<&'a T> {
    match read {
        Ok(read) => Some(read),
        Err(error) => {
            errors.push(ValueError {
                value,
                reason: REASON_READ_FAILED,
                error: error.clone(),
            });
            None
        }
    }
}
