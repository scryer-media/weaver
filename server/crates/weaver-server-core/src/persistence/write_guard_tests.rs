// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The guard that keeps every SQLite write on the writer gate. clippy.toml bans
// sqlx's write, transaction and connection entry points outside the
// persistence runtime. These tests pin the ban and every place it is lifted,
// so widening either is a change reviewed here rather than an accident.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

// Built here so this file never counts as an exception itself.
const LIFT: &str = concat!("clippy::", "disallowed", "_methods");

// How a lift of the write ban begins its reason, which tells it apart from a
// lift of the route ban in proxies/route_guard_tests.rs.
const MARK: &str = "\"writer gate:";

// The calls clippy refuses outside the writer gate.
const BANNED: &[&str] = &[
    "sqlx::query::Query::execute",
    "sqlx::query::Query::execute_many",
    "sqlx::Executor::execute",
    "sqlx::Executor::execute_many",
    "sqlx::RawSql::execute",
    "sqlx::RawSql::execute_many",
    "sqlx::Pool::begin",
    "sqlx::Pool::try_begin",
    "sqlx::Pool::begin_with",
    "sqlx::Pool::try_begin_with",
    "sqlx::Connection::begin",
    "sqlx::Connection::begin_with",
    "sqlx::Acquire::begin",
    "sqlx::Transaction::commit",
    "sqlx::Pool::acquire",
    "sqlx::Pool::try_acquire",
    "sqlx::Acquire::acquire",
];

// Every file that lifts the write ban, and how many times. Each lift names why
// the call there is the gate itself, runs under it, reads only, writes another
// file, runs alone at startup or on Postgres, or is outside the gate by ruling.
const LIFTED: &[(&str, usize)] = &[
    (
        "server/crates/weaver-server-core/src/history/persistence.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/history/timeline.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/jobs/diagnostic_names.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/jobs/persistence.rs",
        3,
    ),
    (
        "server/crates/weaver-server-core/src/operations/async_ops.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/operations/backup.rs",
        9,
    ),
    (
        "server/crates/weaver-server-core/src/operations/backup/logical.rs",
        10,
    ),
    (
        "server/crates/weaver-server-core/src/operations/backup/restore.rs",
        2,
    ),
    (
        "server/crates/weaver-server-core/src/operations/backup/upgrade.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/persistence/connection.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/persistence/postgres_migrations.rs",
        8,
    ),
    (
        "server/crates/weaver-server-core/src/persistence/sql_runtime.rs",
        6,
    ),
    (
        "server/crates/weaver-server-core/src/persistence/sql_services.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/persistence/wal_checkpoint.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/src/schema_migrations.rs",
        12,
    ),
    (
        "server/crates/weaver-server-core/tests/browser_session_contract.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/tests/initial_setup_contract.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/tests/network_access_transaction_contract.rs",
        1,
    ),
    (
        "server/crates/weaver-server-core/tests/semantic_promotion.rs",
        1,
    ),
];

// Whether the lift whose lint name ends right before `rest` is one of the
// write ban's: its reason begins with [`MARK`].
pub(crate) fn writer_gate_lift(rest: &str) -> bool {
    rest.split(")]")
        .next()
        .unwrap_or_default()
        .split_once("reason")
        .and_then(|(_, reason)| reason.trim_start().strip_prefix('='))
        .is_some_and(|reason| reason.trim_start().starts_with(MARK))
}

fn workspace() -> PathBuf {
    std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")).unwrap()
}

#[test]
fn clippy_bans_every_ungated_write() {
    let config = std::fs::read_to_string(workspace().join("clippy.toml")).unwrap();
    let configured: BTreeSet<&str> = config
        .split("path = \"")
        .skip(1)
        .filter_map(|rest| rest.split_once('"').map(|(path, _)| path))
        .filter(|path| path.starts_with("sqlx::"))
        .collect();
    let banned: BTreeSet<&str> = BANNED.iter().copied().collect();
    assert_eq!(
        configured, banned,
        "clippy.toml's sqlx bans changed: review the change here"
    );
}

#[test]
fn every_lift_of_the_write_ban_is_pinned() {
    let root = workspace();
    let mut found = BTreeMap::new();
    for dir in ["engines", "server", "shared", "xtask"] {
        count_lifts(&root, &root.join(dir), &mut found);
    }
    let pinned: BTreeMap<String, usize> = LIFTED
        .iter()
        .map(|(path, count)| ((*path).to_owned(), *count))
        .collect();
    assert_eq!(
        found, pinned,
        "the write ban is lifted somewhere new: write through StoreDatastore::write_exec() \
         instead, or pin the lift here once its reason is reviewed"
    );
}

#[test]
fn a_lift_is_the_write_ban_only_by_its_reason() {
    let lift = format!("#[expect({LIFT}, reason = {MARK} this is the gate\")]");
    let wrapped = format!("#[expect(\n    {LIFT},\n    reason = {MARK} the read lane\"\n)]");
    let route = format!("#[expect({LIFT}, reason = \"dials the route itself\")]");
    let crate_root = format!("#![cfg_attr(test, allow({LIFT}))]");
    let after = |text: &str| text.split_once(LIFT).unwrap().1.to_owned();
    assert!(writer_gate_lift(&after(&lift)));
    assert!(writer_gate_lift(&after(&wrapped)));
    assert!(!writer_gate_lift(&after(&route)));
    assert!(!writer_gate_lift(&after(&crate_root)));
}

fn count_lifts(root: &Path, dir: &Path, found: &mut BTreeMap<String, usize>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if !path
                .file_name()
                .is_some_and(|name| name == "target" || name == "node_modules")
            {
                count_lifts(root, &path, found);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let text = std::fs::read_to_string(&path).unwrap();
            let count = text
                .match_indices(LIFT)
                .filter(|(at, _)| writer_gate_lift(&text[at + LIFT.len()..]))
                .count();
            if count > 0 {
                let relative = path.strip_prefix(root).unwrap();
                found.insert(relative.to_string_lossy().replace('\\', "/"), count);
            }
        }
    }
}
