// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::Database;
use crate::jobs::ids::JobId;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, StoreDatastore, sql_writes};

async fn open() -> Database {
    tokio::task::spawn_blocking(Database::open_in_memory)
        .await
        .unwrap()
        .unwrap()
}

#[test]
fn every_write_kind_is_a_write() {
    for write in [
        "INSERT INTO t VALUES (1)",
        "  update t SET v = 1",
        "DELETE FROM t",
        "REPLACE INTO t VALUES (1)",
        "CREATE TABLE t (v)",
        "DROP TABLE t",
        "ALTER TABLE t RENAME TO u",
        "VACUUM",
        "ANALYZE t",
        "WITH x AS (SELECT 1) UPDATE t SET v = 1",
        "PRAGMA user_version = 3",
        "PRAGMA incremental_vacuum(10)",
        "-- note\nINSERT INTO t VALUES (1)",
    ] {
        assert!(sql_writes(write), "{write}");
    }
    for read in [
        "SELECT * FROM t",
        "select updated_at FROM t",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "PRAGMA page_count",
        "PRAGMA table_info(t)",
        "PRAGMA wal_checkpoint(PASSIVE)",
        "VALUES (1)",
    ] {
        assert!(!sql_writes(read), "{read}");
    }
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "outside the writer gate")]
async fn the_read_executor_refuses_a_write() {
    let db = open().await;
    let datastore = db.datastore();
    let _ = SqlRuntime::execute(
        datastore.read_exec(),
        "UPDATE api_keys SET last_used_at = 1",
        &[],
    )
    .await;
}

// The writer gate is held and SQLite's write lock is taken, as a gated
// transaction holds both. Every kind of autocommit write, and real callers
// that used to write on the raw pool, park on the gate, take nothing from
// SQLite, and land once the transaction commits: none of them can spend
// busy_timeout in SQLite's busy handler and fail with "database is locked".
#[tokio::test(flavor = "multi_thread")]
async fn a_write_waits_for_the_gate_instead_of_failing_busy() {
    let db = open().await;
    let key = {
        let db = db.clone();
        tokio::task::spawn_blocking(move || db.insert_api_key("pin", &[7; 32], "admin"))
            .await
            .unwrap()
            .unwrap()
    };
    let datastore = db.datastore();
    SqlRuntime::execute(datastore.write_exec(), "CREATE TABLE pin (v INTEGER)", &[])
        .await
        .unwrap();
    let StoreDatastore::Sqlite { pool, writer_gate } = datastore.clone() else {
        unreachable!("open_in_memory is SQLite");
    };

    let held = writer_gate.lock().await;
    let mut holder = pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *holder)
        .await
        .unwrap();
    sqlx::query("INSERT INTO pin (v) VALUES (0)")
        .execute(&mut *holder)
        .await
        .unwrap();

    let mut writes = tokio::task::JoinSet::new();
    for template in [
        "INSERT INTO pin (v) VALUES (1)",
        "UPDATE pin SET v = v WHERE v = 0",
        "DELETE FROM pin WHERE v = 99",
    ] {
        let datastore = datastore.clone();
        writes.spawn(async move {
            SqlRuntime::execute(datastore.write_exec(), template, &[])
                .await
                .map(|_| ())
        });
    }
    {
        let datastore = datastore.clone();
        writes.spawn(async move {
            SqlRuntime::execute_returning(
                datastore.write_exec(),
                "INSERT INTO pin (v) VALUES ({}) RETURNING v",
                &[SqlArg::I64(2)],
            )
            .await
            .map(|_| ())
        });
    }
    {
        let db = db.clone();
        writes.spawn_blocking(move || db.touch_api_key_last_used(key, 42));
    }
    {
        let db = db.clone();
        writes.spawn_blocking(move || db.save_active_job_root_phase(JobId(1), "downloading"));
    }
    // Database calls share one serialized writer thread, so of the two only
    // the one that reached the thread first can park on the gate; the other
    // waits its turn on the thread and lands after it.
    let parked = writes.len() - 1;
    writer_gate.until_waiting(parked).await;

    sqlx::query("COMMIT").execute(&mut *holder).await.unwrap();
    drop(holder);
    drop(held);
    while let Some(write) = writes.join_next().await {
        write.unwrap().unwrap();
    }

    let rows = SqlRuntime::fetch_all(datastore.read_exec(), "SELECT v FROM pin ORDER BY v", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.i64("v").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows, [0, 1, 2]);
    let used = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT last_used_at FROM api_keys WHERE id = {}",
        &[SqlArg::I64(key)],
    )
    .await
    .unwrap()
    .unwrap()
    .opt_i64("last_used_at")
    .unwrap();
    assert_eq!(used, Some(42));
}
