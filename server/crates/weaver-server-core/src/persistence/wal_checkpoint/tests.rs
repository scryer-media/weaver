// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::{Path, PathBuf};

use sqlx::{ConnectOptions, Connection};

use crate::persistence::sql_runtime::{SqlRuntime, StoreDatastore};
use crate::persistence::sql_services::{
    SqliteServices, sqlite_connect_options, sqlite_url_with_create,
};
use crate::persistence::wal_checkpoint::WalSignal;
use crate::schema_migrations::MigrationMode;

// Small enough that a few commits cross it; each insert below is about 17
// frames at 4 KiB pages.
const THRESHOLD: u32 = 64;
const INSERT: &str = "INSERT INTO blob (b) VALUES (zeroblob(65536))";

struct Fixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
    services: SqliteServices,
    wal: &'static WalSignal,
    // Checkpoints the task ran before the test began.
    runs: u64,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("weaver.db");
    let services =
        SqliteServices::open_sqlite(&path.to_string_lossy(), MigrationMode::Apply, THRESHOLD)
            .await
            .unwrap();
    let wal = services.wal_signal().unwrap();
    let fixture = Fixture {
        _dir: dir,
        path,
        services,
        wal,
        runs: 0,
    };
    fixture.write("CREATE TABLE blob (b BLOB)").await;
    // The migrations leave their frames in the WAL, so that commit may
    // already have asked the task for a checkpoint: let it finish, then
    // start from an empty WAL.
    fixture.wal.caught_up().await;
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(fixture.services.pool())
        .await
        .unwrap();
    let runs = fixture.wal.runs();
    Fixture { runs, ..fixture }
}

impl Fixture {
    fn datastore(&self) -> StoreDatastore {
        self.services.datastore()
    }

    async fn write(&self, template: &str) {
        SqlRuntime::execute(self.datastore().write_exec(), template, &[])
            .await
            .unwrap();
    }

    fn file_len(&self) -> u64 {
        file_len(&self.path)
    }

    fn runs(&self) -> u64 {
        self.wal.runs() - self.runs
    }
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

// The checkpoint task is held, so any checkpoint would have to come from
// the commit. The commit that takes the WAL past the threshold returns
// with nothing copied into the database file: the file has not grown, and
// the WAL holds every frame. The next write waits for the task; once the
// task runs, the file has the pages and that write restarts the WAL.
#[tokio::test(flavor = "multi_thread")]
async fn a_gated_commit_never_checkpoints() {
    let fixture = fixture().await;
    let start = fixture.file_len();
    fixture.wal.hold(true);

    let mut previous = 0;
    loop {
        fixture.write(INSERT).await;
        let frames = fixture.wal.frames();
        assert!(frames > previous, "the WAL restarted with no checkpoint");
        previous = frames;
        if frames >= THRESHOLD {
            break;
        }
    }
    assert_eq!(fixture.file_len(), start, "a commit copied pages back");
    assert_eq!(fixture.runs(), 0);

    let next = {
        let datastore = fixture.datastore();
        tokio::spawn(async move {
            SqlRuntime::execute(datastore.write_exec(), INSERT, &[])
                .await
                .map(|_| ())
        })
    };
    let StoreDatastore::Sqlite { writer_gate, .. } = fixture.datastore() else {
        unreachable!("a file database is SQLite");
    };
    writer_gate.until_waiting(1).await;
    assert_eq!(fixture.file_len(), start);

    fixture.wal.hold(false);
    next.await.unwrap().unwrap();
    assert_eq!(fixture.runs(), 1);
    assert!(fixture.file_len() > start, "the task did not checkpoint");
    assert!(
        fixture.wal.frames() < previous,
        "the write after the checkpoint did not restart the WAL"
    );
}

// The same crossing on a connection that keeps SQLite's autocheckpoint
// copies pages back inside the commit, so the file grows: the observation
// above would catch a commit that checkpoints.
#[tokio::test]
async fn an_autocheckpoint_commit_grows_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control.db");
    let url = sqlite_url_with_create(&path.to_string_lossy());
    let mut connection = sqlite_connect_options(&url, false)
        .unwrap()
        .pragma("wal_autocheckpoint", THRESHOLD.to_string())
        .connect()
        .await
        .unwrap();
    sqlx::query("CREATE TABLE blob (b BLOB)")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut connection)
        .await
        .unwrap();
    let start = file_len(&path);
    let mut written = 0;
    while file_len(&path) == start {
        sqlx::query(INSERT).execute(&mut connection).await.unwrap();
        written += 1;
        assert!(written <= 2 * THRESHOLD, "no commit checkpointed");
    }
    connection.close().await.unwrap();
}

// Commits keep coming with nothing held. Every commit that crosses the
// threshold is followed by a checkpoint before the next write, so the WAL
// restarts again and again and never holds more than the threshold plus
// one transaction's frames.
#[tokio::test(flavor = "multi_thread")]
async fn the_wal_stays_within_the_threshold_and_one_transaction() {
    let fixture = fixture().await;
    let mut previous = 0;
    let mut peak = 0;
    let mut largest_transaction = 0;
    let mut restarts = 0;
    for _ in 0..60 {
        fixture.write(INSERT).await;
        let frames = fixture.wal.frames();
        if frames > previous {
            largest_transaction = largest_transaction.max(frames - previous);
        } else {
            restarts += 1;
            largest_transaction = largest_transaction.max(frames);
        }
        peak = peak.max(frames);
        previous = frames;
    }
    assert!(restarts >= 5, "the WAL restarted only {restarts} times");
    assert!(
        peak <= THRESHOLD + largest_transaction,
        "the WAL reached {peak} frames, threshold {THRESHOLD}, largest commit {largest_transaction}"
    );
}

// The last handle to the database runs one more checkpoint as it drops.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_runs_one_checkpoint() {
    let fixture = fixture().await;
    fixture.write(INSERT).await;
    let (wal, before) = (fixture.wal, fixture.wal.runs());
    assert_eq!(fixture.runs(), 0);
    drop(fixture);
    assert_eq!(wal.runs(), before + 1);
}

// Pool connections leave checkpoints to the task.
#[tokio::test]
async fn pool_connections_do_not_autocheckpoint() {
    let fixture = fixture().await;
    let (pages,): (i64,) = sqlx::query_as("PRAGMA wal_autocheckpoint")
        .fetch_one(fixture.services.pool())
        .await
        .unwrap();
    assert_eq!(pages, 0);
}
