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

// A read transaction on a pool connection, open on the snapshot it read.
struct Reader(sqlx::pool::PoolConnection<sqlx::Sqlite>);

impl Reader {
    async fn open(fixture: &Fixture) -> Self {
        let mut connection = fixture.services.pool().acquire().await.unwrap();
        sqlx::query("BEGIN")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("SELECT count(*) FROM blob")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        Self(connection)
    }

    async fn close(mut self) {
        sqlx::query("COMMIT").execute(&mut *self.0).await.unwrap();
    }
}

// What a run of writes under an open read did.
struct Held {
    // Frames the writes added to the WAL.
    written: u32,
    // Checkpoints the task ran for them.
    runs: u64,
}

const HELD_WRITES: usize = 40;

// Writes past the threshold while `reader` stays open. Nothing can restart
// the WAL, so its frame count only grows. Every write waits for the
// checkpoint the one before it asked for, and so does the final wait, so
// every pass asked for has run when this returns. The fixture's TRUNCATE
// reports no frames, so after it the first write may report fewer frames
// than the last commit did: that write starts the WAL from empty.
async fn write_while_held(fixture: &Fixture, _reader: &Reader) -> Held {
    let before = fixture.runs();
    let mut previous = fixture.wal.frames();
    let mut written = 0;
    for write in 0..HELD_WRITES {
        fixture.write(INSERT).await;
        let frames = fixture.wal.frames();
        if frames > previous {
            written += frames - previous;
        } else {
            assert_eq!(write, 0, "the WAL restarted under an open read");
            written += frames;
        }
        previous = frames;
    }
    fixture.wal.caught_up().await;
    assert!(written > 4 * THRESHOLD, "too few frames to judge");
    Held {
        written,
        runs: fixture.runs() - before,
    }
}

// One checkpoint per threshold of new frames, and at most one more for the
// first crossing.
fn assert_passes_per_threshold(held: &Held) {
    let bound = u64::from(held.written / THRESHOLD) + 1;
    assert!(
        held.runs <= bound,
        "{} writes under an open read ran {} checkpoints for {} frames; at most {bound} expected",
        HELD_WRITES,
        held.runs,
        held.written
    );
    assert!(held.runs > 0, "no checkpoint ran past the threshold");
}

// After the read ends, a few writes ask for a checkpoint that copies the
// backlog, and the write after it restarts the WAL.
async fn assert_backlog_copied(fixture: &Fixture, start: u64) {
    let restarts = fixture.wal.metrics_snapshot().restarts;
    let mut previous = fixture.wal.frames();
    let mut writes = 0;
    loop {
        fixture.write(INSERT).await;
        writes += 1;
        let frames = fixture.wal.frames();
        if frames < previous {
            break;
        }
        previous = frames;
        // A threshold of frames asks for the checkpoint; one more write
        // restarts. An insert is more than one frame.
        assert!(
            writes <= THRESHOLD as usize + 1,
            "the WAL did not restart after the read ended"
        );
    }
    assert!(fixture.file_len() > start, "the backlog was not copied");
    let metrics = fixture.wal.metrics_snapshot();
    assert_eq!(metrics.restarts, restarts + 1);
    assert_eq!(metrics.log_frames, metrics.checkpointed_frames);
}

// A read open on an older snapshot keeps the WAL from restarting, so every
// write under it leaves the WAL past the threshold. The writes ask for a
// checkpoint once per threshold of new frames, not once each, and each
// checkpoint leaves the frames the read still needs.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_read_asks_for_one_checkpoint_per_threshold() {
    let fixture = fixture().await;
    // Something in the WAL first, so the read takes a read mark past 0.
    fixture.write(INSERT).await;
    let reader = Reader::open(&fixture).await;
    let passes = fixture.wal.metrics_snapshot().passes;
    let held = write_while_held(&fixture, &reader).await;
    assert_passes_per_threshold(&held);
    let metrics = fixture.wal.metrics_snapshot();
    assert_eq!(metrics.passes - passes, held.runs);
    assert!(metrics.passes_left_behind >= held.runs);
    assert!(metrics.checkpointed_frames < metrics.log_frames);
    assert!(
        metrics.checkpointed_frames > 0,
        "nothing before the read was copied"
    );
    reader.close().await;
}

// Once the read ends, the backlog it held is copied and the WAL restarts.
#[tokio::test(flavor = "multi_thread")]
async fn the_backlog_is_copied_after_the_read_ends() {
    let fixture = fixture().await;
    fixture.write(INSERT).await;
    let reader = Reader::open(&fixture).await;
    let held = write_while_held(&fixture, &reader).await;
    assert_passes_per_threshold(&held);
    let start = fixture.file_len();
    reader.close().await;
    assert_backlog_copied(&fixture, start).await;
}

// A read that began on a fully copied WAL holds read mark 0, and while it
// lasts no checkpoint can copy anything: the database file does not grow.
// The writes still ask once per threshold, and the backlog is copied and
// the WAL restarts once the read ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_on_mark_zero_holds_every_frame() {
    let fixture = fixture().await;
    let start = fixture.file_len();
    let reader = Reader::open(&fixture).await;
    let held = write_while_held(&fixture, &reader).await;
    assert_passes_per_threshold(&held);
    assert_eq!(
        fixture.file_len(),
        start,
        "a checkpoint copied under read mark 0"
    );
    let metrics = fixture.wal.metrics_snapshot();
    assert_eq!(metrics.checkpointed_frames, 0);
    assert_eq!(metrics.copied_frames, 0);
    reader.close().await;
    assert_backlog_copied(&fixture, start).await;
}

// A pass copies the whole WAL while a short read is open on its newest
// snapshot, and that read is still open for the write right after the pass,
// so that write cannot restart the WAL. Once the read ends, the WAL grows
// until another threshold of frames past what the pass saw asks for the
// next pass, and the write after that pass restarts it: the overlap costs at
// most one extra threshold and one transaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_short_read_after_a_complete_pass_delays_the_restart_one_threshold() {
    let fixture = fixture().await;
    // Cross the threshold with the task held, so the crossing commit has
    // asked for a pass that has not run.
    fixture.wal.hold(true);
    let mut previous = 0;
    let mut largest_transaction = 0;
    loop {
        fixture.write(INSERT).await;
        let frames = fixture.wal.frames();
        if frames > previous {
            largest_transaction = largest_transaction.max(frames - previous);
        } else {
            // The fixture's TRUNCATE: this write started the WAL afresh.
            largest_transaction = largest_transaction.max(frames);
        }
        previous = frames;
        if frames >= THRESHOLD {
            break;
        }
    }
    let seen = previous;
    // A read on the newest snapshot: everything it needs is in the WAL, so
    // it holds a read mark past 0, and a pass can still copy every frame.
    let reader = Reader::open(&fixture).await;
    fixture.wal.hold(false);
    fixture.wal.caught_up().await;
    let metrics = fixture.wal.metrics_snapshot();
    assert_eq!(metrics.log_frames, u64::from(seen));
    assert_eq!(
        metrics.checkpointed_frames, metrics.log_frames,
        "the pass did not copy the whole WAL"
    );

    // The write right after the pass overlaps the read: no restart.
    fixture.write(INSERT).await;
    let frames = fixture.wal.frames();
    assert!(frames > seen, "the WAL restarted under an open read");
    largest_transaction = largest_transaction.max(frames - seen);
    previous = frames;
    reader.close().await;

    // No read overlaps from here on.
    let mut peak = previous;
    let mut writes = 0;
    loop {
        fixture.write(INSERT).await;
        writes += 1;
        let frames = fixture.wal.frames();
        if frames < previous {
            break;
        }
        largest_transaction = largest_transaction.max(frames - previous);
        peak = peak.max(frames);
        previous = frames;
        assert!(
            writes <= THRESHOLD as usize + 1,
            "the WAL did not restart once the read ended"
        );
    }
    let grown = peak - seen;
    assert!(
        grown <= THRESHOLD + largest_transaction,
        "the WAL grew {grown} frames past the complete pass before restarting; \
         threshold {THRESHOLD}, largest commit {largest_transaction}"
    );
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
