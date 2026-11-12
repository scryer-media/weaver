// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 85: each job's stored NZB moves to `job_nzbs`, from
// `active_jobs.nzb_zstd` for a job in the queue and from `job_history.nzb_zstd`
// for a finished one, each job's outline rows in `active_job_nzb_files` become
// one compressed `active_job_outlines` row, and the two old columns and the
// old table go. Migration 84 made the two tables.
//
// The NZBs are most of a database, so on SQLite they move before the
// migration's own transaction, a few jobs at a time, each batch copied and
// cleared on its old row and then committed: first every active job's, then
// every finished job's. The pages a batch frees hold the next batch's copy,
// the write-ahead log never holds more than one batch, and an upgrade that is
// cut short carries on from the jobs still to move: a batch only takes rows
// that still hold their NZB, and the history move starts again only once no
// active row holds one. The transaction that follows finds none left and drops
// the columns; on Postgres it moves them all.
//
// A job id is in both tables only when a build that restarted a finished job
// stopped between writing its active row and deleting its history row. Both
// rows then held the same bytes, read from the history row, and the active
// row's are kept.
//
// An outline is read as the build below 85 read it: a job's rows in file
// order, from index 0, stopping at the first one out of place; only jobs
// whose active row has a file count are taken. A job whose rows do not read
// gets no outline row and, like any job whose files do not match its count,
// reads its stored NZB once more and stores the outline it reads.

use std::collections::BTreeMap;

use crate::StateError;
use crate::ingest::outline::OutlineFile;
use crate::jobs::outline_persistence::encode_outline_files;
use sqlx::SqlitePool;

use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "move_nzbs_and_outlines_v85";

// Jobs whose NZBs move in one batch: a few tens of MB of NZB at most.
const NZB_BATCH: i64 = 32;
// Jobs whose outlines are compressed in one batch, two binds each.
const OUTLINE_BATCH: i64 = 256;

// The rows a stored NZB moves from, in the order they move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NzbSource {
    Active,
    History,
}

impl NzbSource {
    const ALL: [Self; 2] = [Self::Active, Self::History];

    fn table(self) -> &'static str {
        match self {
            Self::Active => "active_jobs",
            Self::History => "job_history",
        }
    }

    // A job in both tables keeps its active row's NZB, which moves first.
    fn on_conflict(self) -> &'static str {
        match self {
            Self::Active => "",
            Self::History => " ON CONFLICT (job_id) DO NOTHING",
        }
    }
}

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn move_nzbs_and_outlines(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    for source in NzbSource::ALL {
        move_nzbs(conn, source).await?;
    }
    compress_outlines(conn).await?;
    conn.execute("DROP TABLE active_job_nzb_files", &[]).await?;
    conn.execute("ALTER TABLE active_jobs DROP COLUMN nzb_zstd", &[])
        .await?;
    conn.execute("ALTER TABLE job_history DROP COLUMN nzb_zstd", &[])
        .await?;
    Ok(())
}

// Move the NZBs before the migration's transaction, one committed batch at a
// time. Alone at startup, on the migration's one connection.
pub(crate) async fn move_nzbs_in_committed_batches(pool: &SqlitePool) -> Result<(), StateError> {
    for source in NzbSource::ALL {
        move_source_in_committed_batches(pool, source).await?;
    }
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
async fn move_source_in_committed_batches(
    pool: &SqlitePool,
    source: NzbSource,
) -> Result<(), StateError> {
    let mut after = i64::MIN;
    loop {
        let mut tx = pool
            .begin()
            .await
            .map_err(|error| StateError::Database(error.to_string()))?;
        let moved = move_nzb_batch(&mut SqlConn::Sqlite(&mut tx), source, after).await?;
        tx.commit()
            .await
            .map_err(|error| StateError::Database(error.to_string()))?;
        match moved {
            Some(last) => after = last,
            None => return Ok(()),
        }
    }
}

async fn move_nzbs(conn: &mut SqlConn<'_>, source: NzbSource) -> Result<(), StateError> {
    let mut after = i64::MIN;
    while let Some(last) = move_nzb_batch(conn, source, after).await? {
        after = last;
    }
    Ok(())
}

// Move the NZBs of the next jobs after `after` whose row in `source` still
// holds one, and give the last of them; None once no row holds one. The
// batch is read along the table's job id key from `after` on.
async fn move_nzb_batch(
    conn: &mut SqlConn<'_>,
    source: NzbSource,
    after: i64,
) -> Result<Option<i64>, StateError> {
    let table = source.table();
    let ids = conn
        .fetch_all(
            &format!(
                "SELECT job_id FROM {table}
                  WHERE job_id > {{}} AND nzb_zstd IS NOT NULL ORDER BY job_id LIMIT {{}}"
            ),
            &[SqlArg::I64(after), SqlArg::I64(NZB_BATCH)],
        )
        .await?;
    let Some(last) = ids.last().map(|row| row.i64("job_id")).transpose()? else {
        return Ok(None);
    };
    let range = [SqlArg::I64(after), SqlArg::I64(last)];
    conn.execute(
        &format!(
            "INSERT INTO job_nzbs (job_id, nzb_zstd)
             SELECT job_id, nzb_zstd FROM {table}
              WHERE job_id > {{}} AND job_id <= {{}} AND nzb_zstd IS NOT NULL{}",
            source.on_conflict()
        ),
        &range,
    )
    .await?;
    conn.execute(
        &format!(
            "UPDATE {table} SET nzb_zstd = NULL
              WHERE job_id > {{}} AND job_id <= {{}} AND nzb_zstd IS NOT NULL"
        ),
        &range,
    )
    .await?;
    Ok(Some(last))
}

// The last job id of the next batch of outlines after `after`.
async fn batch_end(
    conn: &mut SqlConn<'_>,
    sql: &str,
    after: i64,
    batch: i64,
) -> Result<Option<i64>, StateError> {
    let ids = conn
        .fetch_all(sql, &[SqlArg::I64(after), SqlArg::I64(batch)])
        .await?;
    ids.last().map(|row| row.i64("job_id")).transpose()
}

async fn compress_outlines(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let mut after = i64::MIN;
    while let Some(last) = batch_end(
        conn,
        "SELECT job_id FROM active_jobs
          WHERE job_id > {} AND nzb_files IS NOT NULL ORDER BY job_id LIMIT {}",
        after,
        OUTLINE_BATCH,
    )
    .await?
    {
        let range = [SqlArg::I64(after), SqlArg::I64(last)];
        let rows = conn
            .fetch_all(
                "SELECT f.job_id, f.file_index, f.full_name, f.groups, f.posted_at,
                        f.articles, f.bytes
                   FROM active_job_nzb_files AS f
                   JOIN active_jobs AS a ON a.job_id = f.job_id
                  WHERE f.job_id > {} AND f.job_id <= {} AND a.nzb_files IS NOT NULL
                  ORDER BY f.job_id, f.file_index",
                &range,
            )
            .await?;
        after = last;
        // Each job's files, or None once one of its rows does not read.
        let mut jobs: BTreeMap<i64, Option<Vec<OutlineFile>>> = BTreeMap::new();
        for row in rows {
            let job_id = row.i64("job_id")?;
            let Some(files) = jobs.entry(job_id).or_insert_with(|| Some(Vec::new())) else {
                continue;
            };
            if row.i64("file_index")? != files.len() as i64 {
                continue;
            }
            let Ok(groups) = serde_json::from_str(&row.text("groups")?) else {
                tracing::warn!(
                    job_id,
                    "a job's stored NZB outline could not be read and was not kept"
                );
                jobs.insert(job_id, None);
                continue;
            };
            files.push(OutlineFile {
                full_name: row.text("full_name")?,
                groups,
                date: row.i64("posted_at")? as u64,
                articles: u32::try_from(row.i64("articles")?).unwrap_or(u32::MAX),
                bytes: row.i64("bytes")? as u64,
            });
        }
        // Compressing is CPU work: it runs off the async runtime.
        let blobs = tokio::task::spawn_blocking(move || {
            jobs.into_iter()
                .filter_map(|(job_id, files)| Some((job_id, files?)))
                .map(|(job_id, files)| Ok((job_id, encode_outline_files(&files)?)))
                .collect::<Result<Vec<_>, StateError>>()
        })
        .await
        .map_err(|error| StateError::Database(format!("outline compression failed: {error}")))??;
        if blobs.is_empty() {
            continue;
        }
        let values = vec!["({}, {})"; blobs.len()].join(", ");
        let args: Vec<SqlArg> = blobs
            .into_iter()
            .flat_map(|(job_id, blob)| [SqlArg::I64(job_id), SqlArg::Bytes(blob)])
            .collect();
        conn.execute(
            &format!("INSERT INTO active_job_outlines (job_id, outline) VALUES {values}"),
            &args,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::super::{embedded_catalog, embedded_payload_bytes, replay_catalog_into_fresh_db};
    use super::{NZB_BATCH, NzbSource, OUTLINE_BATCH, SqlitePool, move_nzb_batch};
    use crate::ingest::outline::{NzbOutline, OutlineFile};
    use crate::jobs::ids::JobId;
    use crate::persistence::Database;
    use crate::persistence::sql_runtime::SqlConn;

    // The schema the step upgrades from.
    const BELOW: i64 = 84;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    async fn pool(path: &Path) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(true),
            )
            .await
            .unwrap()
    }

    fn outline(job: u64, files: usize) -> NzbOutline {
        NzbOutline {
            title: Some(format!("Title {job}")),
            password: job.is_multiple_of(2).then(|| format!("secret-{job}")),
            files: (0..files)
                .map(|index| OutlineFile {
                    full_name: format!("job{job}.part{index:03}.rar"),
                    groups: if index % 3 == 0 {
                        vec!["alt.binaries.a".to_string()]
                    } else {
                        vec!["alt.binaries.a".to_string(), format!("alt.binaries.{job}")]
                    },
                    date: 1_700_000_000 + index as u64,
                    articles: index as u32 * 7 + 1,
                    bytes: index as u64 * 716_800 + 1,
                })
                .collect(),
        }
    }

    // An NZB's stored bytes as a build below 85 kept them: here, bytes that
    // differ per job and do not compress away.
    fn nzb_bytes(job: u64, len: usize) -> Vec<u8> {
        let mut state = job.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    async fn add_job(
        pool: &SqlitePool,
        job: i64,
        nzb: Option<Vec<u8>>,
        head: Option<(&NzbOutline, i64)>,
    ) {
        sqlx::query(
            "INSERT INTO active_jobs (job_id, nzb_hash, nzb_path, nzb_zstd, output_dir, status,
                 created_at, metadata, queue_position, nzb_title, nzb_password, nzb_files)
             VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?)",
        )
        .bind(job)
        .bind(vec![job as u8; 32])
        .bind(format!("/nzbs/job-{job}.nzb"))
        .bind(nzb)
        .bind(format!("/out/job-{job}"))
        .bind(1_700_000_000 + job)
        .bind(format!(r#"[["k","{job}"]]"#))
        .bind(job * 10)
        .bind(head.and_then(|(outline, _)| outline.title.clone()))
        .bind(head.and_then(|(outline, _)| outline.password.clone()))
        .bind(head.map(|(_, files)| files))
        .execute(pool)
        .await
        .unwrap();
    }

    // The outline rows a build below 85 wrote, one per file, at the given
    // indexes.
    async fn add_rows(pool: &SqlitePool, job: i64, files: &[(i64, &OutlineFile)]) {
        for (index, file) in files {
            add_row(
                pool,
                job,
                *index,
                file,
                &serde_json::to_string(&file.groups).unwrap(),
            )
            .await;
        }
    }

    async fn add_row(pool: &SqlitePool, job: i64, index: i64, file: &OutlineFile, groups: &str) {
        sqlx::query(
            "INSERT INTO active_job_nzb_files
                 (job_id, file_index, full_name, groups, posted_at, articles, bytes)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(job)
        .bind(index)
        .bind(&file.full_name)
        .bind(groups)
        .bind(file.date as i64)
        .bind(i64::from(file.articles))
        .bind(file.bytes as i64)
        .execute(pool)
        .await
        .unwrap();
    }

    // The outlines as the build below 85 read them from its rows.
    async fn old_outlines(pool: &SqlitePool) -> HashMap<JobId, NzbOutline> {
        let heads: Vec<(i64, Option<String>, Option<String>, i64)> = sqlx::query_as(
            "SELECT job_id, nzb_title, nzb_password, nzb_files FROM active_jobs
             WHERE nzb_files IS NOT NULL",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        let mut outlines: HashMap<JobId, (i64, NzbOutline)> = heads
            .into_iter()
            .map(|(job, title, password, files)| {
                let outline = NzbOutline {
                    title,
                    password,
                    files: Vec::new(),
                };
                (JobId(job as u64), (files, outline))
            })
            .collect();
        let rows: Vec<(i64, i64, String, String, i64, i64, i64)> = sqlx::query_as(
            "SELECT job_id, file_index, full_name, groups, posted_at, articles, bytes
             FROM active_job_nzb_files ORDER BY job_id, file_index",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        for (job, index, full_name, groups, date, articles, bytes) in rows {
            let Some((_, outline)) = outlines.get_mut(&JobId(job as u64)) else {
                continue;
            };
            if index != outline.files.len() as i64 {
                continue;
            }
            outline.files.push(OutlineFile {
                full_name,
                groups: serde_json::from_str(&groups).unwrap(),
                date: date as u64,
                articles: articles as u32,
                bytes: bytes as u64,
            });
        }
        outlines
            .into_iter()
            .filter(|(_, (files, outline))| *files == outline.files.len() as i64)
            .map(|(job, (_, outline))| (job, outline))
            .collect()
    }

    type Narrow = (
        i64,
        Vec<u8>,
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
    );

    async fn narrow(pool: &SqlitePool) -> Vec<Narrow> {
        sqlx::query_as(
            "SELECT job_id, nzb_hash, nzb_path, queue_position, nzb_title, nzb_password,
                    nzb_files, metadata
             FROM active_jobs ORDER BY job_id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    // A finished job's row as a build below 85 wrote it, its stored NZB on
    // the row, every other column given a value that differs per job.
    async fn add_history(pool: &SqlitePool, job: i64, nzb: Option<Vec<u8>>) {
        sqlx::query(
            "INSERT INTO job_history (job_id, job_hash, name, status, error_message, total_bytes,
                 downloaded_bytes, failed_bytes, health, category, output_dir, nzb_path,
                 nzb_zstd, created_at, completed_at, metadata, failure_kind)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(job)
        .bind(vec![job as u8; 32])
        .bind(format!("History {job}"))
        .bind(if job % 3 == 0 { "failed" } else { "complete" })
        .bind((job % 3 == 0).then(|| format!("error {job}")))
        .bind(job * 1_000)
        .bind(job * 900)
        .bind(job * 3)
        .bind(1_000 - job % 100)
        .bind((job % 2 == 0).then_some("tv"))
        .bind(format!("/done/job-{job}"))
        .bind(format!("/nzbs/history-{job}.nzb"))
        .bind(nzb)
        .bind(1_600_000_000 + job)
        .bind(1_600_100_000 + job)
        .bind(format!(r#"[["h","{job}"]]"#))
        .bind((job % 3 == 0).then_some("download"))
        .execute(pool)
        .await
        .unwrap();
    }

    // Every history column but the stored NZB, each job's rendered as text.
    async fn history_without_nzbs(pool: &SqlitePool) -> Vec<(i64, String)> {
        let columns: Vec<String> = sqlx::query_scalar(
            // `release_name` and `search_key` are added after 85 and filled by their own steps.
            "SELECT name FROM pragma_table_info('job_history')
              WHERE name NOT IN ('nzb_zstd', 'release_name', 'search_key') ORDER BY cid",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        assert!(columns.len() > 20);
        let rendered = columns
            .iter()
            .map(|column| format!("quote({column})"))
            .collect::<Vec<_>>()
            .join(" || '|' || ");
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT job_id, {rendered} FROM job_history ORDER BY job_id"
        )))
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn count(pool: &SqlitePool, sql: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_string()))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn has_column(pool: &SqlitePool, table: &str, column: &str) -> bool {
        count(
            pool,
            &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = '{column}'"),
        )
        .await
            == 1
    }

    // Every stored NZB, active and finished, moves byte for byte and reads
    // back through the job's own read; every outline reads back as the build
    // below 85 read it; every other column of the active and history rows
    // reads the same; and the old columns and table are gone. More jobs than
    // one batch of each kind.
    #[test]
    fn the_upgrade_carries_every_nzb_and_outline_over() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("weaver.db");
        let rt = runtime();
        let (nzbs, history, before, narrow_before, history_before) = rt.block_on(async {
            let pool = pool(&path).await;
            replay_catalog_into_fresh_db(
                &pool,
                &embedded_catalog().unwrap(),
                &embedded_payload_bytes().unwrap(),
                Some(BELOW),
                true,
            )
            .await
            .unwrap();
            let mut nzbs: HashMap<i64, Vec<u8>> = HashMap::new();
            // Jobs past both batch sizes, each with an NZB and an outline.
            for job in 10..10 + 2 * OUTLINE_BATCH + 3 {
                let nzb = nzb_bytes(job as u64, 2_000 + job as usize % 7 * 1_000);
                let outline = outline(job as u64, job as usize % 9 + 1);
                add_job(
                    &pool,
                    job,
                    Some(nzb.clone()),
                    Some((&outline, outline.files.len() as i64)),
                )
                .await;
                let files: Vec<(i64, &OutlineFile)> =
                    outline.files.iter().zip(0..).map(|(f, i)| (i, f)).collect();
                add_rows(&pool, job, &files).await;
                nzbs.insert(job, nzb);
            }
            // One NZB of several MB.
            let big = nzb_bytes(1, 6 << 20);
            add_job(&pool, 1, Some(big.clone()), None).await;
            nzbs.insert(1, big);
            // No stored NZB, and an outline that holds no file.
            let empty = outline(2, 0);
            add_job(&pool, 2, None, Some((&empty, 0))).await;
            // An empty stored NZB, and rows with no head: not an outline.
            add_job(&pool, 3, Some(Vec::new()), None).await;
            nzbs.insert(3, Vec::new());
            let stray = outline(3, 2);
            add_rows(&pool, 3, &[(0, &stray.files[0]), (1, &stray.files[1])]).await;
            // A row out of place: the outline is short of its count.
            let gap = outline(4, 3);
            add_job(&pool, 4, None, Some((&gap, 3))).await;
            add_rows(&pool, 4, &[(0, &gap.files[0]), (2, &gap.files[2])]).await;
            // Rows of a job with no active row.
            add_rows(&pool, 999, &[(0, &stray.files[0])]).await;
            let before = old_outlines(&pool).await;
            // Groups that do not read: the build below 85 failed the whole
            // read; the job now has no outline and reads its stored NZB.
            let bad = outline(5, 2);
            add_job(&pool, 5, None, Some((&bad, 2))).await;
            add_rows(&pool, 5, &[(0, &bad.files[0])]).await;
            add_row(&pool, 5, 1, &bad.files[1], "not json").await;

            // Finished jobs past three batches, every seventh without a
            // stored NZB.
            let mut history: HashMap<i64, Option<Vec<u8>>> = HashMap::new();
            for job in 20_000..20_000 + 3 * NZB_BATCH + 5 {
                let nzb =
                    (job % 7 != 0).then(|| nzb_bytes(job as u64, 1_500 + job as usize % 11 * 700));
                add_history(&pool, job, nzb.clone()).await;
                history.insert(job, nzb);
            }
            // One finished job's NZB of several MB, one empty, and one that
            // has none.
            let big = nzb_bytes(30_001, 6 << 20);
            add_history(&pool, 30_001, Some(big.clone())).await;
            history.insert(30_001, Some(big));
            add_history(&pool, 30_002, Some(Vec::new())).await;
            history.insert(30_002, Some(Vec::new()));
            add_history(&pool, 30_003, None).await;
            history.insert(30_003, None);
            // A restart a build cut short between writing the active row and
            // deleting the history row: one id in both, the same bytes.
            let shared = nzb_bytes(7, 9_000);
            add_job(&pool, 7, Some(shared.clone()), None).await;
            nzbs.insert(7, shared.clone());
            add_history(&pool, 7, Some(shared)).await;

            let narrow_before = narrow(&pool).await;
            let history_before = history_without_nzbs(&pool).await;
            pool.close().await;
            (nzbs, history, before, narrow_before, history_before)
        });
        assert!(before.contains_key(&JobId(2)));
        assert!(!before.contains_key(&JobId(4)));
        assert_eq!(before.len(), 2 * OUTLINE_BATCH as usize + 3 + 1);

        // Opening the database runs the upgrade.
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_job_outlines(None).unwrap(), before);
        let ids: Vec<JobId> = before.keys().copied().chain([JobId(5)]).collect();
        assert_eq!(db.load_job_outlines(Some(&ids)).unwrap(), before);
        for (job, nzb) in &nzbs {
            let stored = db
                .load_active_job_persisted_nzb(JobId(*job as u64))
                .unwrap()
                .unwrap();
            assert_eq!(stored.1.as_deref(), Some(nzb.as_slice()), "job {job}");
        }
        let (_, missing) = db.load_active_job_persisted_nzb(JobId(2)).unwrap().unwrap();
        assert_eq!(missing, None);
        for (job, nzb) in &history {
            let (path, stored) = db
                .load_history_job_persisted_nzb(*job as u64)
                .unwrap()
                .unwrap();
            assert_eq!(
                path.to_str(),
                Some(format!("/nzbs/history-{job}.nzb").as_str())
            );
            assert_eq!(stored, *nzb, "history job {job}");
        }
        let (_, shared) = db.load_history_job_persisted_nzb(7).unwrap().unwrap();
        assert_eq!(shared.as_deref(), Some(nzbs[&7].as_slice()));
        drop(db);

        rt.block_on(async {
            let pool = pool(&path).await;
            assert_eq!(narrow(&pool).await, narrow_before);
            assert_eq!(history_without_nzbs(&pool).await, history_before);
            let stored_history = history.values().filter(|nzb| nzb.is_some()).count();
            assert_eq!(
                count(&pool, "SELECT COUNT(*) FROM job_nzbs").await,
                (nzbs.len() + stored_history) as i64
            );
            assert!(!has_column(&pool, "active_jobs", "nzb_zstd").await);
            assert!(!has_column(&pool, "job_history", "nzb_zstd").await);
            let table = count(
                &pool,
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'active_job_nzb_files'",
            )
            .await;
            assert_eq!(table, 0);
            pool.close().await;
        });
    }

    // How far an upgrade got before it was cut short.
    #[derive(Clone, Copy, Debug)]
    enum Cut {
        // Some active batches committed.
        InActiveMove,
        // Every active NZB moved, no finished job's yet.
        BetweenMoves,
        // Every active NZB and some finished jobs' moved.
        InHistoryMove,
    }

    // An upgrade cut short after some committed batches leaves every NZB in
    // exactly one place, and the next start moves the rest.
    fn cut_short_upgrade(cut: Cut) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("weaver.db");
        let rt = runtime();
        let jobs = 3 * NZB_BATCH + 5;
        let (nzbs, history) = rt.block_on(async {
            let pool = pool(&path).await;
            replay_catalog_into_fresh_db(
                &pool,
                &embedded_catalog().unwrap(),
                &embedded_payload_bytes().unwrap(),
                Some(BELOW),
                true,
            )
            .await
            .unwrap();
            let mut nzbs: HashMap<i64, Vec<u8>> = HashMap::new();
            for job in 1..=jobs {
                let nzb = nzb_bytes(job as u64, 3_000 + job as usize % 5 * 500);
                add_job(&pool, job, Some(nzb.clone()), None).await;
                nzbs.insert(job, nzb);
            }
            let mut history: HashMap<i64, Option<Vec<u8>>> = HashMap::new();
            for job in 1_000..1_000 + jobs {
                let nzb =
                    (job % 5 != 0).then(|| nzb_bytes(job as u64, 2_500 + job as usize % 3 * 400));
                add_history(&pool, job, nzb.clone()).await;
                history.insert(job, nzb);
            }
            let with_nzb = history.values().filter(|nzb| nzb.is_some()).count() as i64;

            let batch = async |source, after| {
                let mut tx = pool.begin().await.unwrap();
                let last = move_nzb_batch(&mut SqlConn::Sqlite(&mut tx), source, after)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                last
            };
            let (active_batches, history_batches) = match cut {
                Cut::InActiveMove => (2, 0),
                Cut::BetweenMoves => (usize::MAX, 0),
                Cut::InHistoryMove => (usize::MAX, 2),
            };
            for (source, batches) in [
                (NzbSource::Active, active_batches),
                (NzbSource::History, history_batches),
            ] {
                let mut after = i64::MIN;
                for _ in 0..batches {
                    match batch(source, after).await {
                        Some(last) => after = last,
                        None => break,
                    }
                }
            }
            let (active_moved, history_moved) = match cut {
                Cut::InActiveMove => (2 * NZB_BATCH, 0),
                Cut::BetweenMoves => (jobs, 0),
                Cut::InHistoryMove => (jobs, 2 * NZB_BATCH),
            };
            assert!(history_moved < with_nzb);
            assert_eq!(
                count(&pool, "SELECT COUNT(*) FROM job_nzbs").await,
                active_moved + history_moved,
                "{cut:?}"
            );
            // Each job's NZB is in the new table or on its old row, never
            // both and never neither.
            let active_both_or_neither = count(
                &pool,
                "SELECT COUNT(*) FROM active_jobs AS a
                  LEFT JOIN job_nzbs AS n ON n.job_id = a.job_id
                  WHERE (a.nzb_zstd IS NULL) = (n.job_id IS NULL)",
            )
            .await;
            assert_eq!(active_both_or_neither, 0, "{cut:?}");
            let history_both = count(
                &pool,
                "SELECT COUNT(*) FROM job_history AS h
                   JOIN job_nzbs AS n ON n.job_id = h.job_id
                  WHERE h.nzb_zstd IS NOT NULL",
            )
            .await;
            assert_eq!(history_both, 0, "{cut:?}");
            let history_on_rows = count(
                &pool,
                "SELECT COUNT(*) FROM job_history WHERE nzb_zstd IS NOT NULL",
            )
            .await;
            assert_eq!(history_on_rows, with_nzb - history_moved, "{cut:?}");
            pool.close().await;
            (nzbs, history)
        });

        // The next start runs the upgrade from where it stopped.
        let db = Database::open(&path).unwrap();
        for (job, nzb) in &nzbs {
            let stored = db
                .load_active_job_persisted_nzb(JobId(*job as u64))
                .unwrap()
                .unwrap();
            assert_eq!(stored.1.as_deref(), Some(nzb.as_slice()), "job {job}");
        }
        for (job, nzb) in &history {
            let (_, stored) = db
                .load_history_job_persisted_nzb(*job as u64)
                .unwrap()
                .unwrap();
            assert_eq!(stored, *nzb, "history job {job}");
        }
        drop(db);
        rt.block_on(async {
            let pool = pool(&path).await;
            let stored_history = history.values().filter(|nzb| nzb.is_some()).count() as i64;
            assert_eq!(
                count(&pool, "SELECT COUNT(*) FROM job_nzbs").await,
                jobs + stored_history
            );
            assert!(!has_column(&pool, "job_history", "nzb_zstd").await);
            pool.close().await;
        });
    }

    #[test]
    fn an_upgrade_cut_short_carries_on_from_the_jobs_still_to_move() {
        cut_short_upgrade(Cut::InActiveMove);
    }

    #[test]
    fn an_upgrade_cut_short_between_the_moves_carries_on_with_the_history() {
        cut_short_upgrade(Cut::BetweenMoves);
    }

    #[test]
    fn an_upgrade_cut_short_in_the_history_move_carries_on_from_the_jobs_still_to_move() {
        cut_short_upgrade(Cut::InHistoryMove);
    }

    // Each batch reads and writes along the job id key from where the last
    // one stopped, so a history of any size costs one pass, not a scan per
    // batch.
    #[test]
    fn a_batch_reads_along_the_job_id_key() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("weaver.db");
        runtime().block_on(async {
            let pool = pool(&path).await;
            replay_catalog_into_fresh_db(
                &pool,
                &embedded_catalog().unwrap(),
                &embedded_payload_bytes().unwrap(),
                Some(BELOW),
                true,
            )
            .await
            .unwrap();
            for source in NzbSource::ALL {
                let table = source.table();
                for sql in [
                    format!(
                        "SELECT job_id FROM {table}
                          WHERE job_id > 1 AND nzb_zstd IS NOT NULL ORDER BY job_id LIMIT 32"
                    ),
                    format!(
                        "SELECT job_id, nzb_zstd FROM {table}
                          WHERE job_id > 1 AND job_id <= 40 AND nzb_zstd IS NOT NULL"
                    ),
                    format!(
                        "UPDATE {table} SET nzb_zstd = NULL
                          WHERE job_id > 1 AND job_id <= 40 AND nzb_zstd IS NOT NULL"
                    ),
                ] {
                    let plan: Vec<(i64, i64, i64, String)> =
                        sqlx::query_as(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {sql}")))
                            .fetch_all(&pool)
                            .await
                            .unwrap();
                    let detail = plan
                        .iter()
                        .map(|row| row.3.as_str())
                        .collect::<Vec<_>>()
                        .join("; ");
                    assert!(
                        detail.contains(&format!("SEARCH {table} USING INTEGER PRIMARY KEY")),
                        "{sql}: {detail}"
                    );
                    assert!(!detail.contains("TEMP B-TREE"), "{sql}: {detail}");
                }
            }
            pool.close().await;
        });
    }
}
