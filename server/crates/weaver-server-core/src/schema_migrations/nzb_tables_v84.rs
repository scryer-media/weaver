// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 84, step after the new tables: each active job's stored NZB moves
// from `active_jobs.nzb_zstd` to `active_job_nzbs`, each job's outline rows
// in `active_job_nzb_files` become one compressed `active_job_outlines` row,
// and the old column and table go.
//
// The NZBs move a few jobs at a time, each batch copied and then cleared on
// the active row, so the pages a batch frees hold the next batch's copy and
// the database does not grow by every stored NZB at once.
//
// An outline is read as the build below 84 read it: a job's rows in file
// order, from index 0, stopping at the first one out of place; only jobs
// whose active row has a file count are taken. A job whose rows do not read
// gets no outline row and, like any job whose files do not match its count,
// reads its stored NZB once more and stores the outline it reads.

use std::collections::BTreeMap;

use crate::StateError;
use crate::ingest::outline::OutlineFile;
use crate::jobs::outline_persistence::encode_outline_files;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "move_nzbs_and_outlines_v84";

// Jobs whose NZBs move in one batch: a few tens of MB of NZB at most.
const NZB_BATCH: i64 = 32;
// Jobs whose outlines are compressed in one batch, two binds each.
const OUTLINE_BATCH: i64 = 256;

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn move_nzbs_and_outlines(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    move_nzbs(conn).await?;
    compress_outlines(conn).await?;
    conn.execute("DROP TABLE active_job_nzb_files", &[]).await?;
    conn.execute("ALTER TABLE active_jobs DROP COLUMN nzb_zstd", &[])
        .await?;
    Ok(())
}

// The last job id of the next batch after `after`, read from the key alone.
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

async fn move_nzbs(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let mut after = i64::MIN;
    while let Some(last) = batch_end(
        conn,
        "SELECT job_id FROM active_jobs WHERE job_id > {} ORDER BY job_id LIMIT {}",
        after,
        NZB_BATCH,
    )
    .await?
    {
        let range = [SqlArg::I64(after), SqlArg::I64(last)];
        conn.execute(
            "INSERT INTO active_job_nzbs (job_id, nzb_zstd)
             SELECT job_id, nzb_zstd FROM active_jobs
              WHERE job_id > {} AND job_id <= {} AND nzb_zstd IS NOT NULL",
            &range,
        )
        .await?;
        conn.execute(
            "UPDATE active_jobs SET nzb_zstd = NULL
              WHERE job_id > {} AND job_id <= {} AND nzb_zstd IS NOT NULL",
            &range,
        )
        .await?;
        after = last;
    }
    Ok(())
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

    use sqlx::SqlitePool;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::super::{embedded_catalog, embedded_payload_bytes, replay_catalog_into_fresh_db};
    use super::OUTLINE_BATCH;
    use crate::ingest::outline::{NzbOutline, OutlineFile};
    use crate::jobs::ids::JobId;
    use crate::persistence::Database;

    // The schema the step upgrades from.
    const BELOW: i64 = 83;

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

    // An NZB's stored bytes as a build below 84 kept them: here, bytes that
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

    // The outline rows a build below 84 wrote, one per file, at the given
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

    // The outlines as the build below 84 read them from its rows.
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

    // Every stored NZB moves byte for byte, every outline reads back as the
    // build below 84 read it, every other column of the active row reads the
    // same, and the old column and table are gone. More jobs than one batch
    // of either kind.
    #[test]
    fn the_upgrade_carries_every_nzb_and_outline_over() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("weaver.db");
        let rt = runtime();
        let (nzbs, before, narrow_before) = rt.block_on(async {
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
            // Groups that do not read: the build below 84 failed the whole
            // read; the job now has no outline and reads its stored NZB.
            let bad = outline(5, 2);
            add_job(&pool, 5, None, Some((&bad, 2))).await;
            add_rows(&pool, 5, &[(0, &bad.files[0])]).await;
            add_row(&pool, 5, 1, &bad.files[1], "not json").await;
            let narrow_before = narrow(&pool).await;
            pool.close().await;
            (nzbs, before, narrow_before)
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
        drop(db);

        rt.block_on(async {
            let pool = pool(&path).await;
            assert_eq!(narrow(&pool).await, narrow_before);
            let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM active_job_nzbs")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(stored, nzbs.len() as i64);
            let column: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM pragma_table_info('active_jobs') WHERE name = 'nzb_zstd'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(column, 0);
            let table: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'active_job_nzb_files'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(table, 0);
            pool.close().await;
        });
    }
}
