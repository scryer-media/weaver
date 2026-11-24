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

use sqlx::SqlitePool;
use weaver_server_db::persistence::StateError;
use weaver_server_jobs::jobs::outline_persistence::{OutlineFile, encode_outline_files};

use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlConn};

pub const HOOK_ID: &str = "move_nzbs_and_outlines_v85";

// Jobs whose NZBs move in one batch: a few tens of MB of NZB at most.
pub const NZB_BATCH: i64 = 32;
// Jobs whose outlines are compressed in one batch, two binds each.
pub const OUTLINE_BATCH: i64 = 256;

// The rows a stored NZB moves from, in the order they move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NzbSource {
    Active,
    History,
}

impl NzbSource {
    pub const ALL: [Self; 2] = [Self::Active, Self::History];

    pub fn table(self) -> &'static str {
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
pub async fn move_nzbs_and_outlines(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
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
pub async fn move_nzbs_in_committed_batches(pool: &SqlitePool) -> Result<(), StateError> {
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
pub async fn move_nzb_batch(
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
