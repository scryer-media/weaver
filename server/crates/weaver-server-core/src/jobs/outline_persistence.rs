// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's NZB outline in the database: the NZB's title, first password and
// file count on its active row, and every posted file of it as one
// compressed blob, one `active_job_outlines` row per job. The row goes with
// the job's other active rows when it leaves.
//
// The blob is a format byte, then one zstd frame (level 3) of a MessagePack
// body: the job's distinct group lists once, then one entry per file, in the
// NZB's file order, naming its group list by index:
//
//   ([groups, ...], [(full_name, groups_index, date, articles, bytes), ...])

use std::collections::HashMap;

use rayon::prelude::*;

use crate::StateError;
use crate::ingest::outline::{NzbOutline, OutlineFile};
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, db_err, max_rows_for_engine};

// The one format this build writes and reads.
const OUTLINE_FORMAT: u8 = 1;
const OUTLINE_ZSTD_LEVEL: i32 = 3;

type StoredFile<'a> = (&'a str, u32, u64, u32, u64);
type ReadFile = (String, u32, u64, u32, u64);

// The stored blob of an outline's files.
pub(crate) fn encode_outline_files(files: &[OutlineFile]) -> Result<Vec<u8>, StateError> {
    let mut lists: Vec<&[String]> = Vec::new();
    let mut index: HashMap<&[String], u32> = HashMap::new();
    let rows: Vec<StoredFile<'_>> = files
        .iter()
        .map(|file| {
            let next = lists.len() as u32;
            let at = *index.entry(file.groups.as_slice()).or_insert_with(|| {
                lists.push(file.groups.as_slice());
                next
            });
            (
                file.full_name.as_str(),
                at,
                file.date,
                file.articles,
                file.bytes,
            )
        })
        .collect();
    let body = rmp_serde::to_vec(&(lists, rows)).map_err(db_err)?;
    let packed = zstd::bulk::compress(&body, OUTLINE_ZSTD_LEVEL).map_err(db_err)?;
    let mut blob = Vec::with_capacity(1 + packed.len());
    blob.push(OUTLINE_FORMAT);
    blob.extend_from_slice(&packed);
    Ok(blob)
}

// The files of a stored blob. Every failure is total: a blob that is not
// exactly one outline of this format holds no file.
pub(crate) fn decode_outline_files(blob: &[u8]) -> Result<Vec<OutlineFile>, String> {
    let (&format, packed) = blob.split_first().ok_or("empty outline")?;
    if format != OUTLINE_FORMAT {
        return Err(format!("outline format {format} is not {OUTLINE_FORMAT}"));
    }
    let body = zstd::stream::decode_all(packed).map_err(|error| error.to_string())?;
    let (lists, rows): (Vec<Vec<String>>, Vec<ReadFile>) =
        rmp_serde::from_slice(&body).map_err(|error| error.to_string())?;
    rows.into_iter()
        .map(|(full_name, at, date, articles, bytes)| {
            let groups = lists
                .get(at as usize)
                .cloned()
                .ok_or_else(|| format!("outline file names group list {at}, which it lacks"))?;
            Ok(OutlineFile {
                full_name,
                groups,
                date,
                articles,
                bytes,
            })
        })
        .collect()
}

// The outline's columns on the active row, the password sealed, and its
// files' stored blob.
#[derive(Clone)]
pub(crate) struct OutlineHead {
    title: Option<String>,
    password: Option<String>,
    files: i64,
    blob: Vec<u8>,
}

impl OutlineHead {
    pub(crate) fn of(
        key: Option<&crate::persistence::encryption::EncryptionKey>,
        outline: &NzbOutline,
    ) -> Result<Self, StateError> {
        Ok(Self {
            title: outline.title.clone(),
            password: super::persistence::encrypt_archive_password(
                key,
                outline.password.as_deref(),
            )?,
            files: outline.files.len() as i64,
            blob: encode_outline_files(&outline.files)?,
        })
    }
}

// Write the outline of a job whose active row exists, in the caller's
// transaction: its head, then its blob. A job with no active row is left
// with no row here.
pub(crate) async fn write_outline_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    head: &OutlineHead,
) -> Result<(), StateError> {
    let id = job_id.0 as i64;
    let updated = tx
        .execute(
            "UPDATE active_jobs SET nzb_title = {}, nzb_password = {}, nzb_files = {}
             WHERE job_id = {}",
            &[
                SqlArg::OptText(head.title.clone()),
                SqlArg::OptText(head.password.clone()),
                SqlArg::I64(head.files),
                SqlArg::I64(id),
            ],
        )
        .await?;
    if updated == 0 {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO active_job_outlines (job_id, outline) VALUES ({}, {})
         ON CONFLICT(job_id) DO UPDATE SET outline = excluded.outline",
        &[SqlArg::I64(id), SqlArg::Bytes(head.blob.clone())],
    )
    .await?;
    Ok(())
}

// One stored outline as read, before its blob is opened.
struct StoredOutline {
    job_id: JobId,
    title: Option<String>,
    password: Option<String>,
    files: i64,
    blob: Option<Vec<u8>>,
}

impl Database {
    // Write a job's outline in one transaction of its own.
    pub(crate) fn write_job_outline(
        &self,
        job_id: JobId,
        outline: &NzbOutline,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let head = OutlineHead::of(self.encryption_key(), outline)?;
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "write_job_outline", |tx| {
                let head = head.clone();
                Box::pin(async move { write_outline_tx(tx, job_id, &head).await })
            })
            .await
        })
    }

    // The stored outline of every active job that has one, or of each of the
    // jobs named, those read in bind-safe chunks. An outline whose files do
    // not match its count, or whose blob does not open, is not one. The blobs
    // are opened after the read, off the SQL runtime.
    pub(crate) fn load_job_outlines(
        &self,
        only: Option<&[JobId]>,
    ) -> Result<HashMap<JobId, NzbOutline>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        let chunks: Vec<Vec<i64>> = match only {
            None => vec![Vec::new()],
            Some(ids) => {
                let mut ids: Vec<i64> = ids.iter().map(|id| id.0 as i64).collect();
                ids.sort_unstable();
                ids.dedup();
                let size = max_rows_for_engine(datastore.engine(), 1);
                ids.chunks(size).map(<[i64]>::to_vec).collect()
            }
        };
        let filtered = only.is_some();
        let stored = self.run_sql_blocking_read(async move {
            let mut stored = Vec::new();
            for chunk in chunks {
                if filtered && chunk.is_empty() {
                    continue;
                }
                let filter = if filtered {
                    let placeholders = vec!["{}"; chunk.len()].join(", ");
                    format!(" AND a.job_id IN ({placeholders})")
                } else {
                    String::new()
                };
                let args: Vec<SqlArg> = chunk.iter().copied().map(SqlArg::I64).collect();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT a.job_id, a.nzb_title, a.nzb_password, a.nzb_files, o.outline
                         FROM active_jobs AS a
                         LEFT JOIN active_job_outlines AS o ON o.job_id = a.job_id
                         WHERE a.nzb_files IS NOT NULL{filter}"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    stored.push(StoredOutline {
                        job_id: JobId(row.i64("job_id")? as u64),
                        title: row.opt_text("nzb_title")?,
                        password: super::persistence::decrypt_archive_password(
                            key.as_ref(),
                            row.opt_text("nzb_password")?,
                        )?,
                        files: row.i64("nzb_files")?,
                        blob: row.opt_bytes("outline")?,
                    });
                }
            }
            Ok(stored)
        })?;
        Ok(stored
            .into_par_iter()
            .filter_map(|stored| {
                // No blob: no file was stored for the job.
                let files = match stored.blob.as_deref().map(decode_outline_files) {
                    None => Vec::new(),
                    Some(Ok(files)) => files,
                    Some(Err(error)) => {
                        tracing::warn!(
                            job_id = stored.job_id.0,
                            %error,
                            "a stored NZB outline could not be read; the job reads its stored NZB"
                        );
                        return None;
                    }
                };
                if usize::try_from(stored.files).ok() != Some(files.len()) {
                    return None;
                }
                Some((
                    stored.job_id,
                    NzbOutline {
                        title: stored.title,
                        password: stored.password,
                        files,
                    },
                ))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::ActiveJob;

    fn active_job(id: u64) -> ActiveJob {
        ActiveJob {
            job_id: JobId(id),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from(format!("job-{id}.nzb")),
            nzb_zstd: Vec::new(),
            output_dir: PathBuf::from(format!("job-{id}")),
            created_at: 1_700_000_000 + id,
            category: None,
            metadata: vec![],
            status: "queued",
            download_state: "queued",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            password_override: None,
            supplied_password_candidates: Vec::new(),
        }
    }

    // More files than one statement can bind.
    fn outline(files: usize) -> NzbOutline {
        NzbOutline {
            title: Some("Title".to_string()),
            password: Some("secret".to_string()),
            files: (0..files)
                .map(|index| OutlineFile {
                    full_name: format!("file.{index}.bin"),
                    groups: vec!["alt.binaries.a".to_string(), format!("g{index}")],
                    date: index as u64,
                    articles: index as u32 + 1,
                    bytes: index as u64 * 700,
                })
                .collect(),
        }
    }

    fn outline_rows(db: &Database) -> i64 {
        let datastore = db.datastore();
        db.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) FROM active_job_outlines",
                &[],
            )
            .await?
            .expect("a count")
            .i64_at(0)
        })
        .unwrap()
    }

    // An outline written with its job's admission reads back whole, past one
    // statement's binds, and is written again in place. A job with no active
    // row gets no outline.
    #[test]
    fn an_outline_reads_back_as_written() {
        let db = Database::open_in_memory().unwrap();
        let large = outline(6_000);
        assert!(
            db.materialize_admission_with_outline(
                &active_job(1),
                &[],
                Some(&large),
                None,
                None,
                false
            )
            .unwrap()
        );
        db.create_active_job(&active_job(2)).unwrap();
        let small = outline(2);
        db.write_job_outline(JobId(2), &small).unwrap();
        db.write_job_outline(JobId(2), &small).unwrap();
        db.write_job_outline(JobId(3), &small).unwrap();
        let outlines = db.load_job_outlines(None).unwrap();
        assert_eq!(outlines.len(), 2);
        assert_eq!(outlines[&JobId(1)], large);
        assert_eq!(outlines[&JobId(2)], small);
        let one = db.load_job_outlines(Some(&[JobId(2), JobId(3)])).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[&JobId(2)], small);
        assert_eq!(outline_rows(&db), 2);
    }

    // The outlines of more jobs than one statement can bind read back in one
    // call, each job's own, and a job named twice or with none is read once
    // or not at all.
    #[test]
    fn the_outlines_of_a_long_id_list_read_back_whole() {
        let db = Database::open_in_memory().unwrap();
        let jobs = max_rows_for_engine(db.datastore().engine(), 1) as u64 + 50;
        for id in 1..=jobs {
            db.create_active_job(&active_job(id)).unwrap();
            db.write_job_outline(JobId(id), &outline(id as usize % 3 + 1))
                .unwrap();
        }
        let mut wanted: Vec<JobId> = (1..=jobs).map(JobId).collect();
        wanted.push(JobId(1));
        wanted.push(JobId(jobs + 1));
        let outlines = db.load_job_outlines(Some(&wanted)).unwrap();
        assert_eq!(outlines, db.load_job_outlines(None).unwrap());
        assert_eq!(outlines.len(), jobs as usize);
        for id in 1..=jobs {
            assert_eq!(
                outlines[&JobId(id)],
                outline(id as usize % 3 + 1),
                "job {id}"
            );
        }
    }

    // The outline goes with the job's active rows, archived or deleted.
    #[test]
    fn an_outline_leaves_with_its_job() {
        let db = Database::open_in_memory().unwrap();
        for id in [1, 2] {
            db.create_active_job(&active_job(id)).unwrap();
            db.write_job_outline(JobId(id), &outline(3)).unwrap();
        }
        db.delete_active_job(JobId(1)).unwrap();
        db.archive_job(
            JobId(2),
            &crate::JobHistoryRow {
                job_id: 2,
                job_hash: None,
                name: "job-2".into(),
                status: "failed".into(),
                error_message: None,
                total_bytes: 0,
                downloaded_bytes: 0,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: 0,
                category: None,
                output_dir: None,
                nzb_path: None,
                created_at: 1,
                completed_at: 2,
                metadata: None,
                server_attribution: None,
                files_removed_at: None,
            },
        )
        .unwrap();
        assert!(db.load_job_outlines(None).unwrap().is_empty());
        assert_eq!(outline_rows(&db), 0);
    }

    // The stored blob holds each file as it was given: names, group lists
    // shared or not, dates, counts and sizes at their limits, in order. The
    // group lists are stored once each.
    #[test]
    fn an_outline_blob_round_trips_its_files() {
        let mut files = outline(500).files;
        files.push(OutlineFile {
            full_name: "ünïcode name with spaces.mkv".to_string(),
            groups: Vec::new(),
            date: u64::MAX,
            articles: u32::MAX,
            bytes: u64::MAX,
        });
        files.push(OutlineFile {
            full_name: String::new(),
            groups: vec!["alt.binaries.a".to_string(), "g3".to_string()],
            date: 0,
            articles: 0,
            bytes: 0,
        });
        let blob = encode_outline_files(&files).unwrap();
        assert_eq!(blob[0], OUTLINE_FORMAT);
        assert_eq!(decode_outline_files(&blob).unwrap(), files);
        assert_eq!(
            decode_outline_files(&encode_outline_files(&[]).unwrap()).unwrap(),
            vec![]
        );

        let same_groups: Vec<OutlineFile> = (0..1_000)
            .map(|index| OutlineFile {
                groups: vec![
                    "alt.binaries.one".to_string(),
                    "alt.binaries.two".to_string(),
                ],
                ..outline(1_000).files[index].clone()
            })
            .collect();
        let body =
            zstd::stream::decode_all(&encode_outline_files(&same_groups).unwrap()[1..]).unwrap();
        let (lists, rows): (Vec<Vec<String>>, Vec<ReadFile>) =
            rmp_serde::from_slice(&body).unwrap();
        assert_eq!(lists.len(), 1);
        assert_eq!(rows.len(), 1_000);
    }

    // A blob that is not one outline of this format holds no file, and its
    // job is left with no outline, so it reads its stored NZB; the other
    // jobs read back.
    #[test]
    fn an_unreadable_outline_blob_is_not_an_outline() {
        assert!(decode_outline_files(&[]).is_err());
        assert!(decode_outline_files(&[2, 0, 0]).is_err());
        assert!(decode_outline_files(&[OUTLINE_FORMAT, 1, 2, 3]).is_err());
        // A file naming a group list the blob lacks.
        let body = rmp_serde::to_vec(&(
            Vec::<Vec<String>>::new(),
            vec![("a".to_string(), 0u32, 0u64, 1u32, 1u64)],
        ))
        .unwrap();
        let mut blob = vec![OUTLINE_FORMAT];
        blob.extend(zstd::bulk::compress(&body, 3).unwrap());
        assert!(decode_outline_files(&blob).is_err());

        let db = Database::open_in_memory().unwrap();
        for id in [1, 2] {
            db.create_active_job(&active_job(id)).unwrap();
            db.write_job_outline(JobId(id), &outline(3)).unwrap();
        }
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_job_outlines SET outline = {} WHERE job_id = 1",
                &[SqlArg::Bytes(vec![OUTLINE_FORMAT, 9, 9, 9])],
            )
            .await?;
            Ok(())
        })
        .unwrap();
        let outlines = db.load_job_outlines(None).unwrap();
        assert_eq!(outlines.len(), 1);
        assert_eq!(outlines[&JobId(2)], outline(3));
    }

    // A job whose outline holds no file has no blob it needs: its head alone
    // is its outline. A head whose count the blob does not match is not one.
    #[test]
    fn an_outline_with_no_blob_holds_no_file() {
        let db = Database::open_in_memory().unwrap();
        for id in [1, 2] {
            db.create_active_job(&active_job(id)).unwrap();
        }
        db.write_job_outline(JobId(1), &outline(0)).unwrap();
        db.write_job_outline(JobId(2), &outline(2)).unwrap();
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "DELETE FROM active_job_outlines",
                &[],
            )
            .await?;
            Ok(())
        })
        .unwrap();
        let outlines = db.load_job_outlines(None).unwrap();
        assert_eq!(outlines.len(), 1);
        assert_eq!(outlines[&JobId(1)], outline(0));
    }
}
