// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::jobs::persistence::JobsPersistenceQueries;
use crate::jobs::repository::JobsRepositoryQueries;
use std::path::PathBuf;

use super::*;
use crate::ActiveJob;
use weaver_server_jobs::jobs::outline_persistence::{OUTLINE_FORMAT, OutlineFile};

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
        db.materialize_admission_with_outline(&active_job(1), &[], Some(&large), None, None, false)
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
            archived_at: None,
            protocol: crate::jobs::Protocol::Nzb,
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
    let body = zstd::stream::decode_all(&encode_outline_files(&same_groups).unwrap()[1..]).unwrap();
    type ReadFile = (String, u32, u64, u32, u64);
    let (lists, rows): (Vec<Vec<String>>, Vec<ReadFile>) = rmp_serde::from_slice(&body).unwrap();
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
