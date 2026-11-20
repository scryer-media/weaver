// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::history::timeline::HistoryTimelineQueries;
use crate::jobs::JobsServerAttributionPersistenceInternalQueries;
use crate::jobs::JobsSupportFactsPersistenceQueries;
use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::jobs::persistence::JobsPersistenceQueries;
use crate::jobs::queries::JobsInternalQueries;
use crate::jobs::queries::JobsQueries;
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;
use std::collections::HashSet;

use super::*;
use crate::jobs::{FileSpec, JobSpec, SegmentSpec};
use weaver_model::files::FileRole;
use weaver_server_base::jobs::ids::{JobId, NzbFileId};

fn sorted_debug<K: std::fmt::Debug, V: std::fmt::Debug>(map: &HashMap<K, V>) -> Vec<String> {
    let mut entries = map
        .iter()
        .map(|(key, value)| format!("{key:?}={value:?}"))
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

// An install batch reads its rows in a fixed number of statements; each
// job's rows match what the per-job reads it replaces return.
#[test]
fn a_batch_reads_each_install_what_the_per_job_reads_did() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let db = crate::Database::open_in_memory().unwrap();
    let active = (1..=40u64).map(JobId).collect::<Vec<_>>();
    for id in &active {
        db.create_active_job(&crate::jobs::record::ActiveJob {
            job_id: *id,
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from(format!("/nonexistent/batch-{}.nzb", id.0)),
            nzb_zstd: Vec::new(),
            output_dir: PathBuf::from(format!("/nonexistent/batch-out-{}", id.0)),
            created_at: 1_700_000_000 + id.0,
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
        })
        .unwrap();
    }
    // A job with no active row: its rows read as the per-job reads did.
    let gone = JobId(77);
    let mut checkpointed = active
        .iter()
        .copied()
        .filter(|id| id.0 % 3 == 0)
        .collect::<Vec<_>>();
    checkpointed.push(gone);
    for id in &checkpointed {
        let job = id.0 as i64;
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            for node in 0..2 {
                SqlRuntime::execute(
                    datastore.write_exec(),
                    "INSERT INTO graph_nodes (job_id, node_id, kind, state, facts_version, facts)
                         VALUES ({}, {}, 'root', 'open', 1, {})",
                    &[SqlArg::I64(job), SqlArg::I64(node), SqlArg::Bytes(vec![0])],
                )
                .await?;
            }
            Ok(())
        })
        .unwrap();
    }
    for id in active.iter().filter(|id| id.0 % 4 == 0) {
        db.set_active_job_staging_dir(*id, Some(std::path::Path::new(&format!("/stage/{}", id.0))))
            .unwrap();
    }
    db.save_active_server_attribution(
        active
            .iter()
            .filter(|id| id.0 % 5 == 0)
            .map(|id| (*id, format!("not stored json {}", id.0)))
            .collect(),
    )
    .unwrap();
    db.save_active_support_facts(
        active
            .iter()
            .filter(|id| id.0 % 6 == 0)
            .map(|id| (*id, Some(format!("{{\"x\":{}}}", id.0))))
            .collect(),
    )
    .unwrap();
    for id in active.iter().filter(|id| id.0 % 7 == 0) {
        db.set_setting(
            &crate::jobs::working_dir::rename_journal_key(*id),
            &format!("[\"/a/{0}\",\"/b/{0}\"]", id.0),
        )
        .unwrap();
    }
    for id in active.iter().chain([&gone]) {
        let marker = JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER;
        let events: &[(&str, Option<&str>)] = match id.0 % 4 {
            0 => &[("JobCreated", None), ("DownloadFinished", Some(marker))],
            1 => &[
                ("DownloadFinished", Some(marker)),
                ("DownloadFinished", None),
            ],
            2 => &[("SomethingElse", None)],
            _ => &[
                ("JobCreated", None),
                ("JobCompleted", None),
                ("JobCreated", None),
            ],
        };
        for (n, (kind, file_id)) in events.iter().enumerate() {
            db.insert_job_event(id.0, n as i64, kind, &format!("{n}"), *file_id)
                .unwrap();
        }
    }

    let jobs = active
        .iter()
        .map(|id| (*id, id.0 % 2 == 0))
        .chain([(gone, true)])
        .collect::<Vec<_>>();
    let mut batch = prefetch_installs(&db, &jobs);
    assert_eq!(batch.len(), jobs.len());
    for (id, restart) in jobs {
        let rows = batch.remove(&id).unwrap().unwrap();
        let recovered = restart.then(|| db.load_active_job(id).unwrap()).flatten();
        assert_eq!(
            rows.recovered.as_ref().map(|job| (
                sorted_debug(&job.file_identities),
                sorted_debug(&job.detected_archives)
            )),
            recovered.as_ref().map(|job| (
                sorted_debug(&job.file_identities),
                sorted_debug(&job.detected_archives)
            )),
            "{id:?}"
        );
        assert_eq!(
            rows.facts.staging_dir,
            db.load_active_job_staging_dir(id).unwrap(),
            "{id:?}"
        );
        assert_eq!(
            rows.facts
                .server_attribution
                .as_deref()
                .map(crate::jobs::server_attribution::JobServerAttribution::from_storage_json)
                .unwrap_or_default(),
            db.load_active_server_attribution(id).unwrap(),
            "{id:?}"
        );
        assert_eq!(
            crate::jobs::support_facts::JobSupportFacts::from_storage(
                rows.facts.support_facts.as_deref()
            ),
            db.load_job_support_facts(id).unwrap(),
            "{id:?}"
        );
        assert_eq!(
            rows.facts.has_checkpoint,
            db.has_graph_checkpoint(id).unwrap(),
            "{id:?}"
        );
        assert_eq!(
            rows.journal,
            db.get_setting(&crate::jobs::working_dir::rename_journal_key(id))
                .unwrap(),
            "{id:?}"
        );
        assert_eq!(
            format!("{:?}", rows.finalization),
            format!(
                "{:?}",
                db.get_last_job_event_of_kinds(
                    id.0,
                    Pipeline::DOWNLOAD_FINALIZATION_KINDS,
                    JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER,
                )
                .unwrap()
            ),
            "{id:?}"
        );
    }
}

fn sparse_segment_job_spec() -> JobSpec {
    JobSpec {
        name: "Sparse Segments".to_string(),
        password: None,
        total_bytes: 60,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "episode.bin".to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                SegmentSpec {
                    ordinal: 0,
                    article_number: 1,
                    bytes: 10,
                    message_ids: crate::jobs::MessageIds::new(vec!["one@example.com".to_string()]),
                },
                SegmentSpec {
                    ordinal: 1,
                    article_number: 4,
                    bytes: 20,
                    message_ids: crate::jobs::MessageIds::new(vec!["four@example.com".to_string()]),
                },
                SegmentSpec {
                    ordinal: 2,
                    article_number: 6,
                    bytes: 30,
                    message_ids: crate::jobs::MessageIds::new(vec!["six@example.com".to_string()]),
                },
            ],
        }],
    }
}

#[test]
fn sparse_article_numbers_queue_dense_ordinals() {
    let job_id = JobId(41);
    let spec = sparse_segment_job_spec();
    let (assembly, mut download_queue) =
        Pipeline::build_job_assembly(job_id, &spec, &HashSet::new(), &HashSet::new());

    let mut queued = Vec::new();
    while let Some(work) = download_queue.pop() {
        queued.push(work.segment_id.segment_number);
    }
    queued.sort_unstable();

    assert_eq!(queued, vec![0, 1, 2]);
    assert_eq!(
        assembly
            .file(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .total_segments(),
        3
    );
}

#[test]
fn sparse_article_numbers_do_not_overflow_committed_assembly() {
    let job_id = JobId(42);
    let spec = sparse_segment_job_spec();
    let (_, mut queue) =
        Pipeline::build_job_assembly(job_id, &spec, &HashSet::new(), &HashSet::new());
    let mut committed = HashSet::new();
    while let Some(work) = queue.pop() {
        committed.insert(work.segment_id);
    }
    let (assembly, mut download_queue) =
        Pipeline::build_job_assembly(job_id, &spec, &committed, &HashSet::new());

    assert!(download_queue.pop().is_none());
    assert!(
        assembly
            .file(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .is_complete()
    );
}
