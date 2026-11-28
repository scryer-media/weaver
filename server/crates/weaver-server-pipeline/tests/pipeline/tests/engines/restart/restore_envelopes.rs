// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use weaver_server_composition::JobStatus;
use weaver_server_pipeline::pipeline::tests::{
    engines::{
        harness::{JOB, Post, Scenario, collect, finished, kinds, run, serve_work, take_retry},
        restart_fixtures::{backlog_base_bytes, next_work},
        restart_move_fixture::{
            awaits_graph_mail, hear_one_told, restart_with_recorded_envelope,
            restart_with_recorded_store_envelope,
        },
    },
    history_fixture::check_completion,
    insert_active_job, new_direct_pipeline_with_backlog, pump_pipeline_runtime_queues,
    rar5_fixture_bytes, retire_pipeline_database,
    uu_fixture::job_status,
};

// Who feeds the run whose recorded input goes missing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Feeds {
    // The RAR engine bound over a solid set, which it reads whole.
    Engine,
    // The RAR store that kept a non-solid set: it streams the stored files
    // and its own run makes the compressed one from its shelf.
    Store,
}

// A solid set the graph hands to the RAR engine whole.
#[tokio::test]
async fn a_missing_recorded_engine_envelope_waits_for_refetch_and_keeps_surviving_inputs() {
    missing_recorded_envelope(
        rar5_fixture_bytes("../extraction_profiles/rar5_solid_small.rar"),
        Feeds::Engine,
    )
    .await;
}

// Two stored files and a compressed one, not solid: the store keeps the
// set, streams the stored files, and its run makes the compressed one from
// the shelf whose recorded file goes missing.
#[tokio::test]
async fn a_missing_recorded_store_shelf_envelope_waits_for_refetch_and_keeps_surviving_inputs() {
    missing_recorded_envelope(rar5_fixture_bytes("rar5_multifile_lz.rar"), Feeds::Store).await;
}

// The store's members, as it numbers them: two stored, then the one its
// run makes.
const MADE: &str = "MemberId(2)";

// The recorded input of what feeds the run is gone between shutdown and
// restore: the restored job starts no run over stale coverage, fetches
// that input again and no surviving one, and ends as the job that never
// went down ends.
async fn missing_recorded_envelope(archive: Vec<u8>, feeds: Feeds) {
    let scenario = Scenario::new(vec![
        Post::plain("survivor.bin", b"a complete surviving input".to_vec()),
        Post::plain("mixed.rar", archive),
    ])
    .framed(224)
    .first(&[(0, 0), (1, 0)]);
    let expected = run(&scenario).await;
    assert!(expected.completed(), "{:?}", expected.failure);
    if feeds == Feeds::Store {
        assert!(
            kinds(&expected).contains(&"rar_store") && !kinds(&expected).contains(&"rar_engine"),
            "the store keeps the set: {:?}",
            kinds(&expected)
        );
    }
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_backlog(&temp, backlog_base_bytes(), 1).await;
    scenario.prepare(&mut pipeline);
    let working_dir = insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    for article in [(0, 0), (1, 0)] {
        let work = next_work(&mut pipeline, &scenario).unwrap();
        assert_eq!(
            (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number
            ),
            article
        );
        serve_work(&mut pipeline, &scenario, &work).await;
    }
    let mut removed = false;
    let remove = |name: String| {
        std::fs::remove_file(working_dir.join(name)).unwrap();
        removed = true;
    };
    pipeline = match feeds {
        Feeds::Engine => restart_with_recorded_envelope(&temp, pipeline, &scenario, remove).await,
        Feeds::Store => {
            restart_with_recorded_store_envelope(&temp, pipeline, &scenario, remove).await
        }
    };
    assert!(removed);
    // What the restored job's runs said.
    let mut told = Vec::new();

    // Let every restored worker finish before any missing byte is fetched.
    // A shelf with no backing bytes must not start a run over stale coverage.
    while awaits_graph_mail(&pipeline) {
        told.push(hear_one_told(&mut pipeline, "a pending owner answers").await);
    }
    let mut refetched = BTreeSet::new();
    while !finished(&pipeline) {
        if let Some(work) = next_work(&mut pipeline, &scenario) {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            assert_ne!(
                article,
                (0, 0),
                "the surviving committed input was fetched again"
            );
            refetched.insert(article);
            serve_work(&mut pipeline, &scenario, &work).await;
            continue;
        }
        check_completion(&mut pipeline, JOB).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if finished(&pipeline) {
            break;
        }
        if awaits_graph_mail(&pipeline) {
            told.push(hear_one_told(&mut pipeline, "a live run speaks").await);
            continue;
        }
        assert!(take_retry(&mut pipeline).await, "restored engine stalled");
    }
    assert!(
        refetched.contains(&(1, 0)),
        "deleted input must be fetched again"
    );
    assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Complete));
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert_eq!(files, expected.files);
    if feeds == Feeds::Store {
        // The store wrote each stored file once, as it streamed: no run
        // named, made or announced one.
        for said in told
            .iter()
            .filter(|said| said.starts_with("Emit(") || said.starts_with("Paid("))
        {
            assert!(
                !said.starts_with("Emit(Member {"),
                "a run announced: {said}"
            );
            let mut rest = said.as_str();
            while let Some(at) = rest.find("MemberId(") {
                let named = &rest[at..at + rest[at..].find(')').unwrap() + 1];
                assert_eq!(named, MADE, "a run named a stored file: {said}");
                rest = &rest[at + named.len()..];
            }
        }
    }
    retire_pipeline_database(pipeline).await;
}
