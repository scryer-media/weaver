// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use weaver_server_pipeline::JobStatus;
use weaver_server_pipeline::pipeline::tests::{
    engines::{
        harness::{JOB, Post, Scenario, collect, finished, run, serve_work, take_retry},
        restart_fixtures::{backlog_base_bytes, next_work},
        restart_move_fixture::{awaits_graph_mail, hear_one, restart_with_recorded_envelope},
    },
    history_fixture::check_completion,
    insert_active_job, new_direct_pipeline_with_backlog, pump_pipeline_runtime_queues,
    rar5_fixture_bytes, retire_pipeline_database,
    uu_fixture::job_status,
};

#[tokio::test]
async fn a_missing_recorded_engine_envelope_waits_for_refetch_and_keeps_surviving_inputs() {
    let scenario = Scenario::new(vec![
        Post::plain("survivor.bin", b"a complete surviving input".to_vec()),
        Post::plain("mixed.rar", rar5_fixture_bytes("rar5_multifile_lz.rar")),
    ])
    .framed(224)
    .first(&[(0, 0), (1, 0)]);
    let expected = run(&scenario).await;
    assert!(expected.completed(), "{:?}", expected.failure);
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
    pipeline = restart_with_recorded_envelope(&temp, pipeline, &scenario, |name| {
        std::fs::remove_file(working_dir.join(name)).unwrap();
        removed = true;
    })
    .await;
    assert!(removed);

    // Let every restored worker finish before any missing byte is fetched.
    // A shelf with no backing bytes must not start a run over stale coverage.
    while awaits_graph_mail(&pipeline) {
        hear_one(&mut pipeline, "a pending owner answers").await;
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
            hear_one(&mut pipeline, "a live run speaks").await;
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
    retire_pipeline_database(pipeline).await;
}
