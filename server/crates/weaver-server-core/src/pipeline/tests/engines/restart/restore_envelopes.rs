// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[tokio::test]
async fn a_missing_recorded_engine_envelope_waits_for_refetch_and_keeps_surviving_inputs() {
    let scenario = Scenario::new(vec![
        Post::plain("survivor.bin", b"a complete surviving input".to_vec()),
        Post::plain(
            "mixed.rar",
            crate::pipeline::tests::rar5_fixture_bytes("rar5_multifile_lz.rar"),
        ),
    ])
    .framed(224)
    .first(&[(0, 0), (1, 0)]);
    let expected = run(&scenario).await;
    assert!(expected.completed(), "{:?}", expected.failure);
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_buffers(&temp, buffers(), 1).await;
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
    pipeline = restart(
        &temp,
        pipeline,
        &scenario,
        &mut BTreeSet::new(),
        &mut |rows| {
            let engine = rows
                .nodes
                .iter()
                .find(|node| node.kind == "rar_engine")
                .expect("the complete compressed-member header bound an engine");
            let envelope = rows
                .envelopes
                .iter()
                .find(|envelope| envelope.node == engine.node)
                .expect("the engine kept its input");
            let name = envelope.file.clone().unwrap_or_else(|| {
                format!(
                    ".weaver-chunks/graph-envelope-{}-{}",
                    envelope.node, envelope.volume
                )
            });
            std::fs::remove_file(working_dir.join(name)).unwrap();
            removed = true;
        },
    )
    .await;
    assert!(removed);

    // Let every restored worker finish before any missing byte is fetched.
    // A shelf with no backing bytes must not start a run over stale coverage.
    while pipeline.graph_jobs[&JOB].awaits_mail() {
        let said = pipeline
            .graph_runs
            .recv()
            .await
            .expect("a pending owner answers");
        pipeline.graph_run_said(said);
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
        pipeline.check_job_completion(JOB).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if finished(&pipeline) {
            break;
        }
        if pipeline.graph_jobs[&JOB].awaits_mail() {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        assert!(take_retry(&mut pipeline).await, "restored engine stalled");
    }
    assert!(
        refetched.contains(&(1, 0)),
        "deleted input must be fetched again"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert_eq!(files, expected.files);
    retire_pipeline_database(pipeline).await;
}
