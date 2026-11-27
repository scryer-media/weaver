// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::Scenario;
use crate::jobs::queries::JobsQueries;

pub async fn explicit_stream(
    scenario: &Scenario,
    articles: &[(u32, u32)],
    after: Option<usize>,
    duplicates: &[(u32, u32)],
) -> super::restart_fixtures::Restarted {
    drive_stream(scenario, articles, after, duplicates, true).await
}

pub async fn drive_stream(
    scenario: &Scenario,
    articles: &[(u32, u32)],
    after: Option<usize>,
    duplicates: &[(u32, u32)],
    defer_missing: bool,
) -> super::restart_fixtures::Restarted {
    use super::super::{
        insert_active_job, job_status_for_assert, new_direct_pipeline,
        pump_pipeline_runtime_queues, retire_pipeline_database,
    };
    use super::harness::{self, JOB};
    use crate::jobs::model::{DownloadState, PostState, RunState};
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    scenario.prepare(&mut pipeline);
    let mut spec = scenario.spec();
    // NZB sizes describe encoded transport bytes. Decoded volume geometry
    // becomes authoritative only when the yEnc headers arrive.
    for file in &mut spec.files {
        for segment in &mut file.segments {
            segment.bytes += segment.bytes.div_ceil(32) + 2;
        }
    }
    spec.total_bytes = spec
        .files
        .iter()
        .flat_map(|file| &file.segments)
        .map(|segment| u64::from(segment.bytes))
        .sum();
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    let mut graph = harness::GraphView::default();
    let mut reads = 0;
    for (step, &article) in articles.iter().enumerate() {
        if Some(step) == after {
            pipeline.flush_graph_checkpoint(JOB);
            retire_pipeline_database(pipeline).await;
            (pipeline, _, _) = new_direct_pipeline(&root).await;
            scenario.prepare(&mut pipeline);
            let recovered = pipeline
                .db
                .load_active_jobs()
                .unwrap()
                .remove(&JOB)
                .expect("active job at crash");
            pipeline
                .restore_job(crate::RestoreJobRequest {
                    job_id: JOB,
                    job_hash: recovered.nzb_hash,
                    spec: spec.clone(),
                    detected_archives: recovered.detected_archives,
                    file_identities: recovered.file_identities,
                    status: crate::jobs::model::job_status_from_persisted_str(
                        &recovered.status,
                        recovered.error.as_deref(),
                    ),
                    download_state: recovered
                        .download_state
                        .as_deref()
                        .and_then(DownloadState::parse),
                    post_state: recovered.post_state.as_deref().and_then(PostState::parse),
                    run_state: recovered.run_state.as_deref().and_then(RunState::parse),
                    queued_repair_at_epoch_ms: recovered.queued_repair_at_epoch_ms,
                    queued_extract_at_epoch_ms: recovered.queued_extract_at_epoch_ms,
                    paused_resume_status: None,
                    paused_resume_download_state: None,
                    paused_resume_post_state: None,
                    working_dir: recovered.output_dir,
                    restart: None,
                })
                .await
                .unwrap();
            assert!(
                pipeline.graph_jobs.contains_key(&JOB),
                "checkpoint restores"
            );
        }
        // Some responses are already in flight when the server reports
        // missing articles; other witnesses answer that loss inline.
        if defer_missing && scenario.lost.contains(&article) {
            continue;
        }
        if duplicates.contains(&article) {
            explicit_article_body(
                &mut pipeline,
                (&mut graph, &mut reads),
                scenario,
                article,
                true,
            )
            .await;
        }
        explicit_article(&mut pipeline, (&mut graph, &mut reads), scenario, article).await;
    }
    // Metadata is supplied after the posted archive bytes, as a late index
    // completes its own download while extraction is already in progress.
    for (file, post) in scenario
        .posts
        .iter()
        .enumerate()
        .filter(|(_, post)| post.role.is_recovery() && !post.name.contains(".vol"))
    {
        for ordinal in 0..post.articles(scenario.article).count() {
            if articles.contains(&(file as u32, ordinal as u32)) {
                continue;
            }
            let article = (file as u32, ordinal as u32);
            explicit_article(&mut pipeline, (&mut graph, &mut reads), scenario, article).await;
        }
    }
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut()?.download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            let look = (&mut graph, &mut reads);
            if duplicates.contains(&article) {
                explicit_article_body(&mut pipeline, look, scenario, article, true).await;
            }
            explicit_article(&mut pipeline, (&mut graph, &mut reads), scenario, article).await;
        }
        if harness::finished(&pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        harness::observe(&pipeline, &mut graph, &mut reads);
        if harness::finished(&pipeline) {
            break;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline
                .graph_runs
                .recv()
                .await
                .expect("owned worker returns");
            pipeline.graph_run_said(said);
            // The mail may carry the ruling the job waited on last: look
            // before the completion gate ends the job and takes it away.
            harness::observe(&pipeline, &mut graph, &mut reads);
        } else if harness::take_retry(&mut pipeline).await {
            continue;
        } else {
            assert!(
                pipeline.jobs.get(&JOB).is_some_and(|state| !state
                    .articles
                    .active()
                    .unwrap()
                    .download_queue
                    .is_empty()),
                "no pending work: delivery={:?}, rulings={:?}",
                graph.delivery,
                graph.rulings
            );
        }
    }
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    let mut retained_files = std::collections::BTreeMap::new();
    harness::collect(
        &pipeline.intermediate_dir,
        &pipeline.intermediate_dir,
        &mut retained_files,
    );
    retire_pipeline_database(pipeline).await;
    super::restart_fixtures::Restarted {
        status,
        files,
        retained_files,
        graph,
        from_disk: Default::default(),
        in_memory: Default::default(),
        restarts: usize::from(after.is_some()),
        order: articles.to_vec(),
    }
}

async fn explicit_article(
    pipeline: &mut crate::pipeline::Pipeline,
    look: (&mut super::harness::GraphView, &mut u64),
    scenario: &Scenario,
    (file, ordinal): (u32, u32),
) {
    explicit_article_body(pipeline, look, scenario, (file, ordinal), false).await;
}

// Hear what the runs said, looking once the mail is in and again after the
// completion gate turned: the mail may carry the ruling the job waited on
// last, and a job the gate ends leaves the pipeline with its rulings.
async fn heard(
    pipeline: &mut crate::pipeline::Pipeline,
    (graph, reads): (&mut super::harness::GraphView, &mut u64),
) {
    pipeline.hear_graph_runs();
    super::harness::observe(pipeline, graph, reads);
    super::super::pump_pipeline_runtime_queues(pipeline).await;
    super::harness::observe(pipeline, graph, reads);
}

async fn explicit_article_body(
    pipeline: &mut crate::pipeline::Pipeline,
    look: (&mut super::harness::GraphView, &mut u64),
    scenario: &Scenario,
    (file, ordinal): (u32, u32),
    corrupt: bool,
) {
    use super::harness::JOB;
    use crate::pipeline::{DecodeDone, DownloadPayload, Pipeline, SegmentSource};
    use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};
    use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};
    if super::harness::finished(pipeline) {
        return;
    }
    let id = SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: ordinal,
    };
    let active = pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .articles
        .active_mut()
        .unwrap();
    for queue in [&mut active.download_queue, &mut active.recovery_queue] {
        let kept = queue.drain_all();
        for work in kept {
            if work.segment_id != id {
                queue.push(work);
            }
        }
    }
    if scenario.lost.contains(&(file, ordinal)) {
        pipeline.book_failed_segment(id);
        heard(pipeline, look).await;
        return;
    }
    let post = &scenario.posts[file as usize];
    let offset = ordinal as usize * post.article(scenario.article);
    let body = post
        .articles(scenario.article)
        .nth(ordinal as usize)
        .unwrap();
    let altered;
    let body = if corrupt {
        altered = body.iter().map(|byte| byte ^ 0x5a).collect::<Vec<_>>();
        altered.as_slice()
    } else {
        body
    };
    let mut encoded = Vec::new();
    weaver_yenc::encode_part(
        body,
        &mut encoded,
        128,
        &post.name,
        ordinal + 1,
        post.articles(scenario.article).count() as u32,
        offset as u64 + 1,
        (offset + body.len()) as u64,
        post.bytes.len() as u64,
    )
    .unwrap();
    let mut raw = b"222 article follows\r\n".to_vec();
    for line in encoded.split_inclusive(|byte| *byte == b'\n') {
        if line.first() == Some(&b'.') {
            raw.push(b'.');
        }
        raw.extend_from_slice(line);
    }
    raw.extend_from_slice(b".\r\n");
    let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
    decoder.set_checkpoint_plan(super::super::checkpoint_plan(pipeline, id.file_id));
    let decoded = decoder
        .decode_available(&mut bytes::BytesMut::from(raw.as_slice()))
        .unwrap()
        .unwrap();
    let trace = DecodedBodyTrace {
        attempts: Vec::new(),
        result: Ok(DecodedBody {
            raw_size: raw.len() as u32,
            decoded: decoded.chunks,
            body: decoded.body,
            cpu: Default::default(),
            io: Default::default(),
        }),
    };
    let (result, _, _) = Pipeline::download_data_from_decoded_trace(id, trace);
    let Ok(DownloadPayload::Decoded(result)) = result else {
        panic!("valid transport decode")
    };
    let source = SegmentSource {
        servers: crate::pipeline::PoolServers::stamped(
            pipeline.pool_generation,
            Some(0),
            weaver_nntp::ServerSet::EMPTY,
        ),
    };
    pipeline.note_decode_started(id, raw.len() as u64);
    pipeline
        .handle_decode_done(DecodeDone::Success { result, source })
        .await;
    heard(pipeline, look).await;
}

pub(super) async fn decode_wire_article(
    pipeline: &mut crate::pipeline::Pipeline,
    id: weaver_server_base::jobs::ids::SegmentId,
    raw: &[u8],
) {
    use crate::pipeline::tests::checkpoint_plan;
    use crate::pipeline::{DecodeDone, DownloadPayload, Pipeline, SegmentSource};
    use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};
    let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
    decoder.set_checkpoint_plan(checkpoint_plan(pipeline, id.file_id));
    let decoded = decoder
        .decode_available(&mut bytes::BytesMut::from(raw))
        .unwrap()
        .unwrap();
    let trace = DecodedBodyTrace {
        attempts: Vec::new(),
        result: Ok(DecodedBody {
            raw_size: raw.len() as u32,
            decoded: decoded.chunks,
            body: decoded.body,
            cpu: Default::default(),
            io: Default::default(),
        }),
    };
    let (payload, _, _) = Pipeline::download_data_from_decoded_trace(id, trace);
    let Ok(DownloadPayload::Decoded(result)) = payload else {
        panic!("valid yEnc");
    };
    pipeline.note_decode_started(id, raw.len() as u64);
    pipeline
        .handle_decode_done(DecodeDone::Success {
            result,
            source: SegmentSource {
                servers: crate::pipeline::PoolServers::stamped(
                    pipeline.pool_generation,
                    Some(0),
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        })
        .await;
}
