// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;

use super::super::direct_store::scenarios::{HeaderCheck, header_encrypted_store_set};
use super::super::{
    build_test_par2_index_for_files, insert_active_job, job_status_for_assert, new_direct_pipeline,
    pump_pipeline_runtime_queues, retire_pipeline_database, submit_decoded_segment_declaring,
};
use super::harness::{JOB, collect, finished};
use crate::jobs::ids::{NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec, JobStatus, SegmentSpec};
use crate::pipeline::Pipeline;
use weaver_graph::NodeId;
use weaver_model::files::FileRole;

const PASSWORD: &str = "moonlit-harbour";

// These are four explicit saved arrival sequences, not a schedule generator.
#[derive(Clone, Copy, Debug)]
enum Saved {
    Bare,
    Misnumbered,
    Swapped,
    Gap,
}

struct Fixture {
    spec: JobSpec,
    sources: Vec<(String, Vec<u8>)>,
    expected: Vec<u8>,
    order: Vec<(u32, u32)>,
}

fn fixture(saved: Saved) -> Fixture {
    let mut state = 13_u64;
    let expected: Vec<u8> = (0..70_001)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let count = if matches!(saved, Saved::Gap) { 3 } else { 2 };
    let written = header_encrypted_store_set(
        "nested/feature.mkv",
        &expected,
        count,
        PASSWORD,
        HeaderCheck::For(PASSWORD),
    );
    let posted = if count == 3 { [0, 2] } else { [0, 1] };
    let real: Vec<_> = posted.iter().map(|&index| written[index].clone()).collect();
    let stem = "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41";
    let names = match saved {
        Saved::Bare => [
            format!("{:032x}", 0xd1c7_0000_u128),
            format!("{:032x}", 0xd1c7_0001_u128),
        ],
        Saved::Misnumbered => [format!("{stem}.002"), format!("{stem}.001")],
        Saved::Swapped => [format!("{stem}.r00"), format!("{stem}.rar")],
        Saved::Gap => [format!("{stem}.001"), format!("{stem}.003")],
    };
    let slice = real
        .iter()
        .map(|(_, bytes)| bytes.len())
        .max()
        .unwrap()
        .div_ceil(32)
        .div_ceil(4)
        * 4;
    let described: Vec<_> = real
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let metadata = build_test_par2_index_for_files(&described, slice.max(256) as u64);
    let mut sources: Vec<_> = names
        .into_iter()
        .zip(real.into_iter().map(|(_, bytes)| bytes))
        .collect();
    sources.push(("silver.horizon.par2".to_string(), metadata));
    let files: Vec<_> = sources
        .iter()
        .enumerate()
        .map(|(file, (name, bytes))| {
            let articles = if file == 2 { 1 } else { 2 };
            let frame = bytes.len().div_ceil(articles);
            FileSpec {
                filename: name.clone(),
                role: FileRole::from_filename(name),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..articles)
                    .map(|ordinal| {
                        let length = bytes.len().min((ordinal + 1) * frame) - ordinal * frame;
                        let length = length as u32;
                        SegmentSpec {
                            ordinal: ordinal as u32,
                            article_number: ordinal as u32,
                            bytes: length + length.div_ceil(32) + 2,
                            message_ids: crate::jobs::MessageIds::new(vec![format!(
                                "direct-{file}-{ordinal}@example.com"
                            )]),
                        }
                    })
                    .collect(),
            }
        })
        .collect();
    let order = match saved {
        Saved::Bare | Saved::Swapped => vec![(2, 0), (0, 1), (0, 0), (0, 0), (1, 1), (1, 0)],
        Saved::Misnumbered => vec![(2, 0), (0, 1), (0, 0), (0, 0), (1, 0), (1, 1)],
        Saved::Gap => vec![(2, 0), (0, 1), (1, 1), (1, 0), (1, 0), (0, 0)],
    };
    Fixture {
        spec: JobSpec {
            name: "Archive schedules".to_string(),
            password: Some(PASSWORD.to_string()),
            total_bytes: files
                .iter()
                .flat_map(|file| &file.segments)
                .map(|part| u64::from(part.bytes))
                .sum(),
            category: None,
            metadata: Vec::new(),
            files,
        },
        sources,
        expected,
        order,
    }
}

fn coarse(pipeline: &Pipeline) -> String {
    let queued = pipeline.jobs.get(&JOB).map(|state| {
        (
            state.articles.active().unwrap().download_queue.len(),
            state.articles.active().unwrap().recovery_queue.len(),
            state.status.clone(),
        )
    });
    format!(
        "{queued:?} {} {}",
        pipeline.inflight_moves.contains(&JOB),
        pipeline.pending_completion_checks.len()
    )
}

fn full_state(pipeline: &Pipeline) -> String {
    let mut text = format!(
        "coarse={} admitted={} pipeline_work={} retiring_row={} live_graph={} retiring_graph={} pending_terminal={} moves={:?} deferred={:?} retries={:?} post_processing={:?} scripts={:?}/{:?} dispatch_wake={} dispatch_retry={} probes_out={} checkpoint_tasks={} repair_slots={:?}",
        coarse(pipeline),
        pipeline.nzb_admitted.contains(&JOB),
        pipeline.job_has_pending_download_pipeline_work(JOB),
        pipeline.shared_state.queue.read(|inner| inner
            .jobs
            .iter()
            .any(|row| row.id == JOB && row.report.retiring)),
        pipeline.graph_jobs.contains_key(&JOB),
        pipeline.retiring_graph_jobs.contains_key(&JOB),
        pipeline.pending_graph_terminals.contains_key(&JOB),
        pipeline.inflight_moves,
        pipeline.deferred_moves,
        pipeline.pending_retries_by_job.get(&JOB),
        pipeline.inflight_terminal_post_processing,
        pipeline.queue_script_waiters,
        pipeline.added_script_holds,
        pipeline.download_dispatch_wake,
        pipeline.download_dispatch_retry,
        pipeline.post_probes.out(JOB),
        pipeline.fire_and_forget_tasks.lock().unwrap().len(),
        pipeline.graph_status.repairs,
    );
    if let Some(job) = pipeline
        .graph_jobs
        .get(&JOB)
        .or_else(|| pipeline.retiring_graph_jobs.get(&JOB))
    {
        let settled: Vec<_> = (0..2)
            .flat_map(|file| (0..2).map(move |ordinal| (file, ordinal)))
            .map(|(file, ordinal)| {
                (
                    (file, ordinal),
                    job.article_settled(weaver_graph::VolumeIndex(file), ordinal),
                )
            })
            .collect();
        text.push_str(&format!("\nsettled_articles={settled:?}"));
        text.push_str(&format!("\nphase={:?} awaits_mail={} repair_out={} moves_waiting={} awaited={} checkpoint_stamp={} settled_bytes={} tally={:?} runs_live={} runs_inflight={} recovery={:?}",
            job.phase(), job.awaits_mail(), job.repair_out(), job.moves_waiting(), job.awaited(),
            job.pending().stamp(), job.pending().settled_bytes(), job.pending().tally(),
            job.runs().live(), job.runs().in_flight(), job.recovery_summary()));
        for node in std::iter::once(NodeId::ROOT).chain(job.tree().live()) {
            if let Some(namespace) = job.tree().namespace(node) {
                text.push_str(&format!(
                    "\nnode={node:?} kind={:?} generation={:?} binding={:?}",
                    job.tree().kind(node),
                    job.runs().generation(node),
                    job.tree().volumes(node)
                ));
                for member in namespace.members() {
                    text.push_str(&format!("\n member={:?} name={} output={} length={:?} closed={} bound={:?} coverage={:?}", member.id, member.name, member.output_name, member.length, member.closed, member.bound_to, member.coverage));
                }
            }
        }
    }
    text
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Acknowledgments {
    Ready,
    Early,
    Deferred,
}

fn acknowledge(pipeline: &mut Pipeline, said: crate::pipeline::graph::engine::Said) {
    pipeline.graph_run_said(said);
}

async fn arrive(
    pipeline: &mut Pipeline,
    fixture: &Fixture,
    (file, ordinal): (u32, u32),
    metadata_true_size: bool,
    acknowledgments: Acknowledgments,
) {
    if !pipeline.jobs.contains_key(&JOB) {
        return;
    }
    let id = SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: ordinal,
    };
    if let Some(active) = pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.articles.active_mut())
    {
        for queue in [&mut active.download_queue, &mut active.recovery_queue] {
            for work in queue.drain_all() {
                if work.segment_id != id {
                    queue.push(work);
                }
            }
        }
    }
    let (name, bytes) = &fixture.sources[file as usize];
    let articles = if file == 2 { 1 } else { 2 };
    let frame = bytes.len().div_ceil(articles);
    let offset = ordinal as usize * frame;
    let until = bytes.len().min(offset + frame);
    let length = if file == 2 && !metadata_true_size {
        pipeline.jobs[&JOB]
            .articles
            .assembly()
            .unwrap()
            .file(id.file_id)
            .unwrap()
            .total_bytes()
    } else {
        bytes.len() as u64
    };
    // This deliberately matches the saved harness's manual decoder seam:
    // one whole-article CRC even when metadata supplied a checkpoint plan.
    submit_decoded_segment_declaring(
        pipeline,
        id.file_id,
        ordinal,
        offset as u64,
        &bytes[offset..until],
        name,
        None,
        true,
        None,
        length,
    )
    .await;
    if acknowledgments != Acknowledgments::Deferred {
        for said in pipeline.graph_runs.waiting() {
            acknowledge(pipeline, said);
        }
    }
    while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued).await;
    }
    pump_pipeline_runtime_queues(pipeline).await;
}

async fn saved_sequence(saved: Saved, metadata_true_size: bool) {
    saved_with_acknowledgments(saved, metadata_true_size, Acknowledgments::Ready).await;
}

async fn saved_with_acknowledgments(
    saved: Saved,
    metadata_true_size: bool,
    acknowledgments: Acknowledgments,
) {
    let fixture = fixture(saved);
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    insert_active_job(&mut pipeline, JOB, fixture.spec.clone()).await;
    for &article in &fixture.order {
        arrive(
            &mut pipeline,
            &fixture,
            article,
            metadata_true_size,
            acknowledgments,
        )
        .await;
        if acknowledgments == Acknowledgments::Early {
            while pipeline
                .graph_jobs
                .get(&JOB)
                .is_some_and(|job| job.repair_out())
            {
                let said = pipeline
                    .graph_runs
                    .recv()
                    .await
                    .expect("an outstanding loan speaks");
                acknowledge(&mut pipeline, said);
                pump_pipeline_runtime_queues(&mut pipeline).await;
            }
        }
    }
    let mut stopped = None;
    let mut stalled = None;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            arrive(
                &mut pipeline,
                &fixture,
                (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                ),
                metadata_true_size,
                Acknowledgments::Ready,
            )
            .await;
        }
        if finished(&pipeline) {
            break;
        }
        let before = coarse(&pipeline);
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if finished(&pipeline) {
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
                .expect("a live worker speaks");
            acknowledge(&mut pipeline, said);
            continue;
        }
        if pipeline
            .pending_retries_by_job
            .get(&JOB)
            .is_some_and(|pending| *pending > 0)
        {
            let retry = pipeline
                .retry_rx
                .recv()
                .await
                .expect("a scheduled retry speaks");
            pipeline.receive_retry_work(retry);
            continue;
        }
        let after = coarse(&pipeline);
        if before == after && stalled.replace(after.clone()) == Some(after) {
            stopped = Some(full_state(&pipeline));
            break;
        }
    }
    let status = job_status_for_assert(&pipeline, JOB);
    let mut output = BTreeMap::new();
    collect(&complete, &complete, &mut output);
    retire_pipeline_database(pipeline).await;
    assert!(
        stopped.is_none(),
        "{saved:?} true_metadata={metadata_true_size}: {}",
        stopped.unwrap_or_default()
    );
    if matches!(saved, Saved::Gap) {
        assert!(
            matches!(status, Some(JobStatus::Failed { .. })),
            "{status:?}"
        );
    } else {
        assert_eq!(status, Some(JobStatus::Complete));
        assert!(output.values().any(|bytes| bytes == &fixture.expected));
    }
}

#[tokio::test]
async fn saved_bare_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Bare, false).await;
}
#[tokio::test]
async fn saved_misnumbered_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Misnumbered, false).await;
}
#[tokio::test]
async fn saved_swapped_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Swapped, false).await;
}
#[tokio::test]
async fn saved_gap_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Gap, false).await;
}
#[tokio::test]
async fn bare_encrypted_metadata_true_length_control() {
    saved_sequence(Saved::Bare, true).await;
}

#[tokio::test]
async fn bare_encrypted_sequence_acknowledges_each_early_loan() {
    saved_with_acknowledgments(Saved::Bare, false, Acknowledgments::Early).await;
}

#[tokio::test]
async fn bare_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Bare, false, Acknowledgments::Deferred).await;
}

#[tokio::test]
async fn misnumbered_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Misnumbered, false, Acknowledgments::Deferred).await;
}
#[tokio::test]
async fn swapped_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Swapped, false, Acknowledgments::Deferred).await;
}
#[tokio::test]
async fn gap_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Gap, false, Acknowledgments::Deferred).await;
}
