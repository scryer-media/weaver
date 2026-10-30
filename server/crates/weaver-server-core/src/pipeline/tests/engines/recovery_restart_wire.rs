// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{self, JOB, Post};
use crate::jobs::ids::{NzbFileId, SegmentId};
use crate::jobs::model::{DownloadState, JobStatus, PostState, RunState};
use crate::pipeline::Pipeline;
use crate::pipeline::tests::direct_store::{
    build_test_par2_with_recovery, direct_store_job_spec, obfuscate_volumes,
    single_member_rar4_store_set,
};
use crate::pipeline::tests::{
    insert_active_job, job_status_for_assert, new_direct_pipeline, pump_pipeline_runtime_queues,
    retire_pipeline_database, submit_decoded_segment_declaring,
};
use weaver_graph::{ByteRange, MemberId, NodeId};

async fn turn(pipeline: &mut Pipeline) {
    pipeline.hear_graph_runs();
    while let Some(job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(job).await;
    }
    pump_pipeline_runtime_queues(pipeline).await;
}

async fn article(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    file: u32,
    ordinal: u32,
    parts: usize,
    lost: Option<(u32, u32)>,
) {
    article_in(pipeline, posts, file, ordinal, parts, lost, posts.len() - 1).await;
}

async fn article_in(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    file: u32,
    ordinal: u32,
    parts: usize,
    lost: Option<(u32, u32)>,
    data_files: usize,
) {
    let id = SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: ordinal,
    };
    let Some(active) = pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|job| job.articles.active_mut())
    else {
        return;
    };
    for queue in [&mut active.download_queue, &mut active.recovery_queue] {
        for work in queue.drain_all() {
            if work.segment_id != id {
                queue.push(work);
            }
        }
    }
    if lost == Some((file, ordinal)) {
        pipeline.book_failed_segment(id);
    } else {
        let (name, bytes) = &posts[file as usize];
        let data = (file as usize) < data_files;
        let cut = if data {
            bytes.len().div_ceil(parts)
        } else {
            bytes.len()
        };
        let start = (ordinal as usize * cut).min(bytes.len());
        let end = (start + cut).min(bytes.len());
        // The ordinary direct-success fixture states true data lengths;
        // its one-article PAR2 carrier inherits the encoded NZB length.
        let declared = if data {
            bytes.len()
        } else {
            bytes.len() + bytes.len().div_ceil(32) + 2
        };
        submit_decoded_segment_declaring(
            pipeline,
            id.file_id,
            ordinal,
            start as u64,
            &bytes[start..end],
            name,
            None,
            true,
            None,
            declared as u64,
        )
        .await;
    }
    turn(pipeline).await;
}

async fn settle(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    parts: usize,
    lost: Option<(u32, u32)>,
) {
    settle_in(pipeline, posts, parts, lost, posts.len() - 1).await;
}

async fn settle_in(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    parts: usize,
    lost: Option<(u32, u32)>,
    data_files: usize,
) {
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|job| job.articles.active_mut()?.download_queue.pop())
        {
            article_in(
                pipeline,
                posts,
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
                parts,
                lost,
                data_files,
            )
            .await;
        }
        if harness::finished(pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(pipeline).await;
        if harness::finished(pipeline) {
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
                .expect("owned work returns");
            pipeline.graph_run_said(said);
        } else if harness::take_retry(pipeline).await {
            continue;
        } else {
            assert!(
                pipeline.jobs.get(&JOB).is_some_and(|job| !job
                    .articles
                    .active()
                    .unwrap()
                    .download_queue
                    .is_empty()),
                "no queued work or worker owns progress"
            );
        }
    }
}

async fn late_rar_index(lost_tail: bool) {
    let lost = lost_tail.then_some((1, 1));
    let payload: Vec<_> = (0..70_001)
        .map(|n| ((n * 7 + n / 251) % 253) as u8)
        .collect();
    let written = single_member_rar4_store_set("nested/feature.mkv", &payload, 2);
    let slice = (written
        .iter()
        .map(|(_, bytes)| bytes.len())
        .max()
        .unwrap()
        .div_ceil(32)
        .div_ceil(4)
        * 4)
    .max(256);
    let blocks = if lost_tail {
        written
            .iter()
            .map(|(_, bytes)| bytes.len().div_ceil(slice))
            .sum()
    } else {
        0
    };
    let sources: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let parity = build_test_par2_with_recovery(&sources, slice as u64, blocks);
    let mut posts = obfuscate_volumes(&written);
    let mut spec = direct_store_job_spec("Archive schedules", &posts);
    let mut carrier = harness::spec(
        "",
        &[Post::plain("silver.horizon.par2", parity.clone())],
        usize::MAX,
    )
    .files
    .remove(0);
    carrier.segments[0].bytes += carrier.segments[0].bytes.div_ceil(32) + 2;
    spec.total_bytes += u64::from(carrier.segments[0].bytes);
    spec.files.push(carrier);
    posts.push(("silver.horizon.par2".into(), parity));
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    // No scenario preparation: the pool has no server and no connections.
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    article(&mut pipeline, &posts, 1, 0, 2, lost).await;
    let graph = &pipeline.graph_jobs[&JOB];
    let owner = graph
        .tree()
        .child_of(NodeId::ROOT, MemberId(1))
        .expect("the nonfirst article has an inverse owner");
    let kind = graph.tree().kind(owner.0).unwrap().to_string();
    let working = pipeline.jobs[&JOB].working_dir.clone();
    let extent = ByteRange::new(0, posts[1].1.len().div_ceil(2) as u64);
    let mut before = Vec::new();
    assert!(graph.writer().access(graph.tree()).read_at(
        NodeId::ROOT,
        MemberId(1),
        extent,
        &mut before
    ));
    assert_eq!(before, posts[1].1[..extent.end as usize]);
    retire_pipeline_database(pipeline).await;
    (pipeline, _, _) = new_direct_pipeline(&root).await;
    let checkpoint = pipeline
        .db
        .load_graph_checkpoint(JOB)
        .unwrap()
        .expect("the crash left a graph checkpoint");
    let saved_owner = checkpoint
        .nodes
        .iter()
        .find(|node| node.node == i64::from(owner.0.0))
        .expect("the inverse owner is recorded");
    assert_eq!(saved_owner.kind, kind);
    assert_eq!(saved_owner.parent, Some(0));
    let envelopes: Vec<_> = checkpoint
        .envelopes
        .iter()
        .filter(|row| row.node == i64::from(owner.0.0))
        .collect();
    eprintln!(
        "crash custody owner={owner:?} kind={kind} extent={extent:?} envelopes={envelopes:?} root={:?}",
        checkpoint
            .members
            .iter()
            .filter(|row| row.node == 0 && row.member == 1)
            .collect::<Vec<_>>()
    );
    assert!(
        !envelopes.is_empty(),
        "the owner recorded envelope backing: {checkpoint:?}"
    );
    for kept in envelopes {
        let name = kept.file.clone().unwrap_or_else(|| {
            crate::pipeline::graph::envelope::envelope_name(owner.0, kept.volume as u32)
        });
        let bytes =
            std::fs::read(working.join(name)).expect("a recorded envelope exists after the crash");
        assert!(kept.start >= 0 && kept.end <= extent.end as i64);
        let range = kept.start as usize..kept.end as usize;
        assert_eq!(
            &bytes[range.clone()],
            &posts[1].1[range],
            "recorded envelope bytes retain their source identity"
        );
    }
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&JOB)
        .unwrap();
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
        })
        .await
        .unwrap();
    assert_eq!(
        pipeline.graph_jobs[&JOB]
            .tree()
            .child_of(NodeId::ROOT, MemberId(1)),
        Some(owner)
    );
    for (file, ordinal) in [(0, 0), (0, 1), (1, 1), (2, 0)] {
        article(&mut pipeline, &posts, file, ordinal, 2, lost).await;
    }
    settle(&mut pipeline, &posts, 2, lost).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    assert_eq!(status, Some(JobStatus::Complete));
    assert_eq!(files.len(), 1);
    assert!(files.values().any(|bytes| bytes == &payload));
}

#[tokio::test]
async fn a_late_rar_index_reuses_only_recorded_envelopes_after_a_crash() {
    late_rar_index(false).await;
}

#[tokio::test]
async fn a_late_rar_index_repairs_the_tail_with_no_stat_server_after_a_crash() {
    late_rar_index(true).await;
}

#[tokio::test]
async fn an_opaque_sevenz_with_decoded_nzb_sizes_and_inline_loss_has_no_usable_output() {
    use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
        Entry, build_7z_shaped, split_volumes,
    };
    let mut state = 13_u64;
    let payload: Vec<_> = (0..70_001)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let archive = build_7z_shaped(
        &[Entry::file("feature.mkv", payload)],
        sevenz_turbo::EncoderMethod::COPY,
        None,
        false,
    );
    let written = split_volumes(&archive, 1);
    assert_eq!(written[0].0, "silver.horizon.7z");
    let slice = archive.len().div_ceil(32).div_ceil(4) * 4;
    let index = crate::pipeline::tests::build_test_par2_index_for_files(
        &[(written[0].0.as_str(), archive.as_slice())],
        slice as u64,
    );
    let name = "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41";
    let frame = archive.len().div_ceil(4);
    let mut spec = harness::spec(
        "Silver Horizon",
        &[Post::plain(name, archive.clone())],
        frame,
    );
    let mut carrier = harness::spec(
        "",
        &[Post::plain("silver.horizon.par2", index.clone())],
        usize::MAX,
    )
    .files
    .remove(0);
    carrier.segments[0].bytes += carrier.segments[0].bytes.div_ceil(32) + 2;
    spec.total_bytes += u64::from(carrier.segments[0].bytes);
    spec.files.push(carrier);
    assert_eq!(
        spec.files[0]
            .segments
            .iter()
            .map(|segment| u64::from(segment.bytes))
            .sum::<u64>(),
        archive.len() as u64
    );
    spec.files[1].segments[0].message_ids =
        crate::jobs::MessageIds::new(vec!["recovery-index@example.invalid".into()]);
    let mut ids = std::collections::BTreeSet::new();
    for file in &spec.files {
        for segment in &file.segments {
            for id in segment.message_ids.all() {
                assert!(
                    ids.insert(id),
                    "every posted article has its own message id"
                );
            }
        }
    }
    let posts = vec![
        (name.to_string(), archive),
        ("silver.horizon.par2".to_string(), index),
    ];
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    insert_active_job(&mut pipeline, JOB, spec).await;
    let lost = Some((0, 1));
    for (file, ordinal) in [(1, 0), (0, 0), (0, 1), (0, 2), (0, 3)] {
        article(&mut pipeline, &posts, file, ordinal, 4, lost).await;
    }
    settle(&mut pipeline, &posts, 4, lost).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    assert!(
        matches!(status, Some(JobStatus::Failed { .. })),
        "status={status:?}; files={:?}",
        files.keys()
    );
    assert!(
        files.is_empty(),
        "no downloaded archive or partial payload is usable"
    );
}

async fn solid_rar_after_nonfirst_half(encrypted: bool) {
    let volumes: [&[u8]; 2] = if encrypted {
        [
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/extraction_profiles/rar4_solid_enc_two.part01.rar"
            )),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/extraction_profiles/rar4_solid_enc_two.part02.rar"
            )),
        ]
    } else {
        [
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/extraction_profiles/rar4_solid_two.part01.rar"
            )),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/extraction_profiles/rar4_solid_two.part02.rar"
            )),
        ]
    };
    let password = encrypted.then_some("moonlit-harbour");
    let mut reader = match password {
        Some(password) => unrar_rs::RarArchive::open_with_password(
            std::io::Cursor::new(volumes[0].to_vec()),
            password,
        ),
        None => unrar_rs::RarArchive::open(std::io::Cursor::new(volumes[0].to_vec())),
    }
    .unwrap();
    reader
        .add_volume(1, Box::new(std::io::Cursor::new(volumes[1].to_vec())))
        .unwrap();
    let mut expected = std::collections::BTreeMap::new();
    for index in 0..reader.len() {
        let name = reader.member_info(index).unwrap().name.clone();
        let mut bytes = Vec::new();
        reader.by_index(index).unwrap().copy_to(&mut bytes).unwrap();
        assert!(expected.insert(name, bytes).is_none());
    }
    assert_eq!(expected.len(), 3);
    let posts: Vec<_> = volumes
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            (
                format!("{:032x}", 0xd1c7_0000_u128 + index as u128),
                bytes.to_vec(),
            )
        })
        .collect();
    let mut spec = direct_store_job_spec("Archive schedules", &posts);
    spec.password = password.map(str::to_owned);
    for file in &spec.files {
        assert_eq!(file.segments.len(), 2);
    }
    if !encrypted {
        assert_eq!(
            volumes.map(|bytes| bytes.len().div_ceil(2)),
            [12_288, 8_759]
        );
    }
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    article_in(&mut pipeline, &posts, 1, 0, 2, None, 2).await;
    let graph = &pipeline.graph_jobs[&JOB];
    let owner = graph
        .tree()
        .child_of(NodeId::ROOT, MemberId(1))
        .expect("nonfirst half has a recorded owner");
    let kind = graph.tree().kind(owner.0).unwrap().to_string();
    let extent = ByteRange::new(0, posts[1].1.len().div_ceil(2) as u64);
    let mut before = Vec::new();
    assert!(graph.writer().access(graph.tree()).read_at(
        NodeId::ROOT,
        MemberId(1),
        extent,
        &mut before
    ));
    assert_eq!(before, posts[1].1[..extent.end as usize]);
    let working = pipeline.jobs[&JOB].working_dir.clone();
    retire_pipeline_database(pipeline).await;
    (pipeline, _, _) = new_direct_pipeline(&root).await;
    let checkpoint = pipeline
        .db
        .load_graph_checkpoint(JOB)
        .unwrap()
        .expect("persisted crash checkpoint");
    assert!(
        checkpoint
            .nodes
            .iter()
            .any(|node| node.node == i64::from(owner.0.0)
                && node.kind == kind
                && node.parent == Some(0))
    );
    let envelopes: Vec<_> = checkpoint
        .envelopes
        .iter()
        .filter(|row| row.node == i64::from(owner.0.0))
        .collect();
    assert!(
        !envelopes.is_empty(),
        "nonfirst half retains recorded backing"
    );
    for kept in envelopes {
        let name = kept.file.clone().unwrap_or_else(|| {
            crate::pipeline::graph::envelope::envelope_name(owner.0, kept.volume as u32)
        });
        let bytes =
            std::fs::read(working.join(name)).expect("recorded backing survives retirement");
        assert!(kept.start >= 0 && kept.end <= extent.end as i64);
        let range = kept.start as usize..kept.end as usize;
        assert_eq!(&bytes[range.clone()], &posts[1].1[range]);
    }
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&JOB)
        .unwrap();
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: JOB,
            job_hash: recovered.nzb_hash,
            spec,
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
        })
        .await
        .unwrap();
    assert_eq!(
        pipeline.graph_jobs[&JOB]
            .tree()
            .child_of(NodeId::ROOT, MemberId(1)),
        Some(owner)
    );
    let order = if encrypted {
        [(1, 1), (0, 1), (0, 0)]
    } else {
        [(0, 1), (0, 0), (1, 1)]
    };
    for (file, ordinal) in order {
        article_in(&mut pipeline, &posts, file, ordinal, 2, None, 2).await;
    }
    settle_in(&mut pipeline, &posts, 2, None, 2).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    assert_eq!(
        status,
        Some(JobStatus::Complete),
        "files={:?}",
        files.keys()
    );
    assert_eq!(files.len(), expected.len());
    for (name, bytes) in expected {
        assert!(
            files
                .iter()
                .any(|(path, actual)| path.ends_with(&name) && actual == &bytes),
            "byte-exact {name}"
        );
    }
}

#[tokio::test]
async fn solid_rar4_restores_after_exactly_half_of_the_nonfirst_volume() {
    solid_rar_after_nonfirst_half(false).await;
}

#[tokio::test]
async fn encrypted_solid_rar4_restores_after_exactly_half_of_the_nonfirst_volume() {
    solid_rar_after_nonfirst_half(true).await;
}
