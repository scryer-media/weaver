// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;

use super::harness::{self, JOB, Post, Scenario};
use crate::jobs::ids::{NzbFileId, SegmentId};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::{
    checkpoint_plan, insert_active_job, job_status_for_assert, new_direct_pipeline,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use crate::pipeline::{DecodeDone, DownloadPayload, Pipeline, SegmentSource};

const ARTICLE: usize = 768;
const SLICE: usize = 1048;

#[derive(Clone, Copy)]
enum Shape {
    TruncatedMiddle,
    WrongFinalLength { mixed: bool },
    // The truncated article of `TruncatedMiddle`, posted right behind the
    // missing article of `WrongFinalLength`, so it arrives ahead of its
    // file's cursor and waits there.
    TruncatedBehindAMissingArticle,
}

impl Shape {
    fn lost(self) -> Option<(u32, u32)> {
        matches!(
            self,
            Self::WrongFinalLength { .. } | Self::TruncatedBehindAMissingArticle
        )
        .then_some((0, 16))
    }

    fn cut(self) -> Option<(u32, u32)> {
        match self {
            Self::TruncatedMiddle => Some((1, 16)),
            Self::TruncatedBehindAMissingArticle => Some((0, 17)),
            Self::WrongFinalLength { .. } => None,
        }
    }
}

struct Fixture {
    scenario: Scenario,
    payload: Vec<u8>,
    shape: Shape,
}

impl Fixture {
    fn new(shape: Shape) -> Self {
        // The exact size and deterministic byte stream of the UU fixture:
        // four physical pieces, each carrying thirty-two BODY articles.
        let mut state = 37u64;
        let payload: Vec<_> = (0..4 * 32 * ARTICLE - 512)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 56) as u8
            })
            .collect();
        let archive = build_7z_shaped(
            &[Entry::file("lantern.mkv", payload.clone())],
            sevenz_turbo::EncoderMethod::COPY,
            None,
            false,
        );
        let written = split_volumes(&archive, 4);
        assert!(
            written
                .iter()
                .all(|(_, bytes)| bytes.len().div_ceil(ARTICLE) == 32)
        );
        let upper = (written[1].1.len().div_ceil(SLICE) - (16 * ARTICLE / SLICE)) as u32;
        let blocks = upper + (upper / 10).max(2);
        let mut posts: Vec<_> = written
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect();
        posts.extend(recovery(&posts, blocks));
        let mut scenario = Scenario::new(posts).framed(ARTICLE);
        if let Some((file, ordinal)) = shape.lost() {
            scenario = scenario.lost(file, ordinal);
        }
        Self {
            scenario,
            payload,
            shape,
        }
    }

    fn body(&self, file: u32, ordinal: u32) -> Vec<u8> {
        let post = &self.scenario.posts[file as usize];
        let data = post.articles(ARTICLE).nth(ordinal as usize).unwrap();
        let count = post.articles(ARTICLE).count() as u32;
        let uu = file < 4
            && !matches!(self.shape, Shape::WrongFinalLength { mixed: true } if file % 2 == 1);
        if uu {
            return uu_body(
                &post.name,
                data,
                ordinal,
                count,
                self.shape.cut() == Some((file, ordinal)),
                matches!(self.shape, Shape::WrongFinalLength { .. }),
            );
        }
        let mut encoded = Vec::new();
        let offset = u64::from(ordinal) * ARTICLE as u64;
        weaver_yenc::encode_part(
            data,
            &mut encoded,
            128,
            &post.name,
            ordinal + 1,
            count,
            offset + 1,
            offset + data.len() as u64,
            post.bytes.len() as u64,
        )
        .unwrap();
        encoded
    }
}

fn recovery(posts: &[Post], blocks: u32) -> Vec<Post> {
    use par2_rs::create::{
        BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount, VolumeScheme,
    };
    let directory = tempfile::tempdir().unwrap();
    let paths = posts
        .iter()
        .map(|post| {
            let path = directory.path().join(&post.name);
            std::fs::write(&path, &post.bytes).unwrap();
            path
        })
        .collect();
    let mut options = Par2CreatorOptions::with_output(
        directory.path().join("silver.horizon.par2"),
        Some(directory.path().to_owned()),
        paths,
    );
    options.block_sizing = BlockSizing::Bytes(SLICE as u64);
    options.recovery_amount = RecoveryAmount::Count(blocks);
    options.volume_scheme = VolumeScheme::Uniform;
    options.volume_count = Some(4);
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let mut posts: Vec<_> = created
        .output_paths
        .into_iter()
        .map(|path| {
            Post::plain(
                path.file_name().unwrap().to_str().unwrap(),
                std::fs::read(&path).unwrap(),
            )
        })
        .collect();
    posts.sort_by_key(|post| (post.name.contains(".vol"), post.name.clone()));
    posts
}

// A real UU BODY: the malformed line remains inside the middle article,
// and every later line/article is present. No decoded offset is supplied.
fn uu_body(name: &str, data: &[u8], ordinal: u32, count: u32, cut: bool, wrong: bool) -> Vec<u8> {
    let sextet = |value: u8| {
        if value & 63 == 0 {
            b'`'
        } else {
            (value & 63) + 32
        }
    };
    let mut encoded = Vec::new();
    if ordinal == 0 {
        encoded.extend_from_slice(format!("begin 644 {name}\r\n").as_bytes());
    }
    let lines: Vec<_> = data.chunks(45).collect();
    for (index, line) in lines.iter().enumerate() {
        encoded.push(sextet(
            line.len() as u8 + u8::from(wrong && index + 1 == lines.len()),
        ));
        let mut body = Vec::new();
        for triple in line.chunks(3) {
            let a = triple[0];
            let b = triple.get(1).copied().unwrap_or(0);
            let c = triple.get(2).copied().unwrap_or(0);
            body.extend([
                sextet(a >> 2),
                sextet((a << 4) | (b >> 4)),
                sextet((b << 2) | (c >> 6)),
                sextet(c),
            ]);
        }
        if cut && index == lines.len() / 2 {
            body.truncate(body.len() / 2);
        }
        encoded.extend(body);
        encoded.extend_from_slice(b"\r\n");
    }
    if ordinal + 1 == count {
        encoded.extend_from_slice(b"`\r\nend\r\n");
    }
    encoded
}

async fn article(pipeline: &mut Pipeline, fixture: &Fixture, file: u32, ordinal: u32) {
    use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};
    if harness::finished(pipeline) {
        return;
    }
    let segment_id = SegmentId {
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
        for work in queue.drain_all() {
            if work.segment_id != segment_id {
                queue.push(work);
            }
        }
    }
    if fixture.scenario.lost.contains(&(file, ordinal)) {
        pipeline.book_failed_segment(segment_id);
        return;
    }
    let mut raw = b"222 article follows\r\n".to_vec();
    for line in fixture
        .body(file, ordinal)
        .split_inclusive(|byte| *byte == b'\n')
    {
        if line.first() == Some(&b'.') {
            raw.push(b'.');
        }
        raw.extend_from_slice(line);
    }
    raw.extend_from_slice(b".\r\n");
    let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
    decoder.set_checkpoint_plan(checkpoint_plan(pipeline, JOB));
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
    let (result, _, _) = Pipeline::download_data_from_decoded_trace(segment_id, trace);
    let Ok(DownloadPayload::Decoded(result)) = result else {
        panic!("the UU article decodes")
    };
    pipeline.note_decode_started(segment_id, raw.len() as u64);
    pipeline
        .handle_decode_done(DecodeDone::Success {
            result,
            source: SegmentSource {
                source_server_idx: Some(0),
                exclude_servers: Vec::new(),
            },
        })
        .await;
    pipeline.hear_graph_runs();
    pump_pipeline_runtime_queues(pipeline).await;
}

async fn witness(shape: Shape) {
    let fixture = Fixture::new(shape);
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    fixture.scenario.prepare(&mut pipeline);
    let mut spec = fixture.scenario.spec();
    for file in &mut spec.files {
        for segment in &mut file.segments {
            segment.bytes += segment.bytes.div_ceil(32) + 2;
        }
    }
    spec.total_bytes = spec
        .files
        .iter()
        .flat_map(|file| &file.segments)
        .map(|part| u64::from(part.bytes))
        .sum();
    insert_active_job(&mut pipeline, JOB, spec).await;
    let index = 4;
    let index_first = shape.lost().is_some();
    if index_first {
        for ordinal in 0..fixture.scenario.posts[index].articles(ARTICLE).count() as u32 {
            article(&mut pipeline, &fixture, index as u32, ordinal).await;
        }
    }
    let order: &[u32] = if index_first {
        &[0, 1, 2, 3]
    } else {
        &[0, 0, 1, 2, 3]
    };
    for &file in order {
        for ordinal in 0..32 {
            if !fixture.scenario.lost.contains(&(file, ordinal)) {
                article(&mut pipeline, &fixture, file, ordinal).await;
            }
        }
    }
    if !index_first {
        for ordinal in 0..fixture.scenario.posts[index].articles(ARTICLE).count() as u32 {
            article(&mut pipeline, &fixture, index as u32, ordinal).await;
        }
    }
    let mut graph = harness::GraphView::default();
    let mut reads = 0;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut()?.download_queue.pop())
        {
            article(
                &mut pipeline,
                &fixture,
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            )
            .await;
            harness::observe(&pipeline, &mut graph, &mut reads);
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
            let returned = pipeline
                .graph_runs
                .recv()
                .await
                .expect("owned worker returns");
            pipeline.graph_run_said(returned);
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
                "no work remains: {:?}",
                graph.delivery
            );
        }
    }
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    assert_eq!(
        status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}",
        graph.delivery,
        graph.rulings
    );
    assert_eq!(files.len(), 1);
    assert_eq!(
        files.values().next(),
        Some(&fixture.payload),
        "byte-exact extracted payload"
    );
}

#[tokio::test]
async fn a_truncated_middle_uu_article_reaches_repair_with_its_tail_present() {
    witness(Shape::TruncatedMiddle).await;
}

#[tokio::test]
async fn wrong_final_uu_lengths_and_a_missing_middle_article_repair() {
    witness(Shape::WrongFinalLength { mixed: false }).await;
}

#[tokio::test]
async fn wrong_final_uu_lengths_in_a_mixed_set_repair() {
    witness(Shape::WrongFinalLength { mixed: true }).await;
}

// A damaged article that waited ahead of its file's cursor is sent on only
// to a server that has not served it. With one server its bytes stand, the
// file closes, and the recovery set repairs it.
#[tokio::test]
async fn a_truncated_uu_article_behind_a_missing_one_reaches_repair() {
    witness(Shape::TruncatedBehindAMissingArticle).await;
}
