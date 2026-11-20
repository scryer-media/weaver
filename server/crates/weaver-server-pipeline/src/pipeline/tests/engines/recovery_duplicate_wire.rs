// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use super::harness::{self, JOB, Post};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::{
    single_member_rar4_store_set_numbered, single_member_store_set,
};
use crate::pipeline::tests::{
    checkpoint_plan, insert_active_job, job_status_for_assert, new_direct_pipeline,
    pump_pipeline_runtime_queues, refusing_harness_server_port, retire_pipeline_database,
};
use crate::pipeline::{DecodeDone, DownloadError, DownloadPayload, Pipeline, SegmentSource};
use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};

const ARTICLE: usize = 768;

#[derive(Clone, Copy)]
pub enum Protection {
    Par2,
    Par2Many {
        tail: bool,
    },
    Par2Silent,
    Par2WrongFinal,
    TruncatedCarrier,
    Par3 {
        broad: bool,
    },
    Par2Transport {
        rar: bool,
        overlong: bool,
    },
    Par2PositionedTransport {
        rar4: bool,
        overlong: bool,
        tail: bool,
    },
}

impl Protection {
    pub fn transport(self) -> Option<bool> {
        match self {
            Self::Par2Transport { overlong, .. }
            | Self::Par2PositionedTransport { overlong, .. } => Some(overlong),
            _ => None,
        }
    }
}

struct Fixture {
    posts: Vec<Post>,
    payload: Vec<u8>,
    protection: Protection,
}

#[derive(Debug, Default)]
pub struct TransportSeen {
    pub replies: BTreeSet<(u32, u32)>,
    pub rejected: BTreeSet<(u32, u32)>,
    pub exhausted: BTreeSet<(u32, u32)>,
    pub graph_damaged: BTreeSet<(u32, u32)>,
    pub accepted_lengths: BTreeMap<(u32, u32), usize>,
    pub truncated: BTreeMap<(u32, u32), usize>,
    pub absent: BTreeSet<(u32, u32)>,
    pub holes: BTreeSet<(u32, u32)>,
}

impl Fixture {
    fn new(protection: Protection) -> Self {
        let mut state = 29_u64;
        let payload: Vec<_> = (0..97_792)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 56) as u8
            })
            .collect();
        let written = if matches!(
            protection,
            Protection::Par2PositionedTransport { rar4: true, .. }
        ) {
            single_member_rar4_store_set_numbered("harbour/lantern.mkv", &payload, 4)
        } else if matches!(
            protection,
            Protection::Par2Transport { rar: true, .. }
                | Protection::Par2PositionedTransport { .. }
                | Protection::TruncatedCarrier
                | Protection::Par2Silent
        ) {
            single_member_store_set("harbour/lantern.mkv", &payload, 4)
        } else {
            let archive = build_7z_shaped(
                &[Entry::file("lantern.mkv", payload.clone())],
                sevenz_turbo::EncoderMethod::COPY,
                None,
                false,
            );
            split_volumes(&archive, 4)
        };
        let mut posts: Vec<_> = written
            .into_iter()
            .map(|(name, bytes)| {
                assert_eq!(bytes.len().div_ceil(ARTICLE), 32);
                Post::plain(&name, bytes)
            })
            .collect();
        posts.extend(match protection {
            Protection::Par2 => par2(&posts, 2),
            Protection::Par2WrongFinal => par2(&posts, 0),
            Protection::Par2Many { tail } => {
                let mut touched = BTreeSet::new();
                for (file, post) in posts.iter().enumerate() {
                    for article in 0..post.articles(ARTICLE).count() as u32 {
                        let affected = if tail {
                            file == 3 && article >= 30
                        } else {
                            transport_target(
                                Protection::Par2Transport {
                                    rar: false,
                                    overlong: false,
                                },
                                &posts,
                                file as u32,
                                article,
                            )
                        };
                        if affected {
                            let start = article as usize * ARTICLE;
                            let end = (start + ARTICLE).min(post.bytes.len());
                            touched.extend(
                                (start / 1048..end.div_ceil(1048)).map(|slice| (file, slice)),
                            );
                        }
                    }
                }
                let needed = touched.len() as u32;
                assert_eq!(needed, if tail { 3 } else { 75 });
                par2(
                    &posts,
                    if tail {
                        needed
                    } else {
                        needed + (needed / 10).max(2)
                    },
                )
            }
            Protection::Par2Silent => par2(&posts, 11),
            Protection::TruncatedCarrier => {
                let carriers = par2(&posts, 5);
                intact_parity(&carriers);
                carriers
            }
            Protection::Par3 { broad } => par3(&posts, broad),
            Protection::Par2Transport { overlong, .. }
            | Protection::Par2PositionedTransport { overlong, .. } => {
                let mut touched = BTreeSet::new();
                for (file, post) in posts.iter().enumerate() {
                    for article in 0..post.articles(ARTICLE).count() as u32 {
                        if transport_target(protection, &posts, file as u32, article) {
                            let start = article as usize * ARTICLE;
                            let end = (start + ARTICLE * if overlong { 2 } else { 1 })
                                .min(post.bytes.len());
                            touched.extend(
                                (start / 1048..end.div_ceil(1048)).map(|slice| (file, slice)),
                            );
                        }
                    }
                }
                let needed = touched.len() as u32;
                par2(&posts, needed + (needed / 10).max(2))
            }
        });
        Self {
            posts,
            payload,
            protection,
        }
    }

    fn conflicting(&self, file: u32, article: u32) -> bool {
        match self.protection {
            Protection::Par2 | Protection::Par3 { broad: false } => file == 0 && article < 2,
            Protection::Par3 { broad: true } => file < 3 && article < 4,
            Protection::Par2Many { tail } => {
                if tail {
                    file == 3 && article >= 30
                } else {
                    transport_target(
                        Protection::Par2Transport {
                            rar: false,
                            overlong: false,
                        },
                        &self.posts,
                        file,
                        article,
                    )
                }
            }
            Protection::Par2Silent => false,
            Protection::Par2WrongFinal => file == 3 && article == 31,
            Protection::Par2Transport { .. }
            | Protection::Par2PositionedTransport { .. }
            | Protection::TruncatedCarrier => false,
        }
    }

    fn absent(&self, file: u32, article: u32) -> bool {
        matches!(self.protection, Protection::TruncatedCarrier) && file < 2 && article == 16
    }

    fn body(&self, file: u32, article: u32, corrupt: bool) -> Vec<u8> {
        let post = &self.posts[file as usize];
        let data = post.articles(ARTICLE).nth(article as usize).unwrap();
        let changed;
        let silent = matches!(self.protection, Protection::Par2Silent)
            && (1..4).contains(&file)
            && article < 4;
        let wrong_final =
            matches!(self.protection, Protection::Par2WrongFinal) && file == 3 && article == 31;
        let data = if corrupt || silent {
            changed = data.iter().map(|byte| byte ^ 0x5a).collect::<Vec<_>>();
            changed.as_slice()
        } else if wrong_final {
            changed = data
                .iter()
                .enumerate()
                .map(|(at, byte)| if at == 0 { byte ^ 0x5a } else { *byte })
                .collect::<Vec<_>>();
            changed.as_slice()
        } else {
            data
        };
        let mut crc = weaver_yenc::crc::Crc32::new();
        crc.update(data);
        let mut trailer_crc = crc.finalize();
        let declared_len = data.len();
        let transmitted;
        let data = if let Some(overlong) = self.protection.transport()
            && transport_target(self.protection, &self.posts, file, article)
        {
            if overlong {
                transmitted = data
                    .iter()
                    .copied()
                    .chain((0..97).map(|n| (n * 31 + 7) as u8))
                    .collect::<Vec<_>>();
                transmitted.as_slice()
            } else {
                trailer_crc = !trailer_crc;
                data
            }
        } else {
            data
        };
        // Multipart BODY framing also applies to one-article carriers.
        // Both multipart header and trailer identify the part.
        let mut body = Vec::new();
        weaver_yenc::encode_part(
            data,
            &mut body,
            128,
            &post.name,
            article + 1,
            post.articles(ARTICLE).count() as u32,
            u64::from(article) * ARTICLE as u64 + 1,
            u64::from(article) * ARTICLE as u64 + declared_len as u64,
            post.bytes.len() as u64,
        )
        .unwrap();
        let trailer = body
            .windows(7)
            .rposition(|bytes| bytes == b"\r\n=yend")
            .unwrap();
        body.truncate(trailer + 2);
        body.extend_from_slice(
            format!(
                "=yend size={declared_len} part={} pcrc32={trailer_crc:08x}\r\n",
                article + 1
            )
            .as_bytes(),
        );
        if matches!(self.protection, Protection::TruncatedCarrier) && file == 5 && article < 2 {
            let start = body
                .windows(2)
                .enumerate()
                .filter(|(_, bytes)| *bytes == b"\r\n")
                .nth(1)
                .unwrap()
                .0
                + 2;
            let end = body
                .windows(7)
                .rposition(|bytes| bytes == b"\r\n=yend")
                .unwrap()
                + 2;
            let lines: Vec<_> = body[start..end]
                .split_inclusive(|byte| *byte == b'\n')
                .collect();
            let retained = lines
                .iter()
                .take(lines.len() / 2)
                .map(|line| line.len())
                .sum::<usize>();
            let mut partial = body[..start + retained].to_vec();
            partial.extend_from_slice(&body[end..]);
            body = partial;
        }
        body
    }
}

fn intact_parity(carriers: &[Post]) {
    let mut all = BTreeSet::new();
    let mut intact = BTreeSet::new();
    for (file, carrier) in carriers.iter().enumerate() {
        let mut at = 0;
        while at < carrier.bytes.len() {
            let packet = &carrier.bytes[at..];
            assert!(packet.starts_with(b"PAR2\0PKT"));
            let length = u64::from_le_bytes(packet[8..16].try_into().unwrap()) as usize;
            assert!(length >= 64 && length <= packet.len());
            assert_eq!(par2_rs::checksum::md5(&packet[32..length]), packet[16..32]);
            if &packet[48..64] == b"PAR 2.0\0RecvSlic" {
                let exponent = u32::from_le_bytes(packet[64..68].try_into().unwrap());
                assert!(all.insert(exponent));
                if file != 1 || at >= 2 * ARTICLE {
                    intact.insert(exponent);
                }
            }
            at += length;
        }
    }
    assert_eq!(all.len(), 5);
    assert_eq!(
        intact.len(),
        4,
        "exactly four MD5-valid rows survive wholly outside the truncated articles: {intact:?}"
    );
    eprintln!(
        "independent intact PAR2 exponents={intact:?}; carrier lengths={:?}",
        carriers
            .iter()
            .map(|post| post.bytes.len())
            .collect::<Vec<_>>()
    );
}

fn transport_target(protection: Protection, posts: &[Post], file: u32, article: u32) -> bool {
    if let Protection::Par2PositionedTransport { tail, .. } = protection {
        let sequence: Vec<_> = if tail {
            [2, 1, 0, 3]
                .into_iter()
                .flat_map(|next| {
                    (0..posts[next].articles(ARTICLE).count() as u32)
                        .rev()
                        .map(move |ordinal| (next as u32, ordinal))
                })
                .collect()
        } else {
            [2, 3, 0, 1]
                .into_iter()
                .flat_map(|next| {
                    (0..posts[next].articles(ARTICLE).count() as u32)
                        .map(move |ordinal| (next as u32, ordinal))
                })
                .skip(16)
                .collect()
        };
        return sequence
            .into_iter()
            .take(100)
            .any(|id| id == (file, article));
    }
    let mut before = 0;
    for next in [1, 2, 3, 0] {
        if next == file {
            return before + article < 100;
        }
        before += posts[next as usize].articles(ARTICLE).count() as u32;
    }
    false
}

fn par2(posts: &[Post], blocks: u32) -> Vec<Post> {
    use par2_rs::create::{
        BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount, VolumeScheme,
    };
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("protected");
    std::fs::create_dir_all(&base).unwrap();
    let paths = posts
        .iter()
        .map(|post| {
            let path = base.join(&post.name);
            std::fs::write(&path, &post.bytes).unwrap();
            path
        })
        .collect();
    let output = directory.path().join("out");
    std::fs::create_dir_all(&output).unwrap();
    let mut options =
        Par2CreatorOptions::with_output(output.join("silver.horizon.par2"), Some(base), paths);
    options.block_sizing = BlockSizing::Bytes(1048);
    options.recovery_amount = RecoveryAmount::Count(blocks);
    options.volume_scheme = VolumeScheme::Uniform;
    options.volume_count = (blocks > 0).then_some(blocks.min(4));
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let posts: Vec<_> = created
        .output_paths
        .iter()
        .map(|path| {
            Post::plain(
                path.file_name().unwrap().to_str().unwrap(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect();
    assert_eq!(posts.len(), blocks.min(4) as usize + 1);
    assert!(posts[0].name.ends_with(".par2") && !posts[0].name.contains(".vol"));
    posts
}

fn par3(posts: &[Post], broad: bool) -> Vec<Post> {
    use par3_rs::creation::{
        CreationCodec, CreationOptions, CreationPlan, CreationSource, VolumeLayout,
    };
    use par3_rs::source::{MemorySourceAccess, SourceId};
    let mut access = MemorySourceAccess::default();
    let sources: Vec<_> = posts
        .iter()
        .enumerate()
        .map(|(index, post)| {
            let source = SourceId(index as u64 + 1);
            access.insert(source, 1, post.bytes.clone().into());
            CreationSource {
                name: post.name.clone(),
                source,
            }
        })
        .collect();
    let recovery_count = if broad { 11_u64 } else { 9 };
    let plan = CreationPlan::build(
        Arc::new(access),
        &sources,
        CreationOptions {
            block_size: 1024,
            recovery_count,
            codec: if broad {
                CreationCodec::Fft {
                    capacity_log2: recovery_count.next_power_of_two().trailing_zeros() as i8,
                    interleave: 0,
                }
            } else {
                CreationCodec::Cauchy
            },
            volumes: VolumeLayout::Uniform(recovery_count.div_ceil(4)),
            ..Default::default()
        },
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut paths = plan
        .execute(&directory.path().join("silver.horizon"), directory.path())
        .unwrap();
    paths.sort_by_key(|path| {
        (
            path.file_name().unwrap().to_string_lossy().contains(".vol"),
            path.file_name().unwrap().to_owned(),
        )
    });
    paths
        .iter()
        .map(|path| {
            Post::plain(
                path.file_name().unwrap().to_str().unwrap(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect()
}

async fn turn(pipeline: &mut Pipeline) {
    pipeline.hear_graph_runs();
    while let Some(job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(job).await;
    }
    pump_pipeline_runtime_queues(pipeline).await;
}

async fn article(
    pipeline: &mut Pipeline,
    fixture: &Fixture,
    file: u32,
    ordinal: u32,
    observed: &mut TransportSeen,
) {
    let copies: &[bool] = if fixture.conflicting(file, ordinal) {
        &[true, false]
    } else {
        &[false]
    };
    article_copies(pipeline, fixture, file, ordinal, observed, copies).await;
}

async fn article_copies(
    pipeline: &mut Pipeline,
    fixture: &Fixture,
    file: u32,
    ordinal: u32,
    observed: &mut TransportSeen,
    copies: &[bool],
) {
    use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};
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
    // Retire one request once. The two in-flight replies are separate actor
    // turns; work queued by the first remains queued after the second.
    for queue in [&mut active.download_queue, &mut active.recovery_queue] {
        let pending = queue.drain_all();
        for work in pending {
            if work.segment_id != id {
                queue.push(work);
            }
        }
    }
    if fixture.absent(file, ordinal) {
        observed.absent.insert((file, ordinal));
        pipeline.book_failed_segment(id);
        pipeline.maybe_finish_download_pass(JOB);
        turn(pipeline).await;
        return;
    }
    for &corrupt in copies {
        if harness::finished(pipeline) {
            return;
        }
        let body = fixture.body(file, ordinal, corrupt);
        let mut raw = b"222 0 <article@example.invalid> body follows\r\n".to_vec();
        for line in body.split_inclusive(|byte| *byte == b'\n') {
            if line.first() == Some(&b'.') {
                raw.push(b'.');
            }
            raw.extend_from_slice(line);
        }
        raw.extend_from_slice(b".\r\n");
        let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
        decoder.set_checkpoint_plan(checkpoint_plan(pipeline, JOB));
        let failed = |error: String| DecodeDone::Failed {
            segment_id: id,
            raw_size: raw.len() as u64,
            error,
            servers: crate::pipeline::PoolServers::stamped(
                pipeline.pool_generation,
                Some(0),
                weaver_nntp::ServerSet::EMPTY,
            ),
        };
        let done = match decoder.decode_available(&mut bytes::BytesMut::from(raw.as_slice())) {
            Ok(Some(decoded)) => {
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
                match result {
                    Ok(DownloadPayload::Decoded(result)) => DecodeDone::Success {
                        result,
                        source: SegmentSource {
                            servers: crate::pipeline::PoolServers::stamped(
                                pipeline.pool_generation,
                                Some(0),
                                weaver_nntp::ServerSet::EMPTY,
                            ),
                        },
                    },
                    Err(DownloadError::Decode { error, .. }) => failed(error),
                    _ => panic!("fused BODY yields decoded output or failure"),
                }
            }
            Ok(None) => failed("article ended before its terminator".into()),
            Err(error) => failed(error.to_string()),
        };
        let targeted = fixture.protection.transport().is_some()
            && transport_target(fixture.protection, &fixture.posts, file, ordinal);
        let truncated =
            matches!(fixture.protection, Protection::TruncatedCarrier) && file == 5 && ordinal < 2;
        if truncated {
            let DecodeDone::Success { result, .. } = &done else {
                panic!("truncated BODY must retain its actual prefix");
            };
            let length = result.data.len_bytes();
            assert!(length > 0 && length < ARTICLE);
            assert!(result.truncation_suspected || !result.crc_valid);
            observed.truncated.insert((file, ordinal), length);
        }
        if targeted {
            observed.replies.insert((file, ordinal));
            match &done {
                DecodeDone::Success { result, .. }
                    if result.crc_valid && !result.truncation_suspected =>
                {
                    observed
                        .accepted_lengths
                        .insert((file, ordinal), result.data.len_bytes());
                }
                _ => {
                    observed.rejected.insert((file, ordinal));
                }
            }
        }
        pipeline.note_decode_started(id, raw.len() as u64);
        pipeline.handle_decode_done(done).await;
        turn(pipeline).await;
        if truncated {
            let length = observed.truncated[&(file, ordinal)];
            let gap = weaver_graph::ByteRange::new(
                (ordinal as usize * ARTICLE + length) as u64,
                ((ordinal as usize + 1) * ARTICLE) as u64,
            );
            let record = pipeline.graph_jobs[&JOB]
                .tree()
                .namespace(weaver_graph::NodeId::ROOT)
                .unwrap()
                .get(weaver_graph::MemberId(file))
                .unwrap();
            // Its header declared where the part ends: the absent suffix is
            // lost, told as a lost article's bytes are, at once.
            assert!(
                record.coverage.covered(gap)
                    && record
                        .coverage
                        .overlapping(gap)
                        .all(|run| run.provenance == weaver_graph::Provenance::Lost),
                "the settled article's absent suffix is stated lost: {:?}",
                record.coverage
            );
            observed.holes.insert((file, ordinal));
        }
        if targeted && pipeline.segment_terminal_states.contains_key(&id) {
            observed.exhausted.insert((file, ordinal));
        }
        if targeted
            && pipeline
                .graph_jobs
                .get(&JOB)
                .and_then(|job| job.tree().namespace(weaver_graph::NodeId::ROOT))
                .and_then(|space| space.get(weaver_graph::MemberId(file)))
                .is_some_and(|member| {
                    member.coverage.damaged(weaver_graph::ByteRange::new(
                        u64::from(ordinal) * ARTICLE as u64,
                        ((ordinal as usize + 1) * ARTICLE)
                            .min(fixture.posts[file as usize].bytes.len())
                            as u64,
                    ))
                })
        {
            observed.graph_damaged.insert((file, ordinal));
        }
    }
}

pub async fn drive_wire(protection: Protection, batches: &[u32]) -> Witness {
    let fixture = Fixture::new(protection);
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: "127.0.0.1".into(),
                    port: refusing_harness_server_port(),
                    tls: false,
                    ..Default::default()
                },
                max_connections: 1,
                ..Default::default()
            }],
            max_idle_age: Duration::from_secs(1),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        },
    ));
    let mut spec = harness::spec("Silver Horizon", &fixture.posts, ARTICLE);
    for file in &mut spec.files {
        for segment in &mut file.segments {
            segment.bytes += segment.bytes.div_ceil(32) + 2;
        }
    }
    spec.total_bytes = fixture
        .posts
        .iter()
        .map(|post| post.bytes.len() as u64 + (post.bytes.len() as u64).div_ceil(32) + 2)
        .sum();
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    let mut trace = Vec::new();
    let mut observed = TransportSeen::default();
    for &file in batches {
        for ordinal in 0..32 {
            if fixture.absent(file, ordinal) {
                continue;
            }
            article(&mut pipeline, &fixture, file, ordinal, &mut observed).await;
        }
        trace.push(format!("batch {file}"));
    }
    // Only the index arrives unsolicited. Every recovery carrier is served
    // from the ordinary download queue after the graph requests it.
    for ordinal in 0..fixture.posts[4].articles(ARTICLE).count() as u32 {
        article(&mut pipeline, &fixture, 4, ordinal, &mut observed).await;
    }
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|job| job.articles.active_mut()?.download_queue.pop())
        {
            let (file, ordinal) = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            trace.push(format!("requested {file}:{ordinal}"));
            article(&mut pipeline, &fixture, file, ordinal, &mut observed).await;
        }
        if harness::finished(&pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
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
        } else {
            assert!(
                pipeline.jobs.get(&JOB).is_some_and(|job| !job
                    .articles
                    .active()
                    .unwrap()
                    .download_queue
                    .is_empty()),
                "no pending work; {trace:?}"
            );
        }
    }
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    Witness {
        expected: fixture.payload,
        files,
        observed,
        status,
        trace,
    }
}

pub struct Witness {
    pub expected: Vec<u8>,
    pub files: BTreeMap<String, Vec<u8>>,
    pub observed: TransportSeen,
    pub status: Option<JobStatus>,
    pub trace: Vec<String>,
}
