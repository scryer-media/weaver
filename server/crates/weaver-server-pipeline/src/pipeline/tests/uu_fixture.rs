// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// Submit one uuencode part.
//
// Every argument is in DECODED units except the job spec's declared sizes,
// which stay encoded exactly as an NZB would carry them — that mismatch is the
// whole point of these tests.
pub async fn submit_uu_segment(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    damaged: bool,
    ended: bool,
) {
    submit_uu_segment_named(
        pipeline,
        file_id,
        segment_number,
        data,
        crate::pipeline::UuSegmentFacts { damaged, ended },
        "silver-horizon.bin",
    )
    .await;
}

// Submit one uuencode part, stating what its `begin` header called the file.
//
// A real post states the name exactly once, on the part that opens the body,
// and continuation parts carry none at all — which is what `""` means here.
pub(super) async fn submit_uu_segment_named(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    facts: crate::pipeline::UuSegmentFacts,
    begin_name: &str,
) {
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Uu(facts),
                segment_id: SegmentId {
                    file_id,
                    segment_number,
                },
                raw_size: data.len() as u64,
                yenc_layout: YencLayoutAssertions {
                    file_size: 0,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: facts.clean(),
                part_crc_verified: false,
                part_crc: 0,
                truncation_suspected: false,
                expected_file_crc: None,
                segments: Vec::new(),
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: begin_name.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            },
            SegmentSource {
                servers: PoolServers::stamped(
                    pipeline.pool_generation,
                    None,
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        )
        .await;
}

// A job whose declared segment sizes are uuencode-encoded (~1.38x the decoded
// bytes), which is what an NZB really carries for a uuencode post.
pub fn uu_job_spec(decoded_sizes: &[usize]) -> JobSpec {
    let declared: Vec<u32> = decoded_sizes
        .iter()
        .map(|len| (*len as f64 * 1.38).ceil() as u32)
        .collect();
    let mut spec = standalone_job_spec(
        "Silver Horizon UU",
        &[("silver-horizon.bin".to_string(), declared[0])],
    );
    let file = &mut spec.files[0];
    file.segments.clear();
    for (index, bytes) in declared.iter().enumerate() {
        file.segments.push(segment_spec! {
            number: index as u32,
            bytes: *bytes,
            message_id: format!("uu-{index}@example.com"),
        });
    }
    // The post's size is every part it declares.
    spec.total_bytes = declared.iter().map(|bytes| u64::from(*bytes)).sum();
    spec
}

// Number of segments queued for download on a job.
pub fn queued_segment_count(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.articles.active().unwrap().download_queue.len())
        .unwrap_or(0)
}

// A job carrying both encodings: yEnc files whose declared segment sizes run
// ~1.03x their decoded bytes, and one uuencode file whose declared sizes run
// ~1.38x — exactly as the two encodings appear in a real NZB, since the
// `<segment bytes>` attribute is always the ENCODED figure.
pub fn mixed_encoding_job_spec(
    yenc_files: &[(&str, &[usize])],
    uu_file: (&str, &[usize]),
) -> (JobSpec, Vec<u32>, u32) {
    let mut files = Vec::new();
    let mut yenc_indices = Vec::new();

    for (filename, decoded_sizes) in yenc_files {
        yenc_indices.push(files.len() as u32);
        let index = files.len();
        files.push(FileSpec {
            filename: (*filename).to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: decoded_sizes
                .iter()
                .enumerate()
                .map(|(ordinal, decoded)| {
                    segment_spec! {
                        number: ordinal as u32,
                        bytes: (*decoded as f64 * 1.03).ceil() as u32,
                        message_id: format!("mixed-y{index}-{ordinal}@example.com"),
                    }
                })
                .collect(),
        });
    }

    let uu_index = files.len() as u32;
    files.push(FileSpec {
        filename: uu_file.0.to_string(),
        role: FileRole::Standalone,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: uu_file
            .1
            .iter()
            .enumerate()
            .map(|(ordinal, decoded)| {
                segment_spec! {
                    number: ordinal as u32,
                    bytes: (*decoded as f64 * 1.38).ceil() as u32,
                    message_id: format!("mixed-u-{ordinal}@example.com"),
                }
            })
            .collect(),
    });

    let spec = JobSpec {
        name: "Silver Horizon Mixed".to_string(),
        password: None,
        total_bytes: files
            .iter()
            .flat_map(|file| file.segments.iter())
            .map(|segment| u64::from(segment.bytes))
            .sum(),
        category: None,
        metadata: vec![],
        files,
    };
    (spec, yenc_indices, uu_index)
}

// Every part of the file is laid in the job's root, or ruled lost.
pub fn every_part_settled(pipeline: &Pipeline, file_id: NzbFileId) -> bool {
    let count = pipeline.jobs[&file_id.job_id].spec.files[file_id.file_index as usize]
        .segments
        .len() as u32;
    (0..count).all(|segment_number| {
        pipeline.graph_article_settled(SegmentId {
            file_id,
            segment_number,
        })
    })
}

// Submit one yEnc part of a file `file_len` decoded bytes long, as its
// header states.
pub async fn submit_yenc_part(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    file_len: u64,
) {
    submit_decoded_segment_declaring(
        pipeline,
        file_id,
        segment_number,
        file_offset,
        data,
        filename,
        None,
        false,
        None,
        file_len,
    )
    .await;
}

pub async fn submit_named(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    ordinal: u32,
    data: &[u8],
    damaged: bool,
    ended: bool,
    name: &str,
) {
    submit_uu_segment_named(
        pipeline,
        file_id,
        ordinal,
        data,
        UuSegmentFacts { damaged, ended },
        name,
    )
    .await;
}

pub fn set_park_limit(pipeline: &mut Pipeline, limit: usize) {
    pipeline.uu_park_max_segments = limit;
}
pub fn retries_empty(pipeline: &Pipeline) -> bool {
    pipeline.decode_retries.is_empty()
}
pub fn has_job(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.jobs.contains_key(&id)
}
pub fn parked_count(pipeline: &Pipeline, file: NzbFileId) -> Option<usize> {
    pipeline.uu_files.get(&file).map(|uu| uu.parked.len())
}
pub fn cursor(pipeline: &Pipeline, file: NzbFileId) -> Option<u32> {
    pipeline.uu_files.get(&file).map(|uu| uu.next_index.get())
}
pub fn filename(pipeline: &Pipeline, file: NzbFileId) -> Option<&str> {
    pipeline
        .uu_files
        .get(&file)
        .and_then(|uu| uu.filename.as_deref())
}
pub fn saw_end(pipeline: &Pipeline, file: NzbFileId) -> Option<bool> {
    pipeline.uu_files.get(&file).map(|uu| uu.saw_end)
}

// How many held parts' spool files the job's working directory holds.
pub fn held_spool_files(pipeline: &Pipeline, job_id: JobId) -> usize {
    let dir = crate::pipeline::held::held_spool_dir(&pipeline.jobs[&job_id].working_dir);
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(crate::pipeline::held::HELD_SPOOL_PREFIX)
            })
            .count()
    })
}

pub fn seed_uu_spool_headroom(pipeline: &mut Pipeline) {
    pipeline.uu_spool_available_bytes_for_test =
        Some(Some(pipeline.uu_spool_min_free_bytes + 64 * 1024 * 1024));
}

// Two jobs of 3,000 files each. A file in three holds a parked uuencode
// part, one in three a part held for the part before it, and one in three
// nothing; every file has an article in each of the three requeue lists.
pub fn seed_write_backlog(pipeline: &mut Pipeline, jobs: [JobId; 2]) {
    const FILES: u32 = 3_000;
    let generation = pipeline.pool_generation;
    let source = move || SegmentSource {
        servers: PoolServers::stamped(generation, None, weaver_nntp::ServerSet::EMPTY),
    };
    for job_id in jobs {
        for file_index in 0..FILES {
            let file_id = NzbFileId { job_id, file_index };
            let segment = |segment_number| SegmentId {
                file_id,
                segment_number,
            };
            match file_index % 3 {
                0 => {
                    let mut uu = UuFileAssembly::default();
                    uu.parked.insert(
                        1,
                        crate::pipeline::ParkedUu {
                            bytes: crate::pipeline::ParkedBytes::Memory(DecodedChunk::from(vec![
                                1u8;
                                8
                            ])),
                            facts: crate::pipeline::UuSegmentFacts {
                                damaged: false,
                                ended: false,
                            },
                            source: source(),
                        },
                    );
                    pipeline.uu_files.insert(file_id, uu);
                    pipeline.note_write_buffered(8, 1);
                    pipeline.note_uu_parked_segment();
                }
                1 => {
                    let data = vec![3u8; 16];
                    pipeline.unanchored_parked.insert(
                        file_id,
                        1,
                        crate::pipeline::UnanchoredPart {
                            result: DecodeResult {
                                encoding: SegmentEncoding::Yenc,
                                segment_id: segment(1),
                                raw_size: 16,
                                yenc_layout: YencLayoutAssertions {
                                    file_size: 0,
                                    part: None,
                                    total: None,
                                    begin: None,
                                    end: None,
                                },
                                crc_valid: true,
                                part_crc_verified: true,
                                part_crc: par2_rs::checksum::crc32(&data),
                                truncation_suspected: false,
                                expected_file_crc: None,
                                segments: Vec::new(),
                                data: DecodedChunk::from(Vec::<u8>::new()),
                                yenc_name: "held.bin".to_string(),
                                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                            },
                            bytes: crate::pipeline::ParkedBytes::Memory(DecodedChunk::from(data)),
                            source: source(),
                        },
                    );
                    pipeline.note_write_buffered(16, 1);
                }
                _ => {}
            }
            pipeline.uu_park_requeues.insert(segment(1), (1, 1));
            pipeline.unanchored_requeues.insert(segment(2), (1, 1));
            pipeline.pending_unanchored_release.push(segment(3));
        }
    }
}

// Everything the cleanup can change, in a form two pipelines compare by.
#[allow(clippy::type_complexity)]
pub fn write_backlog_state(
    pipeline: &Pipeline,
) -> (
    Vec<(u64, u32)>,
    Vec<(u64, u32)>,
    Vec<(u64, u32, u32)>,
    Vec<(u64, u32, u32)>,
    Vec<(u64, u32, u32)>,
    [usize; 4],
) {
    let file = |id: &NzbFileId| (id.job_id.0, id.file_index);
    let segment = |id: &SegmentId| {
        (
            id.file_id.job_id.0,
            id.file_id.file_index,
            id.segment_number,
        )
    };
    fn sorted<T: Ord>(mut list: Vec<T>) -> Vec<T> {
        list.sort_unstable();
        list
    }
    (
        sorted(pipeline.uu_files.keys().map(file).collect()),
        sorted(pipeline.unanchored_parked.keys().map(file).collect()),
        sorted(pipeline.uu_park_requeues.keys().map(segment).collect()),
        sorted(pipeline.unanchored_requeues.keys().map(segment).collect()),
        // Kept in its own order: the release works through it front to back.
        pipeline
            .pending_unanchored_release
            .iter()
            .map(segment)
            .collect(),
        [
            pipeline.unanchored_parked.parts(),
            pipeline.write_buffered_bytes,
            pipeline.write_buffered_segments,
            pipeline.uu_parked_segments,
        ],
    )
}

// What the download stage makes of one uuencode article off the wire.
pub fn uu_decode_result(raw: &[u8]) -> UuDecoded {
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JobId(1),
            file_index: 0,
        },
        segment_number: 0,
    };
    let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
    let decoded = decoder
        .decode_available(&mut bytes::BytesMut::from(raw))
        .expect("the article decodes")
        .expect("the article is whole on the wire");
    let trace = weaver_nntp::client::DecodedBodyTrace {
        attempts: Vec::new(),
        result: Ok(weaver_nntp::client::DecodedBody {
            raw_size: raw.len() as u32,
            decoded: decoded.chunks,
            body: decoded.body,
            cpu: Default::default(),
            io: Default::default(),
        }),
    };
    match Pipeline::download_data_from_decoded_trace(segment_id, trace).0 {
        Ok(DownloadPayload::Decoded(result)) => UuDecoded {
            is_uu: result.encoding.is_uu(),
            damaged: result
                .encoding
                .uu_facts()
                .is_some_and(|facts| facts.damaged),
            bytes: result.data.len_bytes(),
            crc_valid: result.crc_valid,
        },
        _ => panic!("a uuencode article yields a decoded result"),
    }
}

pub struct UuDecoded {
    pub is_uu: bool,
    pub damaged: bool,
    pub bytes: usize,
    pub crc_valid: bool,
}

pub enum ParkLocation<'a> {
    Memory,
    Spilled(&'a std::path::Path),
}

pub fn parked_location(
    pipeline: &Pipeline,
    file: NzbFileId,
    ordinal: u32,
) -> Option<ParkLocation<'_>> {
    let part = pipeline.uu_files.get(&file)?.parked.get(&ordinal)?;
    Some(match &part.bytes {
        ParkedBytes::Memory(_) => ParkLocation::Memory,
        ParkedBytes::Spilled { path, .. } => ParkLocation::Spilled(path),
    })
}

pub fn damage(pipeline: &Pipeline, file: NzbFileId) -> Option<bool> {
    pipeline.uu_files.get(&file).map(|uu| uu.damaged)
}

pub fn placement(pipeline: &Pipeline, file: NzbFileId, ordinal: u32) -> bool {
    pipeline.jobs[&file.job_id]
        .articles
        .assembly()
        .and_then(|assembly| assembly.file(file))
        .expect("the file is assembling")
        .placement_of(ordinal)
        .is_some()
}

pub fn job_status(pipeline: &Pipeline, job: JobId) -> Option<JobStatus> {
    job_status_for_assert(pipeline, job)
}

pub struct BacklogCounts {
    pub write_buffered_bytes: usize,
    pub write_buffered_segments: usize,
    pub uu_spooled_bytes: usize,
    pub uu_spooled_segments: usize,
    pub uu_parked_segments: usize,
}
pub fn backlog(pipeline: &Pipeline) -> BacklogCounts {
    BacklogCounts {
        write_buffered_bytes: pipeline.write_buffered_bytes,
        write_buffered_segments: pipeline.write_buffered_segments,
        uu_spooled_bytes: pipeline.uu_spooled_bytes,
        uu_spooled_segments: pipeline.uu_spooled_segments,
        uu_parked_segments: pipeline.uu_parked_segments,
    }
}

pub fn set_available(pipeline: &mut Pipeline, bytes: Option<u64>) {
    pipeline.uu_spool_available_bytes_for_test = Some(bytes);
}
pub fn min_free_bytes(pipeline: &Pipeline) -> u64 {
    pipeline.uu_spool_min_free_bytes
}

pub fn set_write_budget(pipeline: &mut Pipeline, value: usize) {
    pipeline.write_backlog_budget_bytes = value;
}

pub fn set_spool_segments(pipeline: &mut Pipeline, value: usize) {
    pipeline.uu_spool_max_segments = value;
}

pub fn set_spool_bytes(pipeline: &mut Pipeline, value: usize) {
    pipeline.uu_spool_max_bytes = value;
}

pub fn set_capacity_reader(pipeline: &mut Pipeline, value: crate::operations::CapacityReader) {
    pipeline.uu_spool_capacity = value;
}

pub fn clear_job_write_backlog(pipeline: &mut Pipeline, job: JobId) {
    pipeline.clear_job_write_backlog(job);
}

pub fn clear_job_write_backlog_per_file(pipeline: &mut Pipeline, job: JobId) {
    pipeline.clear_job_write_backlog_per_file(job);
}

pub fn refresh_download_pressure(pipeline: &mut Pipeline) {
    pipeline.refresh_download_pressure();
}

pub fn book_failed_segment(pipeline: &mut Pipeline, id: SegmentId) -> bool {
    pipeline.book_failed_segment(id)
}

pub fn skip_failed_uu_segment(pipeline: &mut Pipeline, id: SegmentId) -> bool {
    pipeline.skip_failed_uu_segment(id)
}

pub fn admit_uu_spill(pipeline: &mut Pipeline, bytes: usize) -> bool {
    pipeline.admit_uu_spill(bytes)
}

pub fn uu_spool_capacity(pipeline: &mut Pipeline) -> crate::operations::Capacity {
    pipeline.uu_spool_capacity()
}

pub fn uu_spool_dispatch_capped(pipeline: &mut Pipeline) -> bool {
    pipeline.uu_spool_dispatch_capped()
}

pub fn uu_spool_admission_capped(pipeline: &mut Pipeline, bytes: usize) -> bool {
    pipeline.uu_spool_admission_capped(bytes)
}
